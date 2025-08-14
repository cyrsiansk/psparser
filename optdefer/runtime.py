from contextvars import ContextVar
from functools import wraps
from typing import TypeVar, Callable, Any, cast
import inspect
import asyncio

current_batch = ContextVar("current_batch")
batches = ContextVar("batches")
results_cache = ContextVar("results_cache")
batch_blocked = ContextVar("batch_blocked", default=False)


class DeferredCall:
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __eq__(self, other):
        if not isinstance(other, DeferredCall):
            return False
        return (self.func == other.func and
                self.args == other.args and
                frozenset(self.kwargs.items()) == frozenset(other.kwargs.items()))

    def __hash__(self):
        return hash((
            self.func,
            self.args,
            frozenset(self.kwargs.items())
        ))

    def __call__(self):
        cache = results_cache.get()
        if self in cache:
            return cache[self]

        kw = self.kwargs.copy()
        if "_action_id" in kw:
            kw.pop("_action_id")

        res = self.func(*self.args, **kw)

        if inspect.isawaitable(res):
            return res

        cache[self] = res
        return res

    async def execute_async(self):
        cache = results_cache.get()
        if self in cache:
            return cache[self]

        kw = self.kwargs.copy()
        if "_action_id" in kw:
            kw.pop("_action_id")

        res = self.func(*self.args, **kw)
        if inspect.isawaitable(res):
            final = await res
        else:
            final = res

        cache[self] = final
        return final

    def __repr__(self):
        return f"DeferredCall({getattr(self.func, '__name__', repr(self.func))}, {self.args}, {self.kwargs})"


class ExecutionContext:
    _depth = ContextVar("execution_depth", default=0)

    def __enter__(self):
        depth = self._depth.get()
        self._depth.set(depth + 1)

        if depth == 0:
            batches.set([])
            results_cache.set({})

        current_batch.set(None)
        batch_blocked.set(False)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        depth = self._depth.get() - 1
        self._depth.set(depth)

        if depth == 0:
            batch_blocked.set(True)
            for batch in batches.get():
                for call in batch:
                    res = call()
                    if inspect.isawaitable(res):
                        try:
                            asyncio.run(res)
                        except RuntimeError as e:
                            raise RuntimeError(
                                "Detected async task during synchronous ExecutionContext exit. "
                                "If you run inside an existing event loop (async code), use 'async with ExecutionContext()' "
                                "so coroutines can be awaited properly."
                            ) from e
            batch_blocked.set(False)

            batches.set([])
            current_batch.set(None)
            results_cache.set({})

        return False

    async def __aenter__(self):
        depth = self._depth.get()
        self._depth.set(depth + 1)

        if depth == 0:
            batches.set([])
            results_cache.set({})

        current_batch.set(None)
        batch_blocked.set(False)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        depth = self._depth.get() - 1
        self._depth.set(depth)

        if depth == 0:
            batch_blocked.set(True)
            for batch in batches.get():
                for call in batch:
                    await call.execute_async()
            batch_blocked.set(False)

            batches.set([])
            current_batch.set(None)
            results_cache.set({})

        return False


F = TypeVar("F", bound=Callable[..., Any])


def execution_context(func: F) -> F:
    if inspect.iscoroutinefunction(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            batch = current_batch.get()
            i_am_owner = True
            if batch is None:
                batch = []
                current_batch.set(batch)
            else:
                i_am_owner = False

            res = await func(*args, **kwargs)

            if i_am_owner:
                all_batches = batches.get()
                all_batches.append(batch)
                current_batch.set(None)

            return res
    else:
        @wraps(func)
        def wrapper(*args, **kwargs):
            batch = current_batch.get()
            i_am_owner = True
            if batch is None:
                batch = []
                current_batch.set(batch)
            else:
                i_am_owner = False

            res = func(*args, **kwargs)

            if i_am_owner:
                all_batches = batches.get()
                all_batches.append(batch)
                current_batch.set(None)

            return res

    return cast(F, wrapper)


def _schedule_call(func: Callable[..., DeferredCall]) -> Callable[..., DeferredCall]:
    @wraps(func)
    def wrapper(*args, **kwargs) -> DeferredCall:
        batch = current_batch.get()
        call = DeferredCall(func, *args, **kwargs)
        blocked = batch_blocked.get()
        if not blocked:
            batch.append(call)
        return call

    return wrapper


def schedule_call(func: Callable[..., Any]) -> Callable[..., DeferredCall]:
    return execution_context(_schedule_call(func))


def _schedule_call_blocker(func: Callable[..., Any]) -> Callable[..., DeferredCall]:
    @wraps(func)
    def wrapper(*args, **kwargs) -> DeferredCall:
        batch = current_batch.get()
        batch_blocked.set(True)
        call = DeferredCall(func, *args, **kwargs)
        batch.append(call)
        batch_blocked.set(False)
        return call

    return wrapper


def schedule_call_blocking(func: Callable[..., Any]) -> Callable[..., DeferredCall]:
    return execution_context(_schedule_call_blocker(func))


@schedule_call_blocking
async def activate_on_match(call: DeferredCall, expected, activation_call: DeferredCall):
    res = await call.execute_async()
    if res == expected:
        await activation_call.execute_async()
