from typing import Type, Dict, Tuple, TypeVar

F = TypeVar("F")
C = TypeVar("C")


class AdapterRegistry:
    def __init__(self):
        self._adapters: Dict[Tuple[Type, Type], Type] = {}

    def register_adapter(self, fetcher_cls: Type[F], candle_cls: Type[C], adapter_cls: Type):
        self._adapters[(fetcher_cls, candle_cls)] = adapter_cls

    def find_adapter(self, fetcher_instance: F, candle_cls: Type[C]):
        key = (type(fetcher_instance), candle_cls)
        if key not in self._adapters:
            raise ValueError(f"No adapter registered for: {key}")
        return self._adapters[key]


registry = AdapterRegistry()


def adapter_for(fetcher_cls: Type[F], candle_cls: Type[C]):
    def decorator(adapter_cls: Type):
        registry.register_adapter(fetcher_cls, candle_cls, adapter_cls)
        return adapter_cls

    return decorator


def get_adapter_instance(fetcher_instance: F, candle_cls: Type[C]):
    adapter_cls = registry.find_adapter(fetcher_instance, candle_cls)
    instance = adapter_cls()
    instance.data_fetcher = fetcher_instance
    return instance
