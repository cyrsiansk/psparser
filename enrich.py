import asyncio
import csv
import logging
import os
from typing import Dict, Any, List, Optional
from urllib.parse import urlparse

from core.contactout.manager import ContactOutTokenManager
from core.contactout.manager import OutOfCreditsError, RateLimitError, ContactOutError

log = logging.getLogger("enrich")
log.addHandler(logging.StreamHandler())
log.setLevel(logging.INFO)


def _read_tokens(tokens_path: str) -> List[str]:
    tokens = []
    if not os.path.exists(tokens_path):
        raise FileNotFoundError(f"Tokens file not found: {tokens_path}")
    with open(tokens_path, "r", encoding="utf-8") as f:
        for line in f:
            t = line.strip()
            if not t:
                continue
            if t.startswith("#"):
                continue
            tokens.append(t)
    return tokens


def _extract_domain(website: str) -> Optional[str]:
    try:
        if not website:
            return None
        p = urlparse(website if website.startswith("http") else ("http://" + website))
        host = p.netloc or p.path
        host = host.lower()
        if host.startswith("www."):
            host = host[4:]
        host = host.split(":")[0]
        return host or None
    except Exception:
        return None


def _extract_email_and_phone_from_response(res: Dict[str, Any]) -> Dict[str, Optional[str]]:
    email = None
    phone = None

    if not res:
        return {"email": None, "phone": None}

    for candidate_key in ("data", "result", "person", "profile"):
        if candidate_key in res and isinstance(res[candidate_key], dict):
            sub = res[candidate_key]
            sub_email = sub.get("email") or sub.get("emails") or sub.get("work_email") or sub.get("personal_email")
            sub_phone = sub.get("phone") or sub.get("phones")
            if sub_email and not email:
                if isinstance(sub_email, list):
                    email = sub_email[0]
                else:
                    email = sub_email
            if sub_phone and not phone:
                if isinstance(sub_phone, list):
                    phone = sub_phone[0]
                else:
                    phone = sub_phone

    if not email:
        e = res.get("email") or res.get("emails") or res.get("work_email") or res.get("personal_email")
        if e:
            email = e[0] if isinstance(e, list) and e else (e if isinstance(e, str) else None)
    if not phone:
        p = res.get("phone") or res.get("phones")
        if p:
            phone = p[0] if isinstance(p, list) and p else (p if isinstance(p, str) else None)

    if not email or not phone:
        contacts = res.get("contacts")
        if isinstance(contacts, list):
            for c in contacts:
                if not isinstance(c, dict):
                    continue
                kind = (c.get("type") or "").lower()
                value = c.get("value") or c.get("contact") or c.get("email") or c.get("phone")
                if not value:
                    continue
                if ("email" in kind or "work_email" in c or "personal_email" in c) and not email:
                    email = value
                if ("phone" in kind or "mobile" in kind) and not phone:
                    phone = value

    if isinstance(email, str):
        email = email.strip() or None
    if isinstance(phone, str):
        phone = phone.strip() or None

    return {"email": email, "phone": phone}


async def _enrich_row(
        row: Dict[str, Any],
        manager: ContactOutTokenManager,
        include: Optional[List[str]] = None,
        semaphore: Optional[asyncio.Semaphore] = None,
) -> Dict[str, Any]:
    if semaphore is None:
        sem_ctx = _NullSemaphore()
    else:
        sem_ctx = semaphore

    website = row.get("Website") or row.get("website") or row.get("Site")
    contact_person = row.get("Contact Person") or row.get("Contact") or row.get("ContactPerson") or row.get("contact_person")

    if not website or not contact_person:
        return row

    domain = _extract_domain(website)
    if not domain:
        return row

    payload = {
        "full_name": contact_person,
        "company_domain": [domain],
        "include": include or ["work_email", "phone", "personal_email"],
    }

    async with sem_ctx:
        try:
            res = await manager.enrich(**payload)
        except OutOfCreditsError:
            log.warning("No credits left for any token while enriching %s (%s)", contact_person, website)
            return row
        except RateLimitError as e:
            log.warning("Rate limited while enriching %s (%s): %s", contact_person, website, e)
            return row
        except ContactOutError as e:
            log.warning("ContactOutError while enriching %s (%s): %s", contact_person, website, e)
            return row
        except Exception as e:
            log.exception("Unexpected error while enriching %s (%s): %s", contact_person, website, e)
            return row

    extracted = _extract_email_and_phone_from_response(res or {})
    if extracted.get("email"):
        for key in ("Email", "email", "Work Email", "work_email"):
            if key in row:
                row[key] = extracted["email"]
                break
        else:
            row["Email"] = extracted["email"]

    if extracted.get("phone"):
        for key in ("Phone", "phone", "Mobile", "mobile"):
            if key in row:
                row[key] = extracted["phone"]
                break
        else:
            row["Phone"] = extracted["phone"]

    return row


class _NullSemaphore:
    async def __aenter__(self):
        return None

    async def __aexit__(self, exc_type, exc, tb):
        return False


async def process_csv(
        input_csv: str,
        output_csv: str,
        tokens_path: str = "./tokens",
        cache_dir: str = "./cache",
        concurrency: int = 5,
):
    os.makedirs(cache_dir, exist_ok=True)

    tokens = _read_tokens(tokens_path)
    if not tokens:
        raise ValueError("No tokens found in tokens file")

    manager = ContactOutTokenManager(tokens=tokens, cache_path=os.path.join(cache_dir, "contactout_tokens_cache.json"))
    await manager.initialize()

    with open(input_csv, newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        rows = [r for r in reader]

    sem = asyncio.Semaphore(concurrency)

    tasks = []
    for row in rows:
        website = row.get("Website") or row.get("website")
        contact_person = row.get("Contact Person") or row.get("Contact")
        if website and contact_person:
            tasks.append(_enrich_row(row, manager, semaphore=sem))
        else:
            async def _noop(r):
                return r
            tasks.append(_noop(row))

    enriched_rows = []
    for fut in asyncio.as_completed(tasks):
        r = await fut
        enriched_rows.append(r)

    new_fieldnames = list(dict.fromkeys([key for r in enriched_rows for key in r.keys()]))

    with open(output_csv, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=new_fieldnames)
        writer.writeheader()
        writer.writerows(enriched_rows)

    log.info("Wrote enriched CSV to %s", output_csv)
