from __future__ import annotations
import csv
import re
import unicodedata
import json
import argparse
from typing import List, Dict

NAME_TOKEN_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ'`.-]+")
CAPITALIZED_TOKEN_RE = re.compile(r"^[A-ZÀ-ÖØ-Ý][a-zà-öø-ÿ'`.-]+$")

NON_PERSON_KEYWORDS = {
    'team', 'teams', 'staff', 'events', 'event', 'company', 'group', 'studio',
    'owners', 'owner', 'planners', 'planner', 'collective', 'weddings', 'wedding', 'llc', 'co'
}

SPLIT_RE = re.compile(r"(?i)\s*(?:&|&amp;|\band\b|/|\+|;|•|·)\s*")

def clean_raw_name(raw: str) -> str:
    if not raw:
        return ''
    s = unicodedata.normalize('NFC', raw.strip())
    s = s.replace('&amp;', '&')

    if '|' in s:
        s = s.split('|', 1)[0].strip()

    s = re.split(r'\s+[–—-]\s+', s)[0].strip()

    s = re.sub(r'[\r\n\t]+', ' ', s).strip()
    return s

def looks_like_company_team(raw: str) -> bool:
    if not raw:
        return True
    low = raw.lower()
    if re.search(r"'s\s*team\b", low):
        return True
    if re.search(r"\bevent(s)?\b", low) and re.search(r"\bteam\b", low):
        return True
    if 'team' in low and any(w in low for w in ('event', 'events', 'staff', 'company', 'studio', 'group')):
        return True
    return False

def is_person_name(candidate: str) -> bool:
    if not candidate:
        return False
    c = candidate.strip()
    c = re.sub(r'\(.*?\)', '', c).strip()
    if looks_like_company_team(c):
        return False
    tokens = NAME_TOKEN_RE.findall(c)
    if not tokens:
        return False
    if all(t.lower() in NON_PERSON_KEYWORDS for t in tokens):
        return False
    has_cap = any(CAPITALIZED_TOKEN_RE.match(t) and t.lower() not in NON_PERSON_KEYWORDS for t in tokens)
    return bool(has_cap)

def split_names_from_key(raw_key: str) -> List[str]:
    cleaned = clean_raw_name(raw_key)
    if not cleaned:
        return []
    if looks_like_company_team(cleaned):
        return []
    parts = SPLIT_RE.split(cleaned)
    names = []
    for p in parts:
        p = p.strip()
        p = re.sub(r'\b(owners|owner|lead planners|lead planner|lead|planners|planner)\b\s*$', '', p, flags=re.I).strip()
        if is_person_name(p):
            names.append(p)
    return names

def extract_primary_social_url(urls, platform: str) -> str:
    if not urls:
        return ''
    pattern = re.compile(rf'https?://(?:www\.)?{re.escape(platform)}\.com/[^/?#]+', re.IGNORECASE)
    for url in urls:
        if not isinstance(url, str):
            continue
        m = pattern.search(url)
        if m:
            return m.group(0)
    return ''

def process_company(company: Dict) -> List[Dict]:
    rows = []
    company_name = company.get('name', '') or ''
    website = company.get('url', '') or ''
    min_spend = company.get('minimum_spend', '') or ''

    company_phones = []
    if company.get('phone_number'):
        company_phones.append(company['phone_number'])
    if company.get('url_extra', {}).get('phones'):
        company_phones.extend(company['url_extra'].get('phones') or [])

    company_emails = company.get('url_extra', {}).get('emails') or []

    social_urls = company.get('url_extra', {}).get('urls') or []
    instagram = company.get('instagramUrl', '') or extract_primary_social_url(social_urls, 'instagram')
    facebook = company.get('facebookUrl', '') or extract_primary_social_url(social_urls, 'facebook')

    team_members = company.get('teamMembers') or {}

    if not team_members:
        row = {
            'Company Name': company_name,
            'Website': website,
            'Contact Person': '',
            'Job Title': '',
            'Phone': company_phones[0] if company_phones else '',
            'Email': company_emails[0] if company_emails else '',
            'Minimum spend': min_spend,
            'Instagram Link': instagram,
            'Facebook Link': facebook
        }
        rows.append(row)
        return rows

    any_person_found = False
    for raw_key, title in team_members.items():
        names = split_names_from_key(raw_key)
        if not names:
            continue
        any_person_found = True
        first_for_company = True
        for name in names:
            personal_email = ''
            if company_emails:
                first_name_token = name.split()[0].lower()
                email_pattern = re.compile(rf'\b{re.escape(first_name_token)}.*?@', re.IGNORECASE)
                for email in company_emails:
                    if email_pattern.search(email):
                        personal_email = email
                        break

            if first_for_company:
                row = {
                    'Company Name': company_name,
                    'Website': website,
                    'Contact Person': name,
                    'Job Title': title,
                    'Phone': company_phones[0] if company_phones else '',
                    'Email': personal_email or (company_emails[0] if company_emails else ''),
                    'Minimum spend': min_spend,
                    'Instagram Link': instagram,
                    'Facebook Link': facebook
                }
                first_for_company = False
            else:
                row = {
                    'Company Name': company_name,
                    'Website': website,
                    'Contact Person': name,
                    'Job Title': title,
                    'Phone': '',
                    'Email': personal_email,
                    'Minimum spend': min_spend,
                    'Instagram Link': instagram,
                    'Facebook Link': facebook
                }
            rows.append(row)

    if not any_person_found:
        row = {
            'Company Name': company_name,
            'Website': website,
            'Contact Person': '',
            'Job Title': '',
            'Phone': company_phones[0] if company_phones else '',
            'Email': company_emails[0] if company_emails else '',
            'Minimum spend': min_spend,
            'Instagram Link': instagram,
            'Facebook Link': facebook
        }
        rows.append(row)

    return rows

def run(data: List[Dict], output_csv: str = "miami_vendors.csv") -> None:
    all_rows = []
    for company in data:
        try:
            all_rows.extend(process_company(company))
        except Exception as exc:
            print(f"Warning: error processing company {company.get('name', '<unknown>')}: {exc}")

    fieldnames = [
        'Company Name', 'Website', 'Contact Person', 'Job Title',
        'Phone', 'Email', 'Minimum spend', 'Instagram Link', 'Facebook Link'
    ]

    with open(output_csv, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Wrote {len(all_rows)} rows to {output_csv}")


def main():
    parser = argparse.ArgumentParser(description="Parse JSON companies to CSV (miami_vendors).")
    parser.add_argument('--input', '-i', help='Input JSON file (list of companies)', default='input.json')
    parser.add_argument('--output', '-o', help='Output CSV filename', default='miami_vendors.csv')
    args = parser.parse_args()

    with open(args.input, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise SystemExit("Input JSON must be a list of company objects.")

    run(data, args.output)

if __name__ == "__main__":
    main()
