import csv
import re

def extract_primary_social_url(urls, platform):
    pattern = re.compile(rf'https?://(?:www\.)?{re.escape(platform)}\.com/[^/?]+')
    for url in urls:
        match = pattern.search(url)
        if match:
            return match.group(0)
    return ''

def process_company(company):
    rows = []
    company_name = company.get('name', '')
    website = company.get('url', '')
    min_spend = company.get('minimum_spend', '')

    company_phones = []
    if company.get('phone_number'):
        company_phones.append(company['phone_number'])
    if company.get('url_extra', {}).get('phones'):
        company_phones.extend(company['url_extra']['phones'])

    company_emails = company.get('url_extra', {}).get('emails', [])

    social_urls = company.get('url_extra', {}).get('urls', [])
    instagram = company.get('instagramUrl', '') or extract_primary_social_url(social_urls, 'instagram')
    facebook = company.get('facebookUrl', '') or extract_primary_social_url(social_urls, 'facebook')

    team_members = company.get('teamMembers', {})

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

    first_member = True
    for name, title in team_members.items():
        personal_email = ''
        if company_emails:
            email_pattern = re.compile(rf'\b{re.escape(name.split()[0].lower())}.*?@', re.IGNORECASE)
            for email in company_emails:
                if email_pattern.search(email):
                    personal_email = email
                    break

        if first_member:
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
            first_member = False
        else:
            row = {
                'Company Name': company_name,
                'Website': website,
                'Contact Person': name,
                'Job Title': title,
                'Phone': '',
                'Email': personal_email,
                'Minimum spend': min_spend,
                'Instagram Link': '',
                'Facebook Link': ''
            }
        rows.append(row)

    return rows

def run(data: list[dict], output_csv="miami_vendors.csv"):

    all_rows = []
    for company in data:
        all_rows.extend(process_company(company))

    fieldnames = [
        'Company Name', 'Website', 'Contact Person', 'Job Title',
        'Phone', 'Email', 'Minimum spend', 'Instagram Link', 'Facebook Link'
    ]

    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)