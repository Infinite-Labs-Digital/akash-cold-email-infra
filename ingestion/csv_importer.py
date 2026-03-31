import csv
import logging
from db import insert_lead

logger = logging.getLogger(__name__)

# Common CSV column name mappings → our field names
COLUMN_MAPPINGS = {
    "business_name": ["business_name", "company", "company_name", "name", "store_name"],
    "business_domain": ["domain", "business_domain", "company_domain"],
    "website": ["website", "url", "website_url"],
    "email": ["email", "email_address"],
    "phone": ["phone", "phone_number"],
    "address": ["address", "street_address"],
    "city": ["city"],
    "state": ["state", "state_code"],
    "zip": ["zip", "zip_code", "postal_code"],
    "owner_name": ["owner", "owner_name", "contact_name", "first_name"],
    "industry": ["industry", "category"],
    "rating": ["rating", "google_rating"],
    "review_count": ["reviews", "review_count"],
}


def import_csv(file_path: str, campaign_id: str, source_name: str = "client_csv") -> dict:
    """Import a CSV file into leads table. Returns {imported, skipped, errors}."""
    stats = {"imported": 0, "skipped": 0, "errors": 0}

    with open(file_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        field_map = _build_field_map(reader.fieldnames)

        for row in reader:
            try:
                lead_data = _map_row(row, field_map, source_name)
                if not lead_data.get("business_name") and not lead_data.get("business_domain"):
                    stats["skipped"] += 1
                    continue
                insert_lead(lead_data, campaign_id)
                stats["imported"] += 1
            except Exception as e:
                logger.error(f"CSV import error row {stats['imported'] + stats['errors']}: {e}")
                stats["errors"] += 1

    logger.info(f"CSV import: {stats}")
    return stats


def _build_field_map(csv_headers: list) -> dict:
    """Map CSV headers to our field names."""
    field_map = {}
    normalized_headers = {h.strip().lower().replace(" ", "_"): h for h in csv_headers}

    for our_field, possible_names in COLUMN_MAPPINGS.items():
        for name in possible_names:
            if name in normalized_headers:
                field_map[our_field] = normalized_headers[name]
                break
    return field_map


def _map_row(row: dict, field_map: dict, source_name: str) -> dict:
    """Map a CSV row to a lead dict using the field map."""
    lead = {"sources": [source_name], "raw_data": dict(row)}
    for our_field, csv_header in field_map.items():
        value = row.get(csv_header, "").strip()
        if value:
            lead[our_field] = value
    # Extract domain from website if domain not provided
    if not lead.get("business_domain") and lead.get("website"):
        from ingestion.apify_client import _extract_domain
        lead["business_domain"] = _extract_domain(lead["website"])
    return lead
