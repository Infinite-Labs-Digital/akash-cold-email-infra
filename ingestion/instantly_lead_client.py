import logging
import requests
from config import INSTANTLY_API_KEY, INSTANTLY_BASE_URL
from tracking.cost_tracker import track_cost

logger = logging.getLogger(__name__)


def search_leads(name: str = None, domain: str = None,
                 title: str = None, location: str = None,
                 limit: int = 25,
                 campaign_id: str = None, lead_id: str = None) -> list:
    """Search Instantly Lead Finder. Returns lead dicts."""
    params = {"api_key": INSTANTLY_API_KEY, "limit": limit}
    if name:
        params["name"] = name
    if domain:
        params["domain"] = domain
    if title:
        params["title"] = title
    if location:
        params["location"] = location

    try:
        resp = requests.get(
            f"{INSTANTLY_BASE_URL}/lead/search",
            params=params, timeout=30
        )
        resp.raise_for_status()
        results = resp.json().get("data", [])

        leads = []
        for item in results:
            lead = {
                "business_name": item.get("company_name"),
                "business_domain": item.get("domain"),
                "website": f"https://{item.get('domain')}" if item.get("domain") else None,
                "city": item.get("city"),
                "state": item.get("state"),
                "owner_name": item.get("name"),
                "owner_source": "instantly",
                "owner_confidence": "high",
                "owner_status": "found",
                "email": item.get("email"),
                "email_source": "instantly_lead_finder" if item.get("email") else None,
                "email_type": "personal" if item.get("email") else None,
                "sources": ["instantly"],
                "raw_data": item,
            }
            leads.append(lead)

        track_cost(campaign_id, lead_id, "instantly", "lead_finder", credits_used=len(leads))
        logger.info(f"Instantly Lead Finder: {len(leads)} leads found")
        return leads
    except Exception as e:
        logger.error(f"Instantly lead search error: {e}")
        return []
