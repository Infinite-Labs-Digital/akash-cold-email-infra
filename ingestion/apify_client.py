import logging
import time
import requests
from config import APIFY_TOKEN, APIFY_BASE_URL
from tracking.cost_tracker import track_cost

logger = logging.getLogger(__name__)


def run_actor(actor_id: str, input_data: dict, timeout: int = 300) -> list:
    """Run an Apify actor and wait for results. Returns list of items."""
    headers = {"Authorization": f"Bearer {APIFY_TOKEN}"}

    # Start the actor run
    resp = requests.post(
        f"{APIFY_BASE_URL}/acts/{actor_id}/runs",
        headers=headers,
        json=input_data,
        timeout=30
    )
    resp.raise_for_status()
    run_id = resp.json()["data"]["id"]
    logger.info(f"Apify actor {actor_id} started, run_id={run_id}")

    # Poll until complete
    start = time.time()
    while time.time() - start < timeout:
        status_resp = requests.get(
            f"{APIFY_BASE_URL}/actor-runs/{run_id}",
            headers=headers, timeout=15
        )
        status = status_resp.json()["data"]["status"]
        if status == "SUCCEEDED":
            break
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            logger.error(f"Apify run {run_id} failed: {status}")
            return []
        time.sleep(5)
    else:
        logger.error(f"Apify run {run_id} timed out after {timeout}s")
        return []

    # Fetch results
    dataset_id = status_resp.json()["data"]["defaultDatasetId"]
    items_resp = requests.get(
        f"{APIFY_BASE_URL}/datasets/{dataset_id}/items",
        headers=headers, timeout=30
    )
    return items_resp.json()


def scrape_google_maps(query: str, location: str,
                       max_results: int = 200,
                       campaign_id: str = None) -> list:
    """Scrape Google Maps for businesses. Returns normalized lead dicts."""
    actor_id = "nwua9Gu5YrADL7ZDj"  # Google Maps Scraper actor

    items = run_actor(actor_id, {
        "searchStringsArray": [f"{query} in {location}"],
        "maxCrawledPlacesPerSearch": max_results,
        "language": "en",
        "includeWebResults": False,
    })

    leads = []
    for item in items:
        domain = _extract_domain(item.get("website", ""))
        lead = {
            "business_name": item.get("title"),
            "business_domain": domain,
            "website": item.get("website"),
            "phone": item.get("phone"),
            "address": item.get("address"),
            "city": item.get("city"),
            "state": item.get("state"),
            "zip": item.get("postalCode"),
            "rating": item.get("totalScore"),
            "review_count": item.get("reviewsCount"),
            "sources": ["apify_gmaps"],
            "raw_data": item,
        }
        leads.append(lead)

    track_cost(campaign_id, None, "apify", "google_maps",
               credits_used=len(leads), cost_usd=len(leads) * 0.01)
    logger.info(f"Apify Google Maps: {len(leads)} businesses found for '{query} in {location}'")
    return leads


def _extract_domain(url: str) -> str:
    """Extract domain from URL. Returns None if invalid."""
    if not url:
        return None
    from urllib.parse import urlparse
    parsed = urlparse(url if "://" in url else f"https://{url}")
    domain = parsed.netloc or parsed.path
    domain = domain.replace("www.", "").strip("/")
    return domain if domain else None
