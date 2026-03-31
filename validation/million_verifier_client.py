import logging
import time
import requests
from config import MILLION_VERIFIER_API_KEY, MILLION_VERIFIER_BASE_URL, MILLION_VERIFIER_DELAY
from tracking.cost_tracker import track_cost

logger = logging.getLogger(__name__)


def validate_email(email: str,
                   campaign_id: str = None, lead_id: str = None) -> dict:
    """Validate an email via Million Verifier. Returns {result, quality_score}."""
    try:
        resp = requests.get(
            MILLION_VERIFIER_BASE_URL,
            params={"api": MILLION_VERIFIER_API_KEY, "email": email},
            timeout=15
        )
        resp.raise_for_status()
        data = resp.json()

        track_cost(campaign_id, lead_id, "million_verifier", "email_validate")

        # Rate limit compliance
        time.sleep(MILLION_VERIFIER_DELAY)

        return {
            "result": data.get("result", "unknown"),
            "quality_score": data.get("quality_score", 0),
        }
    except Exception as e:
        logger.error(f"Million Verifier error for {email}: {e}")
        return {"result": "unknown", "quality_score": 0}
