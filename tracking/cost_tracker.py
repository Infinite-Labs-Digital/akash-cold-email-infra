import logging
from db import log_cost_event

logger = logging.getLogger(__name__)

# Known cost-per-credit for each service (USD)
SERVICE_COSTS = {
    "leadmagic": {"email_finder": 0.03, "email_validate": 0.01, "company_search": 0.02},
    "million_verifier": {"email_validate": 0.001},
    "claude_haiku": {"owner_discovery": 0.014, "owner_verification": 0.002},
    "claude_sonnet": {"email_generation": 0.02},
    "apify": {"google_maps": 0.01, "directory": 0.005, "youtube_transcript": 0.005},
    "apollo": {"people_search": 0.0, "email_finder": 0.0},  # existing plan
    "instantly": {"lead_finder": 0.0},  # included in plan
}


def track_cost(campaign_id: str, lead_id: str, service: str,
               operation: str, credits_used: float = 1,
               cost_usd: float = None):
    """Log an API cost event. Auto-calculates cost if not provided."""
    if cost_usd is None:
        cost_usd = SERVICE_COSTS.get(service, {}).get(operation, 0.0) * credits_used

    log_cost_event(campaign_id, lead_id, service, operation, credits_used, cost_usd)
    logger.debug(f"Cost: {service}/{operation} = ${cost_usd:.4f} (campaign={campaign_id})")
