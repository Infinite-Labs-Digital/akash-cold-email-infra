import logging
from db import get_today_spend
from config import DAILY_API_BUDGET_USD, BUDGET_ALERT_THRESHOLD

logger = logging.getLogger(__name__)


def check_budget() -> bool:
    """Check if daily budget allows more API calls.
    Returns True if we can proceed, False if budget exhausted."""
    today_spend = get_today_spend()
    budget = DAILY_API_BUDGET_USD

    if today_spend >= budget:
        logger.warning(
            f"BUDGET EXHAUSTED: ${today_spend:.2f} / ${budget:.2f} — "
            f"pausing all API operations until tomorrow"
        )
        return False

    if today_spend >= budget * BUDGET_ALERT_THRESHOLD:
        logger.warning(
            f"BUDGET ALERT: ${today_spend:.2f} / ${budget:.2f} "
            f"({today_spend/budget:.0%}) — approaching daily limit"
        )

    return True


def get_budget_status() -> dict:
    """Get current budget status. Returns {spent, budget, remaining, percent}."""
    today_spend = get_today_spend()
    budget = DAILY_API_BUDGET_USD
    return {
        "spent": today_spend,
        "budget": budget,
        "remaining": max(0, budget - today_spend),
        "percent": today_spend / budget if budget > 0 else 0,
    }
