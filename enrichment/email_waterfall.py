import logging
import re
from validation.leadmagic_client import find_email as lm_find_email
from ingestion.instantly_lead_client import search_leads as instantly_search
from db import update_lead_fields
from config import JUNK_EMAIL_PATTERNS

logger = logging.getLogger(__name__)

GENERIC_PREFIXES = {"info", "contact", "support", "hello", "admin", "office", "help", "sales", "enquiries", "inquiries"}


def find_email(lead: dict, campaign_id: str, website_data: dict = None) -> dict:
    """Run the 5-step email waterfall for a single lead.
    Returns {email, email_source, email_type, email_generic}."""

    lead_id = str(lead["lead_id"])
    owner_name = lead.get("owner_name", "")
    domain = lead.get("business_domain", "")
    website = lead.get("website", "")

    result = {"email": None, "email_source": None, "email_type": None, "email_generic": None}

    # Step 1: LeadMagic email finder
    if owner_name and domain:
        lm_result = lm_find_email(owner_name, domain,
                                   campaign_id=campaign_id, lead_id=lead_id)
        email = lm_result.get("email")
        if email and not _is_junk(email):
            if _is_personal(email):
                result.update({"email": email, "email_source": "leadmagic_finder", "email_type": "personal"})
                _save_and_return(lead_id, result)
                return result
            else:
                result["email_generic"] = email

    # Step 2: Instantly Lead Finder
    if owner_name and domain and not result["email"]:
        instantly_results = instantly_search(name=owner_name, domain=domain,
                                             limit=5, campaign_id=campaign_id, lead_id=lead_id)
        for person in instantly_results:
            email = person.get("email")
            if email and not _is_junk(email):
                if _is_personal(email):
                    result.update({"email": email, "email_source": "instantly_lead_finder", "email_type": "personal"})
                    _save_and_return(lead_id, result)
                    return result
                elif not result["email_generic"]:
                    result["email_generic"] = email

    # Step 3: Apollo (check if lead already has email from ingestion)
    if not result["email"]:
        existing_email = lead.get("email")
        if existing_email and not _is_junk(existing_email):
            if _is_personal(existing_email):
                result.update({"email": existing_email, "email_source": lead.get("email_source", "apollo"), "email_type": "personal"})
                _save_and_return(lead_id, result)
                return result
            elif not result["email_generic"]:
                result["email_generic"] = existing_email

    # Step 4: Apify email finder (placeholder — skip for now)
    # Future: Add Apify email finder actors here

    # Step 5: Website scrape for emails
    if not result["email"]:
        emails_from_site = _get_website_emails(website, website_data)
        for email in emails_from_site:
            if not _is_junk(email):
                if _is_personal(email):
                    result.update({"email": email, "email_source": "website_scrape", "email_type": "personal"})
                    _save_and_return(lead_id, result)
                    return result
                elif not result["email_generic"]:
                    result["email_generic"] = email

    # If we only found generic email, use it
    if not result["email"] and result["email_generic"]:
        result.update({
            "email": result["email_generic"],
            "email_source": "generic",
            "email_type": "generic",
        })

    _save_and_return(lead_id, result)
    logger.info(f"Email waterfall for {lead.get('business_name')}: {result['email']} ({result['email_type']})")
    return result


def _save_and_return(lead_id: str, result: dict):
    """Update lead in DB with email results."""
    fields = {}
    if result.get("email"):
        fields["email"] = result["email"]
        fields["email_source"] = result["email_source"]
        fields["email_type"] = result["email_type"]
    if result.get("email_generic"):
        fields["email_generic"] = result["email_generic"]
    if fields:
        update_lead_fields(lead_id, fields)


def _is_personal(email: str) -> bool:
    """Check if email is personal (not generic)."""
    prefix = email.split("@")[0].lower()
    return prefix not in GENERIC_PREFIXES


def _is_junk(email: str) -> bool:
    """Check if email matches junk patterns."""
    email_lower = email.lower()
    return any(junk in email_lower for junk in JUNK_EMAIL_PATTERNS)


def _get_website_emails(website: str, website_data: dict = None) -> list:
    """Get emails from website data, or scrape if not provided."""
    if website_data and website_data.get("emails"):
        return website_data["emails"]

    if website:
        from enrichment.website_scraper import scrape_website
        data = scrape_website(website)
        return data.get("emails", [])

    return []
