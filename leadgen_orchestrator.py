#!/usr/bin/env python3
"""
LeadForge v2 — Cold Email Infrastructure Orchestrator

Runs 3 continuous loops:
  - Scrape loop: Lead ingestion from configured sources (every 5 min)
  - Process loop: Enrichment pipeline — owner discovery, email finding, validation (every 6 hours)
  - Launch loop: Email generation + campaign sync to Instantly (every 24 hours)

Usage:
  python leadgen_orchestrator.py                    # Run all loops
  python leadgen_orchestrator.py --loop scrape      # Run only scrape loop
  python leadgen_orchestrator.py --loop process     # Run only process loop
  python leadgen_orchestrator.py --loop launch      # Run only launch loop
  python leadgen_orchestrator.py --once             # Run one cycle then exit
"""

import argparse
import logging
import time
import sys
from datetime import datetime

from config import (
    SCRAPE_LOOP_INTERVAL,
    PROCESS_LOOP_INTERVAL,
    LAUNCH_LOOP_INTERVAL,
)
from tracking.budget_guard import check_budget, get_budget_status
from db import get_connection
from psycopg2.extras import RealDictCursor

logger = logging.getLogger("leadgen")


def setup_logging(verbose: bool = False):
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def get_active_campaigns() -> list:
    """Get all active campaigns from the database."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM campaigns WHERE status = 'active'")
            return cur.fetchall()
    finally:
        conn.close()


def run_scrape_loop():
    """Loop 1: Continuous lead ingestion from configured sources."""
    from ingestion.source_router import route_and_ingest

    campaigns = get_active_campaigns()
    if not campaigns:
        logger.info("No active campaigns — skipping scrape loop")
        return

    for campaign in campaigns:
        campaign_id = str(campaign["campaign_id"])
        niche = campaign.get("niche")
        location = campaign.get("location_detail")

        if not niche or not location:
            logger.warning(f"Campaign {campaign['campaign_name']} missing niche or location — skipping")
            continue

        logger.info(f"Scrape loop: {campaign['campaign_name']} ({niche} in {location})")

        try:
            stats = route_and_ingest(niche, location, campaign_id)
            logger.info(f"Scrape result: {stats}")
        except Exception as e:
            logger.error(f"Scrape error for campaign {campaign_id}: {e}")


def run_process_loop():
    """Loop 2: Enrichment pipeline — owner discovery, email finding, validation."""
    from enrichment.enrichment_engine import process_batch

    campaigns = get_active_campaigns()
    if not campaigns:
        logger.info("No active campaigns — skipping process loop")
        return

    for campaign in campaigns:
        campaign_id = str(campaign["campaign_id"])

        if not check_budget():
            logger.warning("Budget exhausted — stopping process loop")
            return

        logger.info(f"Process loop: {campaign['campaign_name']}")

        try:
            stats = process_batch(campaign_id)
            logger.info(f"Process result: {stats}")
        except Exception as e:
            logger.error(f"Process error for campaign {campaign_id}: {e}")


def run_launch_loop():
    """Loop 3: Email generation + campaign sync to Instantly."""
    from generation.email_generator import generate_batch
    from campaigns.campaign_launcher import launch_campaign
    from campaigns.campaign_monitor import monitor_campaigns

    campaigns = get_active_campaigns()
    if not campaigns:
        logger.info("No active campaigns — skipping launch loop")
        return

    for campaign in campaigns:
        campaign_id = str(campaign["campaign_id"])

        if not check_budget():
            logger.warning("Budget exhausted — stopping launch loop")
            return

        logger.info(f"Launch loop: {campaign['campaign_name']}")

        try:
            # Generate email sequences for validated leads
            gen_stats = generate_batch(campaign_id)
            logger.info(f"Email generation: {gen_stats}")

            # Sync to Instantly
            launch_stats = launch_campaign(campaign_id)
            logger.info(f"Campaign launch: {launch_stats}")
        except Exception as e:
            logger.error(f"Launch error for campaign {campaign_id}: {e}")

    # Monitor all active campaigns
    try:
        monitor_stats = monitor_campaigns()
        logger.info(f"Campaign monitor: {monitor_stats}")
        for alert in monitor_stats.get("alerts", []):
            logger.warning(f"ALERT: {alert}")
    except Exception as e:
        logger.error(f"Monitor error: {e}")


def main_loop(args):
    """Main orchestrator loop."""
    last_scrape = 0
    last_process = 0
    last_launch = 0

    loop_filter = args.loop  # None = all, or "scrape"/"process"/"launch"

    logger.info("=" * 60)
    logger.info("LeadForge v2 Orchestrator starting")
    logger.info(f"Loop filter: {loop_filter or 'all'}")
    logger.info(f"Budget: {get_budget_status()}")
    logger.info("=" * 60)

    while True:
        now = time.time()

        try:
            # Scrape loop
            if (loop_filter is None or loop_filter == "scrape") and \
               (now - last_scrape >= SCRAPE_LOOP_INTERVAL):
                logger.info("--- SCRAPE LOOP ---")
                run_scrape_loop()
                last_scrape = now

            # Process loop
            if (loop_filter is None or loop_filter == "process") and \
               (now - last_process >= PROCESS_LOOP_INTERVAL):
                logger.info("--- PROCESS LOOP ---")
                run_process_loop()
                last_process = now

            # Launch loop
            if (loop_filter is None or loop_filter == "launch") and \
               (now - last_launch >= LAUNCH_LOOP_INTERVAL):
                logger.info("--- LAUNCH LOOP ---")
                run_launch_loop()
                last_launch = now

            # One-shot mode
            if args.once:
                logger.info("One-shot mode — exiting after first cycle")
                break

        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")

        time.sleep(60)  # Check every minute


def parse_args():
    parser = argparse.ArgumentParser(description="LeadForge v2 Orchestrator")
    parser.add_argument(
        "--loop",
        choices=["scrape", "process", "launch"],
        help="Run only a specific loop (default: all)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one cycle then exit",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    setup_logging(args.verbose)
    main_loop(args)
