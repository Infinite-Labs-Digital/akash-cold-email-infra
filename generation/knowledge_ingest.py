import logging
from ingestion.apify_client import run_actor
from db import get_connection
from tracking.cost_tracker import track_cost

logger = logging.getLogger(__name__)


def ingest_youtube_channel(channel_url: str, max_videos: int = 50,
                           campaign_id: str = None) -> dict:
    """Ingest YouTube transcripts from a channel. Returns {ingested, errors}."""
    actor_id = "starvibe/youtube-video-transcript"

    items = run_actor(actor_id, {
        "channelUrl": channel_url,
        "maxVideos": max_videos,
    }, timeout=600)

    stats = {"ingested": 0, "errors": 0}
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for item in items:
                try:
                    cur.execute("""
                        INSERT INTO training_corpus (source, source_url, title, content)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        "youtube",
                        item.get("url", ""),
                        item.get("title", ""),
                        item.get("transcript", ""),
                    ))
                    stats["ingested"] += 1
                except Exception as e:
                    logger.error(f"Ingest error: {e}")
                    stats["errors"] += 1
            conn.commit()
    finally:
        conn.close()

    track_cost(campaign_id, None, "apify", "youtube_transcript",
               credits_used=len(items))
    logger.info(f"YouTube ingest: {stats}")
    return stats
