import logging
from anthropic import Anthropic
from config import ANTHROPIC_API_KEY, SONNET_MODEL
from db import get_connection
from tracking.cost_tracker import track_cost

logger = logging.getLogger(__name__)

client = Anthropic(api_key=ANTHROPIC_API_KEY)


def build_research_document(campaign_id: str = None) -> str:
    """Synthesize all training corpus content into a research document."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT title, content FROM training_corpus ORDER BY ingested_at")
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return ""

    # Combine all transcripts
    corpus = "\n\n".join([
        f"=== {row[0]} ===\n{row[1]}"
        for row in rows
    ])

    # Truncate to fit context window
    corpus = corpus[:100000]

    prompt = f"""You are synthesizing cold email expertise from YouTube transcripts into a concise research document.

Analyze the following transcripts and extract:
1. Cold email frameworks and structures that work
2. Subject line patterns with high open rates
3. Personalization techniques
4. Proven email sequences (openers, follow-ups, break-ups)
5. What NOT to do (common mistakes)
6. Industry-specific tips

Transcripts:
---
{corpus}
---

Create a structured research document that a cold email generator can use as context. Be specific — include actual templates, phrases, and patterns, not just general advice."""

    try:
        response = client.messages.create(
            model=SONNET_MODEL,
            max_tokens=4000,
            messages=[{"role": "user", "content": prompt}]
        )
        research_doc = response.content[0].text

        # Store the research document
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO training_corpus (source, title, content, summary)
                    VALUES ('synthesis', 'Research Document', %s, 'Auto-generated research document')
                """, (research_doc,))
                conn.commit()
        finally:
            conn.close()

        track_cost(campaign_id, None, "claude_sonnet", "email_generation", cost_usd=0.10)
        logger.info("Research document built and stored")
        return research_doc
    except Exception as e:
        logger.error(f"Research document build error: {e}")
        return ""


def get_research_document() -> str:
    """Retrieve the latest research document."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT content FROM training_corpus
                WHERE source = 'synthesis'
                ORDER BY ingested_at DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            return row[0] if row else ""
    finally:
        conn.close()
