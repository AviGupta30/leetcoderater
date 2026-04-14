"""
pipeline.py — The Orchestrator
==============================
Wires Module A (cloud_etl.py) and Module B (rating_engine.py) into a single
end-to-end pipeline that:
  1. Scrapes the LeetCode contest leaderboard via Cloudflare-evading curl_cffi
  2. Resolves baselines (Saturday Cache → Wednesday DB → 1500)
  3. Runs the Histogram Interpolation Elo math
  4. Returns a sorted, enriched prediction payload

Designed to be called directly OR imported by server.py (FastAPI layer).
"""

import asyncio
import logging
import time

from cloud_etl import scrape_contest_leaderboard, fetch_official_baselines
from rating_engine import RatingEngine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Pipeline")


async def run_pipeline(
    contest_slug: str,
    saturday_cache: dict = None,
    max_pages: int = None,
) -> list[dict]:
    """
    Full end-to-end orchestration: Extract → Transform → Return.

    Args:
        contest_slug:   e.g. "weekly-contest-400"
        saturday_cache: Optional dict {username: predicted_rating} from
                        the previous Saturday run (for biweekly correction).
        max_pages:      Limit scraper pages for testing (None = all pages).

    Returns:
        Sorted list of prediction dicts:
        [{"username", "global_rank", "previous_rating", "predicted_delta", "predicted_rating"}, ...]
    """
    t_start = time.perf_counter()
    saturday_cache = saturday_cache or {}

    # ── Module A: Extract ────────────────────────────────────────────────────────
    logger.info(f"Pipeline starting for contest: {contest_slug}")
    participants = await scrape_contest_leaderboard(contest_slug, max_pages=max_pages)

    if not participants:
        logger.error("Extraction returned 0 participants. Aborting pipeline.")
        return []

    logger.info(f"Extraction complete: {len(participants)} participants scraped.")

    # ── Baseline data: official Wednesday ratings ────────────────────────────────
    wednesday_json = fetch_official_baselines()

    # ── Module B: Transform (Elo Math) ───────────────────────────────────────────
    engine = RatingEngine()
    results = engine.calculate(participants, saturday_cache, wednesday_json)

    elapsed = time.perf_counter() - t_start
    logger.info(f"Pipeline complete in {elapsed:.2f}s — {len(results)} predictions generated.")
    return results


# ── CLI Smoke Test ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json

    TARGET_CONTEST = "weekly-contest-400"
    TEST_PAGES = 10  # ~250 users — fast for testing

    logger.info(f"Running pipeline smoke test on {TARGET_CONTEST} ({TEST_PAGES} pages)...")
    predictions = asyncio.run(run_pipeline(TARGET_CONTEST, max_pages=TEST_PAGES))

    if predictions:
        logger.info(f"\nTop 10 Predictions:")
        logger.info(f"{'Rank':<6} {'Username':<25} {'Prev':>6} {'Delta':>7} {'New':>7}")
        logger.info("-" * 60)
        for p in predictions[:10]:
            sign = "+" if p["predicted_delta"] >= 0 else ""
            logger.info(
                f"#{p['global_rank']:<5} {p['username']:<25} "
                f"{p['previous_rating']:>6.0f} "
                f"{sign}{p['predicted_delta']:>6.1f} "
                f"{p['predicted_rating']:>7.0f}"
            )
    else:
        logger.warning("No predictions generated.")
