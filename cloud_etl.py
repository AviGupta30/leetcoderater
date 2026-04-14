"""
cloud_etl.py — Subsystem for generating GROUND TRUTH baseline data
========================================================================
Liquid Scraper Architecture
- No global pauses.
- Uses `asyncio.as_completed` for real-time progress updates.
- Implements targeted exponential backoff on 403s.
"""

import os
import asyncio
import random
import logging
from math import ceil

import requests
from curl_cffi import requests as cffi_requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ETL")

# ── Liquid Scraper Constants ───────────────────────────────────────────────────
MAX_CONCURRENT  = 8           # Balance between stealth and speed
JITTER_MIN      = 0.5
JITTER_MAX      = 2.0
USERS_PER_PAGE  = 25
IMPERSONATE     = "chrome120"
GRAPHQL_URL     = "https://leetcode.com/graphql"
RANKING_URL     = "https://leetcode.com/contest/api/ranking/{slug}/?pagination={page}&region=global"

async def _scrape_page(
    session: cffi_requests.AsyncSession,
    semaphore: asyncio.Semaphore,
    contest_slug: str,
    page: int,
) -> list[dict]:
    """
    Scrape a single leaderboard page with localized exponential backoff.
    """
    url = RANKING_URL.format(slug=contest_slug, page=page)
    backoff = 5  # Base backoff in seconds

    async with semaphore:
        while True:
            await asyncio.sleep(random.uniform(JITTER_MIN, JITTER_MAX))
            try:
                resp = await session.get(url)

                if resp.status_code in (403, 429):
                    logger.warning(f"[Page {page}] WAF Block ({resp.status_code}). Backing off for {backoff}s...")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)  # Exponential backoff, cap at 60s
                    continue
                
                # Reset backoff on success (if we had retried)
                backoff = 5 
                
                resp.raise_for_status()
                data     = resp.json()
                rankings = data.get('total_rank', [])

                return [
                    {
                        "username":    u.get('username'),
                        "score":       u.get('score'),
                        "finish_time": u.get('finish_time'),
                        "global_rank": u.get('rank'),
                        "data_region": u.get('data_region')
                    }
                    for u in rankings
                ]

            except Exception as e:
                logger.error(f"[Page {page}] Failed: {e}. Retrying in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)


async def scrape_contest_leaderboard(
    contest_slug: str,
    max_pages: int | None = None,
    progress_callback=None,
) -> list[dict]:
    """
    Liquid scraper using asyncio.as_completed for instant UI feedback.
    """
    logger.info(f"Liquid Scrape starting: {contest_slug} | concurrency={MAX_CONCURRENT}")
    all_users = []
    
    # Needs to be created here to attach to the correct event loop
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    async with cffi_requests.AsyncSession(impersonate=IMPERSONATE) as session:
        # 1. Probe for total pages
        probe_url = RANKING_URL.format(slug=contest_slug, page=1)
        try:
            probe = await session.get(probe_url)
            probe.raise_for_status()
            probe_data = probe.json()
            total_users = probe_data.get('user_num', 0)
            total_pages = ceil(total_users / USERS_PER_PAGE)
        except Exception as e:
            logger.error(f"Probe failed for {contest_slug}: {e}")
            return []

        if max_pages:
            total_pages = min(total_pages, max_pages)

        logger.info(f"{total_users} participants across {total_pages} pages.")

        # 2. Launch tasks and process exactly as they finish
        tasks = [_scrape_page(session, semaphore, contest_slug, p) for p in range(1, total_pages + 1)]
        pages_done = 0
        
        for coro in asyncio.as_completed(tasks):
            res = await coro
            all_users.extend(res)
            pages_done += 1
            
            if progress_callback:
                await progress_callback(pages_done, total_pages)

    logger.info(f"Scrape complete: {len(all_users)} rows extracted.")
    return all_users


async def fetch_latest_contests(n: int = 10) -> list[dict]:
    """
    Hit LeetCode's GraphQL API to return the N most recent contests.
    """
    query = """
    {
      allContests {
        title
        titleSlug
        startTime
        duration
      }
    }
    """
    try:
        async with cffi_requests.AsyncSession(impersonate=IMPERSONATE) as session:
            resp = await session.post(
                GRAPHQL_URL,
                json={"query": query},
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            data = resp.json()
            all_contests = data.get("data", {}).get("allContests", [])
            sorted_contests = sorted(all_contests, key=lambda c: c.get("startTime", 0), reverse=True)
            return [
                {
                    "title":     c["title"],
                    "slug":      c["titleSlug"],
                    "startTime": c["startTime"],
                    "duration":  c["duration"],
                }
                for c in sorted_contests[:n]
            ]
    except Exception as e:
        logger.error(f"Failed to fetch contest list: {e}")
        return []
