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
import re
import json
import random
import logging
from math import ceil

import requests
from curl_cffi import requests as cffi_requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ETL")

# ── Liquid Scraper Constants ─────────────────────────────────────────────────
MAX_CONCURRENT  = 8           # Balance between stealth and speed
JITTER_MIN      = 0.5
JITTER_MAX      = 2.0
USERS_PER_PAGE  = 25
IMPERSONATE     = "chrome120"
GRAPHQL_URL     = "https://leetcode.com/graphql"
RANKING_URL     = "https://leetcode.com/contest/api/ranking/{slug}/?pagination={page}&region=global"

# ── JIT Batching Constants ────────────────────────────────────────────────────
BATCH_SIZE       = 15          # Users per single GraphQL request (15 to avoid complexity limit)
JIT_CONCURRENT   = 20          # Concurrent batch requests


def _build_batched_query(usernames: list[str], is_cn: bool = False) -> str:
    """
    Dynamically constructs a single GraphQL document with N aliased fields.
    """
    aliases = []
    for i, username in enumerate(usernames):
        # Escape backslashes first, then double-quotes
        safe = username.replace("\\", "\\\\").replace('"', '\\"')
        if is_cn:
            aliases.append(f'  user{i}: userContestRanking(userSlug: "{safe}") {{ ratingHistory contestHistory }}')
        else:
            aliases.append(f'  user{i}: userContestRankingHistory(username: "{safe}") {{ rating contest {{ titleSlug }} }}')
    return "query {\n" + "\n".join(aliases) + "\n}"


async def _fetch_batch(
    session: cffi_requests.AsyncSession,
    semaphore: asyncio.Semaphore,
    chunk: list[str],
    endpoint_url: str,
    target_contest_slug: str,
    is_cn: bool = False,
) -> dict[str, dict]:
    """
    Sends one batched GraphQL request for a chunk of 50 usernames.
    """
    query = _build_batched_query(chunk, is_cn=is_cn)
    backoff = 3

    async with semaphore:
        while True:
            await asyncio.sleep(random.uniform(0.1, 0.4))  # Light jitter — GraphQL is less guarded
            try:
                resp = await session.post(
                    endpoint_url,
                    json={"query": query},
                    headers={"Content-Type": "application/json"},
                )

                if resp.status_code in (403, 429):
                    logger.warning(f"[JIT] WAF Block on batch starting '{chunk[0]}'. Backoff {backoff}s...")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30)
                    continue

                resp.raise_for_status()
                data = resp.json().get("data", {})

                result = {}
                for i, username in enumerate(chunk):
                    node = data.get(f"user{i}")
                    if node is not None:
                        if is_cn:
                            rh = json.loads(node["ratingHistory"]) if node.get("ratingHistory") else []
                            ch = json.loads(node["contestHistory"]) if node.get("contestHistory") else []
                            
                            valid_history = []
                            for r_val, c_dict in zip(rh, ch):
                                if c_dict.get("contest", {}).get("titleSlug") == target_contest_slug:
                                    break
                                valid_history.append(r_val)
                                
                            if valid_history:
                                result[username] = {
                                    "rating": float(valid_history[-1]),
                                    "k": len(valid_history)
                                }
                            else:
                                result[username] = {
                                    "rating": 1500.0,
                                    "k": 0
                                }
                        else:
                            history_array = node if isinstance(node, list) else []
                            valid_history = []
                            for c in history_array:
                                if c.get("contest", {}).get("titleSlug") == target_contest_slug:
                                    break
                                valid_history.append(c.get("rating"))
                                
                            if valid_history:
                                result[username] = {
                                    "rating": float(valid_history[-1]),
                                    "k": len(valid_history)
                                }
                            else:
                                result[username] = {
                                    "rating": 1500.0,
                                    "k": 0
                                }
                return result

            except Exception as e:
                logger.error(f"[JIT] Batch error for '{chunk[0]}...': {e}. Retrying in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)


async def fetch_exact_baselines(
    user_data_list: list[dict],
    target_contest_slug: str,
    session: cffi_requests.AsyncSession | None = None,
) -> dict[str, dict]:
    """
    Module A.5 — JIT GraphQL Batching
    ===================================
    Given a list of dictionaries with usernames and regions, fetches their EXACT current LeetCode
    contest ratings in ~45 seconds using GraphQL aliasing, routed safely to US or CN servers.

    Returns:
        dict[str, dict]: {username -> {"rating": float, "k": int}}
    """
    if not user_data_list:
        return {}

    us_usernames = [u["username"] for u in user_data_list if u.get("region") != "CN"]
    cn_usernames = [u["username"] for u in user_data_list if u.get("region") == "CN"]

    semaphore = asyncio.Semaphore(JIT_CONCURRENT)
    merged: dict[str, dict] = {}
    
    total_batches = ceil(len(us_usernames) / BATCH_SIZE) + ceil(len(cn_usernames) / BATCH_SIZE)
    logger.info(
        f"[JIT] Starting region-aware baseline fetch: {len(user_data_list)} users "
        f"({len(us_usernames)} US, {len(cn_usernames)} CN) → {total_batches} batches @ {JIT_CONCURRENT} concurrent."
    )

    batches_done = 0

    async def _fetch_batches(usernames: list[str], endpoint_url: str, sess: cffi_requests.AsyncSession, target_slug: str, is_cn: bool = False):
        nonlocal batches_done
        if not usernames:
            return
        chunks = [usernames[i : i + BATCH_SIZE] for i in range(0, len(usernames), BATCH_SIZE)]
        tasks = [_fetch_batch(sess, semaphore, chunk, endpoint_url, target_slug, is_cn) for chunk in chunks]
        for coro in asyncio.as_completed(tasks):
            partial = await coro
            merged.update(partial)
            batches_done += 1
            if batches_done % 50 == 0 or batches_done == total_batches:
                logger.info(
                    f"[JIT] Progress: {batches_done}/{total_batches} batches | "
                    f"{len(merged)} ratings resolved."
                )

    async def _run(sess: cffi_requests.AsyncSession) -> None:
        await asyncio.gather(
            _fetch_batches(us_usernames, "https://leetcode.com/graphql", sess, target_contest_slug, is_cn=False),
            _fetch_batches(cn_usernames, "https://leetcode.cn/graphql", sess, target_contest_slug, is_cn=True)
        )

    if session is not None:
        await _run(session)
    else:
        async with cffi_requests.AsyncSession(impersonate=IMPERSONATE) as s:
            await _run(s)

    found    = len(merged)
    missing  = len(user_data_list) - found
    logger.info(
        f"[JIT] Complete: {found} real ratings fetched | "
        f"{missing} users have no contest history → will default to 1500."
    )
    return merged


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
                submissions_list = data.get('submissions', [])

                valid_participants = []
                for idx, u in enumerate(rankings):
                    # 1. Safely extract the submissions object (handling both parallel lists and embedded objects)
                    submissions = submissions_list[idx] if idx < len(submissions_list) else u.get('submissions', {})
                    score = u.get('score', 0)
                    
                    # 2. Drop User IF: score == 0 AND they have zero submissions. These are true ghosts.
                    if score == 0 and len(submissions) == 0:
                        continue
                        
                    # 3. Keep User IF: score == 0 AND they have greater than zero submissions.
                    valid_participants.append({
                        "username":    _extract_profile_username(u),
                        "display_name": u.get('username'),
                        "score":       score,
                        "finish_time": u.get('finish_time'),
                        "global_rank": u.get('rank'),
                        "data_region": u.get('data_region')
                    })

                return valid_participants

            except Exception as e:
                logger.error(f"[Page {page}] Failed: {e}. Retrying in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)



def _extract_profile_username(entry: dict) -> str:
    """
    Returns the correct LeetCode profile slug for GraphQL lookups.

    The contest leaderboard's 'username' field is a DISPLAY NAME and very
    often does NOT match the profile slug used by LeetCode's GraphQL API.

    The 'user_slug' field is the canonical profile URL slug — this is what
    userContestRanking(username: ...) expects.

    Examples from real contest data:
      display "Snap Dragon" → user_slug "Narendr_Modi" → GraphQL resolves correctly
      display "Aditya Gupta" → user_slug "adocxwork"   → GraphQL resolves correctly
      display "Mark"         → user_slug "LSvr9egntY"  → private, returns null (correct)

    NOTE: We intentionally do NOT use avatar_url parsing. The avatar URL
    encodes the slug of whoever originally uploaded the image, which can differ
    from the account owner (e.g. Arrow2520 had avatar from GammaGuy2520).
    """
    user_slug = (entry.get("user_slug") or "").strip()
    if user_slug:
        return user_slug
    # Fall back to display name (works when username == user_slug, which is common)
    return (entry.get("username") or "").strip()


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
        backoff = 5
        while True:
            try:
                probe = await session.get(probe_url)
                if probe.status_code in (403, 429):
                    logger.warning(f"Probe WAF Block ({probe.status_code}) for {contest_slug}. Backing off for {backoff}s...")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    continue
                probe.raise_for_status()
                probe_data = probe.json()
                total_users = probe_data.get('user_num', 0)
                total_pages = ceil(total_users / USERS_PER_PAGE)
                break
            except Exception as e:
                logger.error(f"Probe error for {contest_slug}: {e}. Retrying in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

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

    # 1. Sort the valid participants by their original API rank
    all_users.sort(key=lambda x: x.get('global_rank', 0))

    # 2. Re-assign contiguous active ranks (handling ties)
    current_rank = 1
    for i in range(len(all_users)):
        # If this user's global rank is worse than the previous user's, 
        # their active rank becomes their index + 1
        if i > 0 and all_users[i].get('global_rank', 0) > all_users[i-1].get('global_rank', 0):
            current_rank = i + 1
            
        all_users[i]['active_rank'] = current_rank

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
