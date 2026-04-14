"""
build_baselines.py — Subsystem for generating GROUND TRUTH baseline data
========================================================================
This runs independently to scrape the official LeetCode Global Ranking.
It builds the `lc_users_dump.json` file with 100% real ratings.
"""

import asyncio
import json
import logging
import random
from curl_cffi import requests as cffi_requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("BaselineBuilder")

GRAPHQL_URL = "https://leetcode.com/graphql"
IMPERSONATE = "chrome120"
MAX_CONCURRENT = 15
JITTER_MIN = 0.3
JITTER_MAX = 0.8
USERS_PER_PAGE = 40  # LeetCode GraphQL returns 40 nodes per page for globalRanking

QUERY = """
query globalRanking($page: Int!) {
  globalRanking(page: $page) {
    totalUsers
    rankingNodes {
      currentRating
      user {
        username
      }
    }
  }
}
"""

async def fetch_page(session, semaphore, page):
    async with semaphore:
        retries = 3
        while retries > 0:
            await asyncio.sleep(random.uniform(JITTER_MIN, JITTER_MAX))
            try:
                resp = await session.post(
                    GRAPHQL_URL,
                    json={"query": QUERY, "variables": {"page": page}},
                    headers={"Content-Type": "application/json"}
                )
                
                if resp.status_code in (403, 429):
                    logger.warning(f"Rate limited on page {page}. Sleeping 30s...")
                    await asyncio.sleep(30)
                    retries -= 1
                    continue
                    
                resp.raise_for_status()
                data = resp.json()
                
                nodes = data.get("data", {}).get("globalRanking", {}).get("rankingNodes", [])
                
                extracted = []
                for node in nodes:
                    username = node.get("user", {}).get("username")
                    rating = node.get("currentRating")
                    if username and rating is not None:
                        extracted.append({"username": username, "rating": float(rating)})
                        
                return extracted

            except Exception as e:
                logger.error(f"Error on page {page}: {e}")
                retries -= 1
                await asyncio.sleep(5)
                
        return []

async def build_database(pages_to_scrape: int = 500, output_file: str = "lc_users_dump.json"):
    """
    Scrape `pages_to_scrape` of the global ranking (500 pages = Top 20,000 users).
    """
    logger.info(f"Starting Ground Truth generation: Scrape Top {pages_to_scrape * USERS_PER_PAGE} users...")
    
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    all_users = []
    
    async with cffi_requests.AsyncSession(impersonate=IMPERSONATE) as session:
        tasks = [fetch_page(session, semaphore, p) for p in range(1, pages_to_scrape + 1)]
        
        # Batch to report progress
        BATCH_SIZE = 50
        for i in range(0, len(tasks), BATCH_SIZE):
            batch = tasks[i:i + BATCH_SIZE]
            results = await asyncio.gather(*batch)
            for res in results:
                all_users.extend(res)
            logger.info(f"Progress: {(i + len(batch))} / {pages_to_scrape} pages parsed...")

    # Deduplicate just in case
    unique_users = {u["username"]: u for u in all_users}.values()
    final_list = list(unique_users)

    # Save to disk
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(final_list, f, indent=2)
        
    logger.info(f"SUCCESS: Ground truth database created with {len(final_list)} real LeetCode users at {output_file}")

if __name__ == "__main__":
    # For testing, grab top 20,000 users (first 500 pages)
    asyncio.run(build_database(pages_to_scrape=500))
