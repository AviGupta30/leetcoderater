"""
diagnose_raw.py - Fetch the raw contest leaderboard entry for rank 1036
to see EVERY field returned by the API, not just the ones we're extracting.
"""
import asyncio
from curl_cffi import requests as cffi_requests
import json

CONTEST = "weekly-contest-497"
TARGET_RANK = 1036  # The rank shown in the screenshot

async def main():
    # Rank 1036 is on page ceil(1036/25) = 42
    page = (TARGET_RANK + 24) // 25
    url = f"https://leetcode.com/contest/api/ranking/{CONTEST}/?pagination={page}&region=global"

    print(f"Fetching page {page} of {CONTEST}...")
    async with cffi_requests.AsyncSession(impersonate="chrome120") as s:
        r = await s.get(url)
        data = r.json()
        entries = data.get("total_rank", [])
        print(f"Got {len(entries)} entries on this page\n")

        # Find the entry at exactly rank 1036
        for e in entries:
            if e.get("rank") == TARGET_RANK:
                print("=== FOUND TARGET ENTRY (rank 1036) ===")
                print(json.dumps(e, indent=2))
                print()

        # Also print first 3 entries to see ALL available fields
        print("=== FIRST 3 ENTRIES (to see all fields) ===")
        for e in entries[:3]:
            print(json.dumps(e, indent=2))
            print()

asyncio.run(main())
