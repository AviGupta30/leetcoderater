"""
diagnose_pipeline.py — Tests the full scrape → JIT pipeline on 1 page
to see exactly what usernames we're getting and how many resolve.
"""
import asyncio
from curl_cffi import requests as cffi_requests
from cloud_etl import fetch_exact_baselines, _extract_profile_username, RANKING_URL, IMPERSONATE
import json

CONTEST = "weekly-contest-497"

async def main():
    async with cffi_requests.AsyncSession(impersonate=IMPERSONATE) as s:
        # Fetch page 1 of the contest
        url = RANKING_URL.format(slug=CONTEST, page=1)
        r = await s.get(url)
        data = r.json()
        entries = data.get("total_rank", [])

        print(f"Got {len(entries)} entries on page 1\n")
        print("=== USERNAME COMPARISON ===")
        print(f"{'RANK':<6} {'username (display)':<30} {'user_slug':<30} {'avatar_extract':<25}")
        print("-" * 95)

        usernames_by_slug     = []
        usernames_by_display  = []
        usernames_by_extract  = []

        for e in entries[:25]:
            display   = e.get("username", "")
            slug      = e.get("user_slug", "")
            extracted = _extract_profile_username(e)
            rank      = e.get("rank", "?")

            usernames_by_slug.append(slug or display)
            usernames_by_display.append(display)
            usernames_by_extract.append(extracted)

            print(f"{rank:<6} {display:<30} {slug:<30} {extracted:<25}")

        # Now test JIT with all three sets
        print("\n=== JIT RESOLUTION RATES ===")

        res_slug    = await fetch_exact_baselines(usernames_by_slug)
        res_display = await fetch_exact_baselines(usernames_by_display)

        print(f"Using user_slug:  {len(res_slug)}/{len(usernames_by_slug)} resolved")
        print(f"Using display:    {len(res_display)}/{len(usernames_by_display)} resolved")

        if res_slug:
            print("\nSample slug results:")
            for k, v in list(res_slug.items())[:5]:
                print(f"  {k}: {v:.2f}")

asyncio.run(main())
