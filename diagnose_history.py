"""
Compare userContestRanking vs userContestRankingHistory resolution rates
for the same set of slugs.
"""
import asyncio
from curl_cffi import requests as cffi_requests
import json

SLUGS = ["sdemycet", "LeviAckerman7", "VeritasVelata", "LSvr9egntY", "homon17651",
         "akayghadwal", "jRKriFLLy1", "ritik221140", "Sharad9919", "im_batman69",
         "Narendr_Modi", "adocxwork", "m8Cz0JF9Cx", "titana", "marycon",
         "krushil2908", "harsh_np", "vreyui67", "chandilier", "ayushkumar980",
         "scale5558", "AM8KhkxNb2", "anshika4-dev", "ovLIi3GSMt", "fudqAQ5uCD"]

def build_history_query(slugs):
    aliases = []
    for i, s in enumerate(slugs):
        safe = s.replace("\\", "\\\\").replace('"', '\\"')
        aliases.append(f'  h{i}: userContestRankingHistory(username: "{safe}") {{ rating attended }}')
    return "query {\n" + "\n".join(aliases) + "\n}"

async def main():
    async with cffi_requests.AsyncSession(impersonate="chrome120") as s:
        q = build_history_query(SLUGS)
        r = await s.post(
            "https://leetcode.com/graphql",
            json={"query": q},
            headers={"Content-Type": "application/json"},
        )
        data = r.json().get("data", {})
        
        resolved = 0
        for i, slug in enumerate(SLUGS):
            history = data.get(f"h{i}")
            if history:
                # Get the latest attended rating
                attended = [e for e in history if e.get("attended")]
                if attended:
                    latest_rating = attended[-1]["rating"]
                    print(f"  {slug:<30} -> {latest_rating:.2f}")
                    resolved += 1
                else:
                    print(f"  {slug:<30} -> no contests attended")
            else:
                print(f"  {slug:<30} -> NULL")
        
        print(f"\nResolved via userContestRankingHistory: {resolved}/{len(SLUGS)}")

asyncio.run(main())
