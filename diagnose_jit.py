import asyncio
from curl_cffi import requests as cffi_requests
import json

async def main():
    async with cffi_requests.AsyncSession(impersonate="chrome120") as s:
        # Test 1: userContestRanking (what we're using)
        q1 = """{ userContestRanking(username: "Avigupta30") { rating attendedContestsCount globalRanking topPercentage } }"""
        r1 = await s.post("https://leetcode.com/graphql", json={"query": q1}, headers={"Content-Type": "application/json"})
        print("=== userContestRanking ===")
        print(json.dumps(r1.json(), indent=2))

        # Test 2: userPublicProfile (check what the public profile returns)
        q2 = """{ matchedUser(username: "Avigupta30") { username profile { ranking realName } } }"""
        r2 = await s.post("https://leetcode.com/graphql", json={"query": q2}, headers={"Content-Type": "application/json"})
        print("\n=== matchedUser (profile) ===")
        print(json.dumps(r2.json(), indent=2))

asyncio.run(main())
