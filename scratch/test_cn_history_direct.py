import asyncio
from curl_cffi import requests as cffi_requests

async def main():
    async with cffi_requests.AsyncSession(impersonate='chrome120') as s:
        q = '''query {
  userContestRankingHistory(userSlug: "qing-kong-rt") {
    rating
    contest {
      titleSlug
    }
  }
}'''
        resp = await s.post('https://leetcode.cn/graphql', json={'query': q}, headers={'Content-Type': 'application/json'})
        print("userContestRankingHistory on CN test:", resp.status_code)
        print(resp.text[:500])
asyncio.run(main())
