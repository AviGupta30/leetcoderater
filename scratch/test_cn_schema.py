import asyncio
from curl_cffi import requests as cffi_requests
async def main():
    async with cffi_requests.AsyncSession(impersonate='chrome120') as s:
        # Try finding the correct leetcode.cn schema
        q = '''query {
  userContestRanking(userSlug: "qing-kong-rt") {
    attendedContestsCount
    rating
    globalRanking
  }
}'''
        resp = await s.post('https://leetcode.cn/graphql', json={'query': q}, headers={'Content-Type': 'application/json'})
        print("userSlug test:", resp.status_code)
        print(resp.text)
asyncio.run(main())
