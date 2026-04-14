import asyncio
from curl_cffi import requests as cffi_requests
async def main():
    async with cffi_requests.AsyncSession(impersonate='chrome120') as s:
        # Introspection query for `UserContestRankingNode`
        q = '''
        query {
          __type(name: "UserContestRankingNode") {
            fields {
              name
              type {
                name
                kind
                ofType { name kind }
              }
            }
          }
        }
        '''
        resp = await s.post('https://leetcode.cn/graphql', json={'query': q}, headers={'Content-Type': 'application/json'})
        print(resp.status_code)
        print(resp.text)
asyncio.run(main())
