import asyncio
from curl_cffi import requests as cffi_requests
async def main():
    async with cffi_requests.AsyncSession(impersonate='chrome120') as s:
        # Introspection query for `userContestRanking` arguments on Query
        q = '''
        query {
          __schema {
            queryType {
              fields {
                name
                args {
                  name
                  type { name }
                }
              }
            }
          }
        }
        '''
        resp = await s.post('https://leetcode.cn/graphql', json={'query': q}, headers={'Content-Type': 'application/json'})
        data = resp.json()
        fields = data['data']['__schema']['queryType']['fields']
        for f in fields:
            if 'Contest' in f['name'] or 'user' in f['name']:
                print(f['name'], [arg['name'] for arg in f['args']])
asyncio.run(main())
