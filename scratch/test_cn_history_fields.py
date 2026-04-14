import asyncio
from curl_cffi import requests as cffi_requests
import json

async def main():
    async with cffi_requests.AsyncSession(impersonate='chrome120') as s:
        q = '''query {
  userContestRanking(userSlug: "qing-kong-rt") {
    contestHistory
    contestRankingHistoryV2
  }
}'''
        resp = await s.post('https://leetcode.cn/graphql', json={'query': q}, headers={'Content-Type': 'application/json'})
        
        data = resp.json()
        if 'data' in data and data['data'].get('userContestRanking'):
            r = data['data']['userContestRanking']
            ch = r.get('contestHistory', '[]')
            crhv2 = r.get('contestRankingHistoryV2', '[]')
            try:
                parsed_ch = json.loads(ch)
                if parsed_ch:
                    print("Last entry in contestHistory:", parsed_ch[-1])
            except Exception as e:
                print("Could not parse contestHistory:", str(e))
                pass
            try:
                parsed_crhv2 = json.loads(crhv2)
                if parsed_crhv2:
                    print("Last entry in contestRankingHistoryV2:", parsed_crhv2[-1])
            except Exception as e:
                pass
        else:
            print(resp.text)
            
asyncio.run(main())
