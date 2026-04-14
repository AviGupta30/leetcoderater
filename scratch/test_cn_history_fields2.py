import asyncio
from curl_cffi import requests as cffi_requests
import json

async def main():
    async with cffi_requests.AsyncSession(impersonate='chrome120') as s:
        q = '''query {
  userContestRanking(userSlug: "qing-kong-rt") {
    contestHistory
  }
}'''
        resp = await s.post('https://leetcode.cn/graphql', json={'query': q}, headers={'Content-Type': 'application/json'})
        
        data = resp.json()
        if 'data' in data and data['data'].get('userContestRanking'):
            r = data['data']['userContestRanking']
            ch = r.get('contestHistory', '[]')
            try:
                parsed_ch = json.loads(ch)
                with open('scratch/ch_output.json', 'w', encoding='utf-8') as f:
                    json.dump(parsed_ch[-1], f, indent=2, ensure_ascii=False)
            except Exception as e:
                print("Could not parse contestHistory:", str(e))
                pass
            
asyncio.run(main())
