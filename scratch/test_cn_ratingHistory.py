import asyncio
from curl_cffi import requests as cffi_requests
import json

async def main():
    async with cffi_requests.AsyncSession(impersonate='chrome120') as s:
        q = '''query {
  userContestRanking(userSlug: "qing-kong-rt") {
    currentRatingRanking
    ratingHistory
  }
}'''
        resp = await s.post('https://leetcode.cn/graphql', json={'query': q}, headers={'Content-Type': 'application/json'})
        print("Status:", resp.status_code)
        
        data = resp.json()
        print("Keys:", data.keys())
        if 'data' in data and data['data'].get('userContestRanking'):
            r = data['data']['userContestRanking']
            print("currentRatingRanking:", r.get('currentRatingRanking'))
            rh = r.get('ratingHistory', '[]')
            try:
                parsed_rh = json.loads(rh)
                print(f"ratingHistory is list of length {len(parsed_rh)}")
                if parsed_rh:
                    print("Last entry in ratingHistory:", parsed_rh[-1])
            except Exception as e:
                print("Could not parse ratingHistory:", str(e)[:100])
        else:
            print(resp.text)
            
asyncio.run(main())
