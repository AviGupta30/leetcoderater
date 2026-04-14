import asyncio
from cloud_etl import fetch_exact_baselines

# Test with the REAL profile slug extracted from avatar_url
REAL_USERNAME = "Avigupta30"

async def main():
    result = await fetch_exact_baselines([REAL_USERNAME])
    print("=== JIT Result with correct username ===")
    rating = result.get(REAL_USERNAME)
    if rating:
        print(f"{REAL_USERNAME}: rating = {rating:.2f}")
    else:
        print(f"{REAL_USERNAME}: NOT FOUND")

asyncio.run(main())
