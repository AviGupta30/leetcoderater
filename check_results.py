import urllib.request, json

r = urllib.request.urlopen("http://localhost:8000/predict/weekly-contest-497?max_pages=1")
data = json.loads(r.read())
preds = data["predictions"]

print(f"Total: {len(preds)} users\n")
print(f"{'Rank':<6} {'Username':<30} {'Prev Rating':<14} {'Delta'}")
print("-" * 70)

real_count = 0
for p in sorted(preds, key=lambda x: x["global_rank"]):
    prev = p["previous_rating"]
    delta = p["predicted_delta"]
    is_real = prev != 1500.0
    if is_real:
        real_count += 1
    flag = "" if is_real else "  [1500 fallback]"
    print(f"{p['global_rank']:<6} {p['username']:<30} {prev:<14.0f} {delta:+.1f}{flag}")

print(f"\n{real_count}/{len(preds)} users have REAL baseline ratings (not 1500 fallback)")
