import urllib.request, json, time

for i in range(8):
    time.sleep(5)
    r = urllib.request.urlopen("http://localhost:8000/predict/weekly-contest-400/progress")
    d = json.loads(r.read())
    status = d["status"]
    done = d["pages_done"]
    total = d["total_pages"]
    pct = d["pct"]
    print(f"t={5*(i+1)}s | status={status} | {done}/{total} pages | {pct}%")
    if status in ("done", "error"):
        break
