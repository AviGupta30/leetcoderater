"""
server.py — FastAPI REST Server (v2 — Dynamic Contest Support)
===============================================================
Endpoints:
  GET  /health
  GET  /contests/latest          — Returns 10 most recent LeetCode contests
  GET  /predict/{contest_slug}   — Full prediction (auto-triggers scrape if uncached)
  GET  /predict/{contest_slug}/user/{username}
"""

import asyncio
import logging
import time
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from cloud_etl import scrape_contest_leaderboard, fetch_latest_contests, fetch_exact_baselines
from rating_engine import RatingEngine
from baseline import load_historical_baselines, get_baseline_rating

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Server")

# ─── Global State ─────────────────────────────────────────────────────────────
# 1. Load the 100k+ Global JSON strictly ONCE into memory at server startup
GROUND_TRUTH_DB_PATH = "lc_users_dump.json"
_global_wednesday_db = load_historical_baselines(GROUND_TRUTH_DB_PATH)

# ─── App Setup ────────────────────────────────────────────────────────────────
app = FastAPI(
    title="LeetCode Rating Predictor API",
    description="High-performance Elo-MMR prediction engine with dynamic contest selection.",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ─── In-Memory State ──────────────────────────────────────────────────────────
# prediction results cache
_prediction_cache: dict[str, list[dict]] = {}

# scraping progress tracker  {contest_slug: {"pages_done": int, "total": int, "status": str}}
_scrape_progress: dict[str, dict] = {}


# ─── Models ──────────────────────────────────────────────────────────────────
class ContestInfo(BaseModel):
    title: str
    slug: str
    startTime: int
    duration: int

class PredictionEntry(BaseModel):
    username: str
    global_rank: int
    score: int | None = None
    finish_time: int | None = None
    previous_rating: float
    predicted_delta: float
    predicted_rating: float

class PredictionResponse(BaseModel):
    contest_slug: str
    total_participants: int
    predictions: list[PredictionEntry]

class ScrapeProgress(BaseModel):
    status: str           # "idle" | "scraping" | "done" | "error"
    pages_done: int
    total_pages: int
    pct: float


# ─── Routes ──────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "version": "2.0.0"}


@app.get("/contests/latest", response_model=list[ContestInfo])
async def contests_latest(n: int = Query(10, ge=1, le=30)):
    """
    Returns the N most recent LeetCode contests fetched live via GraphQL.
    Falls back to hardcoded list if LeetCode is unreachable.
    """
    contests = await fetch_latest_contests(n)
    if not contests:
        raise HTTPException(status_code=503, detail="Could not fetch contest list from LeetCode.")
    return contests


@app.get("/predict/{contest_slug}/progress", response_model=ScrapeProgress)
async def scrape_progress(contest_slug: str):
    """
    Poll this endpoint to track real-time scraping progress.
    Returns percentage completion so the frontend can show a progress bar.
    """
    prog = _scrape_progress.get(contest_slug, {
        "status": "idle", "pages_done": 0, "total": 0
    })
    # Note: progress_cb stores key as 'total' (not 'total_pages')
    total = prog.get("total", 0) or 1
    done  = prog.get("pages_done", 0)
    return ScrapeProgress(
        status=prog.get("status", "idle"),
        pages_done=done,
        total_pages=total,
        pct=round((done / total) * 100, 1),
    )


@app.get("/predict/{contest_slug}")
async def predict_contest(
    contest_slug: str,
    max_pages: Optional[int] = Query(None, description="Limit pages (None = full scrape)"),
    refresh: bool = Query(False, description="Force re-scrape"),
):
    """
    Returns predictions for a contest, auto-triggering a scrape if not cached.
    Poll /predict/{contest_slug}/progress during the initial scrape.
    """
    cache_key = f"{contest_slug}:{max_pages}"

    # Cache hit — return immediately
    if not refresh and cache_key in _prediction_cache:
        logger.info(f"Cache hit: {cache_key}")
        return PredictionResponse(
            contest_slug=contest_slug,
            total_participants=len(_prediction_cache[cache_key]),
            predictions=_prediction_cache[cache_key],
        )

    # Already scraping — tell client to keep polling
    if _scrape_progress.get(contest_slug, {}).get("status") == "scraping":
        raise HTTPException(
            status_code=202,
            detail=f"Scrape already in progress for '{contest_slug}'. Poll /predict/{contest_slug}/progress."
        )

    # --- Background Worker Function ---
    async def run_pipeline():
        async def progress_cb(done: int, total: int):
            _scrape_progress[contest_slug] = {"status": "scraping", "pages_done": done, "total": total}

        try:
            # ── Phase 1: Scrape contest leaderboard ─────────────────────────
            participants = await scrape_contest_leaderboard(
                contest_slug,
                max_pages=max_pages,
                progress_callback=progress_cb,
            )

            if not participants:
                _scrape_progress[contest_slug]["status"] = "error"
                logger.error(f"No participants found for {contest_slug}.")
                return

            # ── Phase 2: JIT baseline fetch (exact real ratings) ─────────────
            # 35k users / 50 per batch = 700 requests @ 15 concurrent = ~45s
            logger.info(f"[Pipeline] Starting JIT baseline fetch for {len(participants)} participants...")
            _scrape_progress[contest_slug]["status"] = "fetching_ratings"
            user_data_list = [{"username": p["username"], "region": p.get("data_region", "US")} for p in participants if p.get("username")]
            
            # jit_results: {username -> {"rating": float, "k": int}}
            jit_results = await fetch_exact_baselines(user_data_list, contest_slug)

            # ── Phase 3: Enrich participants with their real baselines ────────
            saturday_cache = {}  # Future: fetch from Supabase
            
            # Update participants with 'k' from JIT and extract ratings for combined_db
            jit_ratings_only = {}
            for p in participants:
                uname = p["username"]
                if uname in jit_results:
                    res = jit_results[uname]
                    p["k"] = res.get("k", 0)
                    jit_ratings_only[uname] = res["rating"]
                else:
                    p["k"] = 0  # Default for new/skipped users

            # Merge JIT ratings into a combined db:
            # JIT results take priority over the static dump
            combined_db = {**_global_wednesday_db, **jit_ratings_only}
            
            logger.info(
                f"[Pipeline] Combined baseline DB: {len(jit_ratings_only)} JIT + "
                f"{len(_global_wednesday_db)} static = {len(combined_db)} total entries."
            )

            for p in participants:
                p["previous_rating"] = get_baseline_rating(
                    username=p["username"],
                    saturday_cache=saturday_cache,
                    official_wednesday_db=combined_db,
                )

            # ── Phase 4: Run Elo-MMR math engine ────────────────────────────
            engine = RatingEngine()
            predictions = engine.calculate(participants, saturday_cache, combined_db)

            _prediction_cache[cache_key] = predictions
            _scrape_progress[contest_slug]["status"] = "done"

        except Exception as e:
            _scrape_progress[contest_slug]["status"] = "error"
            logger.error(f"Pipeline error for {contest_slug}: {e}")

    # --- Start fresh scrape in background ---
    _scrape_progress[contest_slug] = {"status": "scraping", "pages_done": 0, "total": 0}
    # IMPORTANT: BackgroundTasks.add_task() silently drops async coroutines.
    # asyncio.create_task() is required to properly schedule on uvicorn's event loop.
    asyncio.create_task(run_pipeline())
    
    # IMMEDIATELY return 202 to the client so the UI transitions to the Progress Bar state
    raise HTTPException(
        status_code=202,
        detail=f"Scrape started in background for '{contest_slug}'. Poll /predict/{contest_slug}/progress."
    )



@app.get("/predict/{contest_slug}/user/{username}", response_model=PredictionEntry)
async def predict_user(contest_slug: str, username: str, max_pages: Optional[int] = Query(None)):
    cache_key = f"{contest_slug}:{max_pages}"
    if cache_key not in _prediction_cache:
        raise HTTPException(status_code=404, detail="Contest not yet scraped. Call /predict/{contest_slug} first.")
    match = next((p for p in _prediction_cache[cache_key] if p["username"].lower() == username.lower()), None)
    if not match:
        raise HTTPException(status_code=404, detail=f"User '{username}' not found.")
    return match


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=False)
