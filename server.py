"""
server.py — FastAPI REST Server (v2 — Dynamic Contest Support + ML Ensemble)
=============================================================================
Endpoints:
  GET  /health
  GET  /contests/latest         — Returns 10 most recent LeetCode contests
  GET  /predict/{contest_slug}   — Full prediction (auto-triggers scrape if uncached)
  GET  /predict/{contest_slug}/user/{username}
"""

import asyncio
import logging
import time
from typing import Optional

import uvicorn
import pandas as pd
import numpy as np
import joblib
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

# 2. Load the XGBoost Production Model ONCE into memory
try:
    logger.info("Loading XGBoost Production Model...")
    xgb_model = joblib.load('leetcode_xgboost_production.pkl')
    logger.info("XGBoost Brain loaded successfully!")
except Exception as e:
    logger.warning(f"Could not load XGBoost model: {e}. Ensure 'leetcode_xgboost_production.pkl' is in the folder.")
    xgb_model = None

# ─── App Setup ────────────────────────────────────────────────────────────────
app = FastAPI(
    title="LeetCode Rating Predictor API",
    description="High-performance Elo-MMR prediction engine with dynamic contest selection + ML Ensemble.",
    version="2.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ─── In-Memory State ──────────────────────────────────────────────────────────
_prediction_cache: dict[str, list[dict]] = {}
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
    return {"status": "ok", "version": "2.1.0", "ml_model_loaded": xgb_model is not None}


@app.get("/contests/latest", response_model=list[ContestInfo])
async def contests_latest(n: int = Query(10, ge=1, le=30)):
    contests = await fetch_latest_contests(n)
    if not contests:
        raise HTTPException(status_code=503, detail="Could not fetch contest list from LeetCode.")
    return contests


@app.get("/predict/{contest_slug}/progress", response_model=ScrapeProgress)
async def scrape_progress(contest_slug: str):
    prog = _scrape_progress.get(contest_slug, {
        "status": "idle", "pages_done": 0, "total": 0
    })
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
    cache_key = f"{contest_slug}:{max_pages}"

    if not refresh and cache_key in _prediction_cache:
        logger.info(f"Cache hit: {cache_key}")
        return PredictionResponse(
            contest_slug=contest_slug,
            total_participants=len(_prediction_cache[cache_key]),
            predictions=_prediction_cache[cache_key],
        )

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
            logger.info(f"[Pipeline] Starting JIT baseline fetch for {len(participants)} participants...")
            _scrape_progress[contest_slug]["status"] = "fetching_ratings"
            user_data_list = [{"username": p["username"], "region": p.get("data_region", "US")} for p in participants if p.get("username")]
            
            jit_results = await fetch_exact_baselines(user_data_list, contest_slug)

            # ── Phase 3: Enrich participants with their real baselines ────────
            saturday_cache = {} 
            jit_ratings_only = {}
            for p in participants:
                uname = p["username"]
                if uname in jit_results:
                    res = jit_results[uname]
                    p["k"] = res.get("k", 0)
                    jit_ratings_only[uname] = res["rating"]
                else:
                    p["k"] = 0 

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

            # ── Phase 5: The ML Ensemble (Pandas Vectorized) ────────────────
            if xgb_model is not None:
                logger.info("[Pipeline] Starting Phase 5: ML Ensemble integration...")
                df = pd.DataFrame(predictions)

                # 1. Extract k_value safely
                k_map = {p['username']: p.get('k', 0) for p in participants}
                df['k_value'] = df['username'].map(k_map).fillna(0)

                # 2. Calculate solve_time_seconds robustly (relative to the first finisher to avoid extra network calls)
                valid_times = df[df['finish_time'] > 0]['finish_time']
                min_finish = valid_times.min() if not valid_times.empty else 0
                df['solve_time_seconds'] = (df['finish_time'] - min_finish).clip(lower=0)

                # 3. XGBoost Prediction
                features = ['previous_rating', 'k_value', 'score', 'solve_time_seconds', 'global_rank']
                X = df[features].rename(columns={'previous_rating': 'old_rating', 'global_rank': 'actual_rank'})
                df['ml_delta'] = xgb_model.predict(X)

                # 4. The Dynamic Confidence Curve
                conditions = [
                    df['k_value'] < 5,   # New Users: 80% Math
                    df['k_value'] > 50,  # Extreme Veterans: 80% ML
                    df['k_value'] > 15   # Standard Veterans: 70% ML
                ]
                math_weights = [0.8, 0.2, 0.3]
                ml_weights   = [0.2, 0.8, 0.7]

                df['w_math'] = np.select(conditions, math_weights, default=0.5)
                df['w_ml']   = np.select(conditions, ml_weights, default=0.5)

                # 5. Blend and package
                df['ensemble_delta'] = (df['predicted_delta'] * df['w_math']) + (df['ml_delta'] * df['w_ml'])
                df['predicted_delta'] = df['ensemble_delta'].round(2)
                df['predicted_rating'] = (df['previous_rating'] + df['predicted_delta']).round(2)

                # 6. Cleanup temporary columns
                columns_to_drop = ['ml_delta', 'w_math', 'w_ml', 'ensemble_delta', 'solve_time_seconds', 'k_value']
                predictions = df.drop(columns=columns_to_drop, errors='ignore').to_dict(orient='records')
                logger.info("[Pipeline] Phase 5 Complete: Predictions Blended successfully.")
            else:
                logger.warning("[Pipeline] No XGBoost model found. Proceeding with pure Math predictions.")

            # ── Cache & Complete ────────────────────────────────────────────
            _prediction_cache[cache_key] = predictions
            _scrape_progress[contest_slug]["status"] = "done"

        except Exception as e:
            _scrape_progress[contest_slug]["status"] = "error"
            logger.error(f"Pipeline error for {contest_slug}: {e}")

    _scrape_progress[contest_slug] = {"status": "scraping", "pages_done": 0, "total": 0}
    asyncio.create_task(run_pipeline())
    
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