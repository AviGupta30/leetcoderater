import os
import asyncio
import logging
import requests
from typing import Optional

import uvicorn
import pandas as pd
import numpy as np
import joblib
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client
from dotenv import load_dotenv

from rating_engine import RatingEngine
from baseline import load_historical_baselines, get_baseline_rating

# ─── Load Local Environment Variables ────────────────────────────────────────
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Server")

# ─── Supabase Setup ──────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("✅ Connected to Supabase Cloud!")
except Exception as e:
    logger.error(f"❌ Supabase Connection Failed: {e}")
    supabase = None

# ─── Global State ─────────────────────────────────────────────────────────────
GROUND_TRUTH_DB_PATH = "lc_users_dump.json"
_global_wednesday_db = load_historical_baselines(GROUND_TRUTH_DB_PATH)

try:
    logger.info("Loading XGBoost Production Model...")
    xgb_model = joblib.load('leetcode_xgboost_production.pkl')
    logger.info("✅ XGBoost Brain loaded successfully!")
except Exception as e:
    logger.warning(f"⚠️ Could not load XGBoost model: {e}")
    xgb_model = None

app = FastAPI(title="LeetCode Predictor API (Supabase + Live ML)")

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

# ─── In-Memory Cache ──────────────────────────────────────────────────────────
_prediction_cache: dict[str, list[dict]] = {}

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


# ─── Routes ──────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "db_connected": supabase is not None, "ml_ready": xgb_model is not None}


@app.get("/contests/latest", response_model=list[ContestInfo])
async def contests_latest(n: int = Query(10, ge=1, le=30)):
    """Fetches the latest contests directly from LeetCode's lightweight GraphQL API to populate the frontend."""
    url = "https://leetcode.com/graphql"
    payload = {
        "query": """
        query pastContests($pageNo: Int, $numPerPage: Int) {
            pastContests(pageNo: $pageNo, numPerPage: $numPerPage) {
                data { title titleSlug startTime duration }
            }
        }
        """,
        "variables": {"pageNo": 1, "numPerPage": n}
    }
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    
    try:
        loop = asyncio.get_event_loop()
        resp = await loop.run_in_executor(
            None, lambda: requests.post(url, json=payload, headers=headers, timeout=10)
        )
        
        if resp.status_code != 200:
            raise Exception(f"LeetCode returned {resp.status_code}")
            
        data = resp.json()
        contests = data.get("data", {}).get("pastContests", {}).get("data", [])
        
        return [
            ContestInfo(
                title=c.get("title", ""),
                slug=c.get("titleSlug", ""),
                startTime=c.get("startTime", 0),
                duration=c.get("duration", 0)
            ) for c in contests
        ]
    except Exception as e:
        logger.error(f"Error fetching contests: {e}")
        raise HTTPException(status_code=503, detail="Could not fetch contest list from LeetCode.")


@app.get("/predict/{contest_slug}")
async def predict_contest(contest_slug: str, refresh: bool = Query(False)):
    """
    1. Checks cache.
    2. Pulls raw data from Supabase.
    3. Runs RatingEngine + XGBoost.
    4. Caches final predictions.
    """
    if not refresh and contest_slug in _prediction_cache:
        logger.info(f"Cache hit for {contest_slug}")
        return PredictionResponse(
            contest_slug=contest_slug,
            total_participants=len(_prediction_cache[contest_slug]),
            predictions=_prediction_cache[contest_slug]
        )

    if not supabase:
        raise HTTPException(status_code=500, detail="Database not configured.")

    try:
        # ── Phase 1: Fetch Raw Data from Supabase ────────
        logger.info(f"Fetching raw data for {contest_slug} from Supabase...")
        response = supabase.table("contest_predictions").select("participant_data").eq("contest_slug", contest_slug).execute()
        
        if not response.data:
            raise HTTPException(status_code=404, detail=f"Contest '{contest_slug}' data not available in Supabase yet.")
            
        raw_participants = response.data[0].get("participant_data", [])
        logger.info(f"Retrieved {len(raw_participants)} users from Supabase.")

        # Extract JIT baseline data
        jit_ratings_only = {p["username"]: p.get("previous_rating", 1500.0) for p in raw_participants}
        combined_db = {**_global_wednesday_db, **jit_ratings_only}

        # ── Phase 2: Run Elo-MMR Math Engine ─────────────────────────────────
        engine = RatingEngine()
        predictions = engine.calculate(raw_participants, {}, combined_db)

        # ── Phase 3: The ML Ensemble (Pandas Vectorized) ─────────────────────
        if xgb_model is not None:
            logger.info("[Pipeline] Starting ML Ensemble integration...")
            df = pd.DataFrame(predictions)

            k_map = {p['username']: p.get('k', 0) for p in raw_participants}
            df['k_value'] = df['username'].map(k_map).fillna(0)

            valid_times = df[df['finish_time'] > 0]['finish_time']
            min_finish = valid_times.min() if not valid_times.empty else 0
            df['solve_time_seconds'] = (df['finish_time'] - min_finish).clip(lower=0)

            features = ['previous_rating', 'k_value', 'score', 'solve_time_seconds', 'global_rank']
            X = df[features].rename(columns={'previous_rating': 'old_rating', 'global_rank': 'actual_rank'})
            df['ml_delta'] = xgb_model.predict(X)

            conditions = [
                df['k_value'] < 5,
                df['k_value'] > 50,
                df['k_value'] > 15
            ]
            math_weights = [0.8, 0.2, 0.3]
            ml_weights   = [0.2, 0.8, 0.7]

            df['w_math'] = np.select(conditions, math_weights, default=0.5)
            df['w_ml']   = np.select(conditions, ml_weights, default=0.5)

            df['ensemble_delta'] = (df['predicted_delta'] * df['w_math']) + (df['ml_delta'] * df['w_ml'])
            df['predicted_delta'] = df['ensemble_delta'].round(2)
            df['predicted_rating'] = (df['previous_rating'] + df['predicted_delta']).round(2)

            columns_to_drop = ['ml_delta', 'w_math', 'w_ml', 'ensemble_delta', 'solve_time_seconds', 'k_value']
            predictions = df.drop(columns=columns_to_drop, errors='ignore').to_dict(orient='records')
            logger.info("[Pipeline] Predictions Blended successfully.")
        else:
            logger.warning("[Pipeline] No XGBoost model found. Proceeding with pure Math predictions.")

        # ── Cache & Complete ────────────────────────────────────────────────
        _prediction_cache[contest_slug] = predictions

        return PredictionResponse(
            contest_slug=contest_slug,
            total_participants=len(predictions),
            predictions=predictions
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Pipeline error for {contest_slug}: {e}")
        raise HTTPException(status_code=500, detail="Error processing data.")


@app.get("/predict/{contest_slug}/user/{username}")
async def predict_user(contest_slug: str, username: str):
    if contest_slug not in _prediction_cache:
        raise HTTPException(status_code=404, detail="Contest math not calculated yet. Call /predict/{contest_slug} first.")
    
    match = next((p for p in _prediction_cache[contest_slug] if p["username"].lower() == username.lower()), None)
    if not match:
        raise HTTPException(status_code=404, detail=f"User '{username}' not found.")
    return match


if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=False)