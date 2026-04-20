"""
rating_engine.py — Module B: The Histogram Interpolation Rating Engine
=======================================================================
Implements a high-performance Elo-MMR calculation using NumPy broadcasting.
Designed to process 35,000+ participants in under 1 second.

Algorithm:
  Phase 1 – Baseline Resolution (Cascade: Saturday Cache > Wednesday DB > 1500.0)
  Phase 2 – Histogram Interpolation (O(K × |X|) instead of O(N²))
  Phase 3 – Post-Processing: Volatility Weighting + Zero-Sum Drift Correction + Dampener
"""

import time
import logging
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("RatingEngine")


class RatingEngine:
    """
    High-performance LeetCode rating predictor using O(K × |X|) histogram interpolation.

    Key design decisions:
    - All Phase 2 math is pure NumPy — no Python loops.
    - The "Master Curve" (X → E) is built once and reused for all participants.
    - Interpolation (np.interp) handles both forward and reverse lookups in O(N).
    """

    # ─── Constants ───────────────────────────────────────────────────────────────
    # Rating search space for the master expected-rank curve
    RATING_SPACE_MIN = -500.0
    RATING_SPACE_MAX = 4500.0
    RATING_SPACE_STEP = 1.0

    # Elo scale constant (LeetCode uses 400)
    ELO_SCALE = 400.0

    # Default rating for brand-new contestants
    NEW_USER_BASELINE = 1500.0

    # Volatility weight — moves users 50% toward their performance rating
    VOLATILITY_WEIGHT = 0.5

    # Bin size for rating histogram — reduces K from ~35k to ~3k unique buckets
    # Rounding to nearest 1.0 gives exact integer bins while preserving precision
    HISTOGRAM_BIN_SIZE = 1.0

    # Chunk size for memory-safe matrix multiply (|X_chunk| × K per pass)
    CHUNK_SIZE = 500
    
    # Dampening factor applied strictly to negative rating changes
    NEGATIVE_DAMPENING_FACTOR = 0.6

    # ─── Public Interface ────────────────────────────────────────────────────────

    def calculate(
        self,
        participants_list: list[dict],
        saturday_cache: dict,
        wednesday_json: dict,
    ) -> list[dict]:
        """
        Full pipeline: Phase 1 → Phase 2 → Phase 3.

        Args:
            participants_list: Raw scraper output. Each dict must contain:
                               'username', 'global_rank', 'score', 'finish_time'
            saturday_cache:    {username: predicted_rating} from Saturday's run.
            wednesday_json:    {username: official_rating} from LeetCode's Wednesday sync.

        Returns:
            List of enriched dicts with prediction results, sorted by global_rank.
        """
        if not participants_list:
            logger.warning("Empty participants list. Returning empty results.")
            return []

        n = len(participants_list)
        logger.info(f"RatingEngine starting for {n} participants.")

        # ── Phase 1: Baseline Resolution ─────────────────────────────────────────
        t0 = time.perf_counter()
        usernames, actual_ranks, r_initial = self._resolve_baselines(
            participants_list, saturday_cache, wednesday_json
        )
        logger.info(f"Phase 1 (Baseline Resolution) completed in {time.perf_counter() - t0:.3f}s")

        # ── Assemble Output ───────────────────────────────────────────────────────
        # Build a quick lookup for passthrough fields (score, finish_time) and k
        meta = {p.get("username", ""): p for p in participants_list}
        
        # Extract k (attendedContestsCount) for Veteran Tax.
        # Fallback to 0 if not yet fully implemented upstream.
        k_array = np.array([meta.get(uname, {}).get("k", 0) for uname in usernames], dtype=np.float64)

        # ── Phase 2: Histogram Interpolation & STEP 1───────────────────────────────
        t1 = time.perf_counter()
        delta_raw = self._compute_deltas(r_initial, actual_ranks, k_array)
        logger.info(f"Phase 2 (Histogram Interpolation) completed in {time.perf_counter() - t1:.4f}s")

        # ── Phase 3: Post-Processing (STEPS 2 & 3) ────────────────────────────────
        t2 = time.perf_counter()
        delta_final = self._apply_drift_correction(delta_raw)
        predicted_ratings = r_initial + delta_final
        logger.info(f"Phase 3 (Drift Correction & Dampening) completed in {time.perf_counter() - t2:.4f}s")

        results = []
        for i in range(n):
            uname = usernames[i]
            raw   = meta.get(uname, {})
            results.append({
                "username":        uname,
                "global_rank":     int(actual_ranks[i]),
                "score":           raw.get("score", 0),
                "finish_time":     raw.get("finish_time", 0),
                "previous_rating": round(float(r_initial[i]), 2),
                "predicted_delta": round(float(delta_final[i]), 2),
                "predicted_rating": round(float(predicted_ratings[i]), 2),
            })

        # Sort by actual contest rank
        results.sort(key=lambda x: x["global_rank"])
        logger.info(f"RatingEngine total time: {time.perf_counter() - t0:.4f}s for {n} users.")
        return results

    # ─── Phase 1 ─────────────────────────────────────────────────────────────────

    def _resolve_baselines(
        self,
        participants_list: list[dict],
        saturday_cache: dict,
        wednesday_json: dict,
    ) -> tuple[list[str], np.ndarray, np.ndarray]:
        """
        Resolve R_initial for every participant via the cascade priority:
          1. Saturday Cache  (most recent predicted outcome — avoids Biweekly Trap)
          2. Wednesday DB    (official LeetCode sync)
          3. New User        (default 1500.0)

        Returns:
            usernames   – list[str] preserving order
            actual_ranks – np.ndarray of contest ranks (float64 for math later)
            r_initial   – np.ndarray of resolved baseline ratings
        """
        usernames = []
        actual_ranks = []
        r_initial_list = []

        sat_hits = wed_hits = new_hits = 0

        for p in participants_list:
            uname = p.get("username", "")
            rank  = p.get("active_rank", p.get("global_rank", 0)) or 0
            usernames.append(uname)
            actual_ranks.append(rank)

            if uname in saturday_cache:
                # Priority 1: Saturday cache
                r_initial_list.append(float(saturday_cache[uname]))
                sat_hits += 1
            elif uname in wednesday_json:
                # Priority 2: Official Wednesday baseline
                r_initial_list.append(float(wednesday_json[uname]))
                wed_hits += 1
            else:
                # Priority 3: Brand-new user
                r_initial_list.append(self.NEW_USER_BASELINE)
                new_hits += 1

        logger.info(
            f"Baseline resolution: {sat_hits} from Saturday cache, "
            f"{wed_hits} from Wednesday DB, {new_hits} new users."
        )

        return (
            usernames,
            np.array(actual_ranks, dtype=np.float64),
            np.array(r_initial_list, dtype=np.float64),
        )

    # ─── Phase 2 ─────────────────────────────────────────────────────────────────

    def _compute_deltas(
        self,
        r_initial: np.ndarray,
        actual_ranks: np.ndarray,
        k_array: np.ndarray,
    ) -> np.ndarray:
        """
        Core O(K × |X|) histogram interpolation — fully vectorised.

        Key optimisation: ratings are binned to the nearest integer before
        histogramming.  This collapses K from up to N (35 000) down to the
        number of distinct integer ratings (~3 000), making the broadcast
        matrix ~10× smaller and keeping Phase 2 well under 1 second.
        """
        # ── 2.1: Contest Histogram (binned) ──────────────────────────────────────
        # Bin each R_initial to the nearest integer to reduce unique-K dramatically.
        r_binned = np.round(r_initial / self.HISTOGRAM_BIN_SIZE) * self.HISTOGRAM_BIN_SIZE
        unique_ratings, counts = np.unique(r_binned, return_counts=True)
        K = len(unique_ratings)
        logger.info(
            f"Phase 2.1 — Histogram: {K} unique rating buckets "
            f"(from {len(r_initial)} participants after binning)."
        )

        # ── 2.2: Master Expected-Rank Curve (chunked for memory safety) ──────────
        X = np.arange(
            self.RATING_SPACE_MIN, self.RATING_SPACE_MAX,
            self.RATING_SPACE_STEP, dtype=np.float64
        )  # shape: (|X|,)
        counts_f = counts.astype(np.float64)  # shape: (K,)

        # Build E(x) in chunks to avoid allocating a single |X|×K matrix.
        # Each chunk: (CHUNK_SIZE, K) — safe even for K = 35 000.
        E = np.empty(len(X), dtype=np.float64)
        for start in range(0, len(X), self.CHUNK_SIZE):
            end   = min(start + self.CHUNK_SIZE, len(X))
            x_chunk = X[start:end, np.newaxis]             # (chunk, 1)
            # P(u_i beats x) for every (x, u_i) pair
            exponent   = (unique_ratings[np.newaxis, :] - x_chunk) / self.ELO_SCALE
            p_beaten   = 1.0 / (1.0 + np.power(10.0, -exponent))  # (chunk, K)
            E[start:end] = p_beaten @ counts_f + 0.5       # (chunk,)

        logger.info(
            f"Phase 2.2 — Master curve built: "
            f"{len(X)} X-values × {K} buckets (chunked × {self.CHUNK_SIZE})."
        )

        # ── 2.3: Forward Interpolation — Seed from R_initial ─────────────────────
        # E is monotonically decreasing (higher rating → lower expected rank).
        # Flip so np.interp gets a monotonically increasing xp array.
        X_flip = X[::-1].copy()
        E_flip = E[::-1].copy()
        seed = np.interp(r_initial, X_flip, E_flip)        # shape: (N,)

        # ── 2.4: Geometric Mean ───────────────────────────────────────────────────
        safe_ranks = np.maximum(actual_ranks, 1.0)
        m = np.sqrt(seed * safe_ranks)                     # shape: (N,)

        # ── 2.5: Reverse Interpolation — Performance Rating from m ───────────────
        # Interpolate against (E_flip, X_flip): given expected rank m → rating P.
        performance_rating = np.interp(m, E_flip, X_flip)  # shape: (N,)

        # STEP 1: Calculate Weighted Deltas (Individual)
        # weight = 0.5 - 0.2778 * (1 - math.pow(0.8, k))
        weight = 0.5 - 0.2778 * (1.0 - np.power(0.8, k_array))
        weighted_delta = (performance_rating - r_initial) * weight

        calibration_factor = 0.75 
        weighted_delta = weighted_delta * calibration_factor
        
        return weighted_delta


    # ─── Phase 3 ─────────────────────────────────────────────────────────────────

    def _apply_drift_correction(self, weighted_delta: np.ndarray) -> np.ndarray:
        """
        Zero-sum ecosystem correction + Negative Dampening.

        The sum of all raw Δ drifts away from zero because the participant histogram
        is asymmetric. We subtract the mean drift so the system is rating-conserving.
        Then, we apply a safety net (dampening factor) to strictly negative deltas.
        """
        # STEP 2: Calculate Global Drift (Center of Mass)
        drift = np.mean(weighted_delta)
        logger.info(f"Phase 3 — Drift correction: mean drift = {drift:.4f} rating points.")
        
        # STEP 3: Apply Zero-Sum Correction
        final_predicted_delta = weighted_delta - drift

        # STEP 4: Apply Dampening Factor ONLY to negative drops
        negative_mask = final_predicted_delta < 0
        neg_count = np.sum(negative_mask)
        logger.info(f"Phase 3 — Applying {self.NEGATIVE_DAMPENING_FACTOR}x Dampener to {neg_count} negative deltas.")
        
        # Multiply all negative deltas by 0.6
        final_predicted_delta[negative_mask] *= self.NEGATIVE_DAMPENING_FACTOR

        return final_predicted_delta


# ─── Quick Smoke-Test ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    np.random.seed(42)
    N = 35_000

    # Simulate a realistic 35k-participant contest payload
    fake_participants = [
        {
            "username":    f"user_{i}",
            "global_rank": i + 1,
            "score":       200 - (i // 500),
            "finish_time": 1_717_295_836 + i * 3,
        }
        for i in range(N)
    ]

    # Simulate partial caches (covering ~40% of users from each source)
    saturday_cache = {
        f"user_{i}": float(np.random.normal(1800, 300))
        for i in range(0, N, 5)          # every 5th user
    }
    wednesday_json = {
        f"user_{i}": float(np.random.normal(1600, 350))
        for i in range(2, N, 4)          # every other group
    }

    engine = RatingEngine()
    results = engine.calculate(fake_participants, saturday_cache, wednesday_json)

    logger.info(f"Output sample (top 5 by rank):")
    for r in results[:5]:
        sign = "+" if r["predicted_delta"] >= 0 else ""
        logger.info(
            f"  #{r['global_rank']:>5}  {r['username']:<12}  "
            f"Prev={r['previous_rating']:.1f}  "
            f"Δ={sign}{r['predicted_delta']:.1f}  "
            f"New={r['predicted_rating']:.1f}"
        )

    total_delta = sum(r["predicted_delta"] for r in results)
    logger.info(f"Zero-sum check post-dampening: Σ(Δ_final) = {total_delta:.4f}")