import json
import logging
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Baseline")

def load_historical_baselines(filepath_or_url: str) -> dict[str, float]:
    """
    Loads ground truth historical baselines into a highly optimized O(1) dictionary.
    Expects JSON structure: [{"username": "tourist", "rating": 3902.5}, ...]
    
    Args:
        filepath_or_url: Path to a local JSON file or a URL to a JSON payload.
        
    Returns:
        dict: A flattened dictionary for O(1) lookups -> {"username": rating_float}
    """
    logger.info(f"Loading historical baselines from: {filepath_or_url}")
    baselines = {}
    
    try:
        # Step 1: Detect if URL or Local File
        if filepath_or_url.startswith("http://") or filepath_or_url.startswith("https://"):
            response = requests.get(filepath_or_url, timeout=15)
            response.raise_for_status()
            data = response.json()
        else:
            with open(filepath_or_url, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
        # Step 2: Parse and heavily optimize into a flat dictionary
        # We explicitly cast to float to prevent downstream NumPy type errors
        for entry in data:
            username = entry.get("username")
            rating = entry.get("rating")
            if username and rating is not None:
                baselines[username] = float(rating)
                
        logger.info(f"Successfully loaded {len(baselines)} historical baselines tightly into memory.")
        return baselines
        
    except requests.exceptions.HTTPError as he:
        logger.error(f"HTTP Error fetching baselines from URL: {he}")
    except FileNotFoundError:
        logger.error(f"Local baseline file not found: {filepath_or_url}")
    except json.JSONDecodeError as je:
        logger.error(f"Failed to parse JSON file structure: {je}")
    except Exception as e:
        logger.error(f"Unexpected error loading baselines: {e}")
        
    # Return empty dict on absolute failure to prevent pipeline crash
    return {}

def get_baseline_rating(username: str, saturday_cache: dict[str, float], official_wednesday_db: dict[str, float]) -> float:
    """
    The exact Cascade Logic implementation.
    Resolves the true baseline rating for a participant.
    """
    # Priority 1: Did they compete yesterday?
    if username in saturday_cache:
        return saturday_cache[username]
        
    # Priority 2: Are they an established official user?
    if username in official_wednesday_db:
        return official_wednesday_db[username]
        
    # Priority 3: Genuine new user fallback
    return 1500.0
