from flask import Flask, jsonify
import requests
import time

app = Flask(__name__)

POLYMARKET_URL = "https://gamma-api.polymarket.com/markets"

@app.route("/polymarket-feed")
def polymarket_feed():
    start = time.time()
    try:
        r = requests.get(POLYMARKET_URL, timeout=30)
        duration = round(time.time() - start, 2)
        app.logger.info(f"Polymarket fetch took {duration}s status={r.status_code}")
        r.raise_for_status()
        data = r.json()

        # If the API returns a list directly
        if isinstance(data, list):
            return jsonify(data), 200

        # If the API returns a dict with "markets"
        if isinstance(data, dict) and "markets" in data:
            return jsonify(data["markets"]), 200

        # Unexpected format
        app.logger.warning(f"Unexpected Polymarket API format: {type(data)} keys={list(data) if isinstance(data, dict) else None}")
        return jsonify([]), 200

    except Exception as e:
        duration = round(time.time() - start, 2)
        app.logger.error(f"Polymarket fetch failed after {duration}s: {e}", exc_info=True)
        return jsonify([]), 200
