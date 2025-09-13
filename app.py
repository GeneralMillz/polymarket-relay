from flask import Flask, jsonify
import requests

app = Flask(__name__)

@app.route("/polymarket-feed")
def polymarket_feed():
    try:
        url = "https://gamma.polymarket.com/api/markets"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return jsonify(data.get("markets", []))
    except Exception as e:
        return jsonify({"error": str(e)}), 500