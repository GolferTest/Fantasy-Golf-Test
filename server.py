"""
Golf Odds API Server
Endpoints:
  GET  /api/scores           → all player scores + movements
  GET  /api/picks            → top 5 qualifying picks
  GET  /api/history/<player> → full snapshot history for one player
  GET  /api/events           → all known events
  POST /api/pull             → manually trigger an odds snapshot off-schedule
  GET  /                     → serves dashboard.html
"""

import json
import sqlite3
import threading
from flask import Flask, jsonify, send_from_directory, request
from scorer import calculate_scores, top5_picks, get_history, get_all_events
from tracker import DB_PATH, run_snapshot, init_db

# Ensure DB tables exist before any request hits
init_db()

_pull_lock = threading.Lock()
_pull_running = False

app = Flask(__name__, static_folder=".")


@app.route("/")
def index():
    return send_from_directory(".", "dashboard.html")


@app.route("/api/events")
def api_events():
    return jsonify(get_all_events())


@app.route("/api/scores")
def api_scores():
    sport_key = request.args.get("event")  # optional ?event=golf_pga_xxx
    scores = calculate_scores(sport_key)
    return jsonify([
        {
            "player":         p.player,
            "qualifies":      p.qualifies,
            "score":          round(p.score, 3),
            "winner": {
                "open_implied":    round(p.winner_open, 2),
                "current_implied": round(p.winner_current, 2),
                "move":            round(p.winner_move, 2),
            },
            "top5": {
                "open_implied":    round(p.top5_open, 2),
                "current_implied": round(p.top5_current, 2),
                "move":            round(p.top5_move, 2),
            },
            "top10": {
                "open_implied":    round(p.top10_open, 2),
                "current_implied": round(p.top10_current, 2),
                "move":            round(p.top10_move, 2),
            },
        }
        for p in scores
    ])


@app.route("/api/picks")
def api_picks():
    sport_key = request.args.get("event")
    scores = calculate_scores(sport_key)
    picks  = top5_picks(scores)
    return jsonify([p.player for p in picks])


@app.route("/api/history/<path:player>")
def api_history(player):
    sport_key = request.args.get("event")
    if not sport_key:
        # default to most recent event
        import sqlite3
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute("SELECT sport_key FROM events ORDER BY last_seen DESC LIMIT 1")
        row = cur.fetchone()
        con.close()
        sport_key = row[0] if row else ""
    result = {}
    for market in ("winner", "top5", "top10"):
        result[market] = get_history(sport_key, player, market)
    return jsonify(result)


@app.route("/api/players")
def api_players():
    sport_key = request.args.get("event")
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    if sport_key:
        cur.execute("SELECT DISTINCT player FROM opening_lines WHERE sport_key=? ORDER BY player", (sport_key,))
    else:
        cur.execute("SELECT DISTINCT player FROM opening_lines ORDER BY player")
    players = [row[0] for row in cur.fetchall()]
    con.close()
    return jsonify(players)


@app.route("/api/pull", methods=["POST"])
def api_pull():
    """Manually trigger an odds snapshot off-schedule."""
    global _pull_running
    with _pull_lock:
        if _pull_running:
            return jsonify({"status": "already_running", "message": "A pull is already in progress, check back in a moment."})
        _pull_running = True

    def do_pull():
        global _pull_running
        try:
            run_snapshot()
        finally:
            _pull_running = False

    t = threading.Thread(target=do_pull, daemon=True)
    t.start()
    return jsonify({"status": "started", "message": "Pulling odds now — refresh in about 30 seconds."})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
