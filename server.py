"""
Golf Odds API Server
Endpoints:
  GET  /api/events              → all known events
  GET  /api/scores              → all player scores + movements
  GET  /api/picks               → top 10 qualifying picks
  GET  /api/history/<player>    → snapshot history for charting
  GET  /api/results             → saved tournament results for an event
  POST /api/results             → save/update a player result
  POST /api/pull                → manually trigger an odds snapshot
  GET  /                        → dashboard
"""

import sqlite3
import threading
from flask import Flask, jsonify, send_from_directory, request
from scorer import (calculate_scores, top10_picks, top5_longshots, get_weekly_selections,
                    get_history, get_all_events, get_results, save_result, init_results_table)
from tracker import DB_PATH, run_snapshot, init_db, run_results_fetch

# Ensure all DB tables exist before any request
init_db()
init_results_table()

_pull_lock    = threading.Lock()
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
    sport_key = request.args.get("event")
    scores = calculate_scores(sport_key)
    return jsonify([
        {
            "player":        p.player,
            "event_title":   p.event_title,
            "qualifies":     p.qualifies,
            "is_longshot":   p.is_longshot,
            "all_markets":   p.all_markets,
            "score":         round(p.score, 3),
            "baseline":      round(p.baseline, 2),
            "current_total": round(p.current_total, 2),
            "delta":         round(p.delta, 2),
            "winner_open_american":  p.winner_open_american,
            "top5_open_american":    p.top5_open_american,
            "top10_open_american":   p.top10_open_american,
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


@app.route("/api/longshots")
def api_longshots():
    sport_key = request.args.get("event")
    scores    = calculate_scores(sport_key)
    longshots = top5_longshots(scores)
    return jsonify([p.player for p in longshots])


@app.route("/api/picks")
def api_picks():
    sport_key = request.args.get("event")
    scores = calculate_scores(sport_key)
    picks  = top10_picks(scores)
    return jsonify([p.player for p in picks])


@app.route("/api/history/<path:player>")
def api_history(player):
    sport_key = request.args.get("event")
    if not sport_key:
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


@app.route("/api/results")
def api_get_results():
    sport_key = request.args.get("event")
    if not sport_key:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute("SELECT sport_key FROM events ORDER BY last_seen DESC LIMIT 1")
        row = cur.fetchone()
        con.close()
        sport_key = row[0] if row else ""
    return jsonify(get_results(sport_key))


@app.route("/api/results", methods=["POST"])
def api_save_result():
    body      = request.get_json()
    sport_key = body.get("sport_key") or body.get("event")
    player    = body.get("player")
    finish    = body.get("finish")
    payout    = body.get("payout")
    notes     = body.get("notes")
    if not sport_key or not player or not finish:
        return jsonify({"error": "sport_key, player, and finish are required"}), 400
    save_result(sport_key, player, finish, payout, notes)
    return jsonify({"status": "saved"})


@app.route("/api/fetch-results", methods=["POST"])
def api_fetch_results():
    """Manually trigger a results fetch for a completed tournament."""
    body      = request.get_json() or {}
    sport_key = body.get("sport_key") or body.get("event")
    if not sport_key:
        # default to most recent event
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute("SELECT sport_key FROM events ORDER BY last_seen DESC LIMIT 1")
        row = cur.fetchone()
        con.close()
        sport_key = row[0] if row else None
    if not sport_key:
        return jsonify({"error": "No event found"}), 400

    def do_fetch():
        run_results_fetch(sport_key)

    threading.Thread(target=do_fetch, daemon=True).start()
    return jsonify({"status": "started", "message": f"Fetching results for {sport_key}..."})


@app.route("/api/pull", methods=["POST"])
def api_pull():
    global _pull_running
    with _pull_lock:
        if _pull_running:
            return jsonify({"status": "already_running", "message": "A pull is already in progress."})
        _pull_running = True

    def do_pull():
        global _pull_running
        try:
            run_snapshot()
        finally:
            _pull_running = False

    threading.Thread(target=do_pull, daemon=True).start()
    return jsonify({"status": "started", "message": "Pulling odds now — refresh in about 30 seconds."})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
