"""
Golf Odds API Server + Background Scheduler
Runs everything in one process so Railway hobby tier works correctly.
The scheduler thread starts AFTER Flask is serving, not before.
"""

import os
import time
import sqlite3
import threading
import schedule
import pytz
from datetime import datetime
from flask import Flask, jsonify, send_from_directory, request

from scorer import (calculate_scores, top10_picks, top5_longshots, get_weekly_selections,
                    get_history, get_all_events, get_results, save_result,
                    init_results_table)
from tracker import DB_PATH, run_snapshot, init_db, run_results_fetch, run_staggered_results_fetch

# ── Init DB on startup ────────────────────────────────────────────────────────
init_db()
init_results_table()

# ── Flask app ─────────────────────────────────────────────────────────────────
app = Flask(__name__, static_folder=".")

# ── Background scheduler ──────────────────────────────────────────────────────
_scheduler_started = False
_scheduler_lock    = threading.Lock()
_pull_lock         = threading.Lock()
_pull_running      = False

LOCAL_TZ = pytz.timezone("America/Los_Angeles")


def snapshot_job():
    print(f"\n[{datetime.now()}] Scheduled 2-hour snapshot...")
    try:
        run_snapshot()
    except Exception as e:
        print(f"Snapshot error: {e}")


def wednesday_picks_job():
    from scorer import calculate_scores, top10_picks, top5_longshots
    from scheduler import build_email, send_email
    now_local = datetime.now(LOCAL_TZ)
    if now_local.weekday() != 2:
        return
    print(f"\n[{now_local}] Wednesday picks email...")
    scores    = calculate_scores()
    picks     = top10_picks(scores)
    longshots = top5_longshots(scores)
    event_title = scores[0].event_title if scores else "This Week's Tournament"
    subject, plain, html = build_email(picks, longshots, event_title)
    send_email(subject, plain, html)


def staggered_results_job(batch: int):
    from tracker import run_staggered_results_fetch, fetch_active_pga_events
    print(f"\n[{datetime.now()}] Results batch {batch}...")
    try:
        completed = _get_completed_event_keys()
        for sport_key in completed:
            run_staggered_results_fetch(sport_key, batch)
    except Exception as e:
        print(f"Results batch {batch} error: {e}")


def _get_completed_event_keys():
    from datetime import timedelta, timezone
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        cur.execute("SELECT sport_key FROM events WHERE last_seen < ? ORDER BY last_seen DESC LIMIT 4", (cutoff,))
        keys = [r[0] for r in cur.fetchall()]
        con.close()
        return keys
    except Exception:
        return []


def _run_scheduler():
    global _scheduler_started

    # Wait a few seconds so Flask is fully up before first snapshot
    time.sleep(5)

    print("\n[Scheduler] Starting — running first snapshot now...")
    snapshot_job()

    # Schedule recurring jobs
    schedule.every(2).hours.do(snapshot_job)
    schedule.every().wednesday.at("21:00").do(wednesday_picks_job)
    schedule.every().tuesday.at("01:00").do(lambda: staggered_results_job(1))
    schedule.every().wednesday.at("01:00").do(lambda: staggered_results_job(2))
    schedule.every().wednesday.at("17:00").do(lambda: staggered_results_job(3))

    print("[Scheduler] Running — pulling every 2 hours.")
    while True:
        schedule.run_pending()
        time.sleep(30)


def _ensure_scheduler():
    global _scheduler_started
    with _scheduler_lock:
        if not _scheduler_started:
            _scheduler_started = True
            t = threading.Thread(target=_run_scheduler, daemon=True)
            t.start()


@app.before_request
def start_scheduler_once():
    _ensure_scheduler()


# ── Routes ────────────────────────────────────────────────────────────────────
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


@app.route("/api/picks")
def api_picks():
    sport_key = request.args.get("event")
    scores = calculate_scores(sport_key)
    return jsonify([p.player for p in top10_picks(scores)])


@app.route("/api/longshots")
def api_longshots():
    sport_key = request.args.get("event")
    scores = calculate_scores(sport_key)
    return jsonify([p.player for p in top5_longshots(scores)])


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
    return jsonify({
        market: get_history(sport_key, player, market)
        for market in ("winner", "top5", "top10")
    })


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
    body      = request.get_json() or {}
    sport_key = body.get("sport_key") or body.get("event")
    if not sport_key:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute("SELECT sport_key FROM events ORDER BY last_seen DESC LIMIT 1")
        row = cur.fetchone()
        con.close()
        sport_key = row[0] if row else None
    if not sport_key:
        return jsonify({"error": "No event found"}), 400
    threading.Thread(target=lambda: run_results_fetch(sport_key), daemon=True).start()
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
