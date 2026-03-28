"""
Golf Odds API Server + Background Scheduler
One process handles web serving and scheduled odds pulls.
Results fetching (Slash Golf) only runs Mon/Tue/Wed on schedule - never on startup.
"""

import os
import time
import sqlite3
import threading
import schedule
import pytz
from datetime import datetime
from flask import Flask, jsonify, send_from_directory, request

from scorer import (calculate_scores, top10_picks, top5_longshots,
                    get_history, get_all_events, get_results, save_result,
                    init_results_table)
from tracker import DB_PATH, run_snapshot, init_db

# ── Init DB ───────────────────────────────────────────────────────────────────
init_db()
init_results_table()

app = Flask(__name__, static_folder=".")

# ── Scheduler state ───────────────────────────────────────────────────────────
_scheduler_started = False
_scheduler_lock    = threading.Lock()
_pull_lock         = threading.Lock()
_pull_running      = False

LOCAL_TZ = pytz.timezone("America/Los_Angeles")


# ── Scheduled jobs ─────────────────────────────────────────────────────────────
def snapshot_job():
    """Pull odds from The Odds API. Runs every 2 hours."""
    print(f"\n[{datetime.now()}] Running scheduled snapshot...")
    try:
        run_snapshot()
    except Exception as e:
        print(f"Snapshot error: {e}")


def wednesday_picks_job():
    """Send Wednesday 2 PM picks email."""
    now_local = datetime.now(LOCAL_TZ)
    if now_local.weekday() != 2:
        return
    print(f"\n[{now_local}] Wednesday picks email...")
    try:
        from scorer import calculate_scores, top10_picks, top5_longshots
        from scheduler import build_email, send_email
        scores    = calculate_scores()
        picks     = top10_picks(scores)
        longshots = top5_longshots(scores)
        event_title = scores[0].event_title if scores else "This Week's Tournament"
        subject, plain, html = build_email(picks, longshots, event_title)
        send_email(subject, plain, html)
    except Exception as e:
        print(f"Wednesday email error: {e}")


def staggered_results_job(batch: int):
    """
    Fetch tournament results from Slash Golf API on Mon/Tue/Wed schedule.
    Only imports Slash Golf code here - never at module load time.
    """
    print(f"\n[{datetime.now()}] Results batch {batch}...")
    try:
        from tracker import run_staggered_results_fetch
        from datetime import timedelta, timezone
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        cur.execute("""
            SELECT sport_key FROM events
            WHERE last_seen < ? ORDER BY last_seen DESC LIMIT 4
        """, (cutoff,))
        completed = [r[0] for r in cur.fetchall()]
        con.close()
        for sport_key in completed:
            run_staggered_results_fetch(sport_key, batch)
    except Exception as e:
        print(f"Results batch {batch} error: {e}")


def _run_scheduler():
    """Background thread: runs initial snapshot then schedules recurring jobs."""
    # Short delay so Flask is fully up before first network call
    time.sleep(3)

    print("\n[Scheduler] Starting — running first snapshot...")
    snapshot_job()

    # Odds pulls every 2 hours
    schedule.every(2).hours.do(snapshot_job)

    # Wednesday 2 PM Pacific (21:00 UTC in PDT/summer)
    schedule.every().wednesday.at("21:00").do(wednesday_picks_job)

    # Results batches: Mon 6PM / Tue 6PM / Wed 10AM Pacific
    # PDT (UTC-7): 6PM = 01:00 UTC next day, 10AM = 17:00 UTC
    schedule.every().tuesday.at("01:00").do(lambda: staggered_results_job(1))
    schedule.every().wednesday.at("01:00").do(lambda: staggered_results_job(2))
    schedule.every().wednesday.at("17:00").do(lambda: staggered_results_job(3))

    print("[Scheduler] Running. Odds every 2hr. Results Mon/Tue/Wed on schedule.")
    while True:
        schedule.run_pending()
        time.sleep(30)


@app.before_request
def start_scheduler_once():
    global _scheduler_started
    with _scheduler_lock:
        if not _scheduler_started:
            _scheduler_started = True
            threading.Thread(target=_run_scheduler, daemon=True).start()


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
