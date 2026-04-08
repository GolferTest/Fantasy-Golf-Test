"""
Golf Odds Server + Scheduler
- DataGolf API only for odds (no Odds API)
- Phase 1: hunt every 5 min until all 3 markets appear
- Phase 2: pull every 50 min once all markets captured
- Phase 3: pull at midnight + 6AM during tournament (Thu-Sun)
- Results via Slash Golf on Mon/Tue/Wed post-tournament
"""

import os
import time
import sqlite3
import threading
import schedule
import pytz
from datetime import datetime, timezone
from flask import Flask, jsonify, send_from_directory, request

from scorer import (calculate_scores, top10_picks, top5_longshots,
                    get_history, get_all_events, get_results, save_result,
                    init_results_table, get_event_tracker, get_yearly_tracker)
from tracker import DB_PATH, init_db

app = Flask(__name__, static_folder=".")

_scheduler_started = False
_scheduler_lock    = threading.Lock()
_pull_lock         = threading.Lock()
_pull_running      = False

LOCAL_TZ   = pytz.timezone("America/Los_Angeles")
ALL_MARKETS = {"winner", "top5", "top10"}


# ── Core pull function ────────────────────────────────────────────────────────

def do_datagolf_pull():
    """
    Pull odds from DataGolf for this week's event.
    Called by scheduler and Pull Now button.
    """
    from tracker import fetch_datagolf_current_event, get_datagolf_sport_key, run_datagolf_snapshot
    event = fetch_datagolf_current_event()
    if not event:
        print("[Pull] No current event within 8 days — nothing to pull")
        return
    event_name = event.get("event_name") or event.get("name") or "Unknown"
    start_date = event.get("start_date") or event.get("date") or ""
    sport_key  = get_datagolf_sport_key(event_name, start_date)
    run_datagolf_snapshot(sport_key, event_name, start_date)


# ── Scheduler helpers ─────────────────────────────────────────────────────────

def get_current_sport_key() -> str | None:
    """Return the sport_key for the current week's event from DB."""
    from tracker import fetch_datagolf_current_event, get_datagolf_sport_key
    event = fetch_datagolf_current_event()
    if not event:
        return None
    event_name = event.get("event_name") or event.get("name") or ""
    start_date = event.get("start_date") or event.get("date") or ""
    return get_datagolf_sport_key(event_name, start_date)


def get_captured_markets() -> set:
    """
    Return which markets have an opening line for this week's event.
    Each market hunts independently until its opening line is captured.
    """
    sport_key = get_current_sport_key()
    if not sport_key:
        return set()
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute(
            "SELECT DISTINCT market FROM opening_lines WHERE sport_key=?",
            (sport_key,)
        )
        markets = {r[0] for r in cur.fetchall()}
        con.close()
        return markets
    except Exception:
        return set()


# ── Scheduler ─────────────────────────────────────────────────────────────────

def wednesday_picks_job():
    now_local = datetime.now(LOCAL_TZ)
    if now_local.weekday() != 2:
        return
    print(f"\n[Picks email] {now_local}")
    try:
        from scorer import calculate_scores, top10_picks, top5_longshots
        from scheduler import build_email, send_email
        scores    = calculate_scores()
        picks     = top10_picks(scores)
        longshots = top5_longshots(scores)
        title     = scores[0].event_title if scores else "This Week"
        subject, plain, html = build_email(picks, longshots, title)
        send_email(subject, plain, html)
    except Exception as e:
        print(f"  Email error: {e}")


def staggered_results_job(batch: int):
    print(f"\n[Results batch {batch}]")
    try:
        from tracker import run_staggered_results_fetch
        from datetime import timedelta
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        now_utc = datetime.now(timezone.utc)
        # Find datagolf_ events whose tournament start_date was 1-10 days ago
        # (tournament happened last week — results should be available)
        low  = (now_utc - timedelta(days=10)).strftime("%Y-%m-%d")
        high = (now_utc - timedelta(days=1)).strftime("%Y-%m-%d")
        cur.execute("""
            SELECT sport_key FROM events
            WHERE sport_key LIKE 'datagolf_%'
            AND start_date BETWEEN ? AND ?
            ORDER BY start_date DESC LIMIT 2
        """, (low, high))
        completed = [r[0] for r in cur.fetchall()]
        con.close()
        print(f"  Events to fetch results for: {completed}")
        for sk in completed:
            run_staggered_results_fetch(sk, batch)
    except Exception as e:
        print(f"  Results batch {batch} error: {e}")


def _run_scheduler():
    time.sleep(3)

    # Run first pull immediately on startup
    print("\n[Scheduler] Starting — initial pull...")
    try:
        do_datagolf_pull()
    except Exception as e:
        print(f"  Initial pull error: {e}")

    _last_pull  = [datetime.min.replace(tzinfo=timezone.utc)]
    _prev_captured = [set()]

    def smart_snapshot():
        now_local = datetime.now(LOCAL_TZ)
        weekday   = now_local.weekday()   # 0=Mon ... 6=Sun
        hour      = now_local.hour

        # Phase 3: tournament in progress Thu 6AM -> Sun noon — pull at 0 and 6 only
        in_tourney = (
            (weekday == 3 and hour >= 6) or
            weekday == 4 or
            weekday == 5 or
            (weekday == 6 and hour < 12)
        )
        if in_tourney:
            if hour in (0, 6):
                mins = (datetime.now(timezone.utc) - _last_pull[0]).total_seconds() / 60
                if mins >= 60:
                    print(f"[Phase 3] Tournament pull at {now_local.strftime('%a %-I%p')}")
                    do_datagolf_pull()
                    _last_pull[0] = datetime.now(timezone.utc)
            return

        # Hunting window: Sun 4PM through Wed midnight
        in_window = (
            (weekday == 6 and hour >= 16) or
            weekday in (0, 1, 2)
        )
        if not in_window:
            return

        captured      = get_captured_markets()
        still_hunting = ALL_MARKETS - captured

        # Log newly captured markets
        for m in (captured - _prev_captured[0]):
            remaining = ALL_MARKETS - captured
            print(f"[Phase 1->2] '{m}' captured! Still hunting: {remaining or 'none — all done!'}")
        _prev_captured[0] = captured

        if still_hunting:
            # Phase 1: hunt every 5 min tick
            print(f"[Phase 1 Hunting] Missing: {still_hunting}")
            do_datagolf_pull()
            _last_pull[0] = datetime.now(timezone.utc)
        else:
            # Phase 2: all captured — pull every 50 min
            mins = (datetime.now(timezone.utc) - _last_pull[0]).total_seconds() / 60
            if mins >= 50:
                print(f"[Phase 2 Tracking] 50-min pull at {now_local.strftime('%a %-I%p')}")
                do_datagolf_pull()
                _last_pull[0] = datetime.now(timezone.utc)

    schedule.every(5).minutes.do(smart_snapshot)

    # Wednesday 2PM Pacific picks email (21:00 UTC PDT)
    schedule.every().wednesday.at("21:00").do(wednesday_picks_job)

    # Results batches post-tournament
    schedule.every().tuesday.at("01:00").do(lambda: staggered_results_job(1))
    schedule.every().wednesday.at("01:00").do(lambda: staggered_results_job(2))
    schedule.every().wednesday.at("17:00").do(lambda: staggered_results_job(3))

    print("[Scheduler] Running. Phase 1: 5min hunt. Phase 2: 50min track. Phase 3: 18hr tourney.")
    while True:
        schedule.run_pending()
        time.sleep(30)


@app.before_request
def start_scheduler_once():
    global _scheduler_started
    with _scheduler_lock:
        if not _scheduler_started:
            _scheduler_started = True
            init_db()
            init_results_table()
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
            "player":               p.player,
            "event_title":          p.event_title,
            "qualifies":            p.qualifies,
            "is_longshot":          p.is_longshot,
            "all_markets":          p.all_markets,
            "score":                round(p.score, 3),
            "baseline":             round(p.baseline, 2),
            "current_total":        round(p.current_total, 2),
            "delta":                round(p.delta, 2),
            "winner_open_american":     p.winner_open_american,
            "top5_open_american":       p.top5_open_american,
            "top10_open_american":      p.top10_open_american,
            "winner_current_american":  p.winner_current_american,
            "top5_current_american":    p.top5_current_american,
            "top10_current_american":   p.top10_current_american,
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
        return jsonify({"error": "sport_key, player, finish required"}), 400
    save_result(sport_key, player, finish, payout, notes)
    return jsonify({"status": "saved"})


@app.route("/api/pull", methods=["POST"])
def api_pull():
    global _pull_running
    with _pull_lock:
        if _pull_running:
            return jsonify({"status": "already_running"})
        _pull_running = True

    def do_pull():
        global _pull_running
        try:
            do_datagolf_pull()
        finally:
            _pull_running = False

    threading.Thread(target=do_pull, daemon=True).start()
    return jsonify({"status": "started", "message": "Pulling from DataGolf — refresh in 30s."})


@app.route("/api/fetch-results", methods=["POST"])
def api_fetch_results():
    """Manually trigger results fetch for a specific event."""
    body     = request.get_json() or {}
    sport_key = body.get("sport_key")
    batch     = body.get("batch", 1)
    if not sport_key:
        return jsonify({"error": "sport_key required"}), 400
    def run():
        from tracker import run_staggered_results_fetch
        run_staggered_results_fetch(sport_key, batch)
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "sport_key": sport_key})


@app.route("/api/tracker")
def api_tracker():
    sport_key = request.args.get("event")
    if sport_key:
        return jsonify(get_event_tracker(sport_key))
    return jsonify(get_yearly_tracker())


@app.route("/api/backtest", methods=["POST"])
def api_backtest():
    """Trigger backtest data load. Runs in background thread."""
    def run():
        try:
            from backtest import run_backtest
            run_backtest()
        except Exception as e:
            print(f"Backtest error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "message": "Backtest running — check logs. Takes ~3 min."})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
