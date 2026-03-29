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

    # ── Adaptive pull schedule ──────────────────────────────────────────────
    # Mon 1AM – Wed midnight : every 2 hours  (capture all line movement)
    # Thu 6AM  – Sun noon    : every 18 hours (tournament in progress, save requests)
    # Extra Mon 1AM pull captures lines that dropped Sunday night.
    #
    # Budget:
    #   Active window  (Mon-Wed): ~28 pulls × 4 events × 3 markets = ~336 requests
    #   Tourney window (Thu-Sun): ~5  pulls × 4 events × 3 markets = ~60  requests
    #   Total/week: ~396 requests — well within free 500/month limit

    # ── Pull schedule ───────────────────────────────────────────────────────
    #
    # GOAL: Catch opening lines the moment they drop, then track movement
    # through Wednesday, then go quiet during the tournament Thu-Sun.
    #
    # When do lines typically drop?
    #   Winner odds:    Sunday night or Monday morning of tournament week
    #   Top5/Top10:     Tuesday or Wednesday of tournament week
    #
    # Schedule:
    #   Sun midnight – Wed midnight : every 30 min  ← catches opening lines ASAP
    #   Wed midnight – Thu 6AM      : every 2 hours ← slow down after picks sent
    #   Thu 6AM      – Sun noon     : every 18 hours ← tournament in progress
    #
    # Weekly API budget estimate (4 events, 3 markets each):
    #   Sun-Wed 30min window  : ~144 pulls × 4 × 1-3 mkts  = ~288-864 requests
    #   Wed-Thu 2hr window    : ~6   pulls × 4 × 3 mkts     = ~72  requests
    #   Thu-Sun 18hr window   : ~5   pulls × 4 × 3 mkts     = ~60  requests
    # NOTE: Most Sun-Wed pulls will hit 422 on top5/top10 (0 cost) until they open.
    # Once all 3 markets open (~Tue) budget rises. Still within 500/month free tier.
    # If budget becomes a concern, upgrade to $10/month plan (10k requests).

    # ── Adaptive pull schedule ──────────────────────────────────────────────
    #
    # Phase 1 — HUNTING (every 5 min):
    #   Check every 5 minutes until ALL 3 markets have an opening line for the
    #   current event. 422 responses are free so this costs nothing until lines drop.
    #   Once all 3 markets are captured, automatically switch to Phase 2.
    #
    # Phase 2 — TRACKING (every 1 hour, Mon–Wed midnight):
    #   All opening lines captured. Pull hourly to track movement through Wednesday.
    #
    # Phase 3 — TOURNAMENT (every 18 hours, Thu 6AM – Sun noon):
    #   Tournament in progress. Minimal pulls to conserve request budget.
    #
    # The scheduler runs every 5 minutes and decides which phase it's in.

    # ── Helper: fetch and cache commence_time for all known events ───────
    def refresh_commence_times():
        """
        For every event in our DB that lacks a commence_time, fetch it from
        The Odds API events endpoint (free — no quota cost).
        Also discovers brand-new events (e.g. Valero appearing mid-week) by
        calling fetch_active_pga_events() and upserting any new ones.
        """
        from tracker import fetch_active_pga_events, upsert_event
        from datetime import timezone
        import requests as _req
        ts = datetime.now(timezone.utc).isoformat()

        # Step 1: discover any new events The Odds API just listed
        try:
            fresh_events = fetch_active_pga_events()
            con = sqlite3.connect(DB_PATH)
            for ev in fresh_events:
                upsert_event(con, ev["key"], ev["title"], ts)
            con.commit()
            con.close()
        except Exception as e:
            print(f"  [refresh] event discovery error: {e}")

        # Step 2: fill in missing commence_times
        try:
            con = sqlite3.connect(DB_PATH)
            cur = con.cursor()
            cur.execute("SELECT sport_key FROM events WHERE commence_time IS NULL")
            missing = [r[0] for r in cur.fetchall()]
            con.close()
            for sport_key in missing:
                try:
                    url = (f"https://api.the-odds-api.com/v4/sports/{sport_key}/events"
                           f"?apiKey={os.environ.get('ODDS_API_KEY','')}")
                    r = _req.get(url, timeout=8)
                    if r.status_code == 200:
                        data = r.json()
                        if data:
                            ct = data[0].get("commence_time")
                            con2 = sqlite3.connect(DB_PATH)
                            con2.execute("UPDATE events SET commence_time=? WHERE sport_key=?",
                                         (ct, sport_key))
                            con2.commit()
                            con2.close()
                            print(f"  [refresh] cached commence_time for {sport_key}: {ct}")
                except Exception:
                    pass
        except Exception as e:
            print(f"  [refresh] commence_time fetch error: {e}")

    # ── Helper: identify this week's tournaments ──────────────────────────
    def get_current_event_keys() -> list[str]:
        """
        Return ALL tournaments starting within the next 8 days OR in progress.
        Returns a list so we handle multiple simultaneous events correctly
        (e.g. two PGA Tour events in the same week, or a major + opposite-field event).

        Returns [] if nothing is within 8 days yet — caller keeps watching.
        Events >8 days away (future majors) are intentionally excluded so we
        don't hunt the Masters when this week's event is the Valero.
        """
        from datetime import timezone as tz
        now_utc = datetime.now(tz.utc)
        try:
            con = sqlite3.connect(DB_PATH)
            cur = con.cursor()
            cur.execute("SELECT sport_key, commence_time FROM events WHERE commence_time IS NOT NULL")
            rows = cur.fetchall()
            con.close()
        except Exception:
            return []

        active = []
        for sport_key, commence_time in rows:
            try:
                ct = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
                days_from_now = (ct - now_utc).total_seconds() / 86400
                # In-progress (started 0-4 days ago) OR upcoming within 8 days
                if -4 <= days_from_now <= 8:
                    active.append((sport_key, days_from_now))
            except Exception:
                pass

        if not active:
            return []  # nothing this week yet — keep watching

        # Sort by closest to now (in-progress first, then soonest upcoming)
        active.sort(key=lambda x: abs(x[1]))
        keys = [k for k, _ in active]
        if len(keys) > 1:
            print(f"  [Events] Multiple active events this week: {keys}")
        return keys

    def focused_snapshot_job():
        """Pull all active this-week tournaments (usually 1, occasionally 2)."""
        sport_keys = get_current_event_keys()
        if not sport_keys:
            print("[Scheduler] No active events this week — skipping pull")
            return
        from tracker import fetch_odds_for_event, save_snapshot, upsert_event, MARKETS
        from datetime import timezone
        ts  = datetime.now(timezone.utc).isoformat()
        for sport_key in sport_keys:
            con = sqlite3.connect(DB_PATH)
            cur = con.cursor()
            cur.execute("SELECT title FROM events WHERE sport_key=?", (sport_key,))
            row   = cur.fetchone()
            title = row[0] if row else sport_key
            print(f"\n[Focused pull] {title} ({sport_key})")
            upsert_event(con, sport_key, title, ts)
            for label, market_key in MARKETS.items():
                try:
                    odds = fetch_odds_for_event(sport_key, market_key)
                    if odds is None or not odds:
                        continue
                    save_snapshot(con, ts, sport_key, label, odds)
                except Exception as e:
                    print(f"    ERROR [{label}]: {e}")
            con.commit()
            con.close()


    # ── Helper: captured markets for current event only ────────────────────
    def get_captured_markets() -> set:
        """
        Return which markets have an opening line across ALL this-week events.
        A market is considered captured only when ALL active events have it.
        This way if Valero AND an opposite-field event are both active, we keep
        hunting until both have all 3 markets.
        """
        try:
            sport_keys = get_current_event_keys()
            if not sport_keys:
                return set()
            con = sqlite3.connect(DB_PATH)
            cur = con.cursor()
            # Find markets captured for EVERY active event (intersection)
            captured_per_event = []
            for sport_key in sport_keys:
                cur.execute("""
                    SELECT DISTINCT market FROM opening_lines WHERE sport_key=?
                """, (sport_key,))
                captured_per_event.append({r[0] for r in cur.fetchall()})
            con.close()
            # Intersection: only markets present in ALL events
            if not captured_per_event:
                return set()
            result = captured_per_event[0]
            for s in captured_per_event[1:]:
                result = result & s
            return result
        except Exception:
            return set()

    ALL_MARKETS     = {'winner', 'top5', 'top10'}
    _last_pull_time = [datetime.min]
    _prev_captured  = [set()]

    def smart_snapshot():
        now_local = datetime.now(LOCAL_TZ)
        weekday   = now_local.weekday()   # 0=Mon … 6=Sun
        hour      = now_local.hour
        minute    = now_local.minute

        # ── Phase 3: Tournament in progress — Thu 6AM through Sun noon ────
        in_tourney = (
            (weekday == 3 and hour >= 6) or
            weekday == 4 or
            weekday == 5 or
            (weekday == 6 and hour < 12)
        )
        if in_tourney:
            if hour in (0, 6):
                mins_since = (now_local - _last_pull_time[0]).total_seconds() / 60
                if mins_since >= 60:
                    print(f"[Phase 3 Tourney] Pulling at {now_local.strftime('%a %-I%p')}")
                    focused_snapshot_job()
                    _last_pull_time[0] = now_local
            return

        # ── Hunting window: Sun 4PM PST through Wed midnight ──────────────
        # Sun 4PM PST = weekday 6, hour >= 16 local
        in_hunt_window = (
            (weekday == 6 and hour >= 16) or  # Sun 4PM onward
            weekday == 0 or                    # all Monday
            weekday == 1 or                    # all Tuesday
            weekday == 2                       # all Wednesday
        )
        if not in_hunt_window:
            return  # Outside all windows — do nothing

        # Check which markets are still missing for this week's event
        captured      = get_captured_markets()
        still_hunting = ALL_MARKETS - captured

        # Log transitions
        newly_captured = captured - _prev_captured[0]
        for m in newly_captured:
            remaining = ALL_MARKETS - captured
            print(f"[Phase 1→2] '{m}' opening line captured! "
                  f"Still hunting: {remaining if remaining else 'none — all done!'}")
        _prev_captured[0] = captured

        if still_hunting:
            # Phase 1: hunt every 5 min
            # First refresh our event list — this discovers new events like Valero
            # appearing on The Odds API mid-week, and fills in commence_times.
            # Re-check current event after refresh in case a new one just appeared.
            refresh_commence_times()
            sport_keys = get_current_event_keys()
            if not sport_keys:
                print("[Phase 1 Hunting] No event within 8 days yet — watching for new events...")
            else:
                print(f"[Phase 1 Hunting] Targets: {sport_keys} | Missing markets: {still_hunting}")
                focused_snapshot_job()
                _last_pull_time[0] = now_local
        else:
            # Phase 2: all 3 captured — pull hourly to track movement
            mins_since = (now_local - _last_pull_time[0]).total_seconds() / 60
            if mins_since >= 60:
                print(f"[Phase 2 Tracking] Hourly pull at {now_local.strftime('%a %-I%p')}")
                focused_snapshot_job()
                _last_pull_time[0] = now_local

    # Base tick every 5 minutes
    schedule.every(5).minutes.do(smart_snapshot)

    # Wednesday 2 PM Pacific picks email (21:00 UTC PDT)
    schedule.every().wednesday.at("21:00").do(wednesday_picks_job)

    # Results batches post-tournament Mon/Tue/Wed
    schedule.every().tuesday.at("01:00").do(lambda: staggered_results_job(1))
    schedule.every().wednesday.at("01:00").do(lambda: staggered_results_job(2))
    schedule.every().wednesday.at("17:00").do(lambda: staggered_results_job(3))

    print("[Scheduler] Running. Sun 4PM–Wed: hunt/track current event only. Thu–Sun: 18hr tourney.")
    while True:
        schedule.run_pending()
        time.sleep(30)


@app.before_request
def start_scheduler_once():
    global _scheduler_started
    with _scheduler_lock:
        if not _scheduler_started:
            _scheduler_started = True
            # Init DB here so Railway volume is guaranteed to be mounted first
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
