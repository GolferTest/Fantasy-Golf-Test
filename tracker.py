"""
Golf Odds Tracker
- Pulls every 2 hours (saves ~84 requests/week vs 168 hourly)
- Auto-discovers ALL active PGA Tour events — no manual sport_key updates needed
- First snapshot for any player+event = opening line automatically
"""

import os
import sqlite3
import requests
from datetime import datetime, timezone

# ── Configuration ─────────────────────────────────────────────────────────────
ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "YOUR_ODDS_API_KEY_HERE")
DB_PATH      = os.environ.get("DB_PATH", "golf_odds.db")

BOOKMAKERS  = ["draftkings", "fanduel", "betmgm"]
REGION      = "us"
ODDS_FORMAT = "american"

# The Odds API market keys for golf outrights
MARKETS = {
    "winner": "outrights",
    "top5":   "outrights_top_5",
    "top10":  "outrights_top_10",
}

# Any sport key containing these strings will be tracked automatically.
# This catches all PGA Tour, major, and affiliated events.
# Match any golf outright event — The Odds API uses "golf_" prefix for all golf tournaments.
# Weekly events like Valero, Houston Open etc appear as e.g. "golf_valero_texas_open_winner"
# We catch all of them with the "golf_" prefix, then filter out non-outright keys below.
PGA_KEYWORDS = ["golf_"]
PGA_EXCLUDE  = ["golf_olymp", "golf_ryder", "golf_presidents", "golf_lpga", "golf_liv"]
# ─────────────────────────────────────────────────────────────────────────────


def init_db():
    """Create tables if they don't exist."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            ts            TEXT NOT NULL,
            sport_key     TEXT NOT NULL,
            player        TEXT NOT NULL,
            market        TEXT NOT NULL,
            american_odds INTEGER NOT NULL,
            implied_pct   REAL NOT NULL,
            is_opening    INTEGER DEFAULT 0
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS opening_lines (
            sport_key     TEXT NOT NULL,
            player        TEXT NOT NULL,
            market        TEXT NOT NULL,
            american_odds INTEGER NOT NULL,
            implied_pct   REAL NOT NULL,
            ts            TEXT NOT NULL,
            PRIMARY KEY (sport_key, player, market)
        )
    """)

    # Track which events we've seen so dashboard can show event names
    cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            sport_key    TEXT PRIMARY KEY,
            title        TEXT NOT NULL,
            first_seen   TEXT NOT NULL,
            last_seen    TEXT NOT NULL
        )
    """)

    con.commit()
    con.close()


def american_to_implied(american: int) -> float:
    """Convert American odds to implied probability (0-100)."""
    if american > 0:
        return 100 / (american + 100) * 100
    else:
        return abs(american) / (abs(american) + 100) * 100


def fetch_active_pga_events() -> list[dict]:
    """
    Query The Odds API for all currently active sports.
    Filter to PGA-related golf events. Returns list of {key, title} dicts.
    Costs only 1 API request regardless of how many events are live.
    """
    url = f"https://api.the-odds-api.com/v4/sports/?apiKey={ODDS_API_KEY}&all=false"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    sports = resp.json()

    events = []
    for sport in sports:
        key = sport.get("key", "")
        if any(kw in key for kw in PGA_KEYWORDS) and not any(ex in key for ex in PGA_EXCLUDE):
            events.append({"key": key, "title": sport.get("title", key)})

    print(f"  Found {len(events)} active PGA event(s): {[e['key'] for e in events]}")
    return events


def upsert_event(con, sport_key: str, title: str, ts: str):
    """Insert or update the events table."""
    cur = con.cursor()
    cur.execute("""
        INSERT INTO events (sport_key, title, first_seen, last_seen)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(sport_key) DO UPDATE SET last_seen=excluded.last_seen
    """, (sport_key, title, ts, ts))


def fetch_odds_for_event(sport_key: str, market_key: str) -> dict | None:
    """
    Fetch odds for one event + market.
    Returns {player_name: avg_american_odds} averaged across bookmakers.
    Returns None if the market is not yet available (422) — caller will skip gracefully.
    Returns empty dict {} if the market exists but has no data yet.
    """
    url = (
        f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
        f"?apiKey={ODDS_API_KEY}"
        f"&regions={REGION}"
        f"&markets={market_key}"
        f"&oddsFormat={ODDS_FORMAT}"
        f"&bookmakers={','.join(BOOKMAKERS)}"
    )

    resp = requests.get(url, timeout=15)

    # 422 = market not available yet for this event (e.g. top5/top10 not posted yet)
    # Return None so the caller can skip silently rather than logging a scary error
    if resp.status_code == 422:
        print(f"    [{market_key}] not available yet for {sport_key} — will retry next pull")
        return None

    # Any other non-200 is a real error worth logging
    resp.raise_for_status()
    data = resp.json()

    player_odds: dict[str, list[int]] = {}
    for event in data:
        for bookmaker in event.get("bookmakers", []):
            for market in bookmaker.get("markets", []):
                for outcome in market.get("outcomes", []):
                    name  = outcome.get("name", "").strip()
                    if not name:
                        continue
                    try:
                        price = int(round(float(outcome["price"])))
                    except (ValueError, KeyError):
                        continue
                    player_odds.setdefault(name, []).append(price)

    return {
        name: round(sum(prices) / len(prices))
        for name, prices in player_odds.items()
        if prices
    }


def save_snapshot(con, ts: str, sport_key: str, market_label: str, player_odds: dict):
    """
    Persist snapshot rows; auto-record opening line if first time seen.

    Late additions (e.g. a player added to the field on Tuesday) are handled
    automatically — their first appearance becomes their opening line, same as
    anyone else. No zeros, no placeholders.
    """
    cur = con.cursor()
    new_players = 0

    for player, american in player_odds.items():
        # Skip any malformed entries
        if not player or american is None:
            continue

        try:
            implied = american_to_implied(american)
        except Exception:
            continue

        cur.execute("""
            SELECT 1 FROM opening_lines
            WHERE sport_key=? AND player=? AND market=?
        """, (sport_key, player, market_label))

        is_opening = 0
        if cur.fetchone() is None:
            # First time we have seen this player in this market for this event.
            # This naturally handles late field additions — their opening line is
            # simply whatever their odds are when they first appear, not zero.
            cur.execute("""
                INSERT INTO opening_lines (sport_key, player, market, american_odds, implied_pct, ts)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (sport_key, player, market_label, american, implied, ts))
            is_opening = 1
            new_players += 1

        cur.execute("""
            INSERT INTO snapshots (ts, sport_key, player, market, american_odds, implied_pct, is_opening)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (ts, sport_key, player, market_label, american, implied, is_opening))

    total = len(player_odds)
    if new_players:
        print(f"    [{market_label}] saved {total} players ({new_players} new opening lines)")
    else:
        print(f"    [{market_label}] saved {total} players")


def run_snapshot():
    """
    Main entry: auto-discover all active PGA events, pull all 3 markets for each,
    save snapshots. Opening lines are set automatically on first appearance.

    API request budget per 2-hour pull:
      1  (event discovery)
    + N_events x 3 markets
    Typically: 1 + 1x3 = 4 requests per pull → ~48 requests/week (well under free 500 limit)
    """
    init_db()
    ts = datetime.now(timezone.utc).isoformat()
    print(f"\n[{ts}] Starting 2-hour snapshot...")

    try:
        events = fetch_active_pga_events()
    except Exception as e:
        print(f"  ERROR fetching active events: {e}")
        return

    if not events:
        print("  No active PGA events found — will check again in 2 hours.")
        return

    con = sqlite3.connect(DB_PATH)

    for event in events:
        sport_key = event["key"]
        title     = event["title"]
        print(f"\n  Event: {title} ({sport_key})")
        upsert_event(con, sport_key, title, ts)

        for label, market_key in MARKETS.items():
            try:
                odds = fetch_odds_for_event(sport_key, market_key)

                # None = market not available yet (e.g. top5/top10 before lines post)
                # Skip silently — will be picked up automatically next pull
                if odds is None:
                    continue

                # Empty dict = API responded but no bookmakers have lines yet
                if not odds:
                    print(f"    [{label}] no lines posted yet — skipping")
                    continue

                save_snapshot(con, ts, sport_key, label, odds)

            except Exception as e:
                # Log the error but keep going — one bad market never stops the rest
                print(f"    ERROR [{label}]: {e}")
                continue

    con.commit()
    con.close()
    print("\nSnapshot complete.")


if __name__ == "__main__":
    run_snapshot()


# ── Auto Results Fetching ─────────────────────────────────────────────────────
RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY", "")  # from rapidapi.com — Slash Golf free tier

def fetch_tournament_results(sport_key: str) -> list[dict]:
    """
    Fetch final leaderboard from Slash Golf API (via RapidAPI) for a completed tournament.
    Returns list of {player, finish, earnings} sorted by finish position.
    Only call this after the tournament has ended (Sunday evening or later).

    sport_key format: 'golf_masters_tournament_winner' — we strip to get event name.
    Slash Golf uses numeric tournament IDs; we match by searching the schedule.
    """
    if not RAPIDAPI_KEY:
        print("  RAPIDAPI_KEY not set — skipping auto results fetch")
        return []

    headers = {
        "X-RapidAPI-Key":  RAPIDAPI_KEY,
        "X-RapidAPI-Host": "live-golf-data.p.rapidapi.com",
    }

    # Step 1: Get current/recent PGA schedule to find matching tournament ID
    try:
        sched_resp = requests.get(
            "https://live-golf-data.p.rapidapi.com/schedule",
            params={"orgId": "1"},  # 1 = PGA Tour
            headers=headers,
            timeout=15,
        )
        sched_resp.raise_for_status()
        schedule = sched_resp.json()
    except Exception as e:
        print(f"  Could not fetch schedule: {e}")
        return []

    # Step 2: Match sport_key to a tournament by name keywords
    # sport_key examples: golf_masters_tournament_winner, golf_us_open_winner
    key_lower = sport_key.lower()
    keywords = []
    if "masters"  in key_lower: keywords = ["masters"]
    elif "us_open" in key_lower: keywords = ["u.s. open", "us open"]
    elif "the_open" in key_lower or "open_championship" in key_lower: keywords = ["the open", "open championship", "british open"]
    elif "pga_championship" in key_lower: keywords = ["pga championship"]
    else:
        # Generic: extract words between golf_ and _winner
        parts = sport_key.replace("golf_","").replace("_winner","").replace("_"," ")
        keywords = [parts]

    tourn_id = None
    year     = None
    tourn_name = None

    # Slash Golf may wrap schedule differently
    schedule_list = (
        schedule.get("schedule") or
        schedule.get("tournaments") or
        schedule.get("events") or
        (schedule.get("data") if isinstance(schedule.get("data"), list) else None) or
        []
    )

    for event in schedule_list:
        name = (
            event.get("tournamentName") or
            event.get("tournament_name") or
            event.get("name") or ""
        ).lower()
        if any(kw in name for kw in keywords):
            tourn_id = (
                event.get("tournId") or event.get("id") or
                event.get("tournament_id") or event.get("tournamentId")
            )
            year = (
                event.get("year") or event.get("season") or
                str(datetime.now(timezone.utc).year)
            )
            tourn_name = event.get("tournamentName") or event.get("name") or name
            break

    if not tourn_id and schedule_list:
        print(f"  Schedule sample keys: {list(schedule_list[0].keys()) if schedule_list else 'empty'}")

    if not tourn_id:
        print(f"  Could not match sport_key '{sport_key}' to a tournament in schedule")
        return []

    print(f"  Fetching results for: {tourn_name} (id={tourn_id}, year={year})")

    # Step 3: Fetch leaderboard
    try:
        lb_resp = requests.get(
            "https://live-golf-data.p.rapidapi.com/leaderboard",
            params={"orgId": "1", "tournId": str(tourn_id), "year": str(year)},
            headers=headers,
            timeout=15,
        )
        lb_resp.raise_for_status()
        lb_data = lb_resp.json()
    except Exception as e:
        print(f"  Could not fetch leaderboard: {e}")
        return []

    # Slash Golf may nest leaderboard differently — try multiple structures
    rows = (
        lb_data.get("leaderboard") or
        lb_data.get("players") or
        lb_data.get("results") or
        (lb_data.get("data", {}) or {}).get("leaderboard") or
        []
    )

    players = []
    for row in rows:
        # Name: try several field patterns
        first = row.get("firstName") or row.get("first_name") or ""
        last  = row.get("lastName")  or row.get("last_name")  or ""
        name  = (
            row.get("playerName") or
            row.get("player_name") or
            row.get("name") or
            f"{first} {last}".strip()
        )

        # Position: may be int or string, may have "T" prefix already
        pos_raw = (
            row.get("position") or row.get("pos") or
            row.get("currentPosition") or row.get("current_position") or
            row.get("place") or ""
        )
        position = str(pos_raw).strip() if pos_raw else ""

        # Earnings: may be int or string
        earn_raw = (
            row.get("earnings") or row.get("money") or
            row.get("prizeMoney") or row.get("prize_money") or
            row.get("totalEarnings") or ""
        )
        earnings = str(int(earn_raw)) if isinstance(earn_raw, (int, float)) and earn_raw else str(earn_raw or "")

        if name and position:
            players.append({"player": name, "finish": position, "earnings": earnings})

    if not players:
        print(f"  WARNING: Got response but could not parse players. Keys in response: {list(lb_data.keys())}")
        print(f"  Full response (first 500 chars): {str(lb_data)[:500]}")

    print(f"  Got {len(players)} results from leaderboard")
    return players


def save_tournament_results(sport_key: str, results: list[dict]):
    """Save fetched results into the results table."""
    from scorer import save_result
    saved = 0
    for r in results:
        if r.get("player") and r.get("finish"):
            save_result(
                sport_key = sport_key,
                player    = r["player"],
                finish    = r["finish"],
                payout    = r.get("earnings", ""),
                notes     = "auto-fetched",
            )
            saved += 1
    print(f"  Saved {saved} results for {sport_key}")


def run_results_fetch(sport_key: str):
    """Fetch and save results for a completed tournament."""
    print(f"\nFetching final results for {sport_key}...")
    results = fetch_tournament_results(sport_key)
    if results:
        save_tournament_results(sport_key, results)
    else:
        print("  No results to save.")


def run_staggered_results_fetch(sport_key: str, batch: int):
    """
    Fetch results in staggered batches to stay within Slash Golf's 20 req/day limit.
    Each leaderboard call = 1 request = 1 player's result... actually the leaderboard
    endpoint returns ALL players in one request. So 20 req/day = 20 full leaderboard pulls.
    We only need 1 pull to get all results — but we stagger saving to DB by priority:

    Batch 1 (Monday):   Top 10 picks + Top 5 longshots + 5 best others  = 20 players saved
    Batch 2 (Tuesday):  Next 20 best results not yet saved
    Batch 3 (Wednesday): Next 10

    This guarantees our picks + longshots always have results, and we build out
    to top ~50 results over 3 days using only 1 API request total (the leaderboard).
    We cache the full leaderboard in the DB on first fetch to avoid repeat API calls.
    """
    from scorer import calculate_scores, get_weekly_selections

    print(f"\n[Batch {batch}] Staggered results fetch for {sport_key}...")

    # Check if we already have cached leaderboard data
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # Create leaderboard cache table if needed
    cur.execute("""
        CREATE TABLE IF NOT EXISTS leaderboard_cache (
            sport_key  TEXT NOT NULL,
            player     TEXT NOT NULL,
            finish     TEXT NOT NULL,
            earnings   TEXT,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (sport_key, player)
        )
    """)
    con.commit()

    # Check if cache exists for this event
    cur.execute("SELECT COUNT(*) FROM leaderboard_cache WHERE sport_key=?", (sport_key,))
    cached_count = cur.fetchone()[0]

    if cached_count == 0:
        # First batch — fetch full leaderboard from API (1 request)
        print("  No cache found — fetching full leaderboard from Slash Golf API...")
        raw_results = fetch_tournament_results(sport_key)

        if not raw_results:
            print("  Could not fetch leaderboard — skipping.")
            con.close()
            return

        # Cache entire leaderboard
        ts = datetime.now(timezone.utc).isoformat()
        for r in raw_results:
            if r.get("player"):
                cur.execute("""
                    INSERT OR IGNORE INTO leaderboard_cache (sport_key, player, finish, earnings, fetched_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (sport_key, r["player"], r.get("finish",""), r.get("earnings",""), ts))
        con.commit()
        print(f"  Cached {len(raw_results)} results from leaderboard")
    else:
        print(f"  Using cached leaderboard ({cached_count} players)")

    # Get priority order from scorer
    scores   = calculate_scores(sport_key)
    priority = get_weekly_selections(scores)  # ordered list of player names

    # Find which players already have results saved
    cur.execute("SELECT player FROM results WHERE sport_key=?", (sport_key,))
    already_saved = {r[0] for r in cur.fetchall()}

    # Get all cached leaderboard players mapped by name
    cur.execute("SELECT player, finish, earnings FROM leaderboard_cache WHERE sport_key=?", (sport_key,))
    cache_map = {r[0]: {"finish": r[1], "earnings": r[2]} for r in cur.fetchall()}
    con.close()

    # Batch sizes
    batch_sizes = {1: 20, 2: 20, 3: 10}
    batch_size  = batch_sizes.get(batch, 20)

    # Build save list: priority players first, then remaining cache by finish position
    save_list = []

    # Priority players not yet saved
    for player in priority:
        if player not in already_saved and player in cache_map:
            save_list.append((player, cache_map[player]))
        if len(save_list) >= batch_size:
            break

    # If priority list exhausted, fill with remaining cache sorted by finish
    if len(save_list) < batch_size:
        remaining = [
            (player, data) for player, data in cache_map.items()
            if player not in already_saved and player not in {p for p, _ in save_list}
        ]
        # Sort remaining by finish position numerically where possible
        def finish_sort(item):
            f = item[1]["finish"].replace("T","").replace("=","")
            try: return int(f)
            except: return 999
        remaining.sort(key=finish_sort)
        save_list.extend(remaining[:batch_size - len(save_list)])

    # Save this batch
    from scorer import save_result
    saved = 0
    for player, data in save_list:
        save_result(
            sport_key = sport_key,
            player    = player,
            finish    = data["finish"],
            payout    = data["earnings"],
            notes     = f"auto-fetched batch {batch}",
        )
        saved += 1

    print(f"  Batch {batch} complete — saved {saved} results (total saved: {len(already_saved) + saved})")
