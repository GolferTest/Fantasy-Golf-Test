"""
Golf Odds Tracker
Primary source: DataGolf API (all weekly PGA Tour events - win, top5, top10)
Fallback source: The Odds API (4 majors only)
Opening lines captured automatically on first snapshot.
"""

import os
import sqlite3
import requests
from datetime import datetime, timezone

# Configuration
ODDS_API_KEY   = os.environ.get("ODDS_API_KEY", "YOUR_ODDS_API_KEY_HERE")
DATAGOLF_KEY   = os.environ.get("DATAGOLF_KEY", "59e54d9d17d96388062053ddd0c7")
RAPIDAPI_KEY   = os.environ.get("RAPIDAPI_KEY", "")
DB_PATH        = os.environ.get("DB_PATH", "golf_odds.db")

BOOKMAKERS     = ["draftkings", "fanduel", "betmgm"]
REGION         = "us"
ODDS_FORMAT    = "american"

# The Odds API market keys (majors fallback)
MARKETS = {
    "winner": "outrights",
    "top5":   "outrights_top_5",
    "top10":  "outrights_top_10",
}

# DataGolf market keys
DATAGOLF_MARKETS = {
    "winner": "win",
    "top5":   "top_5",
    "top10":  "top_10",
}

DATAGOLF_BASE = "https://feeds.datagolf.com"

# The Odds API filter for majors
PGA_KEYWORDS = ["golf_"]
PGA_EXCLUDE  = ["golf_olymp", "golf_ryder", "golf_presidents", "golf_lpga", "golf_liv"]


def init_db():
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
    cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            sport_key     TEXT PRIMARY KEY,
            title         TEXT NOT NULL,
            commence_time TEXT,
            first_seen    TEXT NOT NULL,
            last_seen     TEXT NOT NULL
        )
    """)
    try:
        cur.execute("ALTER TABLE events ADD COLUMN commence_time TEXT")
    except Exception:
        pass
    con.commit()
    con.close()


def american_to_implied(american: int) -> float:
    if american > 0:
        return 100 / (american + 100) * 100
    else:
        return abs(american) / (abs(american) + 100) * 100


def upsert_event(con, sport_key: str, title: str, ts: str, commence_time: str = None):
    cur = con.cursor()
    cur.execute("""
        INSERT INTO events (sport_key, title, commence_time, first_seen, last_seen)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(sport_key) DO UPDATE SET
            last_seen=excluded.last_seen,
            commence_time=COALESCE(excluded.commence_time, events.commence_time)
    """, (sport_key, title, commence_time, ts, ts))


def save_snapshot(con, ts: str, sport_key: str, market_label: str, player_odds: dict):
    cur = con.cursor()
    new_players = 0
    for player, american in player_odds.items():
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
            cur.execute("""
                INSERT INTO opening_lines (sport_key, player, market, american_odds, implied_pct, ts)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (sport_key, player, market_label, american, implied, ts))
            is_opening  = 1
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


# ── DataGolf API ──────────────────────────────────────────────────────────────

def fetch_datagolf_current_event() -> dict | None:
    """
    Get the current week PGA Tour event from DataGolf schedule.
    Returns the event dict or None if nothing is within 8 days.
    """
    url = f"{DATAGOLF_BASE}/get-schedule?tour=pga&file_format=json&key={DATAGOLF_KEY}"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  [DataGolf] schedule fetch error: {e}")
        return None

    now = datetime.now(timezone.utc)
    schedule = data.get("schedule", [])
    best = None
    best_days = None

    for event in schedule:
        start = event.get("date") or event.get("start_date") or ""
        if not start:
            continue
        try:
            from datetime import datetime as dt
            event_dt = dt.strptime(start[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            days = (event_dt - now).total_seconds() / 86400
            if -4 <= days <= 8:
                if best_days is None or abs(days) < abs(best_days):
                    best = event
                    best_days = days
        except Exception:
            continue

    return best


def fetch_datagolf_odds(market_key: str) -> dict | None:
    """
    Fetch current week PGA Tour outright odds from DataGolf.
    market_key: 'win', 'top_5', or 'top_10'
    Returns {player_name: avg_american_odds} or None if unavailable.
    """
    url = (
        f"{DATAGOLF_BASE}/betting-tools/outrights"
        f"?tour=pga&market={market_key}&odds_format=american"
        f"&file_format=json&key={DATAGOLF_KEY}"
    )
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code in (403, 401):
            print(f"  [DataGolf] Auth error {resp.status_code} — check API key/subscription")
            return None
        if resp.status_code == 404:
            print(f"  [DataGolf] {market_key} not available yet (404)")
            return None
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  [DataGolf] fetch error for {market_key}: {e}")
        return None

    odds_list = data.get("odds", [])
    if not odds_list:
        return {}

    books = ["draftkings", "fanduel", "betmgm", "bet365", "caesars", "pinnacle"]
    player_odds = {}
    for player in odds_list:
        name = player.get("player_name", "").strip()
        if not name:
            continue
        prices = []
        for book in books:
            val = player.get(book)
            if val is None:
                continue
            # DataGolf returns odds as strings e.g. "+2000" or "-110"
            try:
                price = int(str(val).replace("+", ""))
                if price != 0:
                    prices.append(price)
            except (ValueError, TypeError):
                continue
        if prices:
            player_odds[name] = round(sum(prices) / len(prices))

    return player_odds


def get_datagolf_sport_key(event_name: str, start_date: str) -> str:
    """Generate a stable sport_key for a DataGolf event."""
    year = start_date[:4] if start_date else str(datetime.now(timezone.utc).year)
    slug = event_name.lower()
    for ch in [" ", "-", ".", "'"]:
        slug = slug.replace(ch, "_")
    slug = slug.replace("__", "_").strip("_")
    for word in ["presented_by", "sponsored_by", "powered_by", "hosted_by"]:
        if word in slug:
            slug = slug[:slug.index(word)].rstrip("_")
    return f"datagolf_{slug}_{year}"


def run_datagolf_snapshot(sport_key: str, event_name: str):
    """Pull all 3 markets from DataGolf for the current week event."""
    init_db()
    ts = datetime.now(timezone.utc).isoformat()
    print(f"\n[DataGolf] Pulling {event_name} ({sport_key})...")

    con = sqlite3.connect(DB_PATH)
    upsert_event(con, sport_key, event_name, ts)

    for label, dg_market in DATAGOLF_MARKETS.items():
        try:
            odds = fetch_datagolf_odds(dg_market)
            if odds is None:
                print(f"    [{label}] not available yet")
                continue
            if not odds:
                print(f"    [{label}] no lines posted yet")
                continue
            save_snapshot(con, ts, sport_key, label, odds)
        except Exception as e:
            print(f"    ERROR [{label}]: {e}")

    con.commit()
    con.close()
    print(f"[DataGolf] Snapshot complete for {event_name}")


# ── The Odds API (majors fallback) ────────────────────────────────────────────

def fetch_active_pga_events() -> list[dict]:
    """Query The Odds API for active golf events (majors only)."""
    url = f"https://api.the-odds-api.com/v4/sports/?apiKey={ODDS_API_KEY}&all=false"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    sports = resp.json()

    events = []
    for sport in sports:
        key = sport.get("key", "")
        if any(kw in key for kw in PGA_KEYWORDS) and not any(ex in key for ex in PGA_EXCLUDE):
            events.append({"key": key, "title": sport.get("title", key)})

    print(f"  Found {len(events)} Odds API event(s): {[e['key'] for e in events]}")
    return events


def fetch_odds_for_event(sport_key: str, market_key: str) -> dict | None:
    """
    Fetch odds from The Odds API for one event and market.
    Returns None on 422 (market not available yet).
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

    if resp.status_code == 422:
        print(f"    [{market_key}] not available yet for {sport_key} -- will retry next pull")
        return None

    resp.raise_for_status()
    data = resp.json()

    player_odds: dict[str, list[int]] = {}
    for event in data:
        for bookmaker in event.get("bookmakers", []):
            for market in bookmaker.get("markets", []):
                for outcome in market.get("outcomes", []):
                    name = outcome.get("name", "").strip()
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


def run_snapshot():
    """
    Full snapshot via The Odds API for all active events.
    Used as fallback when DataGolf has no current event.
    """
    init_db()
    ts = datetime.now(timezone.utc).isoformat()
    print(f"\n[{ts}] Starting Odds API snapshot...")

    try:
        events = fetch_active_pga_events()
    except Exception as e:
        print(f"  ERROR fetching active events: {e}")
        return

    if not events:
        print("  No active events found.")
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
                if odds is None or not odds:
                    continue
                save_snapshot(con, ts, sport_key, label, odds)
            except Exception as e:
                print(f"    ERROR [{label}]: {e}")
                continue

    con.commit()
    con.close()
    print("\nOdds API snapshot complete.")


if __name__ == "__main__":
    run_snapshot()


# ── Slash Golf Results Fetching ───────────────────────────────────────────────

def fetch_tournament_results(sport_key: str) -> list[dict]:
    """Fetch final leaderboard from Slash Golf API via RapidAPI."""
    if not RAPIDAPI_KEY:
        print("  RAPIDAPI_KEY not set -- skipping auto results fetch")
        return []

    headers = {
        "X-RapidAPI-Key":  RAPIDAPI_KEY,
        "X-RapidAPI-Host": "live-golf-data.p.rapidapi.com",
    }

    try:
        sched_resp = requests.get(
            "https://live-golf-data.p.rapidapi.com/schedule",
            params={"orgId": "1"},
            headers=headers,
            timeout=15,
        )
        sched_resp.raise_for_status()
        schedule = sched_resp.json()
    except Exception as e:
        print(f"  Could not fetch schedule: {e}")
        return []

    key_lower = sport_key.lower()
    keywords = []
    if "masters"  in key_lower:     keywords = ["masters"]
    elif "us_open" in key_lower:    keywords = ["u.s. open", "us open"]
    elif "the_open" in key_lower or "open_championship" in key_lower:
        keywords = ["the open", "open championship", "british open"]
    elif "pga_championship" in key_lower: keywords = ["pga championship"]
    else:
        parts = sport_key.replace("golf_","").replace("datagolf_","").replace("_winner","").replace("_"," ")
        parts = parts.rsplit(" ", 1)[0] if parts[-4:].isdigit() else parts
        keywords = [parts.strip()]

    tourn_id   = None
    year       = None
    tourn_name = None

    schedule_list = (
        schedule.get("schedule") or schedule.get("tournaments") or
        schedule.get("events") or
        (schedule.get("data") if isinstance(schedule.get("data"), list) else None) or []
    )

    for event in schedule_list:
        name = (
            event.get("tournamentName") or event.get("tournament_name") or
            event.get("name") or ""
        ).lower()
        if any(kw in name for kw in keywords):
            tourn_id = (
                event.get("tournId") or event.get("id") or
                event.get("tournament_id") or event.get("tournamentId")
            )
            year = event.get("year") or event.get("season") or str(datetime.now(timezone.utc).year)
            tourn_name = event.get("tournamentName") or event.get("name") or name
            break

    if not tourn_id:
        print(f"  Could not match sport_key '{sport_key}' to a Slash Golf tournament")
        if schedule_list:
            print(f"  Schedule sample: {list(schedule_list[0].keys())}")
        return []

    print(f"  Fetching results for: {tourn_name} (id={tourn_id}, year={year})")

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

    rows = (
        lb_data.get("leaderboard") or lb_data.get("players") or
        lb_data.get("results") or
        (lb_data.get("data", {}) or {}).get("leaderboard") or []
    )

    players = []
    for row in rows:
        first = row.get("firstName") or row.get("first_name") or ""
        last  = row.get("lastName")  or row.get("last_name")  or ""
        name  = (
            row.get("playerName") or row.get("player_name") or
            row.get("name") or f"{first} {last}".strip()
        )
        pos_raw = (
            row.get("position") or row.get("pos") or
            row.get("currentPosition") or row.get("place") or ""
        )
        position = str(pos_raw).strip() if pos_raw else ""
        earn_raw = (
            row.get("earnings") or row.get("money") or
            row.get("prizeMoney") or row.get("prize_money") or ""
        )
        earnings = str(int(earn_raw)) if isinstance(earn_raw, (int, float)) and earn_raw else str(earn_raw or "")

        if name and position:
            players.append({"player": name, "finish": position, "earnings": earnings})

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
    Fetch results in staggered batches.
    Batch 1 (Monday): top 10 picks + top 5 longshots + 5 best others = 20 players
    Batch 2 (Tuesday): next 20 best results
    Batch 3 (Wednesday): next 10
    Uses cached leaderboard after first fetch -- only 1 Slash Golf API request total.
    """
    from scorer import calculate_scores, get_weekly_selections

    print(f"\n[Batch {batch}] Staggered results fetch for {sport_key}...")

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

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

    cur.execute("SELECT COUNT(*) FROM leaderboard_cache WHERE sport_key=?", (sport_key,))
    cached_count = cur.fetchone()[0]

    if cached_count == 0:
        print("  No cache found -- fetching full leaderboard from Slash Golf API...")
        raw_results = fetch_tournament_results(sport_key)
        if not raw_results:
            print("  Could not fetch leaderboard -- skipping.")
            con.close()
            return
        ts = datetime.now(timezone.utc).isoformat()
        for r in raw_results:
            if r.get("player"):
                cur.execute("""
                    INSERT OR IGNORE INTO leaderboard_cache
                    (sport_key, player, finish, earnings, fetched_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (sport_key, r["player"], r.get("finish",""), r.get("earnings",""), ts))
        con.commit()
        print(f"  Cached {len(raw_results)} results from leaderboard")
    else:
        print(f"  Using cached leaderboard ({cached_count} players)")

    scores   = calculate_scores(sport_key)
    priority = get_weekly_selections(scores)

    cur.execute("SELECT player FROM results WHERE sport_key=?", (sport_key,))
    already_saved = {r[0] for r in cur.fetchall()}

    cur.execute("SELECT player, finish, earnings FROM leaderboard_cache WHERE sport_key=?", (sport_key,))
    cache_map = {r[0]: {"finish": r[1], "earnings": r[2]} for r in cur.fetchall()}
    con.close()

    batch_sizes = {1: 20, 2: 20, 3: 10}
    batch_size  = batch_sizes.get(batch, 20)

    save_list = []
    for player in priority:
        if player not in already_saved and player in cache_map:
            save_list.append((player, cache_map[player]))
        if len(save_list) >= batch_size:
            break

    if len(save_list) < batch_size:
        remaining = [
            (p, d) for p, d in cache_map.items()
            if p not in already_saved and p not in {x for x, _ in save_list}
        ]
        def finish_sort(item):
            f = item[1]["finish"].replace("T","").replace("=","")
            try:
                return int(f)
            except Exception:
                return 999
        remaining.sort(key=finish_sort)
        save_list.extend(remaining[:batch_size - len(save_list)])

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

    print(f"  Batch {batch} complete -- saved {saved} results")
