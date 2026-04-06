"""
Golf Odds Tracker - DataGolf API only
Pulls win / top5 / top10 odds for the current PGA Tour event.
Opening line = first snapshot for any player+market combination.
Results fetching via Slash Golf (RapidAPI) post-tournament.
"""

import os
import sqlite3
import requests
from datetime import datetime, timezone

DATAGOLF_KEY  = os.environ.get("DATAGOLF_KEY", "59e54d9d17d96388062053ddd0c7")
RAPIDAPI_KEY  = os.environ.get("RAPIDAPI_KEY", "")
DB_PATH       = os.environ.get("DB_PATH", "golf_odds.db")

DATAGOLF_BASE = "https://feeds.datagolf.com"

DATAGOLF_MARKETS = {
    "winner": "win",
    "top5":   "top_5",
    "top10":  "top_10",
}


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
            is_opening    INTEGER DEFAULT 0,
            book_count    INTEGER DEFAULT 0
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
            start_date    TEXT,
            first_seen    TEXT NOT NULL,
            last_seen     TEXT NOT NULL
        )
    """)
    try:
        cur.execute("ALTER TABLE events ADD COLUMN start_date TEXT")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE snapshots ADD COLUMN book_count INTEGER DEFAULT 0")
    except Exception:
        pass
    # migrate old commence_time column to start_date if needed
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


def upsert_event(con, sport_key: str, title: str, ts: str, start_date: str = None):
    cur = con.cursor()
    cur.execute("""
        INSERT INTO events (sport_key, title, start_date, first_seen, last_seen)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(sport_key) DO UPDATE SET
            last_seen  = excluded.last_seen,
            start_date = COALESCE(excluded.start_date, events.start_date)
    """, (sport_key, title, start_date, ts, ts))


def save_snapshot(con, ts: str, sport_key: str, market_label: str,
                   player_odds: dict, book_counts: dict = None):
    """
    player_odds:  {player: avg_american_odds}
    book_counts:  {player: num_books_that_offered_odds} — used to flag
                  data points where the average shifted due to a new book
                  appearing rather than existing books moving.
    """
    cur = con.cursor()
    new_players = 0
    for player, american in player_odds.items():
        if not player or american is None:
            continue
        try:
            implied = american_to_implied(american)
        except Exception:
            continue
        bcount = (book_counts or {}).get(player, 0)
        cur.execute(
            "SELECT 1 FROM opening_lines WHERE sport_key=? AND player=? AND market=?",
            (sport_key, player, market_label)
        )
        is_opening = 0
        if cur.fetchone() is None:
            cur.execute("""
                INSERT INTO opening_lines (sport_key, player, market, american_odds, implied_pct, ts)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (sport_key, player, market_label, american, implied, ts))
            is_opening  = 1
            new_players += 1
        cur.execute("""
            INSERT INTO snapshots (ts, sport_key, player, market, american_odds, implied_pct, is_opening, book_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (ts, sport_key, player, market_label, american, implied, is_opening, bcount))
    total = len(player_odds)
    if new_players:
        print(f"    [{market_label}] saved {total} players ({new_players} new opening lines)")
    else:
        print(f"    [{market_label}] saved {total} players")


# ── DataGolf schedule ─────────────────────────────────────────────────────────

def fetch_datagolf_current_event() -> dict | None:
    """
    Return the DataGolf schedule entry for this week's PGA Tour event.
    'This week' = started within last 4 days OR starts within next 8 days.
    Returns None if nothing qualifies (between events / too far out).
    """
    url = f"{DATAGOLF_BASE}/get-schedule?tour=pga&file_format=json&key={DATAGOLF_KEY}"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        schedule = resp.json().get("schedule", [])
    except Exception as e:
        print(f"  [DataGolf] schedule error: {e}")
        return None

    now  = datetime.now(timezone.utc)
    best = None
    best_gap = None

    for event in schedule:
        start = event.get("start_date") or event.get("date") or ""
        if not start:
            continue
        try:
            event_dt = datetime.strptime(start[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            days = (event_dt - now).total_seconds() / 86400
            if -4 <= days <= 8:
                if best_gap is None or abs(days) < abs(best_gap):
                    best     = event
                    best_gap = days
        except Exception:
            continue

    return best


def get_datagolf_sport_key(event_name: str, start_date: str) -> str:
    """Stable sport_key for a DataGolf event: datagolf_{slug}_{year}"""
    year = start_date[:4] if start_date else str(datetime.now(timezone.utc).year)
    slug = event_name.lower()
    for ch in " -.'&":
        slug = slug.replace(ch, "_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    slug = slug.strip("_")
    for word in ["presented_by", "sponsored_by", "powered_by", "hosted_by"]:
        if word in slug:
            slug = slug[:slug.index(word)].rstrip("_")
    return f"datagolf_{slug}_{year}"


# ── DataGolf odds ─────────────────────────────────────────────────────────────

def fetch_datagolf_odds_with_event(market_key: str) -> tuple | None:
    """
    Fetch current odds from DataGolf for one market.
    Returns (event_name_from_api, {player: avg_american_odds})
    or None if unavailable / auth error.
    """
    url = (
        f"{DATAGOLF_BASE}/betting-tools/outrights"
        f"?tour=pga&market={market_key}&odds_format=american"
        f"&file_format=json&key={DATAGOLF_KEY}"
    )
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code in (401, 403):
            print(f"  [DataGolf] Auth error {resp.status_code} — check key/subscription")
            return None
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  [DataGolf] fetch error {market_key}: {e}")
        return None

    api_event_name = data.get("event_name", "")
    odds_list      = data.get("odds", [])
    if not odds_list:
        return (api_event_name, {})

    books = ["draftkings", "fanduel", "betmgm", "bet365", "caesars", "pinnacle"]
    player_odds   = {}
    player_counts = {}
    for player in odds_list:
        name = player.get("player_name", "").strip()
        if not name:
            continue
        prices = []
        for book in books:
            val = player.get(book)
            if val is None:
                continue
            try:
                price = int(str(val).replace("+", ""))
                if price != 0:
                    prices.append(price)
            except (ValueError, TypeError):
                continue
        if prices:
            player_odds[name]   = round(sum(prices) / len(prices))
            player_counts[name] = len(prices)

    return (api_event_name, player_odds, player_counts)


def run_datagolf_snapshot(sport_key: str, event_name: str, start_date: str = None):
    """
    Pull all 3 markets from DataGolf for the current week's event.
    Verifies the API is serving the right event before saving.
    start_date: YYYY-MM-DD of Thursday tee-off — stored so scorer can lock
                closing line at Wednesday 11:59 PM PST.
    """
    init_db()
    ts = datetime.now(timezone.utc).isoformat()
    print(f"\n[DataGolf] Pulling {event_name} ({sport_key})")

    con = sqlite3.connect(DB_PATH)
    upsert_event(con, sport_key, event_name, ts, start_date)

    any_saved = False
    for label, market_key in DATAGOLF_MARKETS.items():
        result = fetch_datagolf_odds_with_event(market_key)
        if result is None:
            print(f"    [{label}] API unavailable")
            continue
        api_event, odds, book_counts = result

        # Verify DataGolf is serving this week's event, not last week's
        if api_event and event_name:
            api_slug  = api_event.lower().replace(" ", "").replace("'", "")
            want_slug = event_name.lower().replace(" ", "").replace("'", "")
            if api_slug != want_slug:
                print(f"    [{label}] DataGolf still showing '{api_event}' not '{event_name}' — waiting")
                continue

        if not odds:
            print(f"    [{label}] no lines yet")
            continue

        save_snapshot(con, ts, sport_key, label, odds, book_counts)
        any_saved = True

    con.commit()
    con.close()

    if any_saved:
        print(f"[DataGolf] Done — {event_name}")
    else:
        print(f"[DataGolf] No data saved — odds not yet posted for {event_name}")


# ── Slash Golf results ────────────────────────────────────────────────────────

def fetch_tournament_results(sport_key: str) -> list[dict]:
    """Fetch final leaderboard from Slash Golf via RapidAPI."""
    if not RAPIDAPI_KEY:
        print("  RAPIDAPI_KEY not set — skipping")
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
    if "masters"       in key_lower: keywords = ["masters"]
    elif "us_open"     in key_lower: keywords = ["u.s. open", "us open"]
    elif "the_open"    in key_lower or "open_championship" in key_lower:
        keywords = ["the open", "open championship", "british open"]
    elif "pga_championship" in key_lower: keywords = ["pga championship"]
    else:
        parts = (sport_key
                 .replace("datagolf_", "")
                 .replace("_winner", "")
                 .replace("_", " "))
        if parts[-4:].isdigit():
            parts = parts[:-5].strip()
        keywords = [parts.strip()]

    schedule_list = (
        schedule.get("schedule") or schedule.get("tournaments") or
        schedule.get("events") or []
    )

    tourn_id = year = tourn_name = None
    for event in schedule_list:
        name = (event.get("tournamentName") or event.get("name") or "").lower()
        if any(kw in name for kw in keywords):
            tourn_id   = event.get("tournId") or event.get("id") or event.get("tournamentId")
            year       = event.get("year") or event.get("season") or str(datetime.now(timezone.utc).year)
            tourn_name = event.get("tournamentName") or event.get("name") or name
            break

    if not tourn_id:
        print(f"  Could not match '{sport_key}' to a Slash Golf tournament")
        return []

    print(f"  Fetching results: {tourn_name} (id={tourn_id}, year={year})")

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
        name  = row.get("playerName") or row.get("player_name") or row.get("name") or f"{first} {last}".strip()
        pos   = str(row.get("position") or row.get("pos") or row.get("place") or "").strip()
        earn  = row.get("earnings") or row.get("money") or row.get("prizeMoney") or ""
        if isinstance(earn, (int, float)):
            earn = str(int(earn))
        if name and pos:
            players.append({"player": name, "finish": pos, "earnings": str(earn)})

    print(f"  Got {len(players)} results")
    return players


def run_staggered_results_fetch(sport_key: str, batch: int):
    """
    Save results in 3 batches (Mon/Tue/Wed) using a cached leaderboard.
    Batch 1: our 15 picks/longshots + 5 best others = 20
    Batch 2: next 20
    Batch 3: next 10
    """
    from scorer import calculate_scores, get_weekly_selections, save_result

    print(f"\n[Batch {batch}] Results fetch for {sport_key}...")

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
    cached = cur.fetchone()[0]

    if cached == 0:
        print("  Fetching leaderboard from Slash Golf...")
        raw = fetch_tournament_results(sport_key)
        if not raw:
            print("  No results — skipping.")
            con.close()
            return
        ts = datetime.now(timezone.utc).isoformat()
        for r in raw:
            if r.get("player"):
                cur.execute("""
                    INSERT OR IGNORE INTO leaderboard_cache
                    (sport_key, player, finish, earnings, fetched_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (sport_key, r["player"], r.get("finish", ""), r.get("earnings", ""), ts))
        con.commit()
        print(f"  Cached {len(raw)} results")
    else:
        print(f"  Using cached leaderboard ({cached} players)")

    scores   = calculate_scores(sport_key)
    priority = get_weekly_selections(scores)

    cur.execute("SELECT player FROM results WHERE sport_key=?", (sport_key,))
    done = {r[0] for r in cur.fetchall()}

    cur.execute("SELECT player, finish, earnings FROM leaderboard_cache WHERE sport_key=?", (sport_key,))
    cache = {r[0]: {"finish": r[1], "earnings": r[2]} for r in cur.fetchall()}
    con.close()

    batch_size = {1: 20, 2: 20, 3: 10}.get(batch, 20)
    save_list  = []

    for player in priority:
        if player not in done and player in cache:
            save_list.append((player, cache[player]))
        if len(save_list) >= batch_size:
            break

    if len(save_list) < batch_size:
        def fsort(item):
            try:
                return int(item[1]["finish"].replace("T", "").replace("=", ""))
            except Exception:
                return 999
        others = sorted(
            [(p, d) for p, d in cache.items() if p not in done and p not in {x for x, _ in save_list}],
            key=fsort
        )
        save_list.extend(others[:batch_size - len(save_list)])

    saved = 0
    for player, data in save_list:
        save_result(
            sport_key=sport_key, player=player,
            finish=data["finish"], payout=data["earnings"],
            notes=f"auto-fetched batch {batch}",
        )
        saved += 1

    print(f"  Batch {batch} done — saved {saved} results")
