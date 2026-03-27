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
PGA_KEYWORDS = ["golf_pga", "golf_masters", "golf_us_open", "golf_the_open", "golf_tour"]
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
        if any(kw in key for kw in PGA_KEYWORDS):
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
