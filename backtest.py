"""
Backtest script — fetches historical opening/closing odds from DataGolf
and populates the DB as if the tracker had been running all season.

Run on Railway: add a one-off job, or hit /api/backtest endpoint.

Events covered:
  WM Phoenix Open          event_id=3
  AT&T Pebble Beach        event_id=5
  The Genesis Invitational event_id=7
  Cognizant Classic        event_id=10
  Arnold Palmer Invitational event_id=9
  THE PLAYERS Championship event_id=11
  Valspar Championship     event_id=475
  Texas Childrens Houston  event_id=20

For each event:
  - Fetch opening lines (earliest available) from 6 books x 3 markets
  - Fetch closing lines (latest available) from 6 books x 3 markets
  - Save opening as is_opening=1 snapshot
  - Save closing as latest snapshot (so delta = closing - opening)
  - Fetch results from Slash Golf and save
"""

import os
import sqlite3
import requests
import time
from datetime import datetime, timezone

DATAGOLF_KEY  = os.environ.get("DATAGOLF_KEY", "59e54d9d17d96388062053ddd0c7")
RAPIDAPI_KEY  = os.environ.get("RAPIDAPI_KEY", "")
DB_PATH       = os.environ.get("DB_PATH", "golf_odds.db")
BASE          = "https://feeds.datagolf.com"

BOOKS = ["draftkings", "fanduel", "betmgm", "bet365", "caesars", "pinnacle"]

MARKETS = {
    "winner": "win",
    "top5":   "top_5",
    "top10":  "top_10",
}

# Past 2026 events to backfill
EVENTS = [
    {"event_id": "3",   "year": "2026", "name": "WM Phoenix Open",                        "start_date": "2026-02-05", "sport_key": "datagolf_wm_phoenix_open_2026"},
    {"event_id": "5",   "year": "2026", "name": "AT&T Pebble Beach Pro-Am",               "start_date": "2026-02-12", "sport_key": "datagolf_at&t_pebble_beach_pro-am_2026"},
    {"event_id": "7",   "year": "2026", "name": "The Genesis Invitational",               "start_date": "2026-02-19", "sport_key": "datagolf_the_genesis_invitational_2026"},
    {"event_id": "10",  "year": "2026", "name": "Cognizant Classic in The Palm Beaches",  "start_date": "2026-02-26", "sport_key": "datagolf_cognizant_classic_in_the_palm_beaches_2026"},
    {"event_id": "9",   "year": "2026", "name": "Arnold Palmer Invitational",             "start_date": "2026-03-05", "sport_key": "datagolf_arnold_palmer_invitational_2026"},
    {"event_id": "11",  "year": "2026", "name": "THE PLAYERS Championship",               "start_date": "2026-03-12", "sport_key": "datagolf_the_players_championship_2026"},
    {"event_id": "475", "year": "2026", "name": "Valspar Championship",                   "start_date": "2026-03-19", "sport_key": "datagolf_valspar_championship_2026"},
    {"event_id": "20",  "year": "2026", "name": "Texas Children's Houston Open",          "start_date": "2026-03-26", "sport_key": "datagolf_texas_childrens_houston_open_2026"},
]


def american_to_implied(american: int) -> float:
    if american > 0:
        return 100 / (american + 100) * 100
    else:
        return abs(american) / (abs(american) + 100) * 100


def fetch_historical_odds(event_id: str, year: str, market_dg: str, book: str) -> dict:
    """
    Fetch historical odds for one event/market/book from DataGolf.
    Returns {player_name: {"open": odds, "close": odds}}
    """
    url = (f"{BASE}/historical-odds/outrights"
           f"?tour=pga&event_id={event_id}&year={year}"
           f"&market={market_dg}&book={book}"
           f"&odds_format=american&file_format=json&key={DATAGOLF_KEY}")
    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            print(f"      [{book}] HTTP {r.status_code}")
            return {}
        data = r.json()
        result = {}
        for player in data.get("odds", []):
            name = player.get("player_name", "").strip()
            if not name:
                continue
            open_val  = player.get("open_odds")
            close_val = player.get("close_odds")
            if open_val is not None or close_val is not None:
                result[name] = {
                    "open":  int(str(open_val).replace("+",""))  if open_val  is not None else None,
                    "close": int(str(close_val).replace("+","")) if close_val is not None else None,
                }
        return result
    except Exception as e:
        print(f"      [{book}] error: {e}")
        return {}


def aggregate_odds(all_book_data: list[dict], price_key: str) -> dict:
    """
    Aggregate odds across books for a given price_key ('open' or 'close').
    Returns {player: avg_american_odds, ...}
    """
    player_prices = {}
    for book_data in all_book_data:
        for player, prices in book_data.items():
            val = prices.get(price_key)
            if val is not None:
                player_prices.setdefault(player, []).append(val)
    return {
        player: round(sum(prices) / len(prices))
        for player, prices in player_prices.items()
        if prices
    }


def save_backtest_event(con, sport_key: str, name: str, start_date: str,
                        market_label: str, open_odds: dict, close_odds: dict):
    """Save opening and closing snapshots for a historical event."""
    from tracker import american_to_implied as a2i, upsert_event
    cur = con.cursor()
    ts_open  = f"{start_date}T08:00:00+00:00"  # Sun night / Mon morning lines
    ts_close = f"{start_date[:8]}10T23:59:00+00:00"  # Wed closing

    all_players = set(open_odds.keys()) | set(close_odds.keys())

    for player in all_players:
        o_am = open_odds.get(player)
        c_am = close_odds.get(player)
        if o_am is None:
            continue

        o_imp = a2i(o_am)
        c_imp = a2i(c_am) if c_am is not None else o_imp
        c_am  = c_am if c_am is not None else o_am

        # Save opening line
        cur.execute("""
            INSERT OR IGNORE INTO opening_lines
            (sport_key, player, market, american_odds, implied_pct, ts)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (sport_key, player, market_label, o_am, o_imp, ts_open))

        # Save opening snapshot
        cur.execute("""
            INSERT OR IGNORE INTO snapshots
            (ts, sport_key, player, market, american_odds, implied_pct, is_opening, book_count)
            VALUES (?, ?, ?, ?, ?, ?, 1, 0)
        """, (ts_open, sport_key, player, market_label, o_am, o_imp))

        # Save closing snapshot (only if different from opening)
        if c_am != o_am:
            cur.execute("""
                INSERT OR IGNORE INTO snapshots
                (ts, sport_key, player, market, american_odds, implied_pct, is_opening, book_count)
                VALUES (?, ?, ?, ?, ?, ?, 0, 0)
            """, (ts_close, sport_key, player, market_label, c_am, c_imp))


def run_backtest(status_cb=None):
    """
    Main backtest runner. Fetches historical data and populates DB.
    status_cb: optional callback(msg) for streaming status updates.
    """
    from tracker import init_db, upsert_event

    def log(msg):
        print(msg)
        if status_cb:
            status_cb(msg)

    init_db()
    con = sqlite3.connect(DB_PATH)

    for event in EVENTS:
        sport_key  = event["sport_key"]
        name       = event["name"]
        event_id   = event["event_id"]
        year       = event["year"]
        start_date = event["start_date"]

        log(f"\n{'='*60}")
        log(f"Processing: {name} ({sport_key})")

        ts = datetime.now(timezone.utc).isoformat()
        commence = f"{start_date}T12:00:00+00:00"
        upsert_event(con, sport_key, name, ts, commence)
        con.commit()

        for market_label, market_dg in MARKETS.items():
            log(f"\n  Market: {market_label}")
            all_book_data = []
            for book in BOOKS:
                log(f"    Fetching {book}...")
                book_data = fetch_historical_odds(event_id, year, market_dg, book)
                all_book_data.append(book_data)
                time.sleep(0.5)  # respect 45 req/min rate limit

            open_odds  = aggregate_odds(all_book_data, "open")
            close_odds = aggregate_odds(all_book_data, "close")

            log(f"    Open:  {len(open_odds)} players")
            log(f"    Close: {len(close_odds)} players")

            if open_odds:
                save_backtest_event(con, sport_key, name, start_date,
                                    market_label, open_odds, close_odds)
                con.commit()

    con.close()
    log("\n✓ Backtest complete — all events loaded into DB")


if __name__ == "__main__":
    run_backtest()
