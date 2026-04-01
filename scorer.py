"""
Golf Odds Scorer
- Players qualify only when ALL 3 markets have data AND all 3 improved from opening
- Qualifying also requires sum of opening American odds <= +5000 (not a longshot)
- Ranked by biggest delta in implied % sum across all 3 buckets
- Top 10 qualifiers returned for Wednesday banner
- Results table lets you record actual finishes after tournament ends
"""

import os
import sqlite3
from dataclasses import dataclass

DB_PATH = os.environ.get("DB_PATH", "golf_odds.db")


@dataclass
class PlayerScore:
    player:         str
    sport_key:      str
    event_title:    str
    # Composite columns
    baseline:       float   # sum of opening implied % across available markets
    current_total:  float   # sum of current implied % across available markets
    delta:          float   # current_total - baseline (ranking metric)
    all_markets:    bool    # True when all 3 markets have opening + current data
    # Per-market
    winner_open:    float
    winner_current: float
    winner_move:    float
    top5_open:      float
    top5_current:   float
    top5_move:      float
    top10_open:     float
    top10_current:  float
    top10_move:     float
    # Opening American odds (for +5000 filter)
    winner_open_american:    int
    top5_open_american:      int
    top10_open_american:     int
    winner_current_american: int
    top5_current_american:   int
    top10_current_american:  int
    score:          float
    qualifies:      bool    # True = all criteria met AND within +5000
    is_longshot:    bool    # True = all criteria met BUT winner odds > +5000


def init_results_table():
    """Create results table if it doesn't exist (called once on startup)."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS results (
            sport_key   TEXT NOT NULL,
            player      TEXT NOT NULL,
            finish      TEXT NOT NULL,   -- e.g. '1', '2', 'T5', 'MC', 'WD'
            payout      TEXT,            -- optional payout from your fantasy league
            notes       TEXT,
            updated_at  TEXT NOT NULL,
            PRIMARY KEY (sport_key, player)
        )
    """)
    con.commit()
    con.close()


def get_latest_implied(cur, sport_key, player, market):
    cur.execute("""
        SELECT implied_pct FROM snapshots
        WHERE sport_key=? AND player=? AND market=?
        ORDER BY ts DESC LIMIT 1
    """, (sport_key, player, market))
    row = cur.fetchone()
    return row[0] if row else None


def get_opening_implied(cur, sport_key, player, market):
    cur.execute("""
        SELECT implied_pct FROM opening_lines
        WHERE sport_key=? AND player=? AND market=?
    """, (sport_key, player, market))
    row = cur.fetchone()
    return row[0] if row else None


def get_opening_american(cur, sport_key, player, market):
    cur.execute("""
        SELECT american_odds FROM opening_lines
        WHERE sport_key=? AND player=? AND market=?
    """, (sport_key, player, market))
    row = cur.fetchone()
    return row[0] if row else None


def get_latest_american(cur, sport_key, player, market):
    cur.execute("""
        SELECT american_odds FROM snapshots
        WHERE sport_key=? AND player=? AND market=?
        ORDER BY ts DESC LIMIT 1
    """, (sport_key, player, market))
    row = cur.fetchone()
    return row[0] if row else None


def american_sum_within_limit(w_am, t5_am, t10_am, limit=5000) -> bool:
    """
    Convert each American odd to a decimal price, sum them, check if
    the combined American equivalent is <= +limit.
    We simply check that each individual winner odds <= limit as a proxy —
    the sum-of-implied filter already handles relative weighting.
    Using winner odds only as the primary longshot gate since it's most liquid.
    """
    if w_am is None:
        return False
    # Positive American odds: +5000 means 50:1. We gate on winner odds only.
    if w_am > 0 and w_am > limit:
        return False
    return True


def calculate_scores(sport_key: str = None) -> list[PlayerScore]:
    """
    Score every player. Returns list sorted:
      1. Qualifying players (all 3 markets improving, within odds limit) by delta desc
      2. Pending players (missing market data) by winner delta desc
      3. Non-qualifying (lines moved wrong way) by delta desc
    """
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    if sport_key is None:
        cur.execute("SELECT sport_key, title FROM events ORDER BY last_seen DESC LIMIT 1")
        row = cur.fetchone()
        if not row:
            con.close()
            return []
        sport_key, event_title = row
    else:
        cur.execute("SELECT title FROM events WHERE sport_key=?", (sport_key,))
        row = cur.fetchone()
        event_title = row[0] if row else sport_key

    cur.execute("""
        SELECT DISTINCT player FROM opening_lines
        WHERE sport_key=? AND market='winner'
    """, (sport_key,))
    players = [r[0] for r in cur.fetchall()]

    results = []
    for player in players:
        w_open   = get_opening_implied(cur, sport_key, player, "winner")
        t5_open  = get_opening_implied(cur, sport_key, player, "top5")
        t10_open = get_opening_implied(cur, sport_key, player, "top10")

        w_cur    = get_latest_implied(cur, sport_key, player, "winner")
        t5_cur   = get_latest_implied(cur, sport_key, player, "top5")
        t10_cur  = get_latest_implied(cur, sport_key, player, "top10")

        w_am_open    = get_opening_american(cur, sport_key, player, "winner") or 0
        t5_am_open   = get_opening_american(cur, sport_key, player, "top5")  or 0
        t10_am_open  = get_opening_american(cur, sport_key, player, "top10") or 0
        w_am_cur     = get_latest_american(cur, sport_key, player, "winner") or 0
        t5_am_cur    = get_latest_american(cur, sport_key, player, "top5")   or 0
        t10_am_cur   = get_latest_american(cur, sport_key, player, "top10")  or 0

        if w_open is None or w_cur is None:
            continue

        w_move = w_cur - w_open

        # Markets available?
        has_t5  = t5_open  is not None and t5_cur  is not None
        has_t10 = t10_open is not None and t10_cur is not None
        all_markets = has_t5 and has_t10

        t5_move  = (t5_cur  - t5_open)  if has_t5  else None
        t10_move = (t10_cur - t10_open) if has_t10 else None

        # Composite values — only sum markets that have data
        t5_open_v  = t5_open  if has_t5  else 0.0
        t5_cur_v   = t5_cur   if has_t5  else 0.0
        t10_open_v = t10_open if has_t10 else 0.0
        t10_cur_v  = t10_cur  if has_t10 else 0.0

        # Implied % sums — used internally for delta tracking
        baseline_impl = w_open + t5_open_v + t10_open_v
        current_impl  = w_cur  + t5_cur_v  + t10_cur_v
        delta         = current_impl - baseline_impl
        score         = delta

        # American sums — displayed to user (e.g. +1000 + +330 + +170 = +1500)
        baseline      = w_am_open + (t5_am_open  if has_t5  else 0) + (t10_am_open  if has_t10 else 0)
        current_total = w_am_cur  + (t5_am_cur   if has_t5  else 0) + (t10_am_cur   if has_t10 else 0)

        # Qualify: all 3 markets present, all 3 improving, winner odds within limit
        if all_markets:
            all_positive = w_move > 0 and t5_move > 0 and t10_move > 0
            within_limit = american_sum_within_limit(w_am_open, t5_am_open, t10_am_open)
            qualifies    = all_positive and within_limit
            is_longshot  = all_positive and not within_limit  # meets criteria but odds > +5000
        else:
            qualifies   = False
            is_longshot = False  # pending — not enough data yet

        results.append(PlayerScore(
            player               = player,
            sport_key            = sport_key,
            event_title          = event_title,
            baseline             = baseline,
            current_total        = current_total,
            delta                = delta,
            all_markets          = all_markets,
            winner_open          = w_open,
            winner_current       = w_cur,
            winner_move          = w_move,
            top5_open            = t5_open_v,
            top5_current         = t5_cur_v,
            top5_move            = t5_move  if t5_move  is not None else 0.0,
            top10_open           = t10_open_v,
            top10_current        = t10_cur_v,
            top10_move           = t10_move if t10_move is not None else 0.0,
            winner_open_american    = w_am_open,
            top5_open_american      = t5_am_open,
            top10_open_american     = t10_am_open,
            winner_current_american = w_am_cur,
            top5_current_american   = t5_am_cur,
            top10_current_american  = t10_am_cur,
            score                   = score,
            qualifies            = qualifies,
            is_longshot          = is_longshot,
        ))

    con.close()

    # Sort: qualifiers first, then longshots, then pending, then rest
    def sort_key(p):
        if p.qualifies:
            return (0, -p.delta)
        elif p.is_longshot:
            return (1, -p.delta)
        elif not p.all_markets:
            return (2, -p.winner_move)
        else:
            return (3, -p.delta)

    results.sort(key=sort_key)
    return results


def top10_picks(scores: list[PlayerScore]) -> list[PlayerScore]:
    """Return top 10 qualifying players (all 3 buckets improving, within odds limit)."""
    return [p for p in scores if p.qualifies][:10]


def top5_longshots(scores: list[PlayerScore]) -> list[PlayerScore]:
    """Return top 5 longshot players — meet all criteria but winner odds > +5000."""
    return [p for p in scores if p.is_longshot][:5]


def get_weekly_selections(scores: list[PlayerScore]) -> list[str]:
    """
    Return ordered list of players to fetch results for, in priority order:
      1. Top 10 picks (our main selections)
      2. Top 5 longshots
      3. Next best qualifiers/movers not already included
    Used by the staggered results fetcher to know who to prioritize.
    """
    picks     = top10_picks(scores)
    longshots = top5_longshots(scores)
    selected  = {p.player for p in picks} | {p.player for p in longshots}

    # Fill remaining slots from best remaining movers
    others = [p for p in scores if p.player not in selected]

    ordered = [p.player for p in picks] + [p.player for p in longshots] + [p.player for p in others]
    return ordered


def get_history(sport_key: str, player: str, market: str) -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT ts, american_odds, implied_pct, COALESCE(book_count, 0)
        FROM snapshots
        WHERE sport_key=? AND player=? AND market=?
        ORDER BY ts ASC
    """, (sport_key, player, market))
    rows = cur.fetchall()
    con.close()
    return [{"ts": r[0], "american": r[1], "implied": round(r[2], 1), "books": r[3]} for r in rows]


def get_all_events() -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT sport_key, title, first_seen, last_seen FROM events ORDER BY last_seen DESC")
    rows = cur.fetchall()
    con.close()
    return [{"key": r[0], "title": r[1], "first_seen": r[2], "last_seen": r[3]} for r in rows]


def get_results(sport_key: str) -> dict:
    """Return {player: {finish, payout, notes}} for a given event."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT player, finish, payout, notes FROM results WHERE sport_key=?
    """, (sport_key,))
    rows = cur.fetchall()
    con.close()
    return {r[0]: {"finish": r[1], "payout": r[2], "notes": r[3]} for r in rows}


def save_result(sport_key: str, player: str, finish: str, payout: str = None, notes: str = None):
    """Insert or update a player's tournament result."""
    from datetime import datetime, timezone
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO results (sport_key, player, finish, payout, notes, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(sport_key, player) DO UPDATE SET
            finish=excluded.finish,
            payout=excluded.payout,
            notes=excluded.notes,
            updated_at=excluded.updated_at
    """, (sport_key, player, finish, payout, notes, datetime.now(timezone.utc).isoformat()))
    con.commit()
    con.close()


if __name__ == "__main__":
    scores    = calculate_scores()
    picks     = top10_picks(scores)
    longshots = top5_longshots(scores)
    if scores:
        print(f"\nEvent: {scores[0].event_title}")
    print("\n=== TOP 10 PICKS ===")
    for i, p in enumerate(picks, 1):
        print(f"{i:2}. {p.player:30s}  delta={p.delta:+.2f}%  W:{p.winner_open_american:+d}")
    print("\n=== TOP 5 LONGSHOTS (odds > +5000) ===")
    for i, p in enumerate(longshots, 1):
        print(f"{i:2}. {p.player:30s}  delta={p.delta:+.2f}%  W:{p.winner_open_american:+d}")


# ── Results Tracker / P&L ────────────────────────────────────────────────────

def parse_finish(finish: str) -> int | None:
    """Parse finish string to integer position. Returns None if MC/WD/DQ."""
    if not finish:
        return None
    f = finish.upper().strip().lstrip("T=")
    if f in ("MC", "CUT", "WD", "DQ", "DNS", "MDF"):
        return None
    try:
        return int(f)
    except ValueError:
        return None


def american_payout(american_odds: int, bet: float = 25.0) -> float:
    """Gross profit on a winning bet."""
    if american_odds > 0:
        return round(bet * american_odds / 100, 2)
    else:
        return round(bet * 100 / abs(american_odds), 2)


def calc_payout(finish_pos: int | None,
                winner_odds: int, top5_odds: int, top10_odds: int,
                bet: float = 25.0) -> dict:
    """
    Calculate P&L for $25 on each of winner, top5, top10 ($75 total).
    Returns {winner_pl, top5_pl, top10_pl, total_pl}
    top5 pays if finish <= 5, top10 pays if finish <= 10, winner pays if finish == 1.
    """
    BET = bet

    if finish_pos is None:
        # MC / WD — all three lose
        return {"winner": -BET, "top5": -BET, "top10": -BET, "total": -BET * 3}

    w_pl  = american_payout(winner_odds,  BET) if finish_pos == 1  else -BET
    t5_pl = american_payout(top5_odds,   BET) if finish_pos <= 5  else -BET
    t10_pl= american_payout(top10_odds,  BET) if finish_pos <= 10 else -BET

    return {
        "winner": round(w_pl, 2),
        "top5":   round(t5_pl, 2),
        "top10":  round(t10_pl, 2),
        "total":  round(w_pl + t5_pl + t10_pl, 2),
    }


def get_event_tracker(sport_key: str) -> dict:
    """
    For one event return full tracker data:
    {
      event_title, sport_key, start_date,
      picks: [{player, delta, winner_open_american, finish, payout_pl}],
      top10_finishers: [{player, finish, delta}],
      event_pl: float,
    }
    Picks = players with delta >= 4.0% (qualifies or longshot).
    Bet $25 on each pick's winner odds at opening line.
    """
    scores  = calculate_scores(sport_key)
    results = get_results(sport_key)

    # Picks: qualified or longshot with delta >= 4.0%
    picks = [p for p in scores if (p.qualifies or p.is_longshot) and p.delta >= 4.0]

    pick_rows = []
    event_pl  = 0.0
    for p in picks:
        res        = results.get(p.player, {})
        finish_str = res.get("finish", "") if res else ""
        pos        = parse_finish(finish_str)
        pl         = calc_payout(pos,
                                 p.winner_open_american,
                                 p.top5_open_american,
                                 p.top10_open_american) if finish_str else None
        if pl is not None:
            event_pl += pl["total"]
        pick_rows.append({
            "player":               p.player,
            "delta":                round(p.delta, 2),
            "winner_open_american": p.winner_open_american,
            "top5_open_american":   p.top5_open_american,
            "top10_open_american":  p.top10_open_american,
            "is_longshot":          p.is_longshot,
            "finish":               finish_str,
            "pl":                   pl,
        })

    # Top 10 finishers from results table with their delta
    score_map = {p.player: p for p in scores}
    top10 = []
    for player, res in results.items():
        pos = parse_finish(res.get("finish", ""))
        if pos is not None and pos <= 10:
            sc = score_map.get(player)
            top10.append({
                "player": player,
                "finish": res.get("finish", ""),
                "delta":  round(sc.delta, 2) if sc else None,
                "was_pick": player in {r["player"] for r in pick_rows},
            })
    top10.sort(key=lambda x: parse_finish(x["finish"]) or 99)

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT title, start_date FROM events WHERE sport_key=?", (sport_key,))
    row = cur.fetchone()
    con.close()
    title      = row[0] if row else sport_key
    start_date = row[1] if row else ""

    return {
        "sport_key":       sport_key,
        "event_title":     title,
        "start_date":      start_date,
        "picks":           pick_rows,
        "top10_finishers": top10,
        "event_pl":        round(event_pl, 2),
        "picks_settled":   bool(pick_rows) and all(r["pl"] is not None for r in pick_rows),
    }


def get_yearly_tracker() -> dict:
    """
    Return tracker data for all completed events this year, plus yearly P&L.
    """
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT sport_key, title, start_date FROM events
        ORDER BY COALESCE(start_date, last_seen) ASC
    """)
    all_events = cur.fetchall()
    con.close()

    events_data = []
    yearly_pl   = 0.0

    for sport_key, title, start_date in all_events:
        data = get_event_tracker(sport_key)
        if data["picks"]:  # only include events where we had picks
            events_data.append(data)
            if data["picks_settled"]:
                for ev_pick in data["picks"]:
                    if ev_pick["pl"] is not None:
                        yearly_pl += ev_pick["pl"]["total"]

    return {
        "events":    events_data,
        "yearly_pl": round(yearly_pl, 2),
    }
