"""
Golf Odds Scorer
Calculates each player's weekly score based on implied probability movement
across all 3 buckets since opening lines.

Scoring formula:
  score = (winner_move * X) + (top5_move * Y) + (top10_move * Z)

Where move = current_implied_pct - opening_implied_pct
A POSITIVE move means odds shortened = more likely to happen = sharp money signal.

Now supports multiple simultaneous PGA events.
"""

import sqlite3
from dataclasses import dataclass

# ── Weighting coefficients — adjust these to tune your rankings ──────────────
WEIGHT_WINNER = float(1.0)   # X
WEIGHT_TOP5   = float(1.0)   # Y
WEIGHT_TOP10  = float(1.0)   # Z
# ─────────────────────────────────────────────────────────────────────────────

import os
DB_PATH = os.environ.get("DB_PATH", "golf_odds.db")


@dataclass
class PlayerScore:
    player:         str
    sport_key:      str
    event_title:    str
    winner_open:    float
    winner_current: float
    winner_move:    float
    top5_open:      float
    top5_current:   float
    top5_move:      float
    top10_open:     float
    top10_current:  float
    top10_move:     float
    score:          float
    qualifies:      bool   # True only if ALL 3 buckets improved since opening


def get_active_events(cur) -> list[dict]:
    """Return all events we have data for."""
    cur.execute("SELECT sport_key, title FROM events ORDER BY first_seen DESC")
    return [{"key": r[0], "title": r[1]} for r in cur.fetchall()]


def get_latest_implied(cur, sport_key: str, player: str, market: str) -> float | None:
    cur.execute("""
        SELECT implied_pct FROM snapshots
        WHERE sport_key=? AND player=? AND market=?
        ORDER BY ts DESC LIMIT 1
    """, (sport_key, player, market))
    row = cur.fetchone()
    return row[0] if row else None


def get_opening_implied(cur, sport_key: str, player: str, market: str) -> float | None:
    cur.execute("""
        SELECT implied_pct FROM opening_lines
        WHERE sport_key=? AND player=? AND market=?
    """, (sport_key, player, market))
    row = cur.fetchone()
    return row[0] if row else None


def get_latest_american(cur, sport_key: str, player: str, market: str) -> int | None:
    cur.execute("""
        SELECT american_odds FROM snapshots
        WHERE sport_key=? AND player=? AND market=?
        ORDER BY ts DESC LIMIT 1
    """, (sport_key, player, market))
    row = cur.fetchone()
    return row[0] if row else None


def calculate_scores(sport_key: str = None) -> list[PlayerScore]:
    """
    Score every player who has data in all 3 markets.
    If sport_key is provided, scores only that event.
    Otherwise scores the most recently seen event.
    Returns list sorted by qualifying status then score descending.
    """
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # Determine which event to score
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

    # Get all players in the winner market for this event
    cur.execute("""
        SELECT DISTINCT player FROM opening_lines
        WHERE sport_key=? AND market='winner'
    """, (sport_key,))
    players = [row[0] for row in cur.fetchall()]

    results = []
    for player in players:
        w_open   = get_opening_implied(cur, sport_key, player, "winner")
        t5_open  = get_opening_implied(cur, sport_key, player, "top5")
        t10_open = get_opening_implied(cur, sport_key, player, "top10")

        w_cur    = get_latest_implied(cur, sport_key, player, "winner")
        t5_cur   = get_latest_implied(cur, sport_key, player, "top5")
        t10_cur  = get_latest_implied(cur, sport_key, player, "top10")

        # Winner data is required — skip if missing
        if w_open is None or w_cur is None:
            continue

        w_move = w_cur - w_open

        # Top5 and Top10 may not be posted yet — treat as unavailable (None)
        # rather than blocking the player from showing up in the table.
        # They will populate automatically once the market goes live.
        t5_move  = (t5_cur  - t5_open)  if (t5_open  is not None and t5_cur  is not None) else None
        t10_move = (t10_cur - t10_open) if (t10_open is not None and t10_cur is not None) else None

        # A player qualifies only when ALL 3 markets are available AND all improved.
        # If top5/top10 are not yet posted, qualifies=False (not penalized, just pending).
        if t5_move is not None and t10_move is not None:
            qualifies = w_move > 0 and t5_move > 0 and t10_move > 0
        else:
            qualifies = False  # markets not fully posted yet

        # Score uses whatever markets are available; missing markets contribute 0
        score = (w_move * WEIGHT_WINNER) +                 ((t5_move  or 0.0) * WEIGHT_TOP5) +                 ((t10_move or 0.0) * WEIGHT_TOP10)

        results.append(PlayerScore(
            player         = player,
            sport_key      = sport_key,
            event_title    = event_title,
            winner_open    = w_open,
            winner_current = w_cur,
            winner_move    = w_move,
            top5_open      = t5_open      if t5_open  is not None else 0.0,
            top5_current   = t5_cur       if t5_cur   is not None else 0.0,
            top5_move      = t5_move      if t5_move  is not None else 0.0,
            top10_open     = t10_open     if t10_open is not None else 0.0,
            top10_current  = t10_cur      if t10_cur  is not None else 0.0,
            top10_move     = t10_move     if t10_move is not None else 0.0,
            score          = score,
            qualifies      = qualifies,
        ))

    con.close()
    results.sort(key=lambda p: (not p.qualifies, -p.score))
    return results


def top5_picks(scores: list[PlayerScore]) -> list[PlayerScore]:
    """Return top 5 qualifying players."""
    return [p for p in scores if p.qualifies][:5]


def get_history(sport_key: str, player: str, market: str) -> list[dict]:
    """Return full snapshot history for a player+market+event (for charting)."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT ts, american_odds, implied_pct FROM snapshots
        WHERE sport_key=? AND player=? AND market=?
        ORDER BY ts ASC
    """, (sport_key, player, market))
    rows = cur.fetchall()
    con.close()
    return [{"ts": r[0], "american": r[1], "implied": r[2]} for r in rows]


def get_all_events() -> list[dict]:
    """Return all events in the DB for the dashboard event switcher."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT sport_key, title, first_seen, last_seen FROM events ORDER BY last_seen DESC")
    rows = cur.fetchall()
    con.close()
    return [{"key": r[0], "title": r[1], "first_seen": r[2], "last_seen": r[3]} for r in rows]


if __name__ == "__main__":
    scores = calculate_scores()
    picks  = top5_picks(scores)
    if scores:
        print(f"\nEvent: {scores[0].event_title}")
    print("\n=== TOP 5 PICKS ===")
    for i, p in enumerate(picks, 1):
        print(f"{i}. {p.player:30s}  score={p.score:+.2f}  "
              f"W:{p.winner_move:+.2f}%  T5:{p.top5_move:+.2f}%  T10:{p.top10_move:+.2f}%")
