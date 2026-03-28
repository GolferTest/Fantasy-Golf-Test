"""
Golf Odds Scheduler
- Pulls odds every 2 hours
- Sends Wednesday 2:00 PM (Pacific) picks via email — no Twilio needed
- Uses Gmail SMTP with an App Password (free, no third-party accounts)

On Railway: set start command → python scheduler.py
"""

import os
import time
import smtplib
import schedule
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import pytz

from tracker import run_snapshot
from scorer  import calculate_scores, top10_picks

# ── Email configuration ───────────────────────────────────────────────────────
# Use your Gmail address + a Gmail App Password (NOT your regular password).
# Setup: Google Account → Security → 2-Step Verification → App Passwords → create one
EMAIL_SENDER   = os.environ.get("EMAIL_SENDER",   "you@gmail.com")       # your Gmail
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD", "xxxx xxxx xxxx xxxx") # Gmail App Password
EMAIL_RECEIVER = os.environ.get("EMAIL_RECEIVER", "you@gmail.com")       # where to send picks

LOCAL_TZ = pytz.timezone("America/Los_Angeles")  # change if not Pacific time
# ─────────────────────────────────────────────────────────────────────────────


def send_email(subject: str, body_text: str, body_html: str = None):
    """Send email via Gmail SMTP."""
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = EMAIL_SENDER
        msg["To"]      = EMAIL_RECEIVER

        msg.attach(MIMEText(body_text, "plain"))
        if body_html:
            msg.attach(MIMEText(body_html, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())

        print(f"Email sent to {EMAIL_RECEIVER}")
    except Exception as e:
        print(f"EMAIL ERROR: {e}")


def build_email(picks, event_title: str = "This Week's Tournament") -> tuple[str, str, str]:  # noqa
    """Returns (subject, plain_text, html) for the Wednesday picks email."""
    subject = f"⛳ Golf Sharp Money Picks — {event_title}"

    if not picks:
        plain = (
            f"Golf Sharp Money Tracker\n\n"
            f"No qualifying picks this week for {event_title}.\n"
            f"A player must show positive movement in ALL THREE markets "
            f"(Winner, Top 5, Top 10) since lines opened.\n\n"
            f"Check your dashboard for full movement data."
        )
        html = f"""
        <div style="font-family:sans-serif;max-width:600px;margin:0 auto">
          <h2 style="color:#00b37a">⛳ Sharp Money Tracker</h2>
          <p><strong>{event_title}</strong></p>
          <p style="color:#666">No qualifying picks this week — all 3 buckets must improve from opening lines.</p>
          <p>Check your dashboard for the full movement breakdown.</p>
        </div>
        """
        return subject, plain, html

    # Plain text
    plain_lines = [
        f"⛳ GOLF SHARP MONEY PICKS",
        f"{event_title}",
        f"Wednesday {datetime.now(LOCAL_TZ).strftime('%B %d, %Y — %I:%M %p %Z')}",
        "",
        "Criteria: ALL 3 markets improving since lines opened",
        "Ranked by: (Winner Δ × X) + (Top5 Δ × Y) + (Top10 Δ × Z)",
        "─" * 50,
        "",
    ]
    for i, p in enumerate(picks, 1):
        plain_lines += [
            f"{i}. {p.player}",
            f"   Score:  {p.score:+.2f}",
            f"   Winner: {p.winner_move:+.2f}%  (open: {p.winner_open:.1f}% → now: {p.winner_current:.1f}%)",
            f"   Top 5:  {p.top5_move:+.2f}%  (open: {p.top5_open:.1f}% → now: {p.top5_current:.1f}%)",
            f"   Top 10: {p.top10_move:+.2f}%  (open: {p.top10_open:.1f}% → now: {p.top10_current:.1f}%)",
            "",
        ]
    plain_lines.append("Good luck! 🏌️")
    plain = "\n".join(plain_lines)

    # HTML
    rows_html = ""
    for i, p in enumerate(picks, 1):
        color = "#00b37a"
        rows_html += f"""
        <tr style="border-bottom:1px solid #eee">
          <td style="padding:14px 8px;font-size:20px;color:#999">{i}</td>
          <td style="padding:14px 8px;font-weight:700;font-size:16px">{p.player}</td>
          <td style="padding:14px 8px;text-align:center">
            <span style="color:{color};font-weight:700">{p.winner_move:+.2f}%</span><br>
            <small style="color:#999">{p.winner_open:.1f}→{p.winner_current:.1f}</small>
          </td>
          <td style="padding:14px 8px;text-align:center">
            <span style="color:{color};font-weight:700">{p.top5_move:+.2f}%</span><br>
            <small style="color:#999">{p.top5_open:.1f}→{p.top5_current:.1f}</small>
          </td>
          <td style="padding:14px 8px;text-align:center">
            <span style="color:{color};font-weight:700">{p.top10_move:+.2f}%</span><br>
            <small style="color:#999">{p.top10_open:.1f}→{p.top10_current:.1f}</small>
          </td>
          <td style="padding:14px 8px;text-align:right;font-family:monospace;font-weight:700">
            {p.score:+.2f}
          </td>
        </tr>
        """

    html = f"""
    <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
                max-width:680px;margin:0 auto;background:#fff;border-radius:12px;
                overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.08)">

      <div style="background:#0a0e14;padding:28px 32px">
        <div style="color:#00e5a0;font-size:28px;font-weight:900;letter-spacing:2px">
          ⛳ SHARP GOLF
        </div>
        <div style="color:#718096;font-size:13px;margin-top:4px">
          {event_title} &nbsp;·&nbsp;
          Wednesday {datetime.now(LOCAL_TZ).strftime('%B %d, %Y')}
        </div>
      </div>

      <div style="padding:24px 32px">
        <p style="color:#4a5568;font-size:13px;margin:0 0 20px">
          Players with <strong>positive movement in all 3 markets</strong> since lines opened,
          ranked by weighted composite score.
        </p>

        <table style="width:100%;border-collapse:collapse">
          <thead>
            <tr style="background:#f7f9fc">
              <th style="padding:10px 8px;text-align:left;font-size:11px;color:#999;letter-spacing:1px">#</th>
              <th style="padding:10px 8px;text-align:left;font-size:11px;color:#999;letter-spacing:1px">PLAYER</th>
              <th style="padding:10px 8px;text-align:center;font-size:11px;color:#999;letter-spacing:1px">WINNER Δ</th>
              <th style="padding:10px 8px;text-align:center;font-size:11px;color:#999;letter-spacing:1px">TOP 5 Δ</th>
              <th style="padding:10px 8px;text-align:center;font-size:11px;color:#999;letter-spacing:1px">TOP 10 Δ</th>
              <th style="padding:10px 8px;text-align:right;font-size:11px;color:#999;letter-spacing:1px">SCORE</th>
            </tr>
          </thead>
          <tbody>
            {rows_html}
          </tbody>
        </table>

        <p style="margin:24px 0 0;font-size:12px;color:#aaa">
          Δ = change in implied probability since lines opened &nbsp;|&nbsp;
          Score = weighted sum of all 3 movements
        </p>
      </div>

      <div style="background:#f7f9fc;padding:16px 32px;font-size:12px;color:#999">
        Good luck this week 🏌️ &nbsp;|&nbsp; Sharp Golf Tracker
      </div>
    </div>
    """

    return subject, plain, html


def snapshot_job():
    """Pull a fresh odds snapshot every 2 hours."""
    print(f"\n[{datetime.now()}] Running 2-hour snapshot...")
    try:
        run_snapshot()
    except Exception as e:
        print(f"Snapshot error: {e}")


def wednesday_picks_job():
    """Send Wednesday 2 PM picks email."""
    now_local = datetime.now(LOCAL_TZ)
    if now_local.weekday() != 2:  # 2 = Wednesday
        return

    print(f"\n[{now_local}] Wednesday picks job running...")
    scores = calculate_scores()
    picks  = top10_picks(scores)
    event_title = scores[0].event_title if scores else "This Week's Tournament"

    subject, plain, html = build_email(picks, event_title)
    print(plain)
    send_email(subject, plain, html)


def main():
    print("Golf Sharp Money Tracker starting...")

    # Immediate snapshot on startup
    snapshot_job()

    # Every 2 hours
    schedule.every(2).hours.do(snapshot_job)

    # Wednesday 2:00 PM Pacific
    # Railway runs UTC. PDT (summer) = UTC-7 → 21:00 UTC. PST (winter) = UTC-8 → 22:00 UTC.
    # Update the time below if you're in a different timezone:
    #   Central Daylight (UTC-5): "19:00"
    #   Eastern Daylight (UTC-4): "18:00"
    schedule.every().wednesday.at("21:00").do(wednesday_picks_job)

    print("Scheduler running. Pulls every 2 hours. Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
