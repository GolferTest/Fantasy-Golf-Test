# ⛳ Golf Sharp Money Tracker

Automatically tracks betting line movement across Winner, Top 5, and Top 10 markets
for all active PGA Tour events. Scores players by implied probability movement,
sends you a formatted email every Wednesday at 2 PM with your top 5 sharp-money picks.

---

## How It Works

**Every 2 hours**, the tracker:
1. Calls The Odds API once to discover all currently active PGA events automatically
2. Pulls Winner, Top 5, and Top 10 outright odds for each event
3. Stores a snapshot — the very first snapshot for any player becomes their opening line

**Every Wednesday at 2:00 PM (Pacific)**, you get an email with:
- Top 5 players whose odds have improved in **all 3 markets** since lines opened
- Ranked by weighted composite score

**No manual updates needed** — new tournaments are detected automatically when they appear.

---

## Scoring Formula

```
score = (winner_move × X) + (top5_move × Y) + (top10_move × Z)
```

Where `move = current_implied_% − opening_implied_%`

A **positive** move = odds shortened = sharp money signal.

**To qualify** for Wednesday picks, a player must show positive movement in **ALL THREE** buckets. Top 5 qualifiers by score are selected.

Adjust X, Y, Z live on the dashboard without redeploying.

---

## API Request Budget

| Action | Requests |
|--------|----------|
| Event discovery (per pull) | 1 |
| Odds per event × 3 markets | 3 |
| **Total per 2-hour pull** | **~4** |
| **Total per week** | **~48** |

Free tier = 500 requests/month. You'll use ~192/month. Plenty of headroom.

---

## Files

| File | Purpose |
|------|---------|
| `tracker.py` | Auto-discovers PGA events, pulls odds, stores snapshots |
| `scorer.py` | Calculates movement scores, finds top 5 picks |
| `scheduler.py` | Runs tracker every 2 hours, sends Wednesday 2 PM email |
| `server.py` | Flask API + serves the dashboard |
| `dashboard.html` | Visual dashboard with charts and event switcher |
| `requirements.txt` | Python dependencies |
| `Procfile` | Railway process config |

---

## Step-by-Step Setup

### 1. Get Your Odds API Key (free)

1. Go to https://the-odds-api.com and sign up
2. Copy your API key from the dashboard

---

### 2. Set Up Gmail App Password (free, no third-party accounts)

This lets the script send email from your Gmail without using your real password.

1. Go to your Google Account → **Security**
2. Make sure **2-Step Verification** is turned on
3. Go to **App Passwords** (search for it in the Security page)
4. Create a new App Password → name it "Golf Tracker"
5. Copy the 16-character password (format: `xxxx xxxx xxxx xxxx`)

> You can send TO any email address — it doesn't have to be the same Gmail.

---

### 3. Deploy to Railway

1. **Push to GitHub** — create a free repo at github.com, add all these files, push.

2. **Create a Railway project** at https://railway.app
   - New Project → Deploy from GitHub repo → select your repo

3. **Set environment variables** in Railway → your project → Variables tab:
   ```
   ODDS_API_KEY      = your_odds_api_key
   EMAIL_SENDER      = you@gmail.com
   EMAIL_PASSWORD    = xxxx xxxx xxxx xxxx   ← Gmail App Password
   EMAIL_RECEIVER    = you@gmail.com          ← where picks get sent (can be any email)
   DB_PATH           = /data/golf_odds.db
   ```

4. **Volume is already configured** — your database at `/data/golf_odds.db` persists through all code pushes and redeploys. Data will not reset when you update files.

5. Railway will detect the `Procfile` and run both the web server and scheduler automatically.

6. Click the Railway-generated URL to open your live dashboard.

---

### 4. Timezone Note for Wednesday Email

The scheduler fires at **21:00 UTC = 2:00 PM Pacific Daylight Time (PDT, UTC-7)**.

If you're in a different timezone, edit this line in `scheduler.py`:
```python
schedule.every().wednesday.at("21:00").do(wednesday_picks_job)
```

| Your Timezone | UTC offset | Change to |
|---------------|-----------|-----------|
| Pacific (PDT, summer) | UTC-7 | `"21:00"` ← default |
| Pacific (PST, winter) | UTC-8 | `"22:00"` |
| Mountain (MDT) | UTC-6 | `"20:00"` |
| Central (CDT) | UTC-5 | `"19:00"` |
| Eastern (EDT) | UTC-4 | `"18:00"` |

---

### 5. Adjust Weights

**On the dashboard**: change X, Y, Z in the Scoring Weights panel → click Recalculate. Instant, no redeploy.

**For the Wednesday email** (uses server-side defaults), edit `scorer.py`:
```python
WEIGHT_WINNER = 1.0   # X
WEIGHT_TOP5   = 1.0   # Y
WEIGHT_TOP10  = 1.0   # Z
```
Then push to GitHub — Railway redeploys automatically.

---

## Local Testing

```bash
pip install -r requirements.txt

export ODDS_API_KEY=your_key
export EMAIL_SENDER=you@gmail.com
export EMAIL_PASSWORD="xxxx xxxx xxxx xxxx"
export EMAIL_RECEIVER=you@gmail.com

# Pull one snapshot manually
python tracker.py

# Check picks
python scorer.py

# Run dashboard locally
python server.py
# → open http://localhost:8080
```

---

## Dashboard Features

- **Event switcher** in the header — if multiple PGA events are live, switch between them
- **All players** sorted by qualifying status then score
- **Green PICK badge** on qualifying players (all 3 buckets improving since open)
- **Click any player** → full line movement chart for all 3 markets
- **Toggle** between implied % and American odds display
- **Live weight adjustment** — tweak X, Y, Z and recalculate instantly
- **Auto-refreshes** every 5 minutes

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| No events showing | Lines may not be posted yet for this week; check back Monday |
| Email not sending | Verify App Password is correct; check Google account has 2FA enabled |
| "No qualifying picks" | Not enough movement yet — check back closer to Wednesday |
| Database resets | Make sure Railway volume is mounted at `/data` |
| Missing top5/top10 markets | Some books don't post these early — will populate as week progresses |
