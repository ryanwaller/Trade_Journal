# SnapTrade → Notion Trading Journal

This service syncs your SnapTrade account activity into a Notion database as a trading journal. It supports both:
- **Automated sync** (via Railway cron, every 15 minutes)
- **Manual sync** (HTTP `POST /sync`)

## 1) Notion setup (new database)
Create a new Notion database with these properties and types:

- `Name` (Title)
- `Ticker` (Rich text)
- `Side` (Select)
- `Qty` (Number)
- `Fill Price` (Number)
- `Fees` (Number)
- `Trade Date` (Date)
- `Strategy` (Select)
- `Setup` (Rich text)
- `Notes` (Rich text)
- `Entry Screenshot` (Files)
- `P/L at Close` (Number)
- `Trade Type` (Select)
- `Tags` (Multi-select)
- `Broker` (Select)
- `Account` (Rich text)
- `SnapTrade ID` (Rich text)
- `Order ID` (Rich text)

Share the database with your Notion integration.

## 2) SnapTrade setup
You need:
- `SNAPTRADE_CLIENT_ID`
- `SNAPTRADE_CONSUMER_KEY`

Then run:

```bash
npm run connect
```

This prints:
- `SNAPTRADE_USER_ID`
- `SNAPTRADE_USER_SECRET`
- A **Connection Portal URL** (open it to connect Fidelity, Robinhood, Public)

## 3) Local run (optional)

```bash
npm install
cp .env.example .env
npm run sync
```

Backfill existing rows (updates existing pages by SnapTrade ID):

```bash
npm run backfill
```

Cleanup zero-quantity rows (archives them in Notion):

```bash
npm run cleanup-zero-qty
```

Rebuild positions (archives all existing rows and creates one row per contract):

```bash
npm run rebuild-positions
```

Audit journal consistency:

```bash
npm run audit-journal
```

Check account freshness (fails on stale accounts by default):

```bash
npm run freshness-check
```

Rebuild calendar daily summary rows:

```bash
npm run rebuild-daily-summary
```

Manual HTTP sync (after deploy):

```bash
curl -X POST "https://YOUR-SERVICE.up.railway.app/sync" \\
  -H "Authorization: Bearer $SYNC_TOKEN"
```

## 4) Railway deploy
1. Create a new Railway project.
2. Add a service from this repo.
3. Set environment variables from `.env`.
4. Add a **Cron Job** with schedule: `*/15 * * * *`
5. Set the Cron command to:

```bash
npm run sync
```

## 5) GitHub Actions cloud automation
If you want jobs to run when your laptop is off, use the included workflows:

- `.github/workflows/daily-refresh.yml`
- `.github/workflows/weekly-review.yml`

### Required GitHub repo secrets
- `SNAPTRADE_CLIENT_ID`
- `SNAPTRADE_CONSUMER_KEY`
- `SNAPTRADE_USER_ID`
- `SNAPTRADE_USER_SECRET`
- `NOTION_TOKEN`
- `NOTION_DATABASE_ID`
- `NOTION_WEEKLY_REVIEWS_PAGE_ID`

### Behavior
- `daily-refresh.yml` runs hourly and executes:
  - `npm run rebuild-positions`
  - `npm run rebuild-daily-summary`
  - `npm run audit-journal`
  - `npm run freshness-check`
- `weekly-review.yml` checks every hour, but only runs at Friday 4PM New York time:
  - `npm run rebuild-positions`
  - `npm run rebuild-daily-summary`
  - `npm run audit-journal`
  - `npm run weekly-review`

### Freshness check env (optional)
- `SNAPTRADE_FRESHNESS_HOURS` (default `24`)
- `SNAPTRADE_FRESHNESS_DAYS` (default `30`)
- `SNAPTRADE_FRESHNESS_FAIL_ON_STALE` (`1` to fail, `0` to report only)

## Notes on “one row per order”
This sync uses SnapTrade’s **Account Orders** endpoint, so each order becomes one Notion row. If you want to collapse multiple partial fills or add execution-level details, we can extend it later.
