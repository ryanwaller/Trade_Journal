import { config } from "./config.js";
import { getAccountOrders, listAccounts } from "./snaptrade.js";

type AccountFreshness = {
  accountId: string;
  broker: string | null;
  latestTradeDate: string | null;
  ageHours: number | null;
  status: "fresh" | "stale" | "unknown";
};

function parseNumber(value: string | undefined, fallback: number): number {
  if (!value) return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function isWeekendInZone(timeZone: string) {
  const day = new Intl.DateTimeFormat("en-US", {
    timeZone,
    weekday: "short"
  }).format(new Date());
  return day === "Sat" || day === "Sun";
}

export async function runFreshnessCheck() {
  const thresholdHours = parseNumber(process.env.SNAPTRADE_FRESHNESS_HOURS, 24);
  const lookbackDays = parseNumber(process.env.SNAPTRADE_FRESHNESS_DAYS, 30);
  const failOnStale = (process.env.SNAPTRADE_FRESHNESS_FAIL_ON_STALE ?? "1") === "1";
  const zone = config.NOTION_TIMEZONE ?? "America/New_York";
  const weekend = isWeekendInZone(zone);
  const now = Date.now();

  const accounts = await listAccounts();
  const accountFreshness: AccountFreshness[] = [];

  for (const account of accounts) {
    const orders = await getAccountOrders(account.id, lookbackDays, true);
    let latestMs: number | null = null;
    let latestIso: string | null = null;

    for (const order of orders) {
      if (!order.trade_date) continue;
      const ms = new Date(order.trade_date).getTime();
      if (Number.isNaN(ms)) continue;
      if (latestMs === null || ms > latestMs) {
        latestMs = ms;
        latestIso = new Date(ms).toISOString();
      }
    }

    const ageHours = latestMs === null ? null : (now - latestMs) / (1000 * 60 * 60);
    const stale = ageHours !== null && ageHours > thresholdHours;

    accountFreshness.push({
      accountId: account.id,
      broker: account.brokerage ?? null,
      latestTradeDate: latestIso,
      ageHours: ageHours === null ? null : Number(ageHours.toFixed(2)),
      status: latestMs === null ? "unknown" : stale ? "stale" : "fresh"
    });
  }

  const staleAccounts = accountFreshness.filter((x) => x.status === "stale");
  const shouldFail = failOnStale && !weekend && staleAccounts.length > 0;

  const result = {
    thresholdHours,
    lookbackDays,
    timezone: zone,
    weekend,
    failOnStale,
    totalAccounts: accountFreshness.length,
    staleCount: staleAccounts.length,
    staleAccounts,
    accounts: accountFreshness
  };

  if (shouldFail) {
    const names = staleAccounts.map((x) => x.broker ?? x.accountId).join(", ");
    throw new Error(`Freshness check failed. Stale broker accounts: ${names}`);
  }

  return result;
}

