import { Snaptrade } from "snaptrade-typescript-sdk";

function required(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

const SNAPTRADE_CLIENT_ID = required("SNAPTRADE_CLIENT_ID");
const SNAPTRADE_CONSUMER_KEY = required("SNAPTRADE_CONSUMER_KEY");
const SNAPTRADE_USER_ID = required("SNAPTRADE_USER_ID");
const SNAPTRADE_USER_SECRET = required("SNAPTRADE_USER_SECRET");

// Baseline where Fidelity appears stuck in SnapTrade.
const BASELINE_TS = process.env.FIDELITY_BASELINE_TS?.trim() || "2026-02-13T05:00:00Z";

const baselineMs = new Date(BASELINE_TS).getTime();
if (Number.isNaN(baselineMs)) throw new Error(`Invalid FIDELITY_BASELINE_TS: ${BASELINE_TS}`);

const snaptrade = new Snaptrade({
  clientId: SNAPTRADE_CLIENT_ID,
  consumerKey: SNAPTRADE_CONSUMER_KEY
});

function asArray(resp) {
  return (resp && typeof resp === "object" && "data" in resp ? resp.data : resp) ?? [];
}

function isoOrNull(value) {
  if (!value) return null;
  const ms = new Date(value).getTime();
  if (Number.isNaN(ms)) return null;
  return new Date(ms).toISOString();
}

const accountsResp = await snaptrade.accountInformation.listUserAccounts({
  userId: SNAPTRADE_USER_ID,
  userSecret: SNAPTRADE_USER_SECRET
});

const accounts = asArray(accountsResp).map((a) => ({
  id: a.id,
  name: a.name ?? null,
  brokerage: a.institution_name ?? a.brokerage ?? null
}));

const fidelityAccounts = accounts.filter((a) => String(a.brokerage ?? "").toLowerCase() === "fidelity");

const perAccount = [];
let globalLatestMs = null;
let globalLatestIso = null;

for (const acct of fidelityAccounts) {
  const ordersResp = await snaptrade.accountInformation.getUserAccountOrders({
    userId: SNAPTRADE_USER_ID,
    userSecret: SNAPTRADE_USER_SECRET,
    accountId: acct.id,
    state: "all",
    days: 60
  });

  const orders = asArray(ordersResp);
  let latestMs = null;
  let latestIso = null;

  for (const o of orders) {
    const dt = o.time_updated ?? o.time_placed ?? null;
    const ms = dt ? new Date(dt).getTime() : NaN;
    if (!Number.isFinite(ms)) continue;
    if (latestMs === null || ms > latestMs) {
      latestMs = ms;
      latestIso = new Date(ms).toISOString();
    }
  }

  perAccount.push({
    accountId: acct.id,
    accountName: acct.name,
    brokerage: acct.brokerage,
    orders60d: orders.length,
    latestOrderTs: latestIso
  });

  if (latestMs !== null && (globalLatestMs === null || latestMs > globalLatestMs)) {
    globalLatestMs = latestMs;
    globalLatestIso = new Date(latestMs).toISOString();
  }
}

const advanced = globalLatestMs !== null && globalLatestMs > baselineMs;

console.log(
  JSON.stringify(
    {
      baselineTs: isoOrNull(BASELINE_TS),
      fidelityAccounts: fidelityAccounts.length,
      globalLatestOrderTs: globalLatestIso,
      advanced,
      accounts: perAccount
    },
    null,
    2
  )
);

