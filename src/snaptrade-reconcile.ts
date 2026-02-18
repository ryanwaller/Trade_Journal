import { createPositionPage, getJournalInfo, getNotionClient, PROPERTY, updatePositionPage, archivePage } from "./notion.js";
import { config, assertSyncConfig } from "./config.js";
import { getAccountOrders, getAccountPositions, listAccounts } from "./snaptrade.js";

type ExistingOpenRow = {
  pageId: string;
  key: string;
  ticker: string;
  contractKey: string;
  account: string;
  broker: string;
};

function getRichText(page: any, key: string) {
  const prop = page.properties?.[key];
  if (!prop || prop.type !== "rich_text") return "";
  return (prop.rich_text ?? []).map((t: any) => t.plain_text ?? "").join("").trim();
}

function getSelect(page: any, key: string) {
  const prop = page.properties?.[key];
  if (!prop || prop.type !== "select") return "";
  return prop.select?.name ?? "";
}

function normalizeContractKey(value: string) {
  return value.trim().replace(/^[-+]/, "").replace(/\s+/g, "").toUpperCase();
}

function isOptionContract(contractKey: string) {
  return /\d{6}[CP]\d+$/i.test(contractKey.replace(/\s+/g, ""));
}

function makeKey(broker: string, account: string, contractKey: string) {
  return `${broker}::${account}::${normalizeContractKey(contractKey)}`;
}

function supportedBroker(name: string | null | undefined) {
  const upper = (name ?? "").toUpperCase();
  return upper === "PUBLIC" || upper === "ROBINHOOD" || upper === "FIDELITY";
}

async function listExistingOpenRows(): Promise<Map<string, ExistingOpenRow>> {
  const info = await getJournalInfo();
  const client = getNotionClient();
  let cursor: string | undefined;
  const rows = new Map<string, ExistingOpenRow>();

  do {
    const response = await client.databases.query({
      database_id: info.databaseId,
      start_cursor: cursor
    });

    for (const page of response.results as any[]) {
      if (page.archived) continue;
      const status = getSelect(page, PROPERTY.status).toUpperCase();
      if (status !== "OPEN") continue;
      const broker = getSelect(page, PROPERTY.broker);
      if (!supportedBroker(broker)) continue;
      const account = getRichText(page, PROPERTY.account);
      const contractKey = getRichText(page, PROPERTY.contractKey);
      if (!broker || !account || !contractKey) continue;
      const key = makeKey(broker, account, contractKey);
      rows.set(key, {
        pageId: page.id,
        key,
        ticker: getRichText(page, PROPERTY.ticker) || "",
        contractKey,
        account,
        broker
      });
    }

    cursor = response.has_more ? response.next_cursor ?? undefined : undefined;
  } while (cursor);

  return rows;
}

export async function runSnaptradeReconcileOpen() {
  assertSyncConfig();
  const accounts = await listAccounts();
  const existing = await listExistingOpenRows();
  const seen = new Set<string>();
  const zone = config.NOTION_TIMEZONE ?? "America/New_York";

  let created = 0;
  let updated = 0;
  let archived = 0;
  let snapshotPositions = 0;
  const reconcileDays = Number.parseInt(process.env.SNAPTRADE_RECONCILE_DAYS ?? "400", 10);

  for (const acct of accounts) {
    const broker = acct.brokerage ?? "";
    if (!supportedBroker(broker)) continue;
    const accountName = acct.name ?? "Brokerage Account";
    const firstTradeByContract = new Map<string, { date: string | null; time: string | null }>();
    const lastBuyByContract = new Map<string, { date: string | null; time: string | null }>();
    try {
      const orders = await getAccountOrders(
        acct.id,
        Number.isNaN(reconcileDays) ? 400 : reconcileDays,
        true
      );
      const sorted = [...orders].sort((a, b) => {
        const aTime = a.trade_date ? new Date(a.trade_date).getTime() : 0;
        const bTime = b.trade_date ? new Date(b.trade_date).getTime() : 0;
        return aTime - bTime;
      });
      for (const o of sorted) {
        const side = (o.type ?? "").toUpperCase();
        if (!side.includes("BUY")) continue;
        const key = normalizeContractKey(o.symbol_key ?? o.symbol?.symbol ?? "");
        if (!key) continue;
        const dt = o.trade_date ? new Date(o.trade_date) : null;
        if (!dt || Number.isNaN(dt.getTime())) {
          if (!firstTradeByContract.has(key)) {
            firstTradeByContract.set(key, { date: null, time: null });
          }
          lastBuyByContract.set(key, { date: null, time: null });
          continue;
        }
        const meta = {
          date: dt.toISOString().slice(0, 10),
          time: new Intl.DateTimeFormat("en-US", {
            timeZone: zone,
            hour: "numeric",
            minute: "2-digit",
            hour12: true
          }).format(dt)
        };
        if (!firstTradeByContract.has(key)) {
          firstTradeByContract.set(key, meta);
        }
        lastBuyByContract.set(key, meta);
      }
    } catch {
      // If orders lookup fails, continue with positions snapshot only.
    }

    const positions = await getAccountPositions(acct.id);
    for (const p of positions) {
      const key = makeKey(broker, accountName, p.symbol_key);
      seen.add(key);
      snapshotPositions += 1;
      const existingRow = existing.get(key);
      const avgPriceRaw = p.average_purchase_price ?? p.price ?? 0;
      // SnapTrade holdings may already return contract-level option prices.
      // Keep as-is to avoid accidental 100x inflation (e.g. 98 -> 9800).
      const avgPrice = Math.round(avgPriceRaw * 100) / 100;
      if (!existingRow) {
        const openMeta = firstTradeByContract.get(normalizeContractKey(p.symbol_key));
        const lastMeta = lastBuyByContract.get(normalizeContractKey(p.symbol_key));
        const lastAddDate =
          openMeta?.date && lastMeta?.date && lastMeta.date !== openMeta.date ? lastMeta.date : null;
        await createPositionPage({
          title: p.ticker,
          ticker: p.ticker,
          contractKey: p.symbol_key,
          qty: p.units,
          avgPrice,
          openDate: openMeta?.date ?? null,
          openTime: openMeta?.time ?? null,
          lastAddDate,
          broker,
          account: accountName
        });
        created += 1;
      } else {
        const openMeta = firstTradeByContract.get(normalizeContractKey(p.symbol_key));
        const lastMeta = lastBuyByContract.get(normalizeContractKey(p.symbol_key));
        const lastAddDate =
          openMeta?.date && lastMeta?.date && lastMeta.date !== openMeta.date ? lastMeta.date : null;
        await updatePositionPage({
          pageId: existingRow.pageId,
          ticker: p.ticker,
          contractKey: p.symbol_key,
          qty: p.units,
          avgPrice,
          // Only set if there's a buy on a later date than the original open date.
          // If we can't determine this, omit it (don't overwrite a previously-set value).
          ...(lastAddDate ? { lastAddDate } : {}),
          status: "OPEN"
        });
        updated += 1;
      }
    }
  }

  for (const [key, row] of existing.entries()) {
    if (seen.has(key)) continue;
    await archivePage(row.pageId);
    archived += 1;
  }

  return {
    timezone: zone,
    snapshotPositions,
    created,
    updated,
    archived
  };
}
