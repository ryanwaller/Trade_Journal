import { config, assertSyncConfig } from "./config.js";
import {
  archivePage,
  archiveTradePagesByExactBroker,
  createPositionPage,
  loadManualStrategyTagsIndexForBroker,
  lookupManualStrategyTags,
  updatePositionPage
} from "./notion.js";
import { getAccountOrdersChunked, getAccountPositions, listAccounts } from "./snaptrade.js";

const TRADE_TYPES = new Set(["BUY", "SELL"]);

function normalizeSide(value: string | null | undefined) {
  const upper = (value ?? "").toUpperCase();
  if (upper.includes("BUY")) return "BUY";
  if (upper.includes("SELL")) return "SELL";
  return upper;
}

function isOptionContract(contractKey: string) {
  const normalized = contractKey.replace(/\s+/g, "").toUpperCase();
  return /\d{6}[CP]\d{8}$/.test(normalized);
}

function toDateString(date: Date) {
  return date.toISOString().slice(0, 10);
}

function resolvePublicApiRange() {
  const endDate = toDateString(new Date());
  const startRaw =
    process.env.PUBLIC_API_START_DATE?.trim() ||
    config.SNAPTRADE_START_DATE ||
    config.SNAPTRADE_FETCH_START_DATE ||
    "2025-01-01";
  const start = new Date(startRaw);
  if (Number.isNaN(start.getTime())) {
    throw new Error(`Invalid PUBLIC_API_START_DATE: ${startRaw}`);
  }
  return {
    startDate: toDateString(start),
    endDate
  };
}

export async function runImportPublicApi() {
  assertSyncConfig();
  const includeAll = config.SNAPTRADE_INCLUDE_ALL === "1";
  const { startDate, endDate } = resolvePublicApiRange();
  const accounts = await listAccounts();
  const publicAccounts = accounts.filter((a) => (a.brokerage ?? "").toUpperCase() === "PUBLIC");

  // Preserve user-owned fields (Strategy/Tags) across rebuilds.
  const manualIndex = await loadManualStrategyTagsIndexForBroker("Public");

  const archivedExisting = await archiveTradePagesByExactBroker("Public");

  let created = 0;
  let updated = 0;
  let ensuredOpenFromSnapshot = 0;
  let publicAccountsProcessed = 0;
  let publicOrdersSeen = 0;

  for (const account of publicAccounts) {
    publicAccountsProcessed += 1;
    const activities = await getAccountOrdersChunked(account.id, startDate, endDate, includeAll);
    const sorted = [...activities].sort((a, b) => {
      const aTime = a.trade_date ? new Date(a.trade_date).getTime() : 0;
      const bTime = b.trade_date ? new Date(b.trade_date).getTime() : 0;
      return aTime - bTime;
    });

    const positions = new Map<
      string,
      {
        pageId: string;
        openDate: string | null;
        qty: number;
        totalBought: number;
        avgPrice: number;
        realizedPl: number;
        lastAddDate: string | null;
      }
    >();

    for (const activity of sorted) {
      const side = normalizeSide(activity.type);
      if (!TRADE_TYPES.has(side)) continue;

      const contractKey = activity.symbol_key ?? activity.symbol?.symbol ?? "";
      const ticker = activity.symbol?.symbol ?? contractKey.split(" ")[0] ?? "";
      const qty = activity.units ?? null;
      const price = activity.price ?? null;
      if (!contractKey || typeof qty !== "number" || typeof price !== "number") continue;
      publicOrdersSeen += 1;

      const multiplier = isOptionContract(contractKey) ? 100 : 1;
      const key = `${account.id}:${contractKey}`;
      let position = positions.get(key);

      if (side === "BUY") {
        if (!position) {
          const openDateTime = activity.trade_date ? new Date(activity.trade_date) : null;
          const openDate = openDateTime ? toDateString(openDateTime) : null;
          // Leave blank until we see a BUY on a later date than the original open.
          const lastAddDate = null;
          const openTime = openDateTime
            ? new Intl.DateTimeFormat("en-US", {
                timeZone: config.NOTION_TIMEZONE ?? "America/New_York",
                hour: "numeric",
                minute: "2-digit",
                hour12: true
              }).format(openDateTime)
            : null;
          const accountName = account.name ?? "Brokerage Account";
          const manual = lookupManualStrategyTags(manualIndex, accountName, contractKey, openDate);
          const page = await createPositionPage({
            title: ticker,
            ticker,
            contractKey,
            qty,
            avgPrice: price * multiplier,
            openDate,
            openTime,
            broker: "Public",
            account: accountName,
            strategy: manual?.strategy ?? undefined,
            tags: manual?.tags ?? undefined
          });
          positions.set(key, {
            pageId: (page as any).id,
            openDate,
            qty,
            totalBought: qty,
            avgPrice: price,
            realizedPl: 0,
            lastAddDate
          });
          created += 1;
        } else {
          const newQty = position.qty + qty;
          const newTotalBought = position.totalBought + qty;
          const newAvg =
            newQty > 0 ? (position.qty * position.avgPrice + qty * price) / newQty : 0;
          position.qty = newQty;
          position.totalBought = newTotalBought;
          position.avgPrice = newAvg;
          const buyDate = activity.trade_date ? toDateString(new Date(activity.trade_date)) : null;
          if (buyDate && position.openDate && buyDate !== position.openDate) {
            position.lastAddDate = buyDate;
          }
          positions.set(key, position);
          await updatePositionPage({
            pageId: position.pageId,
            ticker,
            contractKey,
            qty: position.totalBought,
            avgPrice: position.avgPrice * multiplier,
            ...(position.lastAddDate ? { lastAddDate: position.lastAddDate } : {}),
            status: "OPEN"
          });
          updated += 1;
        }
      } else if (side === "SELL" && position) {
        const realized = (price - position.avgPrice) * qty * multiplier;
        position.realizedPl += Number.isFinite(realized) ? realized : 0;
        position.qty = Math.max(0, position.qty - qty);
        positions.set(key, position);

        if (position.qty === 0) {
          const closeDateTime = activity.trade_date ? new Date(activity.trade_date) : null;
          const closeDate = closeDateTime ? toDateString(closeDateTime) : null;
          const closeTime = closeDateTime
            ? new Intl.DateTimeFormat("en-US", {
                timeZone: config.NOTION_TIMEZONE ?? "America/New_York",
                hour: "numeric",
                minute: "2-digit",
                hour12: true
              }).format(closeDateTime)
            : null;
          await updatePositionPage({
            pageId: position.pageId,
            ticker,
            contractKey,
            qty: position.totalBought,
            avgPrice: position.avgPrice * multiplier,
            status: "CLOSED",
            closeDate,
            closeTime,
            closePrice: price * multiplier,
            realizedPl: Math.round(position.realizedPl * 100) / 100
          });
          updated += 1;
          positions.delete(key);
        } else {
          await updatePositionPage({
            pageId: position.pageId,
            ticker,
            contractKey,
            qty: position.totalBought,
            avgPrice: position.avgPrice * multiplier,
            ...(position.lastAddDate ? { lastAddDate: position.lastAddDate } : {}),
            status: "OPEN"
          });
          updated += 1;
        }
      }
    }

    // Ensure all currently open holdings exist as OPEN rows even if their
    // opening BUY happened before PUBLIC_API_START_DATE.
    const currentPositions = await getAccountPositions(account.id);
    for (const p of currentPositions) {
      const key = `${account.id}:${p.symbol_key}`;
      const avgPriceRaw = p.average_purchase_price ?? p.price ?? 0;
      const avgPrice = Math.round(avgPriceRaw * 100) / 100;
      const existing = positions.get(key);
      if (!existing) {
        const accountName = account.name ?? "Brokerage Account";
        const manual = lookupManualStrategyTags(manualIndex, accountName, p.symbol_key, null);
        await createPositionPage({
          title: p.ticker,
          ticker: p.ticker,
          contractKey: p.symbol_key,
          qty: p.units,
          avgPrice,
          broker: "Public",
          account: accountName,
          strategy: manual?.strategy ?? undefined,
          tags: manual?.tags ?? undefined
        });
        created += 1;
        ensuredOpenFromSnapshot += 1;
      } else {
        await updatePositionPage({
          pageId: existing.pageId,
          ticker: p.ticker,
          contractKey: p.symbol_key,
          qty: p.units,
          avgPrice,
          status: "OPEN"
        });
        updated += 1;
      }
    }

    // Defensive cleanup: if a row somehow remained empty/archived candidate, remove it.
    for (const [, position] of positions) {
      if (!position.pageId) continue;
      if (position.totalBought <= 0) {
        await archivePage(position.pageId);
      }
    }
  }

  return {
    broker: "Public",
    startDate,
    endDate,
    publicAccounts: publicAccounts.length,
    publicAccountsProcessed,
    archivedExistingRows: archivedExisting.archived,
    publicOrdersSeen,
    createdRows: created,
    updatedRows: updated,
    ensuredOpenFromSnapshot
  };
}
