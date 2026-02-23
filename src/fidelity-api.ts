import { config, assertSyncConfig } from "./config.js";
import {
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

function normalizeContractKey(value: string) {
  return value.trim().replace(/^[-+]/, "").replace(/\s+/g, "").toUpperCase();
}

function optionExpiryFromContractKey(contractKey: string): string | null {
  const compact = normalizeContractKey(contractKey);
  const m = compact.match(/(\d{2})(\d{2})(\d{2})[CP]\d+(?:\.\d+)?$/);
  if (!m) return null;
  const yy = Number(m[1]);
  const mm = Number(m[2]);
  const dd = Number(m[3]);
  if (!Number.isInteger(yy) || !Number.isInteger(mm) || !Number.isInteger(dd)) return null;
  if (mm < 1 || mm > 12 || dd < 1 || dd > 31) return null;
  const fullYear = 2000 + yy;
  const asDate = new Date(Date.UTC(fullYear, mm - 1, dd));
  if (
    asDate.getUTCFullYear() !== fullYear ||
    asDate.getUTCMonth() !== mm - 1 ||
    asDate.getUTCDate() !== dd
  ) {
    return null;
  }
  return `${fullYear.toString().padStart(4, "0")}-${mm.toString().padStart(2, "0")}-${dd
    .toString()
    .padStart(2, "0")}`;
}

function round2(value: number) {
  return Math.round(value * 100) / 100;
}

function toDateString(date: Date) {
  return date.toISOString().slice(0, 10);
}

function resolveFidelityApiRange() {
  const endDate = toDateString(new Date());
  const startRaw =
    process.env.FIDELITY_API_START_DATE?.trim() ||
    config.SNAPTRADE_START_DATE ||
    config.SNAPTRADE_FETCH_START_DATE ||
    "2025-01-01";
  const start = new Date(startRaw);
  if (Number.isNaN(start.getTime())) {
    throw new Error(`Invalid FIDELITY_API_START_DATE: ${startRaw}`);
  }
  return {
    startDate: toDateString(start),
    endDate
  };
}

export async function runImportFidelityApi() {
  assertSyncConfig();
  const includeAll = config.SNAPTRADE_INCLUDE_ALL === "1";
  const { startDate, endDate } = resolveFidelityApiRange();
  const accounts = await listAccounts();
  const fidelityAccounts = accounts.filter((a) => (a.brokerage ?? "").toUpperCase() === "FIDELITY");

  const manualIndex = await loadManualStrategyTagsIndexForBroker("Fidelity");

  const archivedExisting = await archiveTradePagesByExactBroker("Fidelity");

  let created = 0;
  let updated = 0;
  let ensuredOpenFromSnapshot = 0;
  let autoExpiredClosed = 0;
  let fidelityAccountsProcessed = 0;
  let fidelityOrdersSeen = 0;

  for (const account of fidelityAccounts) {
    fidelityAccountsProcessed += 1;
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
      fidelityOrdersSeen += 1;

      const multiplier = isOptionContract(contractKey) ? 100 : 1;
      const key = `${account.id}:${contractKey}`;
      let position = positions.get(key);

      if (side === "BUY") {
        if (!position) {
          const openDateTime = activity.trade_date ? new Date(activity.trade_date) : null;
          const openDate = openDateTime ? toDateString(openDateTime) : null;
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
            broker: "Fidelity",
            account: accountName,
            strategies: manual?.strategies ?? undefined,
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

    const currentPositions = await getAccountPositions(account.id);
    for (const p of currentPositions) {
      const key = `${account.id}:${p.symbol_key}`;
      const avgPriceRaw = p.average_purchase_price ?? p.price ?? 0;
      const avgPrice = Math.round(avgPriceRaw * 100) / 100;
      const existing = positions.get(key);
      const expiry = isOptionContract(p.symbol_key) ? optionExpiryFromContractKey(p.symbol_key) : null;
      const isPastExpiry = Boolean(expiry && expiry < endDate);
      if (isPastExpiry) {
        if (!existing) {
          const accountName = account.name ?? "Brokerage Account";
          const manual = lookupManualStrategyTags(manualIndex, accountName, p.symbol_key, null);
          const page = await createPositionPage({
            title: p.ticker,
            ticker: p.ticker,
            contractKey: p.symbol_key,
            qty: p.units,
            avgPrice,
            broker: "Fidelity",
            account: accountName,
            strategies: manual?.strategies ?? undefined,
            tags: manual?.tags ?? undefined
          });
          await updatePositionPage({
            pageId: (page as any).id,
            ticker: p.ticker,
            contractKey: p.symbol_key,
            qty: p.units,
            avgPrice,
            status: "CLOSED",
            closeDate: expiry,
            closePrice: 0,
            realizedPl: round2(-p.units * avgPrice)
          });
          created += 1;
          updated += 1;
          autoExpiredClosed += 1;
        } else {
          const multiplier = isOptionContract(p.symbol_key) ? 100 : 1;
          const realizedPl = round2(
            existing.realizedPl - existing.qty * existing.avgPrice * multiplier
          );
          await updatePositionPage({
            pageId: existing.pageId,
            ticker: p.ticker,
            contractKey: p.symbol_key,
            qty: existing.totalBought,
            avgPrice: existing.avgPrice * multiplier,
            status: "CLOSED",
            closeDate: expiry,
            closePrice: 0,
            realizedPl
          });
          updated += 1;
          autoExpiredClosed += 1;
        }
        // Do not keep/recreate OPEN rows for options already past expiry.
        continue;
      }
      if (!existing) {
        const accountName = account.name ?? "Brokerage Account";
        const manual = lookupManualStrategyTags(manualIndex, accountName, p.symbol_key, null);
        await createPositionPage({
          title: p.ticker,
          ticker: p.ticker,
          contractKey: p.symbol_key,
          qty: p.units,
          avgPrice,
          broker: "Fidelity",
          account: accountName,
          strategies: manual?.strategies ?? undefined,
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
  }

  return {
    broker: "Fidelity",
    startDate,
    endDate,
    fidelityAccounts: fidelityAccounts.length,
    fidelityAccountsProcessed,
    archivedExistingRows: archivedExisting.archived,
    fidelityOrdersSeen,
    createdRows: created,
    updatedRows: updated,
    ensuredOpenFromSnapshot,
    autoExpiredClosed
  };
}
