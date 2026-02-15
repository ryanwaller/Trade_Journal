import { assertSyncConfig, config } from "./config.js";
import {
  createTradePage,
  findExistingBySnapTradeId,
  findOpenPositionPageId,
  findPageIdByOrderId,
  findPageIdBySnapTradeId,
  updatePositionPage,
  updateTradePage,
  createPositionPage,
  archiveAllPages,
  archivePage
} from "./notion.js";
import {
  getAccountOrders,
  getAccountOrdersChunked,
  listAccounts,
  type SnapTradeOrder
} from "./snaptrade.js";

const TRADE_TYPES = new Set(["BUY", "SELL"]);

function toDateString(date: Date) {
  return date.toISOString().slice(0, 10);
}

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

function resolveDateRange() {
  if (config.SNAPTRADE_START_DATE) {
    const start = new Date(config.SNAPTRADE_START_DATE);
    if (!Number.isNaN(start.getTime())) {
      const end = new Date();
      const startDate = toDateString(start);
      const endDate = toDateString(end);
      const ms = end.getTime() - start.getTime();
      const days = Math.max(1, Math.ceil(ms / (1000 * 60 * 60 * 24)));
      return { startDate, endDate, days };
    }
  }

  const days = Number.parseInt(config.SNAPTRADE_DAYS ?? "30", 10);
  const endDate = toDateString(new Date());
  const start = new Date();
  start.setDate(start.getDate() - (Number.isNaN(days) ? 30 : days));
  const startDate = toDateString(start);
  return { startDate, endDate, days: Number.isNaN(days) ? 30 : days };
}

function buildFallbackId(activity: SnapTradeOrder) {
  const parts = [
    activity.account_id ?? "",
    activity.trade_date ?? "",
    activity.type ?? "",
    activity.symbol?.symbol ?? "",
    activity.units ?? "",
    activity.price ?? ""
  ];
  return parts.join("|");
}

export async function runSync({ dryRun = false } = {}) {
  assertSyncConfig();
  const { startDate, endDate, days } = resolveDateRange();

  const accounts = await listAccounts();
  const accountMap = new Map(accounts.map((a) => [a.id, a]));
  const debug = process.env.SNAPTRADE_DEBUG === "1";
  const includeAll = config.SNAPTRADE_INCLUDE_ALL === "1";
  const debugPl = process.env.SNAPTRADE_DEBUG_PL === "1";

  let created = 0;
  let skipped = 0;
  let total = 0;

  for (const account of accounts) {
    const activities = await getAccountOrders(account.id, days, includeAll);
    const sorted = [...activities].sort((a, b) => {
      const aTime = a.trade_date ? new Date(a.trade_date).getTime() : 0;
      const bTime = b.trade_date ? new Date(b.trade_date).getTime() : 0;
      return aTime - bTime;
    });

    if (debug) {
      const byType = sorted.reduce<Record<string, number>>((acc, activity) => {
        const key = (activity.type ?? "UNKNOWN").toUpperCase();
        acc[key] = (acc[key] ?? 0) + 1;
        return acc;
      }, {});
      console.log(`Account ${account.id} (${account.brokerage ?? "Unknown"}):`, byType);
    }

    const lotsMap = new Map<string, { qty: number; cost: number }[]>();

    for (const activity of sorted) {
      const type = normalizeSide(activity.type);
      if (!TRADE_TYPES.has(type)) continue;

      total += 1;
      const id = activity.id ?? buildFallbackId(activity);
      const exists = await findExistingBySnapTradeId(id);
      if (exists) {
        skipped += 1;
        continue;
      }
      if (activity.order_id) {
        const existingByOrder = await findPageIdByOrderId(activity.order_id);
        if (existingByOrder) {
          skipped += 1;
          continue;
        }
      }

      const symbol = activity.symbol_key ?? activity.symbol?.symbol ?? "";
      const qty = activity.units ?? null;
      const price = activity.price ?? null;
      if (symbol && typeof qty === "number" && typeof price === "number") {
        const key = `${account.id}:${symbol}`;
        const lots = lotsMap.get(key) ?? [];

        if (type === "BUY") {
          lots.push({ qty, cost: price });
          lotsMap.set(key, lots);
          activity.realized_pl = null;
        } else if (type === "SELL") {
          let remaining = qty;
          let realized = 0;

          while (remaining > 0 && lots.length > 0) {
            const lot = lots[0];
            const used = Math.min(remaining, lot.qty);
            realized += (price - lot.cost) * used;
            lot.qty -= used;
            remaining -= used;
            if (lot.qty <= 0) {
              lots.shift();
            }
          }

          lotsMap.set(key, lots);
          activity.realized_pl = Number.isFinite(realized)
            ? Math.round(realized * 100) / 100
            : null;
          if (debugPl && debugCount < 10 && lots.length === 0) {
            console.log("PL debug SELL no lots:", {
              account: account.brokerage ?? account.id,
              symbol: key,
              qty,
              price,
              status: activity.status ?? null,
              id
            });
            debugCount += 1;
          }
        }
      }

      if (!dryRun) {
        await createTradePage({ ...activity, id }, accountMap.get(account.id));
      }
      created += 1;
    }
  }

  return { total, created, skipped, startDate, endDate };
}

export async function runBackfill() {
  assertSyncConfig();
  const { startDate, endDate, days } = resolveDateRange();
  const accounts = await listAccounts();
  const accountMap = new Map(accounts.map((a) => [a.id, a]));
  const includeAll = config.SNAPTRADE_INCLUDE_ALL === "1";
  const debugPl = process.env.SNAPTRADE_DEBUG_PL === "1";

  let updated = 0;
  let missing = 0;
  let total = 0;
  let debugCount = 0;

  for (const account of accounts) {
    const activities = await getAccountOrders(account.id, days, includeAll);
    const sorted = [...activities].sort((a, b) => {
      const aTime = a.trade_date ? new Date(a.trade_date).getTime() : 0;
      const bTime = b.trade_date ? new Date(b.trade_date).getTime() : 0;
      return aTime - bTime;
    });

    const lotsMap = new Map<string, { qty: number; cost: number }[]>();

    for (const activity of sorted) {
      const type = normalizeSide(activity.type);
      const symbol = activity.symbol_key ?? activity.symbol?.symbol ?? "";
      const qty = activity.units ?? null;
      const price = activity.price ?? null;

      if (debugPl && debugCount < 10 && type === "SELL") {
        console.log("PL debug SELL snapshot:", {
          account: account.brokerage ?? account.id,
          symbol,
          qty,
          price,
          status: activity.status ?? null,
          id
        });
        debugCount += 1;
      }

      if (debugPl && debugCount < 10 && type === "BUY" && (typeof qty !== "number" || typeof price !== "number")) {
        console.log("PL debug BUY missing data:", {
          account: account.brokerage ?? account.id,
          symbol,
          qty,
          price,
          status: activity.status ?? null,
          id
        });
        debugCount += 1;
      }

      if (symbol && typeof qty === "number" && typeof price === "number") {
        const key = `${account.id}:${symbol}`;
        const lots = lotsMap.get(key) ?? [];

        if (type === "BUY") {
          lots.push({ qty, cost: price });
          lotsMap.set(key, lots);
          activity.realized_pl = null;
        } else if (type === "SELL") {
          let remaining = qty;
          let realized = 0;

          while (remaining > 0 && lots.length > 0) {
            const lot = lots[0];
            const used = Math.min(remaining, lot.qty);
            realized += (price - lot.cost) * used;
            lot.qty -= used;
            remaining -= used;
            if (lot.qty <= 0) {
              lots.shift();
            }
          }

          lotsMap.set(key, lots);
          activity.realized_pl = Number.isFinite(realized)
            ? Math.round(realized * 100) / 100
            : null;
        }
      }

      total += 1;
      const id = activity.id ?? buildFallbackId(activity);
      const pageId =
        (await findPageIdBySnapTradeId(id)) ??
        (activity.order_id ? await findPageIdByOrderId(activity.order_id) : null);
      if (!pageId) {
        missing += 1;
        continue;
      }

      await updateTradePage(pageId, { ...activity, id }, accountMap.get(account.id));
      updated += 1;
    }
  }

  return { total, updated, missing, startDate, endDate };
}

export async function runRebuildPositions() {
  assertSyncConfig();
  const { startDate, endDate, days } = resolveDateRange();
  const fetchStart = config.SNAPTRADE_FETCH_START_DATE;
  const filterStart = config.SNAPTRADE_START_DATE
    ? new Date(config.SNAPTRADE_START_DATE)
    : null;
  const fetchRange = fetchStart
    ? (() => {
        const start = new Date(fetchStart);
        const end = new Date();
        const startDate = toDateString(start);
        const endDate = toDateString(end);
        const ms = end.getTime() - start.getTime();
        const days = Math.max(1, Math.ceil(ms / (1000 * 60 * 60 * 24)));
        return { startDate, endDate, days };
      })()
    : { startDate, endDate, days };
  const accounts = await listAccounts();
  const accountMap = new Map(accounts.map((a) => [a.id, a]));
  const includeAll = config.SNAPTRADE_INCLUDE_ALL === "1";

  await archiveAllPages();

  let created = 0;
  let updated = 0;

  for (const account of accounts) {
    const activities = fetchStart
      ? await getAccountOrdersChunked(
          account.id,
          fetchRange.startDate,
          fetchRange.endDate,
          includeAll
        )
      : await getAccountOrders(account.id, fetchRange.days, includeAll);
    const sorted = [...activities].sort((a, b) => {
      const aTime = a.trade_date ? new Date(a.trade_date).getTime() : 0;
      const bTime = b.trade_date ? new Date(b.trade_date).getTime() : 0;
      return aTime - bTime;
    });

    const positions = new Map<
      string,
      {
        pageId: string;
        qty: number;
        totalBought: number;
        avgPrice: number;
        openDate?: string | null;
        openTime?: string | null;
        realizedPl: number;
      }
    >();

    for (const activity of sorted) {
      const side = normalizeSide(activity.type);
      if (!TRADE_TYPES.has(side)) continue;

      const contractKey = activity.symbol_key ?? activity.symbol?.symbol ?? "";
      const ticker = activity.symbol?.symbol ?? contractKey.split(" ")[0] ?? "";
      const qty = activity.units ?? null;
      const price = activity.price ?? null;
      if (!contractKey || typeof qty !== "number" || typeof price !== "number") {
        continue;
      }

      const multiplier = isOptionContract(contractKey) ? 100 : 1;
      const key = `${account.id}:${contractKey}`;
      let position = positions.get(key);

      const openDateTime = activity.trade_date ? new Date(activity.trade_date) : null;
      const openDate = openDateTime ? toDateString(openDateTime) : null;
      const openTime = openDateTime
        ? new Intl.DateTimeFormat("en-US", {
            timeZone: config.NOTION_TIMEZONE ?? "America/New_York",
            hour: "numeric",
            minute: "2-digit",
            hour12: true
          }).format(openDateTime)
        : null;

      if (side === "BUY") {
        if (!position) {
          const displayPrice = price * multiplier;
          const page = await createPositionPage({
            title: ticker,
            ticker,
            contractKey,
            qty,
            avgPrice: displayPrice,
            openDate,
            openTime,
            broker: account.brokerage ?? null,
            account: account.name ?? null
          });
          positions.set(key, {
            pageId: (page as any).id,
            qty,
            totalBought: qty,
            avgPrice: price,
            openDate,
            openTime,
            realizedPl: 0
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
          positions.set(key, position);
          await updatePositionPage({
            pageId: position.pageId,
            ticker,
            contractKey,
            qty: position.totalBought,
            avgPrice: position.avgPrice * multiplier,
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
          const displayClosePrice = price * multiplier;
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
          const shouldKeep =
            !filterStart || (closeDateTime ? closeDateTime >= filterStart : false);
          await updatePositionPage({
            pageId: position.pageId,
            ticker,
            contractKey,
            qty: position.totalBought,
            avgPrice: position.avgPrice * multiplier,
            status: "CLOSED",
            closeDate,
            closeTime,
            closePrice: displayClosePrice,
            realizedPl: Math.round(position.realizedPl * 100) / 100
          });
          updated += 1;
          positions.delete(key);
          if (!shouldKeep) {
            await archivePage(position.pageId);
          }
        } else {
          await updatePositionPage({
            pageId: position.pageId,
            ticker,
            contractKey,
            qty: position.totalBought,
            avgPrice: position.avgPrice * multiplier,
            status: "OPEN"
          });
          updated += 1;
        }
      }
    }
  }

  return { created, updated, startDate, endDate };
}
