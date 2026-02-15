import { Snaptrade } from "snaptrade-typescript-sdk";
import { config } from "./config.js";

export type SnapTradeAccount = {
  id: string;
  name?: string | null;
  number?: string | null;
  brokerage?: string | null;
};

export type SnapTradeOrder = {
  id?: string | null;
  type?: string | null;
  status?: string | null;
  symbol?: {
    symbol?: string | null;
    description?: string | null;
  } | null;
  symbol_key?: string | null;
  price?: number | null;
  units?: number | null;
  fee?: number | null;
  currency?: string | null;
  trade_date?: string | null;
  account_id?: string | null;
  order_id?: string | null;
  realized_pl?: number | null;
};

const snaptrade = new Snaptrade({
  clientId: config.SNAPTRADE_CLIENT_ID,
  consumerKey: config.SNAPTRADE_CONSUMER_KEY
});

function requireUser() {
  if (!config.SNAPTRADE_USER_ID || !config.SNAPTRADE_USER_SECRET) {
    throw new Error("SNAPTRADE_USER_ID and SNAPTRADE_USER_SECRET are required for sync");
  }
  return {
    userId: config.SNAPTRADE_USER_ID,
    userSecret: config.SNAPTRADE_USER_SECRET
  };
}

export async function listAccounts(): Promise<SnapTradeAccount[]> {
  const { userId, userSecret } = requireUser();
  const response = await snaptrade.accountInformation.listUserAccounts({
    userId,
    userSecret
  });

  const data = (response as any).data ?? response;
  return (data ?? []).map((account: any) => ({
    id: account.id,
    name: account.name ?? null,
    number: account.number ?? null,
    brokerage: account.institution_name ?? account.brokerage ?? null
  }));
}

export async function getAccountOrders(
  accountId: string,
  days: number,
  includeAll = false
): Promise<SnapTradeOrder[]> {
  const { userId, userSecret } = requireUser();
  const response = await snaptrade.accountInformation.getUserAccountOrders({
    userId,
    userSecret,
    accountId,
    state: "all",
    days
  });

  const data = (response as any).data ?? response;
  const toNumber = (value: any) => {
    if (value === null || value === undefined || value === "") return null;
    const parsed = Number(value);
    return Number.isNaN(parsed) ? null : parsed;
  };

  const mapped = (data ?? []).map((order: any) => {
    const status = order.status ?? null;
    const filledQty = toNumber(order.filled_quantity);
    const fallbackQty =
      filledQty === null && (status === "EXECUTED" || status === "PARTIAL")
        ? toNumber(order.total_quantity)
        : filledQty;

    const looksLikeUuid = (value: string | null | undefined) =>
      Boolean(value && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value));

    const candidates = [
      order.option_symbol?.underlying_symbol?.ticker,
      order.universal_symbol?.symbol,
      order.universal_symbol?.raw_symbol,
      order.option_symbol?.ticker,
      order.option_symbol?.underlying_symbol?.ticker,
      order.symbol
    ].filter((value: any) => typeof value === "string" && value.length > 0);

    const cleanSymbol = (value: string) => {
      const trimmed = value.trim();
      const firstToken = trimmed.split(/\s+/)[0];
      return firstToken.replace(/[^A-Za-z0-9.\-]/g, "");
    };

    const resolvedSymbolRaw =
      order.option_symbol?.underlying_symbol?.ticker ??
      candidates.find((value: string) => !looksLikeUuid(value)) ??
      candidates[0] ??
      null;

    const resolvedSymbol = resolvedSymbolRaw ? cleanSymbol(resolvedSymbolRaw) : null;

    const rawSymbolKey =
      order.option_symbol?.ticker ??
      order.universal_symbol?.raw_symbol ??
      order.universal_symbol?.symbol ??
      order.symbol ??
      resolvedSymbolRaw ??
      resolvedSymbol ??
      null;

    const normalizeSymbolKey = (value: string | null) => {
      if (!value) return null;
      return value.trim().replace(/\s+/g, " ").toUpperCase();
    };

    const symbolKey = normalizeSymbolKey(rawSymbolKey);

    const executionPrice = toNumber(order.execution_price);
    const limitPrice = toNumber(order.limit_price);
    const price =
      executionPrice !== null
        ? executionPrice
        : status === "EXECUTED" || status === "PARTIAL"
          ? limitPrice
          : null;

    return {
      id: order.brokerage_order_id ?? null,
      type: order.action ?? null,
      status,
      symbol: resolvedSymbol ? { symbol: resolvedSymbol } : null,
      symbol_key: symbolKey,
      price,
      units: fallbackQty,
      fee: null,
      currency: order.universal_symbol?.currency?.code ?? null,
      trade_date: order.time_placed ?? order.time_updated ?? null,
      account_id: accountId,
      order_id: order.brokerage_order_id ?? null
    };
  });

  const nonZeroQty = (order: SnapTradeOrder) =>
    typeof order.units === "number" && order.units > 0;

  if (includeAll) {
    return mapped.filter(nonZeroQty);
  }

  return mapped.filter((order: SnapTradeOrder) => {
    const filled = nonZeroQty(order);
    const status = (order.status ?? "").toUpperCase();
    const filledStatus = status === "EXECUTED" || status === "PARTIAL";
    return filled && filledStatus;
  });
}

export async function getAccountOrdersChunked(
  accountId: string,
  startDate: string,
  endDate: string,
  includeAll = false,
  chunkDays = 90
): Promise<SnapTradeOrder[]> {
  const start = new Date(startDate);
  const end = new Date(endDate);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
    throw new Error("Invalid start or end date for chunked fetch");
  }

  const results: SnapTradeOrder[] = [];
  let cursor = new Date(start);

  while (cursor <= end) {
    const chunkStart = new Date(cursor);
    const chunkEnd = new Date(cursor);
    chunkEnd.setDate(chunkEnd.getDate() + chunkDays - 1);
    if (chunkEnd > end) {
      chunkEnd.setTime(end.getTime());
    }

    const ms = chunkEnd.getTime() - chunkStart.getTime();
    const days = Math.max(1, Math.ceil(ms / (1000 * 60 * 60 * 24)));
    const chunkOrders = await getAccountOrders(accountId, days, includeAll);
    results.push(...chunkOrders);

    cursor.setDate(cursor.getDate() + chunkDays);
  }

  const uniq = new Map<string, SnapTradeOrder>();
  for (const order of results) {
    const key = order.id ?? order.order_id ?? `${order.account_id}-${order.trade_date}-${order.symbol_key}`;
    if (!key) continue;
    if (!uniq.has(key)) {
      uniq.set(key, order);
    }
  }

  return Array.from(uniq.values());
}

export async function registerUser(userId: string) {
  const response = await snaptrade.authentication.registerSnapTradeUser({
    userId
  });
  const data = (response as any).data ?? response;
  return data;
}

export async function getLoginUrl(userId: string, userSecret: string, redirectURI?: string) {
  const response = await snaptrade.authentication.loginSnapTradeUser({
    userId,
    userSecret,
    redirectURI
  });
  const data = (response as any).data ?? response;
  return data;
}
