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

export type SnapTradePosition = {
  account_id: string;
  symbol_key: string;
  ticker: string;
  units: number;
  average_purchase_price: number | null;
  price: number | null;
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

function normalizeSymbolKey(value: string | null | undefined) {
  if (!value) return null;
  const trimmed = value.trim().replace(/^[-+]/, "");
  if (!trimmed) return null;
  const compact = trimmed.replace(/\s+/g, "").toUpperCase();
  const m = compact.match(/^([A-Z.\-]+)(\d{6}[CP]\d+)$/);
  if (!m) return compact;
  return `${m[1]} ${m[2]}`;
}

function extractTickerFromSymbolKey(symbolKey: string) {
  const compact = symbolKey.replace(/\s+/g, "");
  const m = compact.match(/^([A-Z.\-]+)(\d{6}[CP]\d+)$/);
  if (m) return m[1];
  return compact.match(/^([A-Z.\-]+)/)?.[1] ?? compact;
}

export async function getAccountPositions(accountId: string): Promise<SnapTradePosition[]> {
  const { userId, userSecret } = requireUser();
  const response = await snaptrade.accountInformation.getUserAccountPositions({
    userId,
    userSecret,
    accountId
  });

  const data = (response as any).data ?? response;
  const toNumber = (value: any) => {
    if (value === null || value === undefined || value === "") return null;
    const parsed = Number(value);
    return Number.isNaN(parsed) ? null : parsed;
  };

  const equityLike = (data ?? [])
    .map((position: any) => {
      const units = toNumber(position.units ?? position.fractional_units);
      if (units === null) return null;

      const rawSymbolKey =
        position.symbol?.symbol?.raw_symbol ??
        position.symbol?.symbol?.symbol ??
        position.symbol?.local_id ??
        position.symbol?.description ??
        null;
      const symbolKey = normalizeSymbolKey(rawSymbolKey);
      if (!symbolKey) return null;

      const tickerCandidate =
        position.symbol?.symbol?.symbol ??
        position.symbol?.description ??
        extractTickerFromSymbolKey(symbolKey);
      const ticker = String(tickerCandidate ?? extractTickerFromSymbolKey(symbolKey))
        .trim()
        .split(/\s+/)[0]
        .replace(/[^A-Za-z0-9.\-]/g, "")
        .toUpperCase();

      return {
        account_id: accountId,
        symbol_key: symbolKey,
        ticker: ticker || extractTickerFromSymbolKey(symbolKey),
        units: Math.abs(units),
        average_purchase_price: toNumber(position.average_purchase_price),
        price: toNumber(position.price)
      } satisfies SnapTradePosition;
    })
    .filter((p: SnapTradePosition | null): p is SnapTradePosition => {
      return Boolean(p && Number.isFinite(p.units) && p.units > 0);
    });

  // Some brokerages expose options only through the options holdings endpoint.
  let optionsLike: SnapTradePosition[] = [];
  try {
    const optResponse = await (snaptrade as any).options.listOptionHoldings({
      userId,
      userSecret,
      accountId
    });
    const optData = (optResponse as any).data ?? optResponse;
    optionsLike = (optData ?? [])
      .map((position: any) => {
        const units = toNumber(position.units);
        if (units === null) return null;

        const rawSymbolKey =
          position.symbol?.option_symbol?.ticker ??
          position.symbol?.symbol?.raw_symbol ??
          position.symbol?.symbol?.symbol ??
          position.symbol?.description ??
          null;
        const symbolKey = normalizeSymbolKey(rawSymbolKey);
        if (!symbolKey) return null;

        const tickerCandidate =
          position.symbol?.option_symbol?.underlying_symbol?.ticker ??
          extractTickerFromSymbolKey(symbolKey);
        const ticker = String(tickerCandidate ?? "").trim().toUpperCase();

        return {
          account_id: accountId,
          symbol_key: symbolKey,
          ticker: ticker || extractTickerFromSymbolKey(symbolKey),
          units: Math.abs(units),
          average_purchase_price: toNumber(position.average_purchase_price),
          price: toNumber(position.price)
        } satisfies SnapTradePosition;
      })
      .filter((p: SnapTradePosition | null): p is SnapTradePosition => {
        return Boolean(p && Number.isFinite(p.units) && p.units > 0);
      });
  } catch {
    optionsLike = [];
  }

  const merged = new Map<string, SnapTradePosition>();
  for (const p of [...equityLike, ...optionsLike]) {
    const key = `${p.account_id}|${normalizeSymbolKey(p.symbol_key)}`;
    if (!merged.has(key)) {
      merged.set(key, p);
      continue;
    }
    const prev = merged.get(key)!;
    // Prefer row with non-null average purchase price.
    if (prev.average_purchase_price === null && p.average_purchase_price !== null) {
      merged.set(key, p);
    }
  }

  return Array.from(merged.values());
}

export async function getAccountOrdersChunked(
  accountId: string,
  startDate: string,
  endDate: string,
  includeAll = false,
  _chunkDays = 90
): Promise<SnapTradeOrder[]> {
  const start = new Date(startDate);
  const end = new Date(endDate);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
    throw new Error("Invalid start or end date for chunked fetch");
  }

  const endExclusive = new Date(end);
  endExclusive.setDate(endExclusive.getDate() + 1);
  const ms = endExclusive.getTime() - start.getTime();
  const days = Math.max(1, Math.ceil(ms / (1000 * 60 * 60 * 24)));
  const orders = await getAccountOrders(accountId, days, includeAll);
  const results = orders.filter((order) => {
    if (!order.trade_date) return false;
    const tradeMs = new Date(order.trade_date).getTime();
    if (Number.isNaN(tradeMs)) return false;
    return tradeMs >= start.getTime() && tradeMs < endExclusive.getTime();
  });

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
  const request: any = {
    userId,
    userSecret
  };
  if (redirectURI) {
    request.redirectURI = redirectURI;
  }
  const response = await snaptrade.authentication.loginSnapTradeUser(request);
  const data = (response as any).data ?? response;
  return data;
}
