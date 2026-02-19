import fs from "node:fs/promises";
import path from "node:path";
import {
  archiveTradePagesByBrokerPrefix,
  createPositionPage,
  fetchTradeSnapshotsByBrokers,
  loadManualStrategyTagsIndexForBroker,
  manualKeyForPosition,
  updatePositionPage
} from "./notion.js";

type PublicCsvEvent = {
  dedupeKey: string;
  date: string;
  action: "BUY" | "SELL";
  effect: "BTO" | "STC" | "STO" | "BTC" | "BUY" | "SELL";
  tradeType: "Stock" | "Call" | "Put";
  account: string;
  ticker: string;
  contractKey: string;
  qty: number;
  price: number;
  multiplier: number;
};

type PositionState = {
  account: string;
  ticker: string;
  contractKey: string;
  openQty: number;
  avgOpenPrice: number;
  totalOpenedQty: number;
  totalOpenedNotional: number;
  totalClosedQty: number;
  totalClosedNotional: number;
  realizedPl: number;
  openDate: string | null;
  closeDate: string | null;
};

function parseCsvLine(line: string): string[] {
  const out: string[] = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        cur += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
      continue;
    }
    cur += ch;
  }
  out.push(cur);
  return out;
}

function parseCsv(content: string): Record<string, string>[] {
  const clean = content.replace(/^\uFEFF/, "");
  const lines = clean.split(/\r?\n/).filter((line) => line.trim().length > 0);
  if (lines.length < 2) return [];

  const headers = parseCsvLine(lines[0]).map((h) => h.trim().replace(/^"|"$/g, ""));
  const rows: Record<string, string>[] = [];
  for (let i = 1; i < lines.length; i += 1) {
    const cols = parseCsvLine(lines[i]);
    if (cols.length === 0) continue;
    const row: Record<string, string> = {};
    headers.forEach((h, idx) => {
      row[h] = (cols[idx] ?? "").trim().replace(/^"|"$/g, "");
    });
    rows.push(row);
  }
  return rows;
}

function toNumber(value: string): number | null {
  if (!value) return null;
  const neg = value.includes("(") && value.includes(")");
  const clean = value.replace(/[$,() ]/g, "").trim();
  if (!clean) return null;
  const parsed = Number(clean);
  if (!Number.isFinite(parsed)) return null;
  return neg ? -parsed : parsed;
}

function parseQty(value: string): number | null {
  if (!value) return null;
  const parsed = Number(value.trim());
  if (!Number.isFinite(parsed)) return null;
  const abs = Math.abs(parsed);
  return abs > 0 ? abs : null;
}

function normalizeTicker(value: string) {
  return value.trim().toUpperCase().replace(/[^A-Z0-9.\- ]/g, "");
}

function inferAction(raw: string): "BUY" | "SELL" | null {
  const effect = inferTradeEffect(raw);
  if (!effect) return null;
  if (effect === "BTO" || effect === "BTC" || effect === "BUY") return "BUY";
  return "SELL";
}

function invertEffect(effect: "BTO" | "STC" | "STO" | "BTC" | "BUY" | "SELL") {
  switch (effect) {
    case "BTO":
      return "STC";
    case "STC":
      return "BTO";
    case "STO":
      return "BTC";
    case "BTC":
      return "STO";
    case "BUY":
      return "SELL";
    case "SELL":
      return "BUY";
  }
}

function inferTradeEffect(raw: string): "BTO" | "STC" | "STO" | "BTC" | "BUY" | "SELL" | null {
  const value = raw.trim().toUpperCase();
  if (!value) return null;

  let invert = false;
  let base = value;
  if (base.startsWith("CANCEL_")) {
    invert = true;
    base = base.slice("CANCEL_".length);
  }

  let effect: "BTO" | "STC" | "STO" | "BTC" | "BUY" | "SELL" | null = null;
  if (base === "BUY_TO_OPEN" || base === "BTO") effect = "BTO";
  else if (base === "SELL_TO_CLOSE" || base === "STC") effect = "STC";
  else if (base === "SELL_TO_OPEN" || base === "STO") effect = "STO";
  else if (base === "BUY_TO_CLOSE" || base === "BTC") effect = "BTC";
  else if (base.includes("BUY")) effect = "BUY";
  else if (base.includes("SELL")) effect = "SELL";
  if (!effect) return null;

  if (!invert) return effect;
  return invertEffect(effect);
}

function inferOptionMeta(symbol: string): {
  ticker: string;
  contractKey: string;
  tradeType: "Call" | "Put";
} | null {
  const normalized = symbol.trim().toUpperCase().replace(/\s+/g, " ");
  const m = normalized.match(/^([A-Z.\-]+)\s+(\d{8})([CP])\s+([\d.]+)$/);
  if (!m) return null;

  const ticker = m[1];
  const yyyymmdd = m[2];
  const cp = m[3];
  const strike = Number(m[4]);
  if (!Number.isFinite(strike)) return null;

  const yy = yyyymmdd.slice(2, 4);
  const mm = yyyymmdd.slice(4, 6);
  const dd = yyyymmdd.slice(6, 8);
  const strike8 = String(Math.round(strike * 1000)).padStart(8, "0");
  const contractKey = `${ticker} ${yy}${mm}${dd}${cp}${strike8}`;
  return {
    ticker,
    contractKey,
    tradeType: cp === "C" ? "Call" : "Put"
  };
}

function toEvent(
  row: Record<string, string>,
  startDate: string,
  endDate: string
): PublicCsvEvent | null {
  const type = String(row["Type"] ?? "").trim().toUpperCase();
  if (type && type !== "TRADES") return null;

  const date = String(row["Trade Date"] ?? "").trim();
  if (!date || date < startDate || date > endDate) return null;

  const tradeAction = String(row["Trade Action"] ?? "");
  const effect = inferTradeEffect(tradeAction);
  if (!effect) return null;
  const action = inferAction(tradeAction);
  if (!action) return null;

  const qty = parseQty(String(row["Qty"] ?? ""));
  const price = toNumber(String(row["Price"] ?? ""));
  if (qty === null || price === null) return null;

  const symbolRaw = normalizeTicker(String(row["Symbol"] ?? ""));
  if (!symbolRaw) return null;

  const option = inferOptionMeta(symbolRaw);
  const ticker = option?.ticker ?? symbolRaw;
  const contractKey = option?.contractKey ?? ticker;
  const tradeType = option?.tradeType ?? "Stock";
  const multiplier = option ? 100 : 1;

  const dedupeKey = [
    row["Trade Date"] ?? "",
    row["Settle Date"] ?? "",
    row["Symbol"] ?? "",
    row["Trade Action"] ?? "",
    row["Qty"] ?? "",
    row["Price"] ?? "",
    row["Net Amount"] ?? ""
  ]
    .map((v) => String(v).trim())
    .join("|");

  return {
    dedupeKey,
    date,
    action,
    effect,
    tradeType,
    account: "Brokerage Account",
    ticker,
    contractKey,
    qty,
    price,
    multiplier
  };
}

function round2(value: number) {
  return Math.round(value * 100) / 100;
}

function buildSignature(contractKey: string, ticker: string, tradeDate: string, qty: number, fillPrice: number) {
  return `${contractKey.toUpperCase()}|${ticker.toUpperCase()}|${tradeDate}|${round2(qty)}|${round2(fillPrice)}`;
}

function optionExpiryDate(contractKey: string): string | null {
  const m = contractKey.toUpperCase().match(/\s(\d{2})(\d{2})(\d{2})[CP]\d{8}$/);
  if (!m) return null;
  const year = Number(m[1]);
  const month = Number(m[2]);
  const day = Number(m[3]);
  if (
    !Number.isInteger(year) ||
    !Number.isInteger(month) ||
    !Number.isInteger(day) ||
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > 31
  ) {
    return null;
  }
  const yyyy = year >= 70 ? 1900 + year : 2000 + year;
  return `${yyyy}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function upsertPositionFromEvent(p: PositionState, e: PublicCsvEvent) {
  const q = e.qty;
  const px = e.price;
  const m = e.multiplier;

  // Options should close by intent: BTO<->STC and STO<->BTC.
  if (e.tradeType !== "Stock") {
    if (e.effect === "BTO") {
      const newQty = p.openQty + q;
      p.avgOpenPrice = newQty > 0 ? (p.openQty * p.avgOpenPrice + q * px) / newQty : 0;
      p.openQty = newQty;
      p.totalOpenedQty += q;
      p.totalOpenedNotional += q * px;
      if (!p.openDate) p.openDate = e.date;
      return;
    }
    if (e.effect === "STC") {
      const closingQty = Math.min(q, Math.max(0, p.openQty));
      if (closingQty > 0) {
        p.realizedPl += (px - p.avgOpenPrice) * closingQty * m;
        p.totalClosedQty += closingQty;
        p.totalClosedNotional += closingQty * px;
        p.openQty -= closingQty;
        if (p.openQty === 0) p.closeDate = e.date;
      }
      return;
    }
    if (e.effect === "STO") {
      const shortQty = Math.abs(Math.min(0, p.openQty));
      const newQty = shortQty + q;
      p.avgOpenPrice = newQty > 0 ? (shortQty * p.avgOpenPrice + q * px) / newQty : 0;
      p.openQty = -newQty;
      p.totalOpenedQty += q;
      p.totalOpenedNotional += q * px;
      if (!p.openDate) p.openDate = e.date;
      return;
    }
    if (e.effect === "BTC") {
      const shortQty = Math.abs(Math.min(0, p.openQty));
      const closingQty = Math.min(q, shortQty);
      if (closingQty > 0) {
        p.realizedPl += (p.avgOpenPrice - px) * closingQty * m;
        p.totalClosedQty += closingQty;
        p.totalClosedNotional += closingQty * px;
        p.openQty += closingQty;
        if (p.openQty === 0) p.closeDate = e.date;
      }
      return;
    }
  }

  if (e.action === "BUY") {
    if (p.openQty >= 0) {
      const newQty = p.openQty + q;
      p.avgOpenPrice = newQty > 0 ? (p.openQty * p.avgOpenPrice + q * px) / newQty : 0;
      p.openQty = newQty;
      p.totalOpenedQty += q;
      p.totalOpenedNotional += q * px;
      if (!p.openDate) p.openDate = e.date;
      return;
    }

    const shortQty = Math.abs(p.openQty);
    const closingQty = Math.min(q, shortQty);
    p.realizedPl += (p.avgOpenPrice - px) * closingQty * m;
    p.totalClosedQty += closingQty;
    p.totalClosedNotional += closingQty * px;
    p.openQty += closingQty;
    if (p.openQty === 0) p.closeDate = e.date;

    const remainder = q - closingQty;
    if (remainder > 0) {
      p.openQty = remainder;
      p.avgOpenPrice = px;
      p.totalOpenedQty += remainder;
      p.totalOpenedNotional += remainder * px;
      p.openDate = e.date;
      p.closeDate = null;
    }
    return;
  }

  if (p.openQty <= 0) {
    const shortQty = Math.abs(p.openQty);
    const newQty = shortQty + q;
    p.avgOpenPrice = newQty > 0 ? (shortQty * p.avgOpenPrice + q * px) / newQty : 0;
    p.openQty = -newQty;
    p.totalOpenedQty += q;
    p.totalOpenedNotional += q * px;
    if (!p.openDate) p.openDate = e.date;
    return;
  }

  const closingQty = Math.min(q, p.openQty);
  p.realizedPl += (px - p.avgOpenPrice) * closingQty * m;
  p.totalClosedQty += closingQty;
  p.totalClosedNotional += closingQty * px;
  p.openQty -= closingQty;
  if (p.openQty === 0) p.closeDate = e.date;

  const remainder = q - closingQty;
  if (remainder > 0) {
    p.openQty = -remainder;
    p.avgOpenPrice = px;
    p.totalOpenedQty += remainder;
    p.totalOpenedNotional += remainder * px;
    p.openDate = e.date;
    p.closeDate = null;
  }
}

export async function runImportPublicCsv() {
  const defaultFile = path.join(process.cwd(), "imports", "public", "history", "public.csv");
  const filePath = process.env.PUBLIC_CSV_FILE?.trim() || defaultFile;
  const startDate = process.env.PUBLIC_CSV_START_DATE?.trim() || "2025-01-01";
  const endDate = process.env.PUBLIC_CSV_END_DATE?.trim() || "2025-12-31";
  const closeCutoffDate = process.env.PUBLIC_CSV_CLOSE_CUTOFF_DATE?.trim() || "2026-01-11";

  const raw = await fs.readFile(filePath, "utf8");
  const rows = parseCsv(raw);

  const eventMap = new Map<string, PublicCsvEvent>();
  const eventCountByKey = new Map<string, number>();
  for (const row of rows) {
    const event = toEvent(row, startDate, endDate);
    if (!event) continue;
    const key = event.dedupeKey;
    eventMap.set(key, event);
    eventCountByKey.set(key, (eventCountByKey.get(key) ?? 0) + 1);
  }
  const events: PublicCsvEvent[] = [];
  for (const [key, event] of eventMap.entries()) {
    const count = eventCountByKey.get(key) ?? 1;
    for (let i = 0; i < count; i += 1) events.push(event);
  }
  events.sort((a, b) => {
    const ak = `${a.date}|${a.account}|${a.contractKey}|${a.action}`;
    const bk = `${b.date}|${b.account}|${b.contractKey}|${b.action}`;
    return ak.localeCompare(bk);
  });

  const archivedExisting = await archiveTradePagesByBrokerPrefix("Public (CSV)");
  const manualIndex = await loadManualStrategyTagsIndexForBroker("Public (CSV)");
  const snaptradeRows = await fetchTradeSnapshotsByBrokers(["Public"]);
  const snaptradeContractKeys = new Set(
    snaptradeRows
      .map((p) => (p.contractKey || "").toUpperCase().trim())
      .filter((k) => k.length > 0)
  );
  const snaptradeSignatures = new Set(
    snaptradeRows.map((p) =>
      buildSignature(p.contractKey || p.ticker, p.ticker, p.tradeDate, p.qty, p.fillPrice)
    )
  );

  const positions = new Map<string, PositionState>();
  for (const e of events) {
    const key = `${e.account}::${e.contractKey}`;
    let p = positions.get(key);
    if (!p) {
      p = {
        account: e.account,
        ticker: e.ticker,
        contractKey: e.contractKey,
        openQty: 0,
        avgOpenPrice: 0,
        totalOpenedQty: 0,
        totalOpenedNotional: 0,
        totalClosedQty: 0,
        totalClosedNotional: 0,
        realizedPl: 0,
        openDate: null,
        closeDate: null
      };
      positions.set(key, p);
    }
    upsertPositionFromEvent(p, e);
  }

  let createdRows = 0;
  let openRows = 0;
  let closedRows = 0;
  let skippedAsSnaptradeDuplicate = 0;
  let autoExpiredClosedRows = 0;
  let skippedByCloseCutoff = 0;

  for (const p of positions.values()) {
    if (p.totalOpenedQty <= 0 || !p.openDate) continue;
    const avgOpenPrice = p.totalOpenedQty > 0 ? p.totalOpenedNotional / p.totalOpenedQty : 0;
    const avgClosePrice = p.totalClosedQty > 0 ? p.totalClosedNotional / p.totalClosedQty : null;
    const multiplier = /\s\d{6}[CP]\d{8}$/i.test(p.contractKey) ? 100 : 1;
    const tradeType = multiplier === 100
      ? p.contractKey.match(/\s\d{6}([CP])\d{8}$/i)?.[1]?.toUpperCase() === "C"
        ? "Call"
        : "Put"
      : "Stock";

    const fillPrice = round2(avgOpenPrice * multiplier);
    const signature = buildSignature(
      p.contractKey || p.ticker,
      p.ticker,
      p.openDate,
      round2(p.totalOpenedQty),
      fillPrice
    );
    const contractKeyUpper = (p.contractKey || "").toUpperCase().trim();
    if (
      (contractKeyUpper && snaptradeContractKeys.has(contractKeyUpper)) ||
      snaptradeSignatures.has(signature)
    ) {
      skippedAsSnaptradeDuplicate += 1;
      continue;
    }

    const expiryDate = optionExpiryDate(p.contractKey);
    const todayIso = new Date().toISOString().slice(0, 10);
    const shouldAutoExpireAtZero =
      Math.abs(p.openQty) > 1e-9 &&
      multiplier === 100 &&
      !!expiryDate &&
      expiryDate <= todayIso;
    const resolvedCloseDate =
      Math.abs(p.openQty) < 1e-9 && p.closeDate
        ? p.closeDate
        : shouldAutoExpireAtZero && expiryDate
          ? expiryDate
          : null;
    if (!resolvedCloseDate || resolvedCloseDate > closeCutoffDate) {
      skippedByCloseCutoff += 1;
      continue;
    }

    const manual = manualIndex.get(manualKeyForPosition(p.account, p.contractKey, p.openDate));
    const page = await createPositionPage({
      title: p.ticker,
      ticker: p.ticker,
      contractKey: p.contractKey,
      qty: round2(p.totalOpenedQty),
      avgPrice: fillPrice,
      tradeType,
      openDate: p.openDate,
      openTime: null,
      broker: "Public (CSV)",
      account: p.account,
      strategy: manual?.strategy ?? undefined,
      tags: manual?.tags ?? undefined
    });
    createdRows += 1;

    if (Math.abs(p.openQty) < 1e-9 && p.closeDate) {
      await updatePositionPage({
        pageId: (page as any).id,
        ticker: p.ticker,
        contractKey: p.contractKey,
        qty: round2(p.totalOpenedQty),
        avgPrice: fillPrice,
        tradeType,
        status: "CLOSED",
        closeDate: p.closeDate,
        closeTime: null,
        closePrice: avgClosePrice === null ? null : round2(avgClosePrice * multiplier),
        realizedPl: round2(p.realizedPl)
      });
      closedRows += 1;
    } else if (shouldAutoExpireAtZero && expiryDate) {
      const remainingQty = Math.abs(p.openQty);
      const autoExpiredRealizedPl =
        p.realizedPl +
        (p.openQty > 0
          ? (0 - p.avgOpenPrice) * remainingQty * multiplier
          : (p.avgOpenPrice - 0) * remainingQty * multiplier);
      await updatePositionPage({
        pageId: (page as any).id,
        ticker: p.ticker,
        contractKey: p.contractKey,
        qty: round2(p.totalOpenedQty),
        avgPrice: fillPrice,
        tradeType,
        status: "CLOSED",
        closeDate: expiryDate,
        closeTime: null,
        closePrice: 0,
        realizedPl: round2(autoExpiredRealizedPl)
      });
      closedRows += 1;
      autoExpiredClosedRows += 1;
    } else {
      openRows += 1;
    }
  }

  return {
    filePath,
    startDate,
    endDate,
    closeCutoffDate,
    parsedRows: rows.length,
    uniqueEvents: events.length,
    archivedExistingRows: archivedExisting.archived,
    snaptradeRows: snaptradeRows.length,
    createdRows,
    openRows,
    closedRows,
    skippedAsSnaptradeDuplicate,
    autoExpiredClosedRows,
    skippedByCloseCutoff
  };
}
