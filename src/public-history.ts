import fs from "node:fs/promises";
import path from "node:path";
import {
  archiveTradePagesByBrokerPrefix,
  backfillTradeTypeForBrokerByContractKey,
  createPositionPage,
  fetchOpenPositionSnapshotsByBrokers,
  updatePositionPage
} from "./notion.js";

type PublicHistoryEvent = {
  dedupeKey: string;
  account: string;
  date: string;
  time: string | null;
  action: "BUY" | "SELL";
  tradeType: "Stock" | "Call" | "Put" | null;
  contractKey: string;
  ticker: string;
  qty: number;
  price: number;
  multiplier: number;
};

type PositionState = {
  account: string;
  contractKey: string;
  ticker: string;
  tradeType: "Stock" | "Call" | "Put" | null;
  openQty: number;
  avgOpenPrice: number;
  totalOpenedQty: number;
  totalOpenedNotional: number;
  totalClosedQty: number;
  totalClosedNotional: number;
  realizedPl: number;
  openDate: string | null;
  openTime: string | null;
  closeDate: string | null;
  closeTime: string | null;
};

function mmddyyyyToIso(value: string): string | null {
  const m = value.trim().match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!m) return null;
  return `${m[3]}-${m[1]}-${m[2]}`;
}

function hhmmssToTime(value: string): string | null {
  const clean = value.trim();
  const m = clean.match(/^(\d{2})(\d{2})(\d{2})$/);
  if (!m) return null;
  const hh = Number(m[1]);
  const mm = Number(m[2]);
  if (!Number.isInteger(hh) || !Number.isInteger(mm) || hh < 0 || hh > 23 || mm < 0 || mm > 59) {
    return null;
  }
  const period = hh >= 12 ? "PM" : "AM";
  const h12 = hh % 12 === 0 ? 12 : hh % 12;
  return `${h12}:${String(mm).padStart(2, "0")} ${period}`;
}

function toNumber(value: string): number | null {
  const clean = value.replace(/[$,]/g, "").trim();
  if (!clean) return null;
  const n = Number(clean);
  return Number.isFinite(n) ? n : null;
}

function round2(value: number) {
  return Math.round(value * 100) / 100;
}

function buildPositionSignature(
  ticker: string,
  tradeDate: string,
  qty: number,
  fillPrice: number
) {
  return `${ticker.toUpperCase()}|${tradeDate}|${round2(qty)}|${round2(fillPrice)}`;
}

function extractTicker(symbolText: string) {
  const upper = symbolText.toUpperCase().trim();
  if (!upper) return "";
  const optionMatch = upper.match(/-\s*([A-Z.\-]+)\s+[A-Z]{3}\s+'?\d{2}\s+@\s*[\d.]+\s+(?:CALL|PUT)/);
  if (optionMatch?.[1]) return optionMatch[1];
  const equityMatch = upper.match(/^([A-Z.\-]{1,10})\s+-/);
  if (equityMatch?.[1]) return equityMatch[1];
  const first = upper.match(/^([A-Z.\-]{1,10})/);
  return first?.[1] ?? upper;
}

function normalizeSide(raw: string): "BUY" | "SELL" | null {
  const side = raw.toUpperCase().trim();
  if (!side || side === "T") return null;
  if (side.startsWith("B")) return "BUY";
  if (side.startsWith("S") || side.includes("EXPIRE")) return "SELL";
  return null;
}

function isOptionSymbol(symbolText: string, sideRaw: string, cusip: string): boolean {
  const s = symbolText.toUpperCase();
  return (
    s.includes(" CALL") ||
    s.includes(" PUT") ||
    s.startsWith("- ") ||
    sideRaw.toUpperCase().includes("TO") ||
    /^O/i.test(cusip.trim())
  );
}

function normalizeContractKey(ticker: string, cusip: string, isOption: boolean) {
  if (isOption && cusip.trim()) return cusip.trim().toUpperCase();
  return ticker.toUpperCase();
}

function parseLine(line: string): PublicHistoryEvent | null {
  const cols = line.split("\t").map((c) => c.trim());
  if (cols.length < 14) return null;
  const accountNumber = cols[0];
  const tradeDateRaw = cols[4] ?? "";
  const execTimeRaw = cols[5] ?? "";

  const sideIndex = cols.findIndex((value) =>
    /^(B|S|BTO|STO|BTC|STC|BOT|SLD|EXPIRED)$/i.test(value)
  );
  if (sideIndex < 0 || sideIndex + 4 >= cols.length) return null;
  const sideRaw = cols[sideIndex] ?? "";
  const symbolText = cols[sideIndex + 1] ?? "";
  const cusip = cols[sideIndex + 2] ?? "";
  const qtyRaw = cols[sideIndex + 3] ?? "";
  const priceRaw = cols[sideIndex + 4] ?? "";
  const tradeNumber = (cols[8] ?? "").trim();

  if (!accountNumber || !tradeDateRaw || !sideRaw || !symbolText) return null;

  const action = normalizeSide(sideRaw);
  if (!action) return null;

  const date = mmddyyyyToIso(tradeDateRaw);
  if (!date) return null;

  const qty = Math.abs(toNumber(qtyRaw) ?? NaN);
  const price = toNumber(priceRaw);
  if (!Number.isFinite(qty) || qty <= 0 || price === null || !Number.isFinite(price)) return null;

  const ticker = extractTicker(symbolText);
  if (!ticker) return null;

  const option = isOptionSymbol(symbolText, sideRaw, cusip);
  const contractKey = normalizeContractKey(ticker, cusip, option);
  const dedupeKey = tradeNumber ? `${accountNumber}|${tradeNumber}` : line.trim();
  const upperSymbol = symbolText.toUpperCase();
  const tradeType: "Stock" | "Call" | "Put" | null = option
    ? upperSymbol.includes(" CALL")
      ? "Call"
      : upperSymbol.includes(" PUT")
        ? "Put"
        : null
    : "Stock";

  return {
    dedupeKey,
    account: `Public ${accountNumber}`,
    date,
    time: hhmmssToTime(execTimeRaw),
    action,
    tradeType,
    contractKey,
    ticker,
    qty,
    price,
    multiplier: option ? 100 : 1
  };
}

function upsertPositionFromEvent(p: PositionState, e: PublicHistoryEvent) {
  const q = e.qty;
  const px = e.price;
  const m = e.multiplier;

  if (e.action === "BUY") {
    if (p.openQty >= 0) {
      const newQty = p.openQty + q;
      p.avgOpenPrice = newQty > 0 ? (p.openQty * p.avgOpenPrice + q * px) / newQty : 0;
      p.openQty = newQty;
      p.totalOpenedQty += q;
      p.totalOpenedNotional += q * px;
      if (!p.openDate) {
        p.openDate = e.date;
        p.openTime = e.time;
      }
      return;
    }

    const shortQty = Math.abs(p.openQty);
    const closingQty = Math.min(q, shortQty);
    p.realizedPl += (p.avgOpenPrice - px) * closingQty * m;
    p.totalClosedQty += closingQty;
    p.totalClosedNotional += closingQty * px;
    p.openQty += closingQty;
    if (p.openQty === 0) {
      p.closeDate = e.date;
      p.closeTime = e.time;
    }

    const remainder = q - closingQty;
    if (remainder > 0) {
      p.openQty = remainder;
      p.avgOpenPrice = px;
      p.totalOpenedQty += remainder;
      p.totalOpenedNotional += remainder * px;
      p.openDate = e.date;
      p.openTime = e.time;
      p.closeDate = null;
      p.closeTime = null;
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
    if (!p.openDate) {
      p.openDate = e.date;
      p.openTime = e.time;
    }
    return;
  }

  const closingQty = Math.min(q, p.openQty);
  p.realizedPl += (px - p.avgOpenPrice) * closingQty * m;
  p.totalClosedQty += closingQty;
  p.totalClosedNotional += closingQty * px;
  p.openQty -= closingQty;
  if (p.openQty === 0) {
    p.closeDate = e.date;
    p.closeTime = e.time;
  }

  const remainder = q - closingQty;
  if (remainder > 0) {
    p.openQty = -remainder;
    p.avgOpenPrice = px;
    p.totalOpenedQty += remainder;
    p.totalOpenedNotional += remainder * px;
    p.openDate = e.date;
    p.openTime = e.time;
    p.closeDate = null;
    p.closeTime = null;
  }
}

export async function runImportPublicHistory() {
  const defaultFile = path.join(process.cwd(), "imports", "public", "history", "data.txt");
  const filePath = process.env.PUBLIC_HISTORY_FILE?.trim() || defaultFile;
  const startDate = process.env.PUBLIC_HISTORY_START_DATE?.trim() || "2025-01-01";

  const raw = await fs.readFile(filePath, "utf8");
  const lines = raw.split(/\r?\n/);

  const events: PublicHistoryEvent[] = [];
  for (const line of lines) {
    if (!line.includes("\t")) continue;
    if (!line.match(/^\d{2}-[A-Z0-9]+/)) continue;
    const event = parseLine(line);
    if (!event) continue;
    if (event.date < startDate) continue;
    events.push(event);
  }

  const dedupedMap = new Map<string, PublicHistoryEvent>();
  for (const e of events) {
    const key = e.dedupeKey;
    dedupedMap.set(key, e);
  }
  const deduped = Array.from(dedupedMap.values()).sort((a, b) => {
    const ak = `${a.date}|${a.time ?? ""}|${a.account}|${a.contractKey}|${a.action}`;
    const bk = `${b.date}|${b.time ?? ""}|${b.account}|${b.contractKey}|${b.action}`;
    return ak.localeCompare(bk);
  });

  const archivedHistory = await archiveTradePagesByBrokerPrefix("Public (History)");
  const archivedPdf = await archiveTradePagesByBrokerPrefix("Public (PDF)");
  const openPublicPositions = await fetchOpenPositionSnapshotsByBrokers(["Public"]);
  const publicSignatures = new Set(
    openPublicPositions.map((p) => buildPositionSignature(p.ticker, p.tradeDate, p.qty, p.fillPrice))
  );

  const positions = new Map<string, PositionState>();
  for (const e of deduped) {
    const key = `${e.account}::${e.contractKey}`;
    let p = positions.get(key);
    if (!p) {
      p = {
        account: e.account,
        contractKey: e.contractKey,
        ticker: e.ticker,
        tradeType: e.tradeType,
        openQty: 0,
        avgOpenPrice: 0,
        totalOpenedQty: 0,
        totalOpenedNotional: 0,
        totalClosedQty: 0,
        totalClosedNotional: 0,
        realizedPl: 0,
        openDate: null,
        openTime: null,
        closeDate: null,
        closeTime: null
      };
      positions.set(key, p);
    }
    if (!p.tradeType && e.tradeType) p.tradeType = e.tradeType;
    upsertPositionFromEvent(p, e);
  }

  let created = 0;
  let openRows = 0;
  let closedRows = 0;
  let skippedAsPublicDuplicate = 0;

  for (const p of positions.values()) {
    if (p.totalOpenedQty <= 0 || !p.openDate) continue;
    const avgOpenPrice = p.totalOpenedQty > 0 ? p.totalOpenedNotional / p.totalOpenedQty : 0;
    const avgClosePrice = p.totalClosedQty > 0 ? p.totalClosedNotional / p.totalClosedQty : null;
    const multiplier = p.contractKey.startsWith("O") ? 100 : 1;
    const fillPrice = round2(avgOpenPrice * multiplier);
    const signature = buildPositionSignature(p.ticker, p.openDate, p.totalOpenedQty, fillPrice);
    if (publicSignatures.has(signature)) {
      skippedAsPublicDuplicate += 1;
      continue;
    }

    const page = await createPositionPage({
      title: p.ticker,
      ticker: p.ticker,
      contractKey: p.contractKey,
      qty: round2(p.totalOpenedQty),
      avgPrice: fillPrice,
      tradeType: p.tradeType,
      openDate: p.openDate,
      openTime: p.openTime,
      broker: "Public (History)",
      account: p.account
    });
    created += 1;

    if (Math.abs(p.openQty) < 1e-9 && p.closeDate) {
      await updatePositionPage({
        pageId: (page as any).id,
        ticker: p.ticker,
        contractKey: p.contractKey,
        qty: round2(p.totalOpenedQty),
        avgPrice: round2(avgOpenPrice * multiplier),
        tradeType: p.tradeType,
        status: "CLOSED",
        closeDate: p.closeDate,
        closeTime: p.closeTime,
        closePrice: avgClosePrice === null ? null : round2(avgClosePrice * multiplier),
        realizedPl: round2(p.realizedPl)
      });
      closedRows += 1;
    } else {
      openRows += 1;
    }
  }

  return {
    filePath,
    startDate,
    parsedLines: lines.length,
    parsedEvents: events.length,
    uniqueEvents: deduped.length,
    archivedExistingHistoryRows: archivedHistory.archived,
    archivedExistingPdfRows: archivedPdf.archived,
    createdRows: created,
    openRows,
    closedRows,
    skippedAsPublicDuplicate
  };
}

export async function runBackfillPublicHistoryTradeType() {
  const defaultFile = path.join(process.cwd(), "imports", "public", "history", "data.txt");
  const filePath = process.env.PUBLIC_HISTORY_FILE?.trim() || defaultFile;

  const raw = await fs.readFile(filePath, "utf8");
  const lines = raw.split(/\r?\n/);

  const typeByContractKey = new Map<string, "Stock" | "Call" | "Put">();
  let parsedEvents = 0;
  let conflicts = 0;

  for (const line of lines) {
    if (!line.includes("\t")) continue;
    if (!line.match(/^\d{2}-[A-Z0-9]+/)) continue;
    const event = parseLine(line);
    if (!event || !event.tradeType) continue;
    parsedEvents += 1;
    const key = event.contractKey.toUpperCase();
    const existing = typeByContractKey.get(key);
    if (!existing) {
      typeByContractKey.set(key, event.tradeType);
      continue;
    }
    if (existing !== event.tradeType) {
      conflicts += 1;
    }
  }

  const backfill = await backfillTradeTypeForBrokerByContractKey(
    "Public (History)",
    typeByContractKey
  );

  return {
    filePath,
    parsedEvents,
    uniqueContractKeys: typeByContractKey.size,
    conflicts,
    ...backfill
  };
}
