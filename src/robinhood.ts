import fs from "node:fs/promises";
import path from "node:path";
import {
  archiveTradePagesByBrokerPrefix,
  createPositionPage,
  loadManualStrategyTagsIndexForBroker,
  lookupManualStrategyTags,
  updatePositionPage
} from "./notion.js";

type RobinhoodEvent = {
  dedupeKey: string;
  date: string;
  action: "BUY" | "SELL";
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
  hasLongExposure: boolean;
  hasShortExposure: boolean;
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

function mmddyyyyToIso(value: string): string | null {
  const m = value.trim().match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (!m) return null;
  const mm = m[1].padStart(2, "0");
  const dd = m[2].padStart(2, "0");
  return `${m[3]}-${mm}-${dd}`;
}

function toNumber(value: string): number | null {
  if (!value) return null;
  const neg = value.includes("(") && value.includes(")");
  const clean = value.replace(/[$,()]/g, "").trim();
  if (!clean) return null;
  const parsed = Number(clean);
  if (!Number.isFinite(parsed)) return null;
  return neg ? -parsed : parsed;
}

function parseQty(value: string): number | null {
  if (!value) return null;
  const clean = value.replace(/[^0-9.\-]/g, "");
  const parsed = Number(clean);
  if (!Number.isFinite(parsed)) return null;
  const abs = Math.abs(parsed);
  return abs > 0 ? abs : null;
}

function normalizeAction(code: string, quantityRaw: string): "BUY" | "SELL" | null {
  const upper = code.trim().toUpperCase();
  if (!upper) return null;
  if (upper === "OEXP" || upper === "OCA") {
    return quantityRaw.trim().toUpperCase().endsWith("S") ? "SELL" : "BUY";
  }
  if (upper.startsWith("B")) return "BUY";
  if (upper.startsWith("S")) return "SELL";
  return null;
}

function strikeTo8(value: string) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return String(Math.round(n * 1000)).padStart(8, "0");
}

function optionContractKey(
  ticker: string,
  month: string,
  day: string,
  year: string,
  callOrPut: string,
  strikeRaw: string
) {
  const yy = String(Number(year) % 100).padStart(2, "0");
  const mm = String(Number(month)).padStart(2, "0");
  const dd = String(Number(day)).padStart(2, "0");
  const cp = callOrPut.toUpperCase() === "CALL" ? "C" : "P";
  const strike = strikeTo8(strikeRaw);
  if (!strike) return null;
  return `${ticker} ${yy}${mm}${dd}${cp}${strike}`;
}

function optionMetaFromDescription(
  description: string,
  instrument: string
): { ticker: string; contractKey: string; multiplier: number; tradeType: "Call" | "Put" } | null {
  const d = description.toUpperCase().replace(/\s+/g, " ").trim();
  const standard = d.match(
    /^([A-Z.\-]+)\s+(\d{1,2})\/(\d{1,2})\/(\d{2,4})\s+(CALL|PUT)\s+\$?([\d.]+)$/
  );
  if (standard) {
    const ticker = standard[1] || instrument;
    const contractKey = optionContractKey(
      ticker,
      standard[2],
      standard[3],
      standard[4],
      standard[5],
      standard[6]
    );
    if (!contractKey) return null;
    return {
      ticker,
      contractKey,
      multiplier: 100,
      tradeType: standard[5].toUpperCase() === "CALL" ? "Call" : "Put"
    };
  }

  const expiration = d.match(
    /^OPTION EXPIRATION FOR\s+([A-Z.\-]+)\s+(\d{1,2})\/(\d{1,2})\/(\d{2,4})\s+(CALL|PUT)\s+\$?([\d.]+)$/
  );
  if (!expiration) return null;
  const ticker = expiration[1] || instrument;
  const contractKey = optionContractKey(
    ticker,
    expiration[2],
    expiration[3],
    expiration[4],
    expiration[5],
    expiration[6]
  );
  if (!contractKey) return null;
  return {
    ticker,
    contractKey,
    multiplier: 100,
    tradeType: expiration[5].toUpperCase() === "CALL" ? "Call" : "Put"
  };
}

function normalizeTicker(value: string) {
  return value.trim().toUpperCase().replace(/[^A-Z0-9.\-]/g, "");
}

function toEvent(row: Record<string, string>, startDate: string): RobinhoodEvent | null {
  const date = mmddyyyyToIso(row["Activity Date"] ?? "");
  if (!date || date < startDate) return null;

  const transCode = String(row["Trans Code"] ?? "");
  const quantityRaw = String(row["Quantity"] ?? "");
  const action = normalizeAction(transCode, quantityRaw);
  if (!action) return null;

  const instrument = normalizeTicker(row["Instrument"] ?? "");
  const description = String(row["Description"] ?? "").trim();
  if (!instrument) return null;

  const qty = parseQty(quantityRaw);
  if (qty === null) return null;

  const rawPrice = toNumber(row["Price"] ?? "");
  const upperCode = transCode.toUpperCase();
  const price = rawPrice ?? (upperCode === "OEXP" || upperCode === "OCA" ? 0 : null);
  if (price === null) return null;

  const optionMeta = optionMetaFromDescription(description, instrument);
  const ticker = optionMeta?.ticker ?? instrument;
  const contractKey = optionMeta?.contractKey ?? instrument;
  const multiplier = optionMeta?.multiplier ?? 1;
  const tradeType: "Stock" | "Call" | "Put" = optionMeta?.tradeType ?? "Stock";

  const dedupeKey = [
    row["Activity Date"] ?? "",
    row["Process Date"] ?? "",
    row["Settle Date"] ?? "",
    row["Instrument"] ?? "",
    row["Description"] ?? "",
    row["Trans Code"] ?? "",
    row["Quantity"] ?? "",
    row["Price"] ?? "",
    row["Amount"] ?? ""
  ]
    .map((v) => String(v).trim())
    .join("|");

  return {
    dedupeKey,
    date,
    action,
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

function upsertPositionFromEvent(p: PositionState, e: RobinhoodEvent) {
  const q = e.qty;
  const px = e.price;
  const m = e.multiplier;

  if (e.action === "BUY") {
    if (p.openQty >= 0) {
      p.hasLongExposure = true;
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
      p.hasLongExposure = true;
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
    p.hasShortExposure = true;
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
    p.hasShortExposure = true;
    p.openQty = -remainder;
    p.avgOpenPrice = px;
    p.totalOpenedQty += remainder;
    p.totalOpenedNotional += remainder * px;
    p.openDate = e.date;
    p.closeDate = null;
  }
}

function inferPositionSide(p: PositionState): "BUY" | "SELL" | null {
  if (p.hasLongExposure && !p.hasShortExposure) return "BUY";
  if (p.hasShortExposure && !p.hasLongExposure) return "SELL";
  return null;
}

async function listCsvFiles(dir: string) {
  try {
    const entries = await fs.readdir(dir, { withFileTypes: true });
    return entries
      .filter((e) => e.isFile() && e.name.toLowerCase().endsWith(".csv"))
      .map((e) => path.join(dir, e.name))
      .sort((a, b) => a.localeCompare(b));
  } catch {
    return [];
  }
}

export async function runImportRobinhood() {
  const root = process.cwd();
  const rawDir = path.join(root, "imports", "robinhood", "raw");
  const processedDir = path.join(root, "imports", "robinhood", "processed");
  await fs.mkdir(processedDir, { recursive: true });

  const startDate = process.env.ROBINHOOD_START_DATE?.trim() || "2025-01-01";
  const rawFiles = await listCsvFiles(rawDir);
  const processedFiles = await listCsvFiles(processedDir);
  const allFiles = [...processedFiles, ...rawFiles];

  const eventMap = new Map<string, RobinhoodEvent>();
  const eventCountByKey = new Map<string, number>();
  let parsedRows = 0;

  for (const file of allFiles) {
    const content = await fs.readFile(file, "utf8");
    const rows = parseCsv(content);
    parsedRows += rows.length;
    const perFileCount = new Map<string, number>();
    const perFileEvent = new Map<string, RobinhoodEvent>();
    for (const row of rows) {
      const event = toEvent(row, startDate);
      if (!event) continue;
      const key = event.dedupeKey;
      perFileEvent.set(key, event);
      perFileCount.set(key, (perFileCount.get(key) ?? 0) + 1);
    }
    for (const [key, count] of perFileCount.entries()) {
      eventMap.set(key, perFileEvent.get(key)!);
      const previous = eventCountByKey.get(key) ?? 0;
      if (count > previous) {
        eventCountByKey.set(key, count);
      }
    }
  }

  const events: RobinhoodEvent[] = [];
  for (const [key, event] of eventMap.entries()) {
    const count = eventCountByKey.get(key) ?? 1;
    for (let i = 0; i < count; i += 1) events.push(event);
  }
  events.sort((a, b) => {
    const ak = `${a.date}|${a.account}|${a.contractKey}|${a.action}`;
    const bk = `${b.date}|${b.account}|${b.contractKey}|${b.action}`;
    return ak.localeCompare(bk);
  });

  const archivedExisting = await archiveTradePagesByBrokerPrefix("Robinhood (CSV)");
  const manualIndex = await loadManualStrategyTagsIndexForBroker("Robinhood (CSV)");

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
        closeDate: null,
        hasLongExposure: false,
        hasShortExposure: false
      };
      positions.set(key, p);
    }
    upsertPositionFromEvent(p, e);
  }

  let createdRows = 0;
  let openRows = 0;
  let closedRows = 0;

  for (const p of positions.values()) {
    if (p.totalOpenedQty <= 0 || !p.openDate) continue;
    const avgOpenPrice = p.totalOpenedQty > 0 ? p.totalOpenedNotional / p.totalOpenedQty : 0;
    const avgClosePrice = p.totalClosedQty > 0 ? p.totalClosedNotional / p.totalClosedQty : null;
    const isOption = /\s\d{6}[CP]\d{8}$/i.test(p.contractKey);
    const multiplier = isOption ? 100 : 1;
    const tradeType = isOption
      ? p.contractKey.match(/\s\d{6}([CP])\d{8}$/i)?.[1]?.toUpperCase() === "C"
        ? "Call"
        : "Put"
      : "Stock";
    const side = inferPositionSide(p);

    const manual = lookupManualStrategyTags(manualIndex, p.account, p.contractKey, p.openDate);
    const page = await createPositionPage({
      title: p.ticker,
      ticker: p.ticker,
      contractKey: p.contractKey,
      qty: round2(p.totalOpenedQty),
      avgPrice: round2(avgOpenPrice * multiplier),
      side,
      tradeType,
      openDate: p.openDate,
      openTime: null,
      broker: "Robinhood (CSV)",
      account: p.account,
      strategies: manual?.strategies ?? undefined,
      tags: manual?.tags ?? undefined
    });
    createdRows += 1;

    if (Math.abs(p.openQty) < 1e-9 && p.closeDate) {
      await updatePositionPage({
        pageId: (page as any).id,
        ticker: p.ticker,
        contractKey: p.contractKey,
        qty: round2(p.totalOpenedQty),
        avgPrice: round2(avgOpenPrice * multiplier),
        side,
        tradeType,
        status: "CLOSED",
        closeDate: p.closeDate,
        closeTime: null,
        closePrice: avgClosePrice === null ? null : round2(avgClosePrice * multiplier),
        realizedPl: round2(p.realizedPl)
      });
      closedRows += 1;
    } else {
      openRows += 1;
    }
  }

  for (const file of rawFiles) {
    const base = path.basename(file);
    let target = path.join(processedDir, base);
    try {
      await fs.access(target);
      const stamp = new Date().toISOString().replace(/[:.]/g, "-");
      target = path.join(processedDir, `${path.parse(base).name}-${stamp}.csv`);
    } catch {
      // no-op
    }
    await fs.rename(file, target);
  }

  return {
    startDate,
    files: { raw: rawFiles.length, processed: processedFiles.length, total: allFiles.length },
    parsedRows,
    uniqueEvents: events.length,
    archivedExistingRows: archivedExisting.archived,
    createdRows,
    openRows,
    closedRows,
    brokerLabel: "Robinhood (CSV)"
  };
}
