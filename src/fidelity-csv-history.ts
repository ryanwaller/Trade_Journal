import fs from "node:fs/promises";
import path from "node:path";
import {
  archiveTradePagesByExactBroker,
  createPositionPage,
  loadManualStrategyTagsIndexForBroker,
  manualKeyForPosition,
  updatePositionPage
} from "./notion.js";

type FidelityEvent = {
  date: string;
  account: string;
  broker: string;
  action: "BUY" | "SELL";
  tradeType: "Stock" | "Call" | "Put" | null;
  symbol: string;
  contractKey: string;
  ticker: string;
  qty: number;
  price: number;
  multiplier: number;
};

type PositionState = {
  account: string;
  broker: string;
  contractKey: string;
  ticker: string;
  tradeType: "Stock" | "Call" | "Put" | null;
  qtyOpen: number;
  totalBoughtQty: number;
  avgPrice: number;
  totalSoldQty: number;
  totalSoldPrice: number;
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
  const headerIndex = lines.findIndex((line) => line.includes("Run Date,Account"));
  if (headerIndex < 0) return [];
  const headers = parseCsvLine(lines[headerIndex]).map((h) => h.trim());
  const rows: Record<string, string>[] = [];
  for (let i = headerIndex + 1; i < lines.length; i += 1) {
    const cols = parseCsvLine(lines[i]);
    if (cols.length < 4) continue;
    const row: Record<string, string> = {};
    headers.forEach((h, idx) => {
      row[h] = (cols[idx] ?? "").trim();
    });
    rows.push(row);
  }
  return rows;
}

function toDate(input: string): string | null {
  const [mm, dd, yyyy] = input.split("/");
  if (!mm || !dd || !yyyy) return null;
  if (yyyy.length !== 4) return null;
  return `${yyyy}-${mm.padStart(2, "0")}-${dd.padStart(2, "0")}`;
}

function toNumber(input: string): number | null {
  if (!input) return null;
  const clean = input.replace(/\$/g, "").replace(/,/g, "");
  const parsed = Number(clean);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeContractKey(symbol: string): string {
  return symbol.trim().replace(/^[-+]/, "").toUpperCase();
}

function extractTicker(action: string, description: string, symbol: string): string {
  const byParen = (action.match(/\(([A-Z.\-]+)\)/)?.[1] ??
    description.match(/\(([A-Z.\-]+)\)/)?.[1]) as string | undefined;
  if (byParen) return byParen.toUpperCase();
  const key = normalizeContractKey(symbol);
  const byPrefix = key.match(/^([A-Z.\-]+)/)?.[1];
  return (byPrefix ?? key).toUpperCase();
}

function isOptionSymbol(symbol: string, action: string, description: string): boolean {
  return (
    symbol.trim().startsWith("-") ||
    /\bCALL\b|\bPUT\b/i.test(action) ||
    /\bCALL\b|\bPUT\b/i.test(description)
  );
}

function inferTradeType(symbol: string, action: string, description: string): "Stock" | "Call" | "Put" | null {
  const upper = `${symbol} ${action} ${description}`.toUpperCase();
  if (upper.includes(" CALL")) return "Call";
  if (upper.includes(" PUT")) return "Put";
  if (isOptionSymbol(symbol, action, description)) return null;
  return "Stock";
}

function normalizedAccountFromRaw(account: string): string | null {
  const upper = account.toUpperCase();
  if (upper.includes("FUN")) return "Taxable (Fun)";
  if (upper.includes("ROTH")) return "IRA (Roth)";
  if (upper.includes("TRADITIONAL")) return "IRA (Traditional)";
  return null;
}

function eventFromRow(row: Record<string, string>, startDate: string): FidelityEvent | null {
  const date = toDate(row["Run Date"] ?? "");
  if (!date || date < startDate) return null;

  const account = row["Account"] ?? "";
  const actionRaw = (row["Action"] ?? "").toUpperCase();
  const description = row["Description"] ?? "";
  const symbolRaw = row["Symbol"] ?? "";
  const qtyRaw = toNumber(row["Quantity"] ?? "");

  if (!account || !symbolRaw || qtyRaw === null) return null;

  let action: "BUY" | "SELL" | null = null;
  if (actionRaw.includes("YOU BOUGHT")) action = "BUY";
  if (actionRaw.includes("YOU SOLD")) action = "SELL";
  if (!action && actionRaw.includes("EXPIRED")) action = "SELL";
  if (!action) return null;

  const qty = Math.abs(qtyRaw);
  if (!Number.isFinite(qty) || qty <= 0) return null;

  const isOption = isOptionSymbol(symbolRaw, actionRaw, description);
  const priceRaw = toNumber(row["Price ($)"] ?? "");
  const price = priceRaw ?? (actionRaw.includes("EXPIRED") ? 0 : null);
  if (price === null) return null;

  const contractKey = normalizeContractKey(symbolRaw);
  const ticker = extractTicker(actionRaw, description, symbolRaw);
  const tradeType = inferTradeType(symbolRaw, actionRaw, description);
  const normalizedAccount = normalizedAccountFromRaw(account);
  if (!normalizedAccount) return null;

  return {
    date,
    account: normalizedAccount,
    broker: "Fidelity (CSV)",
    action,
    tradeType,
    symbol: symbolRaw,
    contractKey,
    ticker,
    qty,
    price,
    multiplier: isOption ? 100 : 1
  };
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

function round2(value: number) {
  return Math.round(value * 100) / 100;
}

export async function runImportFidelityCsvHistory() {
  const root = process.cwd();
  const rawDir = path.join(root, "imports", "fidelity", "raw");
  const processedDir = path.join(root, "imports", "fidelity", "processed");
  await fs.mkdir(processedDir, { recursive: true });

  const startDate = process.env.FIDELITY_CSV_START_DATE?.trim() || "2025-01-01";

  const rawFiles = await listCsvFiles(rawDir);
  const processedFiles = await listCsvFiles(processedDir);
  const allFiles = [...processedFiles, ...rawFiles];

  const eventMap = new Map<string, FidelityEvent>();
  let parsedRows = 0;

  for (const file of allFiles) {
    const content = await fs.readFile(file, "utf8");
    const rows = parseCsv(content);
    parsedRows += rows.length;
    for (const row of rows) {
      const event = eventFromRow(row, startDate);
      if (!event) continue;
      const key = [
        event.date,
        event.account,
        event.action,
        event.symbol,
        event.qty.toString(),
        event.price.toString()
      ].join("|");
      eventMap.set(key, event);
    }
  }

  const events = Array.from(eventMap.values()).sort((a, b) => {
    if (a.date === b.date) {
      return `${a.account}|${a.contractKey}|${a.action}`.localeCompare(
        `${b.account}|${b.contractKey}|${b.action}`
      );
    }
    return a.date.localeCompare(b.date);
  });

  const archivedExisting = await archiveTradePagesByExactBroker("Fidelity (CSV)");
  const manualIndex = await loadManualStrategyTagsIndexForBroker("Fidelity (CSV)");

  const positions = new Map<string, PositionState>();
  for (const e of events) {
    const key = `${e.account}::${e.contractKey}`;
    let p = positions.get(key);
    if (!p) {
      p = {
        account: e.account,
        broker: e.broker,
        contractKey: e.contractKey,
        ticker: e.ticker,
        tradeType: e.tradeType,
        qtyOpen: 0,
        totalBoughtQty: 0,
        avgPrice: 0,
        totalSoldQty: 0,
        totalSoldPrice: 0,
        realizedPl: 0,
        openDate: null,
        closeDate: null
      };
      positions.set(key, p);
    }
    if (!p.tradeType && e.tradeType) p.tradeType = e.tradeType;

    if (e.action === "BUY") {
      const newQty = p.qtyOpen + e.qty;
      p.avgPrice = newQty > 0 ? (p.qtyOpen * p.avgPrice + e.qty * e.price) / newQty : 0;
      p.qtyOpen = newQty;
      p.totalBoughtQty += e.qty;
      p.openDate = p.openDate ?? e.date;
      continue;
    }

    if (p.qtyOpen <= 0) continue;

    const closingQty = Math.min(p.qtyOpen, e.qty);
    p.realizedPl += (e.price - p.avgPrice) * closingQty * e.multiplier;
    p.totalSoldQty += closingQty;
    p.totalSoldPrice += e.price * closingQty;
    p.qtyOpen -= closingQty;
    if (p.qtyOpen === 0) {
      p.closeDate = e.date;
    }
  }

  let created = 0;
  let closed = 0;
  let open = 0;

  for (const p of positions.values()) {
    if (p.totalBoughtQty <= 0 || !p.openDate) continue;
    const multiplier = /^\w+\d{6}[CP]/i.test(p.contractKey) ? 100 : 1;
    const manual = manualIndex.get(manualKeyForPosition(p.account, p.contractKey, p.openDate));
    const page = await createPositionPage({
      title: p.ticker,
      ticker: p.ticker,
      contractKey: p.contractKey,
      qty: p.totalBoughtQty,
      avgPrice: round2(p.avgPrice * multiplier),
      tradeType: p.tradeType,
      openDate: p.openDate,
      openTime: null,
      broker: p.broker,
      account: p.account,
      strategy: manual?.strategy ?? undefined,
      tags: manual?.tags ?? undefined
    });
    created += 1;

    if (p.qtyOpen === 0 && p.closeDate) {
      const avgClose =
        p.totalSoldQty > 0 ? round2((p.totalSoldPrice / p.totalSoldQty) * multiplier) : null;
      await updatePositionPage({
        pageId: (page as any).id,
        ticker: p.ticker,
        contractKey: p.contractKey,
        qty: p.totalBoughtQty,
        avgPrice: round2(p.avgPrice * multiplier),
        tradeType: p.tradeType,
        status: "CLOSED",
        closeDate: p.closeDate,
        closeTime: null,
        closePrice: avgClose,
        realizedPl: round2(p.realizedPl)
      });
      closed += 1;
    } else {
      open += 1;
    }
  }

  return {
    broker: "Fidelity (CSV)",
    startDate,
    files: { raw: rawFiles.length, processed: processedFiles.length, total: allFiles.length },
    parsedRows,
    uniqueEvents: events.length,
    archivedExistingRows: archivedExisting.archived,
    createdRows: created,
    openRows: open,
    closedRows: closed
  };
}
