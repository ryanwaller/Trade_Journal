import fs from "node:fs/promises";
import path from "node:path";
import { PROPERTY, getJournalInfo, getNotionClient } from "./notion.js";

type Row = {
  pageId: string;
  broker: string;
  status: string;
  account: string;
  contractKey: string;
  tradeDate: string | null;
  qty: number | null;
  fillPrice: number | null;
};

type FidelityEvent = {
  date: string;
  account: string;
  action: "BUY" | "SELL";
  contractKey: string;
  qty: number;
  price: number;
  multiplier: number;
};

type PositionState = {
  qtyOpen: number;
  totalBoughtQty: number;
  avgPrice: number;
  totalSoldQty: number;
  totalSoldPrice: number;
  realizedPl: number;
  closeDate: string | null;
};

function isOptionContract(contractKey: string) {
  return /^[A-Z.\-]+\d{6}[CP]\d+$/i.test(contractKey.replace(/\s+/g, ""));
}

function optionExpiryFromContractKey(contractKey: string): string | null {
  const compact = contractKey.replace(/\s+/g, "").toUpperCase();
  const m = compact.match(/^[A-Z.\-]+(\d{2})(\d{2})(\d{2})[CP]\d+$/);
  if (!m) return null;
  const yy = Number(m[1]);
  const mm = Number(m[2]);
  const dd = Number(m[3]);
  if (!Number.isFinite(yy) || !Number.isFinite(mm) || !Number.isFinite(dd)) return null;
  const fullYear = 2000 + yy;
  return `${fullYear.toString().padStart(4, "0")}-${mm
    .toString()
    .padStart(2, "0")}-${dd.toString().padStart(2, "0")}`;
}

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
  if (!mm || !dd || !yyyy || yyyy.length !== 4) return null;
  return `${yyyy}-${mm.padStart(2, "0")}-${dd.padStart(2, "0")}`;
}

function toNumber(input: string): number | null {
  if (!input) return null;
  const clean = input.replace(/\$/g, "").replace(/,/g, "");
  const parsed = Number(clean);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeContractKey(value: string) {
  return value.trim().replace(/^[-+]/, "").toUpperCase();
}

function normalizeAccountFromCsv(value: string): string | null {
  const upper = value.toUpperCase();
  if (upper.includes("FUN")) return "TAXABLE FUN";
  if (upper.includes("ROTH")) return "IRA ROTH";
  if (upper.includes("TRADITIONAL") || upper.includes("TRAD")) return "IRA TRADITIONAL";
  return null;
}

function normalizeAccountFromNotion(value: string) {
  const upper = value.toUpperCase();
  if (upper.includes("FUN")) return "TAXABLE FUN";
  if (upper.includes("ROTH")) return "IRA ROTH";
  if (upper.includes("TRADITIONAL") || upper.includes("TRAD")) return "IRA TRADITIONAL";
  return upper.replace(/[()]/g, " ").replace(/\s+/g, " ").trim();
}

function actionFromRow(row: Record<string, string>): "BUY" | "SELL" | null {
  const actionRaw = (row["Action"] ?? "").toUpperCase();
  if (actionRaw.includes("YOU BOUGHT")) return "BUY";
  if (actionRaw.includes("YOU SOLD")) return "SELL";
  if (actionRaw.includes("EXPIRED")) return "SELL";
  return null;
}

function isOptionSymbol(symbol: string, action: string, description: string): boolean {
  return (
    symbol.trim().startsWith("-") ||
    /\bCALL\b|\bPUT\b/i.test(action) ||
    /\bCALL\b|\bPUT\b/i.test(description)
  );
}

function eventFromRow(row: Record<string, string>): FidelityEvent | null {
  const date = toDate(row["Run Date"] ?? "");
  if (!date) return null;
  const accountRaw = row["Account"] ?? "";
  const account = normalizeAccountFromCsv(accountRaw);
  if (!account) return null;
  const action = actionFromRow(row);
  if (!action) return null;
  const symbolRaw = row["Symbol"] ?? "";
  if (!symbolRaw) return null;
  const qtyRaw = toNumber(row["Quantity"] ?? "");
  const qty = qtyRaw === null ? null : Math.abs(qtyRaw);
  if (qty === null || qty <= 0) return null;
  const actionRaw = (row["Action"] ?? "").toUpperCase();
  const description = row["Description"] ?? "";
  const isOption = isOptionSymbol(symbolRaw, actionRaw, description);
  const priceRaw = toNumber(row["Price ($)"] ?? "");
  const price = priceRaw ?? (actionRaw.includes("EXPIRED") ? 0 : null);
  if (price === null) return null;

  return {
    date,
    account,
    action,
    contractKey: normalizeContractKey(symbolRaw),
    qty,
    price,
    multiplier: isOption ? 100 : 1
  };
}

function stateKey(account: string, contractKey: string) {
  return `${account}::${normalizeContractKey(contractKey)}`;
}

function normContract(value: string) {
  return value.replace(/\s+/g, "").toUpperCase().trim();
}

function numKey(value: number | null) {
  if (value === null || !Number.isFinite(value)) return "";
  return (Math.round(value * 10000) / 10000).toString();
}

function statusUpper(value: string) {
  return value.trim().toUpperCase();
}

function fp(row: Row) {
  return [
    normalizeAccountFromNotion(row.account),
    normContract(row.contractKey),
    row.tradeDate ?? "",
    numKey(row.qty),
    numKey(row.fillPrice)
  ].join("|");
}

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

function getNumber(page: any, key: string) {
  const prop = page.properties?.[key];
  if (!prop || prop.type !== "number") return null;
  return prop.number;
}

function getDate(page: any, key: string) {
  const prop = page.properties?.[key];
  if (!prop || prop.type !== "date") return null;
  return prop.date?.start ?? null;
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

function buildCsvStateMap(events: FidelityEvent[]) {
  const map = new Map<string, PositionState>();
  for (const e of events) {
    const key = stateKey(e.account, e.contractKey);
    let p = map.get(key);
    if (!p) {
      p = {
        qtyOpen: 0,
        totalBoughtQty: 0,
        avgPrice: 0,
        totalSoldQty: 0,
        totalSoldPrice: 0,
        realizedPl: 0,
        closeDate: null
      };
      map.set(key, p);
    }

    if (e.action === "BUY") {
      const newQty = p.qtyOpen + e.qty;
      p.avgPrice = newQty > 0 ? (p.qtyOpen * p.avgPrice + e.qty * e.price) / newQty : 0;
      p.qtyOpen = newQty;
      p.totalBoughtQty += e.qty;
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
  return map;
}

export async function runReconcileFidelityCsv() {
  const info = await getJournalInfo();
  const client = getNotionClient();
  const dryRun = process.env.FIDELITY_CSV_RECONCILE_DRY_RUN === "1";
  const forceExpireClose = process.env.FIDELITY_CSV_FORCE_EXPIRE === "1";
  const today = new Date().toISOString().slice(0, 10);

  // Build state from Fidelity CSV files (source-of-truth for CSV history rows).
  const root = process.cwd();
  const rawDir = path.join(root, "imports", "fidelity", "raw");
  const processedDir = path.join(root, "imports", "fidelity", "processed");
  const files = [...(await listCsvFiles(processedDir)), ...(await listCsvFiles(rawDir))];
  const events: FidelityEvent[] = [];
  let parsedRows = 0;
  for (const file of files) {
    const rows = parseCsv(await fs.readFile(file, "utf8"));
    parsedRows += rows.length;
    for (const row of rows) {
      const e = eventFromRow(row);
      if (e) events.push(e);
    }
  }
  events.sort((a, b) => a.date.localeCompare(b.date));
  const csvState = buildCsvStateMap(events);

  let cursor: string | undefined;
  const fidelityRows: Row[] = [];
  const fidelityCsvRows: Row[] = [];

  do {
    const response = await client.databases.query({
      database_id: info.databaseId,
      start_cursor: cursor
    });
    for (const page of response.results as any[]) {
      if (page.archived) continue;
      const rowType = getSelect(page, PROPERTY.rowType);
      if (rowType && rowType !== "Trade") continue;
      const broker = getSelect(page, PROPERTY.broker);
      if (broker !== "Fidelity" && broker !== "Fidelity (CSV)") continue;
      const row: Row = {
        pageId: page.id,
        broker,
        status: getSelect(page, PROPERTY.status),
        account: getRichText(page, PROPERTY.account),
        contractKey: getRichText(page, PROPERTY.contractKey),
        tradeDate: getDate(page, PROPERTY.tradeDate),
        qty: getNumber(page, PROPERTY.qty),
        fillPrice: getNumber(page, PROPERTY.fillPrice)
      };
      if (!row.contractKey || !row.account) continue;
      if (broker === "Fidelity") fidelityRows.push(row);
      if (broker === "Fidelity (CSV)") fidelityCsvRows.push(row);
    }
    cursor = response.has_more ? response.next_cursor ?? undefined : undefined;
  } while (cursor);

  const fidelityFingerprints = new Set(fidelityRows.map(fp));

  let duplicateCsvRows = 0;
  let duplicateCsvRowsArchived = 0;
  let csvOpenRowsConsidered = 0;
  let csvClosedByCsvEvents = 0;
  let csvExpiredForceClosed = 0;
  let csvOpenStillUnresolved = 0;

  for (const row of fidelityCsvRows) {
    const isDuplicate = fidelityFingerprints.has(fp(row));
    if (isDuplicate) {
      duplicateCsvRows += 1;
      if (!dryRun) {
        await client.pages.update({ page_id: row.pageId, archived: true });
      }
      duplicateCsvRowsArchived += 1;
      continue;
    }

    if (statusUpper(row.status) !== "OPEN") continue;
    csvOpenRowsConsidered += 1;

    const key = stateKey(normalizeAccountFromNotion(row.account), row.contractKey);
    const p = csvState.get(key);
    if (!p || p.qtyOpen > 0 || !p.closeDate) {
      if (forceExpireClose) {
        const compactKey = normContract(row.contractKey);
        const option = isOptionContract(compactKey);
        const expiry = option ? optionExpiryFromContractKey(compactKey) : null;
        const isExpired = Boolean(expiry && expiry <= today);
        if (option && expiry && isExpired) {
          const fill = row.fillPrice ?? 0;
          const qty = row.qty ?? 0;
          const realized = -round2(fill * qty);
          if (!dryRun) {
            await client.pages.update({
              page_id: row.pageId,
              properties: {
                [PROPERTY.status]: { select: { name: "CLOSED" } },
                [PROPERTY.closeDate]: { date: { start: expiry } },
                [PROPERTY.closePrice]: { number: 0 },
                [PROPERTY.plAtClose]: { number: realized }
              }
            });
          }
          csvExpiredForceClosed += 1;
          continue;
        }
      }
      csvOpenStillUnresolved += 1;
      continue;
    }

    const avgClose = p.totalSoldQty > 0 ? round2((p.totalSoldPrice / p.totalSoldQty)) : null;
    if (!dryRun) {
      await client.pages.update({
        page_id: row.pageId,
        properties: {
          [PROPERTY.status]: { select: { name: "CLOSED" } },
          [PROPERTY.closeDate]: { date: { start: p.closeDate } },
          [PROPERTY.closePrice]: { number: avgClose },
          [PROPERTY.plAtClose]: { number: round2(p.realizedPl) }
        }
      });
    }
    csvClosedByCsvEvents += 1;
  }

  return {
    dryRun,
    files: files.length,
    parsedRows,
    parsedEvents: events.length,
    fidelityRows: fidelityRows.length,
    fidelityCsvRows: fidelityCsvRows.length,
    duplicateCsvRows,
    duplicateCsvRowsArchived,
    csvOpenRowsConsidered,
    csvClosedByCsvEvents,
    csvExpiredForceClosed,
    csvOpenStillUnresolved
  };
}
