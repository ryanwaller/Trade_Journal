import fs from "node:fs/promises";
import path from "node:path";
import {
  PROPERTY,
  archivePage,
  createPositionPage,
  getJournalInfo,
  getNotionClient,
  updatePositionPage
} from "./notion.js";

type SnapshotPosition = {
  account: "Fun" | "IRA Roth" | "IRA Trad";
  broker: "Fidelity";
  contractKey: string;
  ticker: string;
  qty: number;
  avgPrice: number;
};

type ExistingOpenRow = {
  pageId: string;
  key: string;
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
  const headerIndex = lines.findIndex((line) => line.includes("Account Number,Account Name,Symbol"));
  if (headerIndex < 0) return [];
  const headers = parseCsvLine(lines[headerIndex]).map((h) => h.trim());
  const rows: Record<string, string>[] = [];
  for (let i = headerIndex + 1; i < lines.length; i += 1) {
    const cols = parseCsvLine(lines[i]);
    const row: Record<string, string> = {};
    headers.forEach((h, idx) => {
      row[h] = (cols[idx] ?? "").trim();
    });
    rows.push(row);
  }
  return rows;
}

function toNumber(input: string): number | null {
  if (!input) return null;
  const clean = input.replace(/\$/g, "").replace(/,/g, "");
  const parsed = Number(clean);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeAccount(value: string): SnapshotPosition["account"] | null {
  const upper = value.toUpperCase();
  if (upper.includes("FUN")) return "Fun";
  if (upper.includes("ROTH")) return "IRA Roth";
  if (upper.includes("TRADITIONAL")) return "IRA Trad";
  return null;
}

function normalizeContractKey(symbol: string) {
  return symbol.trim().replace(/^[-+]/, "").toUpperCase();
}

function isOptionContract(key: string) {
  return /^[A-Z.\-]+\d{6}[CP]\d+/i.test(key);
}

function extractTicker(symbol: string, description: string) {
  const match = description.match(/\b([A-Z.\-]{1,10})\b/);
  if (symbol.trim().startsWith("-")) {
    return (symbol.trim().replace(/^[-+]/, "").match(/^([A-Z.\-]+)/)?.[1] ?? "").toUpperCase();
  }
  return (match?.[1] ?? symbol).trim().replace(/^[-+]/, "").toUpperCase();
}

function makeKey(account: string, contractKey: string) {
  return `${account}::${contractKey}`;
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

async function listExistingOpenFidelityRows(): Promise<Map<string, ExistingOpenRow>> {
  const info = await getJournalInfo();
  const client = getNotionClient();
  let cursor: string | undefined;
  const map = new Map<string, ExistingOpenRow>();

  do {
    const response = await client.databases.query({
      database_id: info.databaseId,
      start_cursor: cursor
    });
    for (const page of response.results as any[]) {
      if (page.archived) continue;
      const broker = getSelect(page, PROPERTY.broker);
      const status = getSelect(page, PROPERTY.status).toUpperCase();
      if (broker !== "Fidelity" || status !== "OPEN") continue;
      const account = getRichText(page, PROPERTY.account);
      const contractKey = getRichText(page, PROPERTY.contractKey);
      if (!account || !contractKey) continue;
      const key = makeKey(account, contractKey);
      map.set(key, { pageId: page.id, key });
    }
    cursor = response.has_more ? response.next_cursor ?? undefined : undefined;
  } while (cursor);

  return map;
}

export async function runImportFidelityPositions() {
  const dir = path.join(process.cwd(), "imports", "fidelity", "positions");
  const entries = await fs.readdir(dir, { withFileTypes: true }).catch(() => []);
  const files = entries
    .filter((e) => e.isFile() && e.name.toLowerCase().endsWith(".csv"))
    .map((e) => path.join(dir, e.name))
    .sort((a, b) => a.localeCompare(b));

  const positions = new Map<string, SnapshotPosition>();
  let parsedRows = 0;

  for (const file of files) {
    const content = await fs.readFile(file, "utf8");
    const rows = parseCsv(content);
    parsedRows += rows.length;
    for (const row of rows) {
      const account = normalizeAccount(row["Account Name"] ?? "");
      if (!account) continue;
      const symbol = (row["Symbol"] ?? "").trim();
      if (!symbol || symbol.includes("SPAXX") || symbol.toUpperCase().includes("PENDING")) {
        continue;
      }
      const qtyRaw = toNumber(row["Quantity"] ?? "");
      const avgRaw = toNumber(row["Average Cost Basis"] ?? "");
      if (qtyRaw === null || avgRaw === null) continue;
      const qty = Math.abs(qtyRaw);
      if (!Number.isFinite(qty) || qty <= 0) continue;
      const contractKey = normalizeContractKey(symbol);
      const ticker = extractTicker(symbol, row["Description"] ?? "");
      const avgPrice = isOptionContract(contractKey) ? avgRaw * 100 : avgRaw;
      const key = makeKey(account, contractKey);
      positions.set(key, {
        account,
        broker: "Fidelity",
        contractKey,
        ticker,
        qty,
        avgPrice: Math.round(avgPrice * 100) / 100
      });
    }
  }

  const existing = await listExistingOpenFidelityRows();
  let created = 0;
  let updated = 0;
  let archivedMissing = 0;

  for (const [key, p] of positions.entries()) {
    const found = existing.get(key);
    if (!found) {
      await createPositionPage({
        title: p.ticker,
        ticker: p.ticker,
        contractKey: p.contractKey,
        qty: p.qty,
        avgPrice: p.avgPrice,
        openDate: null,
        openTime: null,
        broker: p.broker,
        account: p.account
      });
      created += 1;
      continue;
    }

    await updatePositionPage({
      pageId: found.pageId,
      ticker: p.ticker,
      contractKey: p.contractKey,
      qty: p.qty,
      avgPrice: p.avgPrice,
      status: "OPEN"
    });
    updated += 1;
  }

  // Archive stale Fidelity OPEN rows that are no longer present in holdings snapshots.
  for (const [key, row] of existing.entries()) {
    if (positions.has(key)) continue;
    await archivePage(row.pageId);
    archivedMissing += 1;
  }

  return {
    files: files.length,
    parsedRows,
    snapshotOpenPositions: positions.size,
    created,
    updated,
    archivedMissing
  };
}

