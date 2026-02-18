import fs from "node:fs/promises";
import path from "node:path";
import { PROPERTY, getJournalInfo, getNotionClient } from "./notion.js";

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

function toIsoDate(input: string): string | null {
  const [mm, dd, yyyy] = input.split("/");
  if (!mm || !dd || !yyyy || yyyy.length !== 4) return null;
  const iso = `${yyyy}-${mm.padStart(2, "0")}-${dd.padStart(2, "0")}`;
  return /^\d{4}-\d{2}-\d{2}$/.test(iso) ? iso : null;
}

function normalizeContractKey(value: string) {
  return value.trim().replace(/^[-+]/, "").toUpperCase();
}

function normalizeAccount(value: string) {
  const upper = value.toUpperCase();
  if (upper.includes("FUN")) return "FUN";
  if (upper.includes("ROTH")) return "IRA_ROTH";
  if (upper.includes("TRADITIONAL") || upper.includes("TRAD")) return "IRA_TRAD";
  return upper.replace(/[^A-Z0-9]/g, "");
}

function actionFromRow(row: Record<string, string>): "BUY" | "SELL" | null {
  const actionRaw = (row["Action"] ?? "").toUpperCase();
  if (actionRaw.includes("YOU BOUGHT")) return "BUY";
  if (actionRaw.includes("YOU SOLD")) return "SELL";
  if (actionRaw.includes("EXPIRED")) return "SELL";
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

export async function runBackfillFidelityOpenDates() {
  const root = process.cwd();
  const rawDir = path.join(root, "imports", "fidelity", "raw");
  const processedDir = path.join(root, "imports", "fidelity", "processed");
  const files = [...(await listCsvFiles(processedDir)), ...(await listCsvFiles(rawDir))];

  const earliestBuyByKey = new Map<string, string>();
  let parsedRows = 0;

  for (const file of files) {
    const rows = parseCsv(await fs.readFile(file, "utf8"));
    parsedRows += rows.length;
    for (const row of rows) {
      const action = actionFromRow(row);
      if (action !== "BUY") continue;
      const date = toIsoDate(row["Run Date"] ?? "");
      const accountRaw = row["Account"] ?? "";
      const symbolRaw = row["Symbol"] ?? "";
      if (!date || !accountRaw || !symbolRaw) continue;
      const key = `${normalizeAccount(accountRaw)}::${normalizeContractKey(symbolRaw)}`;
      const prev = earliestBuyByKey.get(key);
      if (!prev || date < prev) earliestBuyByKey.set(key, date);
    }
  }

  const info = await getJournalInfo();
  const client = getNotionClient();
  let cursor: string | undefined;
  let updated = 0;
  let missingSource = 0;
  let fidelityOpenMissingDate = 0;

  do {
    const response = await client.databases.query({
      database_id: info.databaseId,
      start_cursor: cursor
    });

    for (const page of response.results as any[]) {
      if (page.archived) continue;
      const broker = page.properties?.[PROPERTY.broker]?.select?.name ?? "";
      const status = (page.properties?.[PROPERTY.status]?.select?.name ?? "").toUpperCase();
      if (broker !== "Fidelity" || status !== "OPEN") continue;

      const existingDate = page.properties?.[PROPERTY.tradeDate]?.date?.start ?? null;
      if (existingDate) continue;
      fidelityOpenMissingDate += 1;

      const account = (page.properties?.[PROPERTY.account]?.rich_text ?? [])
        .map((t: any) => t.plain_text ?? "")
        .join("")
        .trim();
      const contractKey = (page.properties?.[PROPERTY.contractKey]?.rich_text ?? [])
        .map((t: any) => t.plain_text ?? "")
        .join("")
        .trim();
      if (!account || !contractKey) {
        missingSource += 1;
        continue;
      }

      const lookupKey = `${normalizeAccount(account)}::${normalizeContractKey(contractKey)}`;
      const fillDate = earliestBuyByKey.get(lookupKey);
      if (!fillDate) {
        missingSource += 1;
        continue;
      }

      await client.pages.update({
        page_id: page.id,
        properties: {
          [PROPERTY.tradeDate]: { date: { start: fillDate } }
        }
      });
      updated += 1;
    }

    cursor = response.has_more ? response.next_cursor ?? undefined : undefined;
  } while (cursor);

  return {
    files: files.length,
    parsedRows,
    fidelityOpenMissingDate,
    updated,
    missingSource
  };
}
