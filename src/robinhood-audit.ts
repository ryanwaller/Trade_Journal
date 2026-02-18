import fs from "node:fs/promises";
import path from "node:path";
import { getJournalInfo, getNotionClient, PROPERTY } from "./notion.js";

type RobinhoodEvent = {
  date: string;
  action: "BUY" | "SELL";
  contractKey: string;
  qty: number;
  price: number;
  multiplier: number;
};

type NotionRow = {
  pageId: string;
  title: string;
  contractKey: string;
  status: string;
  plAtClose: number | null;
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
  return `${m[3]}-${m[1].padStart(2, "0")}-${m[2].padStart(2, "0")}`;
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

function optionMetaFromDescription(description: string, instrument: string) {
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
    return { contractKey, multiplier: 100 };
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
  return { contractKey, multiplier: 100 };
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
  return {
    date,
    action,
    contractKey: optionMeta?.contractKey ?? instrument,
    qty,
    price,
    multiplier: optionMeta?.multiplier ?? 1
  };
}

function getRichText(page: any, key: string) {
  const prop = page.properties?.[key];
  if (!prop || prop.type !== "rich_text") return "";
  return (prop.rich_text ?? []).map((t: any) => t.plain_text ?? "").join("").trim();
}

function getTitle(page: any, key: string) {
  const prop = page.properties?.[key];
  if (!prop || prop.type !== "title") return "";
  return (prop.title ?? []).map((t: any) => t.plain_text ?? "").join("").trim();
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

function round2(value: number) {
  return Math.round(value * 100) / 100;
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

export async function runAuditRobinhoodPl() {
  const root = process.cwd();
  const rawDir = path.join(root, "imports", "robinhood", "raw");
  const processedDir = path.join(root, "imports", "robinhood", "processed");
  const startDate = process.env.ROBINHOOD_START_DATE?.trim() || "2025-01-01";
  const tolerance = Number(process.env.ROBINHOOD_PL_TOLERANCE ?? "1");

  const rawFiles = await listCsvFiles(rawDir);
  const processedFiles = await listCsvFiles(processedDir);
  const allFiles = [...processedFiles, ...rawFiles];

  const eventByKey = new Map<string, RobinhoodEvent>();
  const countByKey = new Map<string, number>();
  let parsedRows = 0;

  for (const file of allFiles) {
    const rows = parseCsv(await fs.readFile(file, "utf8"));
    parsedRows += rows.length;
    const perFileEvent = new Map<string, RobinhoodEvent>();
    const perFileCount = new Map<string, number>();
    for (const row of rows) {
      const event = toEvent(row, startDate);
      if (!event) continue;
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
      perFileEvent.set(dedupeKey, event);
      perFileCount.set(dedupeKey, (perFileCount.get(dedupeKey) ?? 0) + 1);
    }
    for (const [key, event] of perFileEvent.entries()) {
      eventByKey.set(key, event);
      const prev = countByKey.get(key) ?? 0;
      const count = perFileCount.get(key) ?? 1;
      if (count > prev) countByKey.set(key, count);
    }
  }

  const events: RobinhoodEvent[] = [];
  for (const [key, event] of eventByKey.entries()) {
    const count = countByKey.get(key) ?? 1;
    for (let i = 0; i < count; i += 1) events.push(event);
  }
  events.sort((a, b) => {
    const ak = `${a.date}|${a.contractKey}|${a.action}`;
    const bk = `${b.date}|${b.contractKey}|${b.action}`;
    return ak.localeCompare(bk);
  });

  // Full PL model.
  const plModel = new Map<string, { openQty: number; avgOpenPrice: number; realizedPl: number }>();
  for (const e of events) {
    let p = plModel.get(e.contractKey);
    if (!p) {
      p = { openQty: 0, avgOpenPrice: 0, realizedPl: 0 };
      plModel.set(e.contractKey, p);
    }
    const q = e.qty;
    const px = e.price;
    const m = e.multiplier;
    if (e.action === "BUY") {
      if (p.openQty >= 0) {
        const newQty = p.openQty + q;
        p.avgOpenPrice = newQty > 0 ? (p.openQty * p.avgOpenPrice + q * px) / newQty : 0;
        p.openQty = newQty;
      } else {
        const shortQty = Math.abs(p.openQty);
        const closingQty = Math.min(q, shortQty);
        p.realizedPl += (p.avgOpenPrice - px) * closingQty * m;
        p.openQty += closingQty;
        const remainder = q - closingQty;
        if (remainder > 0) {
          p.openQty = remainder;
          p.avgOpenPrice = px;
        }
      }
    } else {
      if (p.openQty <= 0) {
        const shortQty = Math.abs(p.openQty);
        const newQty = shortQty + q;
        p.avgOpenPrice = newQty > 0 ? (shortQty * p.avgOpenPrice + q * px) / newQty : 0;
        p.openQty = -newQty;
      } else {
        const closingQty = Math.min(q, p.openQty);
        p.realizedPl += (px - p.avgOpenPrice) * closingQty * m;
        p.openQty -= closingQty;
        const remainder = q - closingQty;
        if (remainder > 0) {
          p.openQty = -remainder;
          p.avgOpenPrice = px;
        }
      }
    }
  }

  const info = await getJournalInfo();
  const client = getNotionClient();
  const notionRows = new Map<string, NotionRow>();
  let cursor: string | undefined;
  do {
    const response = await client.databases.query({
      database_id: info.databaseId,
      start_cursor: cursor,
      filter: {
        and: [
          {
            property: PROPERTY.broker,
            select: { equals: "Robinhood (CSV)" }
          },
          {
            property: PROPERTY.rowType,
            select: { equals: "Trade" }
          }
        ]
      }
    });
    for (const page of response.results as any[]) {
      if (page.archived) continue;
      const contractKey = getRichText(page, PROPERTY.contractKey).toUpperCase();
      if (!contractKey) continue;
      notionRows.set(contractKey, {
        pageId: page.id,
        title: getTitle(page, info.titleProperty),
        contractKey,
        status: getSelect(page, PROPERTY.status).toUpperCase(),
        plAtClose: getNumber(page, PROPERTY.plAtClose)
      });
    }
    cursor = response.has_more ? response.next_cursor ?? undefined : undefined;
  } while (cursor);

  const plMismatches: Array<{
    contractKey: string;
    csvPl: number;
    notionPl: number | null;
    delta: number;
    status: string;
    title: string;
  }> = [];
  const statusMismatches: Array<{ contractKey: string; csvStatus: string; notionStatus: string }> = [];
  const missingInNotion: string[] = [];
  const extraInNotion: string[] = [];

  for (const [contractKey, p] of plModel.entries()) {
    const notion = notionRows.get(contractKey);
    const csvClosed = Math.abs(p.openQty) < 1e-9;
    const csvStatus = csvClosed ? "CLOSED" : "OPEN";
    if (!notion) {
      missingInNotion.push(contractKey);
      continue;
    }
    if (notion.status !== csvStatus) {
      statusMismatches.push({ contractKey, csvStatus, notionStatus: notion.status || "(blank)" });
    }
    if (csvClosed) {
      const csvPl = round2(p.realizedPl);
      const notionPl = notion.plAtClose;
      const delta = round2(csvPl - (notionPl ?? 0));
      if (notionPl === null || Math.abs(delta) > tolerance) {
        plMismatches.push({
          contractKey,
          csvPl,
          notionPl,
          delta,
          status: notion.status,
          title: notion.title
        });
      }
    }
  }

  for (const contractKey of notionRows.keys()) {
    if (!plModel.has(contractKey)) extraInNotion.push(contractKey);
  }

  let totalCsvRealized = 0;
  let closedContracts = 0;
  let openContracts = 0;
  for (const p of plModel.values()) {
    totalCsvRealized += p.realizedPl;
    if (Math.abs(p.openQty) < 1e-9) closedContracts += 1;
    else openContracts += 1;
  }

  return {
    files: { raw: rawFiles.length, processed: processedFiles.length, total: allFiles.length },
    parsedRows,
    uniqueEvents: events.length,
    csvContracts: plModel.size,
    csvClosedContracts: closedContracts,
    csvOpenContracts: openContracts,
    csvTotalRealizedPl: round2(totalCsvRealized),
    notionRows: notionRows.size,
    plMismatchCount: plMismatches.length,
    statusMismatchCount: statusMismatches.length,
    missingInNotionCount: missingInNotion.length,
    extraInNotionCount: extraInNotion.length,
    samples: {
      plMismatches: plMismatches.slice(0, 25),
      statusMismatches: statusMismatches.slice(0, 25),
      missingInNotion: missingInNotion.slice(0, 25),
      extraInNotion: extraInNotion.slice(0, 25)
    }
  };
}
