import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import {
  archiveTradePagesByBrokerPrefix,
  createPositionPage,
  updatePositionPage
} from "./notion.js";

type PdfEvent = {
  account: string;
  date: string;
  action: "BUY" | "SELL";
  contractKey: string;
  ticker: string;
  qty: number;
  price: number;
};

type PositionState = {
  account: string;
  contractKey: string;
  ticker: string;
  qtyOpen: number;
  totalBoughtQty: number;
  avgPrice: number;
  totalSoldQty: number;
  totalSoldPrice: number;
  realizedPl: number;
  openDate: string | null;
  closeDate: string | null;
};

function mmddyyToIso(value: string): string | null {
  const m = value.match(/^(\d{2})\/(\d{2})\/(\d{2})$/);
  if (!m) return null;
  const yy = Number(m[3]);
  const year = yy >= 70 ? 1900 + yy : 2000 + yy;
  return `${year}-${m[1]}-${m[2]}`;
}

function parseNumber(value: string): number | null {
  const clean = value.replace(/[$,]/g, "").trim();
  if (!clean) return null;
  const n = Number(clean);
  return Number.isFinite(n) ? n : null;
}

function strikeTo8(value: string) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return String(Math.round(n * 1000)).padStart(8, "0");
}

function optionContractFromDesc(desc: string): { key: string; ticker: string } | null {
  const normalized = desc.replace(/\s+/g, " ").trim().toUpperCase();
  const m = normalized.match(/\b(CALL|PUT)\s+([A-Z.\-]+)\s+(\d{2})\/(\d{2})\/(\d{2})\s+([\d.]+)/);
  if (!m) return null;
  const type = m[1] === "CALL" ? "C" : "P";
  const ticker = m[2];
  const yymmdd = `${m[5]}${m[3]}${m[4]}`;
  const strike8 = strikeTo8(m[6]);
  if (!strike8) return null;
  return {
    key: `${ticker} ${yymmdd}${type}${strike8}`,
    ticker
  };
}

function readPdfText(filePath: string): string {
  return execFileSync("/opt/homebrew/bin/pdftotext", ["-layout", filePath, "-"], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
    maxBuffer: 20 * 1024 * 1024
  });
}

function extractAccountLabel(text: string) {
  const m = text.match(/ACCOUNT NUMBER\s+([A-Z0-9\- ]+)\n/);
  const number = m?.[1]?.trim().replace(/\s+/g, " ") ?? "Unknown";
  return `Public Statement ${number}`;
}

function parseEventsFromText(text: string): PdfEvent[] {
  const account = extractAccountLabel(text);
  const lines = text.split(/\r?\n/);
  const events: PdfEvent[] = [];

  for (const rawLine of lines) {
    const line = rawLine.replace(/\s+/g, " ").trim();
    if (!line) continue;

    // Example:
    // BOUGHT 05/01/25 M CALL UAL 09/19/25 85 1 $4.10 $409.99
    const tradeMatch = line.match(
      /^(BOUGHT|SOLD)\s+(\d{2}\/\d{2}\/\d{2})(?:\s+\d{2}\/\d{2}\/\d{2})?\s+M\s+(.+?)\s+(-?\d[\d,.\-]*)\s+\$?(-?\d[\d,.\-]*)\s+\$?(-?\d[\d,.\-]*)$/
    );
    if (tradeMatch) {
      const action = tradeMatch[1] === "BOUGHT" ? "BUY" : "SELL";
      const date = mmddyyToIso(tradeMatch[2]);
      const desc = tradeMatch[3];
      const qty = parseNumber(tradeMatch[4]);
      const price = parseNumber(tradeMatch[5]);
      if (!date || qty === null || price === null) continue;
      const option = optionContractFromDesc(desc);
      if (!option) continue;
      events.push({
        account,
        date,
        action,
        contractKey: option.key,
        ticker: option.ticker,
        qty: Math.abs(qty),
        price
      });
      continue;
    }

    // Example:
    // EXPIRED 01/16/26 M CALL AAPL 01/16/26 285 -2
    const expiredMatch = line.match(
      /^EXPIRED\s+(\d{2}\/\d{2}\/\d{2})(?:\s+\d{2}\/\d{2}\/\d{2})?\s+M\s+(.+?)\s+(-?\d[\d,.\-]*)$/
    );
    if (expiredMatch) {
      const date = mmddyyToIso(expiredMatch[1]);
      const option = optionContractFromDesc(expiredMatch[2]);
      const qty = parseNumber(expiredMatch[3]);
      if (!date || !option || qty === null) continue;
      events.push({
        account,
        date,
        action: "SELL",
        contractKey: option.key,
        ticker: option.ticker,
        qty: Math.abs(qty),
        price: 0
      });
    }
  }

  return events;
}

function round2(value: number) {
  return Math.round(value * 100) / 100;
}

export async function runImportPublicPdf() {
  const dir = path.join(process.cwd(), "imports", "public", "pdfs");
  const files = fs
    .readdirSync(dir)
    .filter((f) => f.toLowerCase().endsWith(".pdf"))
    .map((f) => path.join(dir, f))
    .sort((a, b) => a.localeCompare(b));

  const allEvents: PdfEvent[] = [];
  for (const file of files) {
    const text = readPdfText(file);
    allEvents.push(...parseEventsFromText(text));
  }

  const uniq = new Map<string, PdfEvent>();
  for (const e of allEvents) {
    const key = `${e.account}|${e.date}|${e.action}|${e.contractKey}|${e.qty}|${e.price}`;
    uniq.set(key, e);
  }
  const events = Array.from(uniq.values()).sort((a, b) => {
    if (a.date === b.date) {
      return `${a.account}|${a.contractKey}|${a.action}`.localeCompare(
        `${b.account}|${b.contractKey}|${b.action}`
      );
    }
    return a.date.localeCompare(b.date);
  });

  const archivedExisting = await archiveTradePagesByBrokerPrefix("Public (PDF)");

  const positions = new Map<string, PositionState>();
  for (const e of events) {
    const key = `${e.account}::${e.contractKey}`;
    let p = positions.get(key);
    if (!p) {
      p = {
        account: e.account,
        contractKey: e.contractKey,
        ticker: e.ticker,
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

    if (e.action === "BUY") {
      const newQty = p.qtyOpen + e.qty;
      p.avgPrice = newQty > 0 ? (p.qtyOpen * p.avgPrice + e.qty * e.price) / newQty : 0;
      p.qtyOpen = newQty;
      p.totalBoughtQty += e.qty;
      p.openDate = p.openDate ?? e.date;
    } else {
      if (p.qtyOpen <= 0) continue;
      const closingQty = Math.min(p.qtyOpen, e.qty);
      p.realizedPl += (e.price - p.avgPrice) * closingQty * 100;
      p.totalSoldQty += closingQty;
      p.totalSoldPrice += e.price * closingQty;
      p.qtyOpen -= closingQty;
      if (p.qtyOpen === 0) p.closeDate = e.date;
    }
  }

  let created = 0;
  let openRows = 0;
  let closedRows = 0;
  for (const p of positions.values()) {
    if (p.totalBoughtQty <= 0 || !p.openDate) continue;
    const page = await createPositionPage({
      title: p.ticker,
      ticker: p.ticker,
      contractKey: p.contractKey,
      qty: p.totalBoughtQty,
      avgPrice: round2(p.avgPrice * 100),
      openDate: p.openDate,
      openTime: null,
      broker: "Public (PDF)",
      account: p.account
    });
    created += 1;
    if (p.qtyOpen === 0 && p.closeDate) {
      const avgClose = p.totalSoldQty > 0 ? round2((p.totalSoldPrice / p.totalSoldQty) * 100) : null;
      await updatePositionPage({
        pageId: (page as any).id,
        ticker: p.ticker,
        contractKey: p.contractKey,
        qty: p.totalBoughtQty,
        avgPrice: round2(p.avgPrice * 100),
        status: "CLOSED",
        closeDate: p.closeDate,
        closeTime: null,
        closePrice: avgClose,
        realizedPl: round2(p.realizedPl)
      });
      closedRows += 1;
    } else {
      openRows += 1;
    }
  }

  return {
    files: files.length,
    parsedEvents: allEvents.length,
    uniqueEvents: events.length,
    archivedExisting: archivedExisting.archived,
    createdRows: created,
    openRows,
    closedRows
  };
}

