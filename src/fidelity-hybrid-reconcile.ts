import { PROPERTY, getJournalInfo, getNotionClient } from "./notion.js";

type Row = {
  pageId: string;
  broker: "Fidelity" | "Fidelity (CSV)";
  status: "OPEN" | "CLOSED";
  account: string;
  contractKey: string;
  openDate: string | null;
  closeDate: string | null;
  qty: number | null;
};

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

function getDate(page: any, key: string) {
  const prop = page.properties?.[key];
  if (!prop || prop.type !== "date") return null;
  return prop.date?.start ?? null;
}

function getNumber(page: any, key: string) {
  const prop = page.properties?.[key];
  if (!prop || prop.type !== "number") return null;
  return prop.number;
}

function normAcct(value: string) {
  const upper = value.trim().toUpperCase();
  // Collapse historical account naming variants ("Fun" vs "Taxable (Fun)") to a stable key.
  if (upper.includes("FUN")) return "TAXABLE FUN";
  if (upper.includes("ROTH")) return "IRA ROTH";
  if (upper.includes("TRADITIONAL") || upper.includes("TRAD")) return "IRA TRADITIONAL";
  return upper.replace(/[()]/g, " ").replace(/\s+/g, " ").trim();
}

// Canonicalize option contract keys so "NFLX260417C82" matches "NFLX 260417C00082000".
function canonicalContractKey(value: string) {
  const trimmed = (value ?? "").trim().replace(/^[-+]/, "");
  if (!trimmed) return "";
  const compact = trimmed.replace(/\s+/g, "").toUpperCase();

  // Options: TICKER + YYMMDD + C/P + strikeDigits
  const m = compact.match(/^([A-Z.\-]+)(\d{6})([CP])([0-9.]+)$/);
  if (!m) return compact;

  const [, ticker, yymmdd, cp, strikeRaw] = m;
  let strikeDigits = strikeRaw;
  if (strikeDigits.includes(".")) {
    const n = Number.parseFloat(strikeDigits);
    if (Number.isFinite(n)) {
      strikeDigits = String(Math.round(n * 1000));
    }
  } else if (strikeDigits.length <= 3) {
    // Heuristic for shorthand strikes like "C82" or "C34" -> multiply by 1000.
    strikeDigits = `${strikeDigits}000`;
  }
  // If already scaled (e.g. "42500"), keep as-is and just pad.
  strikeDigits = strikeDigits.replace(/\D/g, "");
  if (strikeDigits.length < 8) strikeDigits = strikeDigits.padStart(8, "0");
  if (strikeDigits.length > 8) strikeDigits = strikeDigits.slice(-8);

  return `${ticker} ${yymmdd}${cp}${strikeDigits}`;
}

function baseKey(row: Row) {
  return `${normAcct(row.account)}|${canonicalContractKey(row.contractKey)}`;
}

function closeKey(row: Row) {
  // Contract key can be reused across multiple opens/closes; close date disambiguates.
  return `${baseKey(row)}|${row.closeDate ?? ""}`;
}

function contractFingerprint(account: string, contractKey: string) {
  const compact = canonicalContractKey(contractKey).replace(/\s+/g, "").toUpperCase();
  const m = compact.match(/^([A-Z.\-]+)(\d{6})([CP])(\d{8})$/);
  if (!m) return `${normAcct(account)}|${compact}`;
  const [, ticker, yymmdd, cp, strike8] = m;
  const strikeNum = Number.parseInt(strike8, 10);
  const strike = Number.isFinite(strikeNum) ? (strikeNum / 1000).toString() : strike8;
  return `${normAcct(account)}|${ticker}|${yymmdd}|${cp}|${strike}`;
}

async function listFidelityAndCsvRows(): Promise<Row[]> {
  const info = await getJournalInfo();
  const client = getNotionClient();
  const rows: Row[] = [];
  let cursor: string | undefined;

  const filter: any = {
    and: [
      {
        or: [
          { property: PROPERTY.broker, select: { equals: "Fidelity" } },
          { property: PROPERTY.broker, select: { equals: "Fidelity (CSV)" } }
        ]
      }
    ]
  };

  do {
    const res = await client.databases.query({
      database_id: info.databaseId,
      start_cursor: cursor,
      filter
    });

    for (const page of res.results as any[]) {
      if (page.archived) continue;
      const broker = getSelect(page, PROPERTY.broker);
      if (broker !== "Fidelity" && broker !== "Fidelity (CSV)") continue;
      const status = getSelect(page, PROPERTY.status).toUpperCase();
      if (status !== "OPEN" && status !== "CLOSED") continue;
      const account = getRichText(page, PROPERTY.account);
      const contractKey = getRichText(page, PROPERTY.contractKey);
      if (!account || !contractKey) continue;
      rows.push({
        pageId: page.id,
        broker,
        status,
        account,
        contractKey,
        openDate: getDate(page, PROPERTY.tradeDate),
        closeDate: getDate(page, PROPERTY.closeDate),
        qty: getNumber(page, PROPERTY.qty)
      });
    }

    cursor = res.has_more ? res.next_cursor ?? undefined : undefined;
  } while (cursor);

  return rows;
}

export async function runReconcileFidelityHybrid() {
  const dryRun = process.env.FIDELITY_HYBRID_DRY_RUN === "1";
  const cutoff = (process.env.FIDELITY_HYBRID_CUTOFF_DATE ?? "2026-01-01").trim();
  const rows = await listFidelityAndCsvRows();
  const client = getNotionClient();

  const isArchivedErr = (err: any) => {
    const msg = String(err?.message ?? "");
    return msg.includes("already archived") || msg.includes("is archived") || msg.includes("archived");
  };

  const safeArchive = async (pageId: string) => {
    if (dryRun) return;
    try {
      await client.pages.update({ page_id: pageId, archived: true });
    } catch (err: any) {
      if (isArchivedErr(err)) return;
      throw err;
    }
  };

  const safeUpdateProps = async (pageId: string, properties: any) => {
    if (dryRun) return;
    try {
      await client.pages.update({ page_id: pageId, properties });
    } catch (err: any) {
      if (isArchivedErr(err)) return;
      throw err;
    }
  };

  const apiOpen = new Map<string, Row>();
  const csvOpen: Row[] = [];
  const apiClosedByBase = new Map<string, Row[]>();
  const csvClosedByBase = new Map<string, Row[]>();
  const apiOpenByBase = new Map<string, Row>();
  const csvOpenByBase = new Map<string, Row[]>();

  const apiBaseKeys = new Set<string>();
  const csvBaseKeys = new Set<string>();
  const apiFingerprints = new Set<string>();
  const csvFingerprints = new Set<string>();

  for (const r of rows) {
    const bKey = baseKey(r);
    const fp = contractFingerprint(r.account, r.contractKey);
    if (r.status === "OPEN") {
      if (r.broker === "Fidelity") {
        apiOpen.set(bKey, r);
        apiOpenByBase.set(bKey, r);
        apiBaseKeys.add(bKey);
        apiFingerprints.add(fp);
      }
      else {
        csvOpen.push(r);
        csvOpenByBase.set(bKey, [...(csvOpenByBase.get(bKey) ?? []), r]);
        csvBaseKeys.add(bKey);
        csvFingerprints.add(fp);
      }
      continue;
    }
    // CLOSED
    if (r.broker === "Fidelity") {
      apiClosedByBase.set(bKey, [...(apiClosedByBase.get(bKey) ?? []), r]);
      apiBaseKeys.add(bKey);
      apiFingerprints.add(fp);
    } else {
      csvClosedByBase.set(bKey, [...(csvClosedByBase.get(bKey) ?? []), r]);
      csvBaseKeys.add(bKey);
      csvFingerprints.add(fp);
    }
  }

  let archivedCsvOpen = 0;
  let archivedCsvClosed = 0;
  let updatedApiOpenDates = 0;
  let overlapClosedPairs = 0;
  let archivedCsvAnyAfterCutoff = 0;
  const archivedCsvPageIds = new Set<string>();

  const afterCutoff = (d: string | null) => Boolean(d && d >= cutoff);
  const inOverlapWindow = (r: Row) => afterCutoff(r.openDate) || afterCutoff(r.closeDate);

  // If it exists in the live API, CSV open rows are duplicates for current holdings.
  for (const r of csvOpen) {
    const k = baseKey(r);
    const api = apiOpen.get(k);
    if (!api) continue;
    // Preserve the earliest-known open date by backfilling onto the API row, then archive CSV.
    if (r.openDate && (!api.openDate || r.openDate < api.openDate)) {
      await safeUpdateProps(api.pageId, { [PROPERTY.tradeDate]: { date: { start: r.openDate } } });
      updatedApiOpenDates += 1;
    }
    await safeArchive(r.pageId);
    archivedCsvPageIds.add(r.pageId);
    archivedCsvOpen += 1;
  }

  // General rule to prevent double-counting: for any CSV row (OPEN or CLOSED) after cutoff,
  // if there exists an API row for the same (Account, Contract Key), archive the CSV row.
  // If the CSV has an earlier open date than the API row, backfill it onto the API row first.
  for (const r of rows) {
    if (r.broker !== "Fidelity (CSV)") continue;
    if (!inOverlapWindow(r)) continue;
    const k = baseKey(r);
    if (!apiBaseKeys.has(k)) continue;
    if (archivedCsvPageIds.has(r.pageId)) continue;

    // Find a representative API row for open-date backfill (prefer OPEN, else any CLOSED).
    const api =
      apiOpenByBase.get(k) ??
      (apiClosedByBase.get(k)?.[0] ?? null);
    if (api && r.openDate && (!api.openDate || r.openDate < api.openDate)) {
      await safeUpdateProps(api.pageId, { [PROPERTY.tradeDate]: { date: { start: r.openDate } } });
      updatedApiOpenDates += 1;
    }

    await safeArchive(r.pageId);
    archivedCsvPageIds.add(r.pageId);
    archivedCsvAnyAfterCutoff += 1;
  }

  // For closed overlaps, we want ONE row (avoid double-counting) and prefer the "more complete" row:
  // - Prefer larger total qty (usually indicates more lots were captured)
  // - If tie/unknown, prefer earlier open date (history)
  // - If still tie, prefer API row (more consistent downstream)
  //
  // We match primarily by (Account, canonical Contract Key), and if both have close dates we require they match
  // or be within 2 days (timezone/reporting differences).
  const daysBetween = (a: string, b: string) => {
    const da = new Date(a);
    const db = new Date(b);
    if (Number.isNaN(da.getTime()) || Number.isNaN(db.getTime())) return null;
    const diff = Math.abs(da.getTime() - db.getTime());
    return Math.round(diff / (1000 * 60 * 60 * 24));
  };

  for (const [k, csvRows] of csvClosedByBase.entries()) {
    const apiRows = apiClosedByBase.get(k) ?? [];
    if (apiRows.length === 0) continue;

    for (const csv of csvRows) {
      // Find an API closed row that likely corresponds to this CSV closed row.
      const candidates = apiRows.filter((api) => {
        if (!csv.closeDate || !api.closeDate) return true;
        if (csv.closeDate === api.closeDate) return true;
        const d = daysBetween(csv.closeDate, api.closeDate);
        return d !== null && d <= 2;
      });
      if (candidates.length === 0) continue;
      overlapClosedPairs += 1;

      // Choose best API candidate to compare against (closest close date).
      const api = candidates.sort((a, b) => {
        if (csv.closeDate && a.closeDate && b.closeDate) {
          const da = daysBetween(csv.closeDate, a.closeDate) ?? 999;
          const db = daysBetween(csv.closeDate, b.closeDate) ?? 999;
          return da - db;
        }
        return 0;
      })[0];

      const csvQty = typeof csv.qty === "number" ? csv.qty : null;
      const apiQty = typeof api.qty === "number" ? api.qty : null;
      const qtyWinner =
        csvQty !== null && apiQty !== null ? (csvQty > apiQty ? "CSV" : apiQty > csvQty ? "API" : null) : null;
      const openWinner =
        !qtyWinner && csv.openDate && api.openDate
          ? csv.openDate < api.openDate
            ? "CSV"
            : api.openDate < csv.openDate
              ? "API"
              : null
          : null;
      const winner = qtyWinner ?? openWinner ?? "API";

      if (winner === "API") {
        // If CSV has earlier open date, carry it over.
        if (csv.openDate && (!api.openDate || csv.openDate < api.openDate)) {
          await safeUpdateProps(api.pageId, { [PROPERTY.tradeDate]: { date: { start: csv.openDate } } });
          updatedApiOpenDates += 1;
        }
        await safeArchive(csv.pageId);
        archivedCsvClosed += 1;
      } else {
        // Keep CSV, archive API (API likely missing lots/history).
        await safeArchive(api.pageId);
      }
    }
  }

  const baseIntersection = Array.from(csvBaseKeys).filter((k) => apiBaseKeys.has(k)).length;
  const fpIntersection = Array.from(csvFingerprints).filter((k) => apiFingerprints.has(k)).length;
  const sampleBaseKeys = Array.from(csvBaseKeys)
    .filter((k) => apiBaseKeys.has(k))
    .slice(0, 10);
  const samples = sampleBaseKeys.map((k) => {
    const apiO = apiOpenByBase.get(k) ?? null;
    const csvO = csvOpenByBase.get(k) ?? [];
    const apiC = apiClosedByBase.get(k) ?? [];
    const csvC = csvClosedByBase.get(k) ?? [];
    const summarize = (r: Row) => ({
      status: r.status,
      openDate: r.openDate,
      closeDate: r.closeDate,
      qty: r.qty,
      contractKey: r.contractKey
    });
    return {
      baseKey: k,
      apiOpen: apiO ? summarize(apiO) : null,
      csvOpen: csvO.map(summarize),
      apiClosed: apiC.map(summarize),
      csvClosed: csvC.map(summarize)
    };
  });

  return {
    dryRun,
    cutoffDate: cutoff,
    fidelityRows: rows.filter((r) => r.broker === "Fidelity").length,
    fidelityCsvRows: rows.filter((r) => r.broker === "Fidelity (CSV)").length,
    overlapOpen: archivedCsvOpen,
    overlapClosedArchivedCsv: archivedCsvClosed,
    overlapClosedPairs,
    updatedApiOpenDates,
    archivedCsvAnyAfterCutoff,
    baseIntersection,
    fingerprintIntersection: fpIntersection,
    samples
  };
}
