import { PROPERTY, getJournalInfo, getNotionClient } from "./notion.js";

type TradeRow = {
  id: string;
  status: string;
  rowType: string;
  closeDate: string;
  pl: number | null;
};

type SummaryRow = {
  id: string;
  summaryDate: string;
};

type DailyAggregate = {
  date: string;
  dayPL: number;
  closedTrades: number;
  winCount: number;
  lossCount: number;
};

function getSelectValue(page: any, key: string) {
  const prop = page.properties?.[key];
  if (!prop || prop.type !== "select") return "";
  return prop.select?.name ?? "";
}

function getDateValue(page: any, key: string) {
  const prop = page.properties?.[key];
  if (!prop || prop.type !== "date") return "";
  return prop.date?.start ?? "";
}

function getNumberValue(page: any, key: string) {
  const prop = page.properties?.[key];
  if (!prop || prop.type !== "number") return null;
  return prop.number;
}

async function fetchAllRows() {
  const info = await getJournalInfo();
  const client = getNotionClient();
  const rows: any[] = [];
  let cursor: string | undefined;

  do {
    const response = await client.databases.query({
      database_id: info.databaseId,
      start_cursor: cursor
    });
    rows.push(...response.results);
    cursor = response.has_more ? response.next_cursor ?? undefined : undefined;
  } while (cursor);

  return { info, rows };
}

function mapTradeRows(rows: any[]): TradeRow[] {
  return rows.map((page) => ({
    id: page.id as string,
    status: getSelectValue(page, PROPERTY.status).toUpperCase(),
    rowType: getSelectValue(page, PROPERTY.rowType),
    closeDate: getDateValue(page, PROPERTY.closeDate),
    pl: getNumberValue(page, PROPERTY.plAtClose)
  }));
}

function mapSummaryRows(rows: any[]): SummaryRow[] {
  return rows
    .map((page) => ({
      id: page.id as string,
      rowType: getSelectValue(page, PROPERTY.rowType),
      summaryDate: getDateValue(page, PROPERTY.summaryDate)
    }))
    .filter((row) => row.rowType === "Daily Summary" && Boolean(row.summaryDate));
}

function buildAggregates(trades: TradeRow[]): DailyAggregate[] {
  const byDate = new Map<string, DailyAggregate>();

  for (const row of trades) {
    if (row.rowType === "Daily Summary") continue;
    if (row.status !== "CLOSED") continue;
    if (!row.closeDate) continue;

    const current = byDate.get(row.closeDate) ?? {
      date: row.closeDate,
      dayPL: 0,
      closedTrades: 0,
      winCount: 0,
      lossCount: 0
    };

    current.closedTrades += 1;
    if (typeof row.pl === "number") {
      current.dayPL += row.pl;
      if (row.pl > 0) current.winCount += 1;
      if (row.pl < 0) current.lossCount += 1;
    }

    byDate.set(row.closeDate, current);
  }

  return [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
}

function maybeAddProperty(
  info: Awaited<ReturnType<typeof getJournalInfo>>,
  properties: Record<string, any>,
  key: string,
  value: any
) {
  if (info.properties[key] && key !== info.titleProperty) {
    properties[key] = value;
  }
}

function buildSummaryProperties(
  info: Awaited<ReturnType<typeof getJournalInfo>>,
  agg: DailyAggregate
) {
  const properties: Record<string, any> = {
    [info.titleProperty]: {
      title: [{ text: { content: `Daily Summary - ${agg.date}` } }]
    }
  };

  maybeAddProperty(info, properties, PROPERTY.rowType, {
    select: { name: "Daily Summary" }
  });
  maybeAddProperty(info, properties, PROPERTY.summaryDate, {
    date: { start: agg.date }
  });
  maybeAddProperty(info, properties, PROPERTY.dayPL, { number: Math.round(agg.dayPL * 100) / 100 });
  maybeAddProperty(info, properties, PROPERTY.closedTrades, { number: agg.closedTrades });
  maybeAddProperty(info, properties, PROPERTY.winCount, { number: agg.winCount });
  maybeAddProperty(info, properties, PROPERTY.lossCount, { number: agg.lossCount });

  return properties;
}

export async function runRebuildDailySummary() {
  const { info, rows } = await fetchAllRows();
  const client = getNotionClient();

  const trades = mapTradeRows(rows);
  const existingSummaries = mapSummaryRows(rows);
  const aggregates = buildAggregates(trades);

  const summaryByDate = new Map(existingSummaries.map((row) => [row.summaryDate, row.id]));
  const aggregateDates = new Set(aggregates.map((agg) => agg.date));

  let created = 0;
  let updated = 0;
  let archived = 0;

  for (const agg of aggregates) {
    const pageId = summaryByDate.get(agg.date);
    const properties = buildSummaryProperties(info, agg);

    if (pageId) {
      await client.pages.update({ page_id: pageId, properties });
      updated += 1;
    } else {
      await client.pages.create({
        parent: { database_id: info.databaseId },
        properties
      });
      created += 1;
    }
  }

  for (const summary of existingSummaries) {
    if (!aggregateDates.has(summary.summaryDate)) {
      await client.pages.update({ page_id: summary.id, archived: true });
      archived += 1;
    }
  }

  return {
    totalTradeRows: trades.length,
    summaryDays: aggregates.length,
    created,
    updated,
    archived
  };
}
