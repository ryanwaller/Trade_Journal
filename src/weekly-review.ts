import { assertWeeklyReviewConfig, config } from "./config.js";
import { getJournalInfo, getNotionClient, PROPERTY } from "./notion.js";

type JournalRow = {
  id: string;
  title: string;
  ticker: string;
  status: string;
  pl: number | null;
  tradeDate: string;
  closeDate: string;
  qty: number | null;
  fillPrice: number | null;
};

type ReviewStats = {
  startDate: string;
  endDate: string;
  totalRealized: number;
  winRate: number | null;
  avgWin: number | null;
  avgLoss: number | null;
  winners: JournalRow[];
  losers: JournalRow[];
  closedTrades: JournalRow[];
  openPositions: JournalRow[];
};

function getTitleValue(page: any, key: string) {
  const prop = page.properties?.[key];
  if (!prop || prop.type !== "title") return "";
  return (prop.title ?? []).map((t: any) => t.plain_text ?? "").join("").trim();
}

function getRichTextValue(page: any, key: string) {
  const prop = page.properties?.[key];
  if (!prop || prop.type !== "rich_text") return "";
  return (prop.rich_text ?? []).map((t: any) => t.plain_text ?? "").join("").trim();
}

function getNumberValue(page: any, key: string) {
  const prop = page.properties?.[key];
  if (!prop || prop.type !== "number") return null;
  return prop.number;
}

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

function dateStringInZone(date: Date, timeZone: string) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).formatToParts(date);

  const year = parts.find((part) => part.type === "year")?.value ?? "";
  const month = parts.find((part) => part.type === "month")?.value ?? "";
  const day = parts.find((part) => part.type === "day")?.value ?? "";

  return `${year}-${month}-${day}`;
}

function addDays(dateString: string, days: number) {
  const [year, month, day] = dateString.split("-").map((part) => Number(part));
  const date = new Date(Date.UTC(year, month - 1, day));
  date.setUTCDate(date.getUTCDate() + days);
  const yyyy = date.getUTCFullYear();
  const mm = String(date.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(date.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function isDateInRange(value: string, start: string, end: string) {
  return value >= start && value <= end;
}

function formatCurrency(value: number) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    signDisplay: "exceptZero",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(value);
}

function formatPercent(value: number) {
  return new Intl.NumberFormat("en-US", {
    style: "percent",
    minimumFractionDigits: 1,
    maximumFractionDigits: 1
  }).format(value);
}

function formatNumber(value: number | null, maxDecimals = 6) {
  if (value === null) return "";
  const fixed = value.toFixed(maxDecimals);
  return fixed.replace(/\.?0+$/, "");
}

async function fetchJournalRows(filter?: any) {
  const info = await getJournalInfo();
  const client = getNotionClient();
  const rows: any[] = [];
  let cursor: string | undefined;

  do {
    const response = await client.databases.query({
      database_id: info.databaseId,
      start_cursor: cursor,
      ...(filter ? { filter } : {})
    });
    rows.push(...response.results);
    cursor = response.has_more ? response.next_cursor ?? undefined : undefined;
  } while (cursor);

  return rows;
}

function mapRow(page: any, titleProperty: string): JournalRow {
  const title = getTitleValue(page, titleProperty) || "(untitled)";
  const ticker = getRichTextValue(page, PROPERTY.ticker) || title;
  const status = getSelectValue(page, PROPERTY.status).toUpperCase();
  const pl = getNumberValue(page, PROPERTY.plAtClose);
  const tradeDate = getDateValue(page, PROPERTY.tradeDate);
  const closeDate = getDateValue(page, PROPERTY.closeDate);
  const qty = getNumberValue(page, PROPERTY.qty);
  const fillPrice = getNumberValue(page, PROPERTY.fillPrice);

  return {
    id: page.id as string,
    title,
    ticker,
    status,
    pl,
    tradeDate,
    closeDate,
    qty,
    fillPrice
  };
}

function summarizeRows(
  closedRows: JournalRow[],
  openRows: JournalRow[],
  startDate: string,
  endDate: string
): ReviewStats {
  const closedTrades = closedRows.filter((row) => {
    if (row.status !== "CLOSED") return false;
    if (typeof row.pl !== "number" || Number.isNaN(row.pl)) return false;
    const dateValue = row.closeDate || row.tradeDate;
    if (!dateValue) return false;
    return isDateInRange(dateValue, startDate, endDate);
  });

  const openPositions = openRows.filter((row) => row.status === "OPEN");

  const winners = [...closedTrades].filter((row) => (row.pl ?? 0) > 0).sort((a, b) => {
    return (b.pl ?? 0) - (a.pl ?? 0);
  });
  const losers = [...closedTrades].filter((row) => (row.pl ?? 0) < 0).sort((a, b) => {
    return (a.pl ?? 0) - (b.pl ?? 0);
  });

  const totalRealized = closedTrades.reduce((sum, row) => sum + (row.pl ?? 0), 0);
  const winRate = closedTrades.length > 0 ? winners.length / closedTrades.length : null;
  const avgWin =
    winners.length > 0
      ? winners.reduce((sum, row) => sum + (row.pl ?? 0), 0) / winners.length
      : null;
  const avgLoss =
    losers.length > 0
      ? losers.reduce((sum, row) => sum + (row.pl ?? 0), 0) / losers.length
      : null;

  return {
    startDate,
    endDate,
    totalRealized,
    winRate,
    avgWin,
    avgLoss,
    winners: winners.slice(0, 3),
    losers: losers.slice(0, 3),
    closedTrades,
    openPositions
  };
}

function buildWeeklyReviewBlocks(stats: ReviewStats) {
  const blocks: any[] = [];

  blocks.push({
    heading_1: {
      rich_text: [{ type: "text", text: { content: `Weekly Review - ${stats.endDate}` } }]
    }
  });

  blocks.push({
    paragraph: {
      rich_text: [
        {
          type: "text",
          text: { content: `Date range: ${stats.startDate} to ${stats.endDate}` }
        }
      ]
    }
  });

  blocks.push({
    heading_2: {
      rich_text: [{ type: "text", text: { content: "Summary" } }]
    }
  });

  const summaryItems = [
    `Closed trades: ${stats.closedTrades.length}`,
    `Total realized P/L: ${formatCurrency(stats.totalRealized)}`,
    `Win rate: ${stats.winRate === null ? "n/a" : formatPercent(stats.winRate)}`,
    `Average win: ${stats.avgWin === null ? "n/a" : formatCurrency(stats.avgWin)}`,
    `Average loss: ${stats.avgLoss === null ? "n/a" : formatCurrency(stats.avgLoss)}`
  ];

  summaryItems.forEach((item) => {
    blocks.push({
      bulleted_list_item: {
        rich_text: [{ type: "text", text: { content: item } }]
      }
    });
  });

  blocks.push({
    heading_2: {
      rich_text: [{ type: "text", text: { content: "Top Winners" } }]
    }
  });

  if (stats.winners.length === 0) {
    blocks.push({
      bulleted_list_item: {
        rich_text: [{ type: "text", text: { content: "None" } }]
      }
    });
  } else {
    stats.winners.forEach((row) => {
      const label = `${row.ticker} — ${formatCurrency(row.pl ?? 0)} (${row.closeDate || row.tradeDate})`;
      blocks.push({
        bulleted_list_item: {
          rich_text: [{ type: "text", text: { content: label } }]
        }
      });
    });
  }

  blocks.push({
    heading_2: {
      rich_text: [{ type: "text", text: { content: "Top Losers" } }]
    }
  });

  if (stats.losers.length === 0) {
    blocks.push({
      bulleted_list_item: {
        rich_text: [{ type: "text", text: { content: "None" } }]
      }
    });
  } else {
    stats.losers.forEach((row) => {
      const label = `${row.ticker} — ${formatCurrency(row.pl ?? 0)} (${row.closeDate || row.tradeDate})`;
      blocks.push({
        bulleted_list_item: {
          rich_text: [{ type: "text", text: { content: label } }]
        }
      });
    });
  }

  blocks.push({
    heading_2: {
      rich_text: [{ type: "text", text: { content: "Open Positions" } }]
    }
  });

  if (stats.openPositions.length === 0) {
    blocks.push({
      bulleted_list_item: {
        rich_text: [{ type: "text", text: { content: "None" } }]
      }
    });
  } else {
    stats.openPositions.forEach((row) => {
      const qty = formatNumber(row.qty, 4);
      const price = row.fillPrice === null ? "" : formatCurrency(row.fillPrice);
      const openDate = row.tradeDate ? ` (opened ${row.tradeDate})` : "";
      const label = `${row.ticker} — Qty ${qty || "?"}${price ? ` @ ${price}` : ""}${openDate}`;
      blocks.push({
        bulleted_list_item: {
          rich_text: [{ type: "text", text: { content: label } }]
        }
      });
    });
  }

  return blocks;
}

export async function runWeeklyReview() {
  assertWeeklyReviewConfig();
  const info = await getJournalInfo();
  const zone = config.NOTION_TIMEZONE ?? "America/New_York";
  const endDate = dateStringInZone(new Date(), zone);
  const startDate = addDays(endDate, -6);

  const hasStatus = Boolean(info.properties[PROPERTY.status]);
  const hasCloseDate = Boolean(info.properties[PROPERTY.closeDate]);
  const hasTradeDate = Boolean(info.properties[PROPERTY.tradeDate]);
  const dateProperty = hasCloseDate
    ? PROPERTY.closeDate
    : hasTradeDate
      ? PROPERTY.tradeDate
      : null;

  const filterParts: any[] = [];
  if (hasStatus) {
    filterParts.push({
      property: PROPERTY.status,
      select: { equals: "CLOSED" }
    });
  }

  if (dateProperty) {
    filterParts.push({
      property: dateProperty,
      date: { on_or_after: startDate, on_or_before: endDate }
    });
  }

  const filter = filterParts.length > 0 ? { and: filterParts } : undefined;
  const closedRawRows = await fetchJournalRows(filter);
  const mappedClosedRows = closedRawRows.map((row) => mapRow(row, info.titleProperty));

  const openFilter = hasStatus
    ? {
        property: PROPERTY.status,
        select: { equals: "OPEN" }
      }
    : undefined;
  const openRawRows = await fetchJournalRows(openFilter);
  const mappedOpenRows = openRawRows.map((row) => mapRow(row, info.titleProperty));

  const stats = summarizeRows(mappedClosedRows, mappedOpenRows, startDate, endDate);
  const children = buildWeeklyReviewBlocks(stats);

  const client = getNotionClient();
  const title = `Weekly Review - ${stats.endDate}`;

  const page = await client.pages.create({
    parent: { page_id: config.NOTION_WEEKLY_REVIEWS_PAGE_ID! },
    properties: {
      title: {
        title: [{ type: "text", text: { content: title } }]
      }
    },
    children
  });

  return {
    title,
    pageId: page.id,
    dateRange: { start: stats.startDate, end: stats.endDate },
    totalRealized: stats.totalRealized,
    closedTrades: stats.closedTrades.length,
    openPositions: stats.openPositions.length
  };
}
