import { Client } from "@notionhq/client";
import { assertNotionConfig, config } from "./config.js";
import type { SnapTradeOrder, SnapTradeAccount } from "./snaptrade.js";

let notion: Client | null = null;
type DatabaseInfo = {
  databaseId: string;
  properties: Record<string, any>;
  titleProperty: string;
};

let databaseInfo: DatabaseInfo | null = null;

export function getNotionClient() {
  assertNotionConfig();
  if (!notion) {
    notion = new Client({ auth: config.NOTION_TOKEN });
  }
  return notion;
}

export const PROPERTY = {
  title: "Name",
  rowType: "Row Type",
  ticker: "Ticker",
  summaryDate: "Summary Date",
  dayPL: "Day P/L",
  closedTrades: "Closed Trades",
  winCount: "Win Count",
  lossCount: "Loss Count",
  side: "Side",
  qty: "Qty",
  fillPrice: "Fill Price",
  fees: "Fees",
  tradeDate: "Trade Date",
  tradeTime: "Trade Time",
  closeDate: "Close Date",
  closeTime: "Close Time",
  closePrice: "Close Price",
  contractKey: "Contract Key",
  status: "Status",
  strategy: "Strategy",
  setup: "Setup",
  notes: "Notes",
  entryScreenshot: "Entry Screenshot",
  plAtClose: "P/L at Close",
  tradeType: "Trade Type",
  tags: "Tags",
  broker: "Broker",
  account: "Account",
  snaptradeId: "SnapTrade ID",
  orderId: "Order ID"
} as const;

export async function getJournalInfo(): Promise<DatabaseInfo> {
  if (databaseInfo) return databaseInfo;
  if (!config.NOTION_DATABASE_ID) {
    throw new Error("NOTION_DATABASE_ID must be set");
  }

  const db = (await getNotionClient().databases.retrieve({
    database_id: config.NOTION_DATABASE_ID
  })) as any;

  const titleProperty =
    Object.entries(db.properties ?? {}).find(([, value]: any) => value?.type === "title")?.[0] ??
    PROPERTY.title;

  databaseInfo = {
    databaseId: config.NOTION_DATABASE_ID,
    properties: db.properties ?? {},
    titleProperty
  };

  return databaseInfo;
}

export async function findExistingBySnapTradeId(id: string) {
  const info = await getJournalInfo();
  const response = await getNotionClient().databases.query({
    database_id: info.databaseId,
    filter: {
      property: PROPERTY.snaptradeId,
      rich_text: {
        equals: id
      }
    }
  });

  return response.results.length > 0;
}

export async function findPageIdByOrderId(orderId: string) {
  const info = await getJournalInfo();
  const response = await getNotionClient().databases.query({
    database_id: info.databaseId,
    filter: {
      property: PROPERTY.orderId,
      rich_text: {
        equals: orderId
      }
    }
  });

  const page = response.results[0] as any;
  return page?.id ?? null;
}

export async function findOpenPositionPageId(contractKey: string) {
  const info = await getJournalInfo();
  const response = await getNotionClient().databases.query({
    database_id: info.databaseId,
    filter: {
      and: [
        {
          property: PROPERTY.contractKey,
          rich_text: { equals: contractKey }
        },
        {
          property: PROPERTY.status,
          select: { equals: "OPEN" }
        }
      ]
    }
  });

  const page = response.results[0] as any;
  return page?.id ?? null;
}
export async function findPageIdBySnapTradeId(id: string) {
  const info = await getJournalInfo();
  const response = await getNotionClient().databases.query({
    database_id: info.databaseId,
    filter: {
      property: PROPERTY.snaptradeId,
      rich_text: {
        equals: id
      }
    }
  });

  const page = response.results[0] as any;
  return page?.id ?? null;
}

export async function createTradePage(
  activity: SnapTradeOrder,
  account?: SnapTradeAccount
) {
  const info = await getJournalInfo();
  const symbol = activity.symbol?.symbol ?? "";
  const side = (activity.type ?? "").toUpperCase();
  const qty = activity.units ?? null;
  const price = activity.price ?? null;
  const fees = activity.fee ?? null;
  const tradeDate = activity.trade_date ?? null;
  const realized = activity.realized_pl ?? null;

  const toLocalDateTime = (iso: string) => {
    const date = new Date(iso);
    const locale = "en-US";
    const zone = config.NOTION_TIMEZONE ?? "America/New_York";
    const dateString = new Intl.DateTimeFormat(locale, {
      timeZone: zone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit"
    }).format(date);
    const timeString = new Intl.DateTimeFormat(locale, {
      timeZone: zone,
      hour: "numeric",
      minute: "2-digit",
      hour12: true
    }).format(date);

    const [month, day, year] = dateString.split("/");
    return {
      date: `${year}-${month}-${day}`,
      time: timeString
    };
  };

  const formatNumber = (value: number | string | null, maxDecimals = 6) => {
    if (value === null) return "";
    const numeric = typeof value === "number" ? value : Number(value);
    if (Number.isNaN(numeric)) return "";
    const fixed = numeric.toFixed(maxDecimals);
    return fixed.replace(/\.?0+$/, "");
  };

  const properties: Record<string, any> = {
    [info.titleProperty]: {
      title: [{ text: { content: symbol || "Trade" } }]
    }
  };

  const addIfExists = (key: string, value: any) => {
    if (info.properties[key] && key !== info.titleProperty) {
      properties[key] = value;
    }
  };

  addIfExists(
    PROPERTY.ticker,
    symbol ? { rich_text: [{ text: { content: symbol } }] } : { rich_text: [] }
  );
  addIfExists(PROPERTY.rowType, { select: { name: "Trade" } });
  addIfExists(PROPERTY.side, side ? { select: { name: side } } : { select: null });
  addIfExists(PROPERTY.qty, typeof qty === "number" ? { number: qty } : { number: null });
  addIfExists(
    PROPERTY.fillPrice,
    typeof price === "number" ? { number: price } : { number: null }
  );
  addIfExists(PROPERTY.fees, typeof fees === "number" ? { number: fees } : { number: null });
  if (tradeDate) {
    const split = toLocalDateTime(tradeDate);
    addIfExists(PROPERTY.tradeDate, { date: { start: split.date } });
    addIfExists(PROPERTY.tradeTime, { rich_text: [{ text: { content: split.time } }] });
  } else {
    addIfExists(PROPERTY.tradeDate, { date: null });
    addIfExists(PROPERTY.tradeTime, { rich_text: [] });
  }
  addIfExists(PROPERTY.strategy, { select: null });
  addIfExists(PROPERTY.setup, { rich_text: [] });
  addIfExists(PROPERTY.notes, { rich_text: [] });
  addIfExists(PROPERTY.entryScreenshot, { files: [] });
  if (typeof realized === "number") {
    addIfExists(PROPERTY.plAtClose, { number: realized });
  } else {
    addIfExists(PROPERTY.plAtClose, { number: null });
  }
  addIfExists(PROPERTY.tradeType, { select: null });
  addIfExists(PROPERTY.tags, { multi_select: [] });
  addIfExists(
    PROPERTY.broker,
    account?.brokerage ? { select: { name: account.brokerage } } : { select: null }
  );
  addIfExists(
    PROPERTY.account,
    account?.name ? { rich_text: [{ text: { content: account.name } }] } : { rich_text: [] }
  );
  addIfExists(
    PROPERTY.snaptradeId,
    activity.id ? { rich_text: [{ text: { content: activity.id } }] } : { rich_text: [] }
  );
  addIfExists(
    PROPERTY.orderId,
    activity.order_id ? { rich_text: [{ text: { content: activity.order_id } }] } : { rich_text: [] }
  );

  return getNotionClient().pages.create({
    parent: { database_id: info.databaseId },
    properties
  });
}

export async function createPositionPage(params: {
  title: string;
  ticker: string;
  contractKey: string;
  qty: number;
  avgPrice: number;
  openDate?: string | null;
  openTime?: string | null;
  broker?: string | null;
  account?: string | null;
}) {
  const info = await getJournalInfo();
  const properties: Record<string, any> = {
    [info.titleProperty]: {
      title: [{ text: { content: params.title || "Position" } }]
    }
  };

  const addIfExists = (key: string, value: any) => {
    if (info.properties[key] && key !== info.titleProperty) {
      properties[key] = value;
    }
  };

  addIfExists(
    PROPERTY.ticker,
    params.ticker ? { rich_text: [{ text: { content: params.ticker } }] } : { rich_text: [] }
  );
  addIfExists(PROPERTY.rowType, { select: { name: "Trade" } });
  addIfExists(
    PROPERTY.contractKey,
    params.contractKey
      ? { rich_text: [{ text: { content: params.contractKey } }] }
      : { rich_text: [] }
  );
  addIfExists(PROPERTY.status, { select: { name: "OPEN" } });
  addIfExists(PROPERTY.qty, { number: params.qty });
  addIfExists(PROPERTY.fillPrice, { number: params.avgPrice });
  if (params.openDate) {
    addIfExists(PROPERTY.tradeDate, { date: { start: params.openDate } });
  }
  if (params.openTime) {
    addIfExists(PROPERTY.tradeTime, { rich_text: [{ text: { content: params.openTime } }] });
  }
  addIfExists(
    PROPERTY.broker,
    params.broker ? { select: { name: params.broker } } : { select: null }
  );
  addIfExists(
    PROPERTY.account,
    params.account ? { rich_text: [{ text: { content: params.account } }] } : { rich_text: [] }
  );

  return getNotionClient().pages.create({
    parent: { database_id: info.databaseId },
    properties
  });
}

export async function updatePositionPage(params: {
  pageId: string;
  ticker: string;
  contractKey: string;
  qty: number;
  avgPrice: number;
  status: "OPEN" | "CLOSED";
  realizedPl?: number | null;
  closeDate?: string | null;
  closeTime?: string | null;
  closePrice?: number | null;
}) {
  const info = await getJournalInfo();
  const properties: Record<string, any> = {};
  const addIfExists = (key: string, value: any) => {
    if (info.properties[key] && key !== info.titleProperty) {
      properties[key] = value;
    }
  };

  properties[info.titleProperty] = {
    title: [{ text: { content: params.ticker || "Position" } }]
  };

  addIfExists(
    PROPERTY.ticker,
    params.ticker ? { rich_text: [{ text: { content: params.ticker } }] } : { rich_text: [] }
  );
  addIfExists(PROPERTY.rowType, { select: { name: "Trade" } });
  addIfExists(
    PROPERTY.contractKey,
    params.contractKey
      ? { rich_text: [{ text: { content: params.contractKey } }] }
      : { rich_text: [] }
  );
  addIfExists(PROPERTY.status, { select: { name: params.status } });
  addIfExists(PROPERTY.qty, { number: params.qty });
  addIfExists(PROPERTY.fillPrice, { number: params.avgPrice });
  if (params.status === "CLOSED") {
    if (params.closeDate) {
      addIfExists(PROPERTY.closeDate, { date: { start: params.closeDate } });
    }
    if (params.closeTime) {
      addIfExists(PROPERTY.closeTime, { rich_text: [{ text: { content: params.closeTime } }] });
    }
    if (typeof params.closePrice === "number") {
      addIfExists(PROPERTY.closePrice, { number: params.closePrice });
    }
    if (typeof params.realizedPl === "number") {
      addIfExists(PROPERTY.plAtClose, { number: params.realizedPl });
    }
  }

  return getNotionClient().pages.update({
    page_id: params.pageId,
    properties
  });
}

export async function archiveAllPages() {
  const info = await getJournalInfo();
  const client = getNotionClient();
  let cursor: string | undefined;
  let archived = 0;

  do {
    const response = await client.databases.query({
      database_id: info.databaseId,
      start_cursor: cursor
    });

    for (const page of response.results) {
      // Notion can return archived pages in query results; skip those to avoid
      // "Can't edit block that is archived" errors on repeated runs.
      if ((page as any).archived) {
        continue;
      }
      // Preserve manually imported Fidelity trade rows during SnapTrade rebuilds.
      const brokerProp = (page as any).properties?.[PROPERTY.broker];
      const rowTypeProp = (page as any).properties?.[PROPERTY.rowType];
      const brokerName =
        brokerProp?.type === "select" ? brokerProp.select?.name ?? "" : "";
      const rowTypeName =
        rowTypeProp?.type === "select" ? rowTypeProp.select?.name ?? "" : "";
      if (rowTypeName === "Trade" && brokerName.startsWith("Fidelity")) {
        continue;
      }
      await client.pages.update({
        page_id: page.id,
        archived: true
      });
      archived += 1;
    }

    cursor = response.has_more ? response.next_cursor ?? undefined : undefined;
  } while (cursor);

  return { archived };
}

export async function archiveTradePagesByBrokerPrefix(prefix: string) {
  const info = await getJournalInfo();
  const client = getNotionClient();
  let cursor: string | undefined;
  let archived = 0;

  do {
    const response = await client.databases.query({
      database_id: info.databaseId,
      start_cursor: cursor
    });

    for (const page of response.results) {
      if ((page as any).archived) continue;
      const brokerProp = (page as any).properties?.[PROPERTY.broker];
      const rowTypeProp = (page as any).properties?.[PROPERTY.rowType];
      const brokerName =
        brokerProp?.type === "select" ? brokerProp.select?.name ?? "" : "";
      const rowTypeName =
        rowTypeProp?.type === "select" ? rowTypeProp.select?.name ?? "" : "";
      if (rowTypeName !== "Trade" || !brokerName.startsWith(prefix)) {
        continue;
      }
      await client.pages.update({
        page_id: (page as any).id,
        archived: true
      });
      archived += 1;
    }

    cursor = response.has_more ? response.next_cursor ?? undefined : undefined;
  } while (cursor);

  return { archived };
}

export async function archivePage(pageId: string) {
  const client = getNotionClient();
  await client.pages.update({
    page_id: pageId,
    archived: true
  });
}

export async function updateTradePage(
  pageId: string,
  activity: SnapTradeOrder,
  account?: SnapTradeAccount
) {
  const info = await getJournalInfo();
  const symbol = activity.symbol?.symbol ?? "";
  const side = (activity.type ?? "").toUpperCase();
  const qty = activity.units ?? null;
  const price = activity.price ?? null;
  const fees = activity.fee ?? null;
  const tradeDate = activity.trade_date ?? null;

  const formatNumber = (value: number | string | null, maxDecimals = 6) => {
    if (value === null) return "";
    const numeric = typeof value === "number" ? value : Number(value);
    if (Number.isNaN(numeric)) return "";
    const fixed = numeric.toFixed(maxDecimals);
    return fixed.replace(/\.?0+$/, "");
  };

  const toLocalDateTime = (iso: string) => {
    const date = new Date(iso);
    const locale = "en-US";
    const zone = config.NOTION_TIMEZONE ?? "America/New_York";
    const dateString = new Intl.DateTimeFormat(locale, {
      timeZone: zone,
      year: "numeric",
      month: "2-digit",
      day: "2-digit"
    }).format(date);
    const timeString = new Intl.DateTimeFormat(locale, {
      timeZone: zone,
      hour: "numeric",
      minute: "2-digit",
      hour12: true
    }).format(date);

    const [month, day, year] = dateString.split("/");
    return {
      date: `${year}-${month}-${day}`,
      time: timeString
    };
  };

  const properties: Record<string, any> = {};
  const addIfExists = (key: string, value: any) => {
    if (info.properties[key] && key !== info.titleProperty) {
      properties[key] = value;
    }
  };

  if (info.titleProperty) {
    properties[info.titleProperty] = {
      title: [{ text: { content: symbol || "Trade" } }]
    };
  }
  addIfExists(PROPERTY.rowType, { select: { name: "Trade" } });

  addIfExists(
    PROPERTY.ticker,
    symbol ? { rich_text: [{ text: { content: symbol } }] } : { rich_text: [] }
  );
  addIfExists(PROPERTY.side, side ? { select: { name: side } } : { select: null });
  addIfExists(PROPERTY.qty, typeof qty === "number" ? { number: qty } : { number: null });
  addIfExists(
    PROPERTY.fillPrice,
    typeof price === "number" ? { number: price } : { number: null }
  );
  addIfExists(PROPERTY.fees, typeof fees === "number" ? { number: fees } : { number: null });
  if (tradeDate) {
    const split = toLocalDateTime(tradeDate);
    addIfExists(PROPERTY.tradeDate, { date: { start: split.date } });
    addIfExists(PROPERTY.tradeTime, { rich_text: [{ text: { content: split.time } }] });
  }
  addIfExists(
    PROPERTY.broker,
    account?.brokerage ? { select: { name: account.brokerage } } : { select: null }
  );
  addIfExists(
    PROPERTY.account,
    account?.name ? { rich_text: [{ text: { content: account.name } }] } : { rich_text: [] }
  );
  addIfExists(
    PROPERTY.snaptradeId,
    activity.id ? { rich_text: [{ text: { content: activity.id } }] } : { rich_text: [] }
  );
  addIfExists(
    PROPERTY.orderId,
    activity.order_id ? { rich_text: [{ text: { content: activity.order_id } }] } : { rich_text: [] }
  );

  return getNotionClient().pages.update({
    page_id: pageId,
    properties
  });
}

export async function archiveZeroQtyPages() {
  const info = await getJournalInfo();
  const client = getNotionClient();
  let cursor: string | undefined;
  let archived = 0;

  do {
    const response = await client.databases.query({
      database_id: info.databaseId,
      start_cursor: cursor,
      filter: {
        property: PROPERTY.qty,
        number: { equals: 0 }
      }
    });

    for (const page of response.results) {
      await client.pages.update({
        page_id: page.id,
        archived: true
      });
      archived += 1;
    }

    cursor = response.has_more ? response.next_cursor ?? undefined : undefined;
  } while (cursor);

  return { archived };
}

type AuditItem = {
  pageId: string;
  title: string;
  reason: string;
};

type AuditResult = {
  totalRows: number;
  closedMissingPL: AuditItem[];
  openWithCloseDate: AuditItem[];
  missingContractKey: AuditItem[];
  invalidQtyOrFillPrice: AuditItem[];
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

export async function auditJournal(limitPerFinding = 25): Promise<AuditResult> {
  const info = await getJournalInfo();
  const client = getNotionClient();
  let cursor: string | undefined;
  const rows: any[] = [];

  do {
    const response = await client.databases.query({
      database_id: info.databaseId,
      start_cursor: cursor
    });
    rows.push(...response.results);
    cursor = response.has_more ? response.next_cursor ?? undefined : undefined;
  } while (cursor);

  const closedMissingPL: AuditItem[] = [];
  const openWithCloseDate: AuditItem[] = [];
  const missingContractKey: AuditItem[] = [];
  const invalidQtyOrFillPrice: AuditItem[] = [];

  for (const page of rows) {
    const title = getTitleValue(page, info.titleProperty) || "(untitled)";
    const status = getSelectValue(page, PROPERTY.status).toUpperCase();
    const qty = getNumberValue(page, PROPERTY.qty);
    const fillPrice = getNumberValue(page, PROPERTY.fillPrice);
    const pl = getNumberValue(page, PROPERTY.plAtClose);
    const closeDate = getDateValue(page, PROPERTY.closeDate);
    const contractKey = getRichTextValue(page, PROPERTY.contractKey);
    const pageId = page.id as string;

    if (status === "CLOSED" && (pl === null || Number.isNaN(pl))) {
      if (closedMissingPL.length < limitPerFinding) {
        closedMissingPL.push({ pageId, title, reason: "Closed row has blank P/L at Close" });
      }
    }

    if (status === "OPEN" && closeDate) {
      if (openWithCloseDate.length < limitPerFinding) {
        openWithCloseDate.push({ pageId, title, reason: `Open row has Close Date ${closeDate}` });
      }
    }

    if (!contractKey) {
      if (missingContractKey.length < limitPerFinding) {
        missingContractKey.push({ pageId, title, reason: "Contract Key is blank" });
      }
    }

    if ((status === "OPEN" || status === "CLOSED") && ((qty ?? 0) <= 0 || (fillPrice ?? 0) <= 0)) {
      if (invalidQtyOrFillPrice.length < limitPerFinding) {
        invalidQtyOrFillPrice.push({
          pageId,
          title,
          reason: `Invalid Qty/Fill Price (Qty=${qty ?? "null"}, Fill Price=${fillPrice ?? "null"})`
        });
      }
    }
  }

  return {
    totalRows: rows.length,
    closedMissingPL,
    openWithCloseDate,
    missingContractKey,
    invalidQtyOrFillPrice
  };
}
