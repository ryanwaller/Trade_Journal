import { Client } from "@notionhq/client";
import { assertNotionConfig, config } from "./config.js";
import type { SnapTradeOrder, SnapTradeAccount } from "./snaptrade.js";

let notion: Client | null = null;
type DatabaseInfo = {
  databaseId: string;
  properties: Record<string, any>;
  titleProperty: string;
};

type TradeTypeName = "Stock" | "Call" | "Put";

let databaseInfo: DatabaseInfo | null = null;

function inferTradeTypeFromContract(contractKey?: string | null, ticker?: string | null): TradeTypeName | null {
  const key = (contractKey ?? "").trim().toUpperCase();
  const t = (ticker ?? "").trim().toUpperCase();
  if (!key && !t) return null;

  const compact = key.replace(/\s+/g, "");
  const optionMatch = compact.match(/^([A-Z.\-]+)\d{6}([CP])\d+(?:\.\d+)?$/);
  if (optionMatch) return optionMatch[2] === "C" ? "Call" : "Put";
  if (/\bCALL\b/.test(key)) return "Call";
  if (/\bPUT\b/.test(key)) return "Put";

  if (key && t && key === t) return "Stock";
  if (key && /^[A-Z.\-]{1,10}$/.test(key)) return "Stock";
  return null;
}

function inferTradeTypeFromSnapTradeSymbolKey(symbolKey?: string | null): TradeTypeName | null {
  const key = (symbolKey ?? "").trim().toUpperCase().replace(/\s+/g, "");
  if (!key) return null;
  const m = key.match(/^([A-Z.\-]+)\d{6}([CP])\d+$/);
  if (!m) return null;
  return m[2] === "C" ? "Call" : "Put";
}

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
  lastAddDate: "Last Add Date",
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

export type OpenPositionSnapshot = {
  ticker: string;
  tradeDate: string;
  qty: number;
  fillPrice: number;
  contractKey: string;
  broker: string;
};

export async function fetchOpenPositionSnapshotsByBrokers(
  brokers: string[]
): Promise<OpenPositionSnapshot[]> {
  const brokerSet = new Set(brokers.map((b) => b.trim()).filter(Boolean));
  if (brokerSet.size === 0) return [];

  const info = await getJournalInfo();
  const client = getNotionClient();
  const snapshots: OpenPositionSnapshot[] = [];
  let cursor: string | undefined;

  const filter = {
    and: [
      {
        property: PROPERTY.status,
        select: { equals: "OPEN" }
      },
      ...(info.properties[PROPERTY.rowType]
        ? [
            {
              property: PROPERTY.rowType,
              select: { equals: "Position" }
            }
          ]
        : [])
    ]
  } as any;

  do {
    const response = await client.databases.query({
      database_id: info.databaseId,
      start_cursor: cursor,
      filter
    });

    for (const page of response.results as any[]) {
      if (page.archived) continue;
      const broker = getSelectValue(page, PROPERTY.broker);
      if (!brokerSet.has(broker)) continue;

      const ticker =
        getRichTextValue(page, PROPERTY.ticker) || getTitleValue(page, info.titleProperty);
      const tradeDate = getDateValue(page, PROPERTY.tradeDate);
      const qty = getNumberValue(page, PROPERTY.qty);
      const fillPrice = getNumberValue(page, PROPERTY.fillPrice);
      const contractKey = getRichTextValue(page, PROPERTY.contractKey);
      if (!ticker || !tradeDate || qty === null || fillPrice === null) continue;

      snapshots.push({
        ticker: ticker.toUpperCase(),
        tradeDate,
        qty,
        fillPrice,
        contractKey: contractKey.toUpperCase(),
        broker
      });
    }

    cursor = response.has_more ? response.next_cursor ?? undefined : undefined;
  } while (cursor);

  return snapshots;
}

export async function fetchTradeSnapshotsByBrokers(
  brokers: string[]
): Promise<OpenPositionSnapshot[]> {
  const brokerSet = new Set(brokers.map((b) => b.trim()).filter(Boolean));
  if (brokerSet.size === 0) return [];

  const info = await getJournalInfo();
  const client = getNotionClient();
  const snapshots: OpenPositionSnapshot[] = [];
  let cursor: string | undefined;

  do {
    const response = await client.databases.query({
      database_id: info.databaseId,
      start_cursor: cursor,
    });

    for (const page of response.results as any[]) {
      if (page.archived) continue;
      const broker = getSelectValue(page, PROPERTY.broker);
      if (!brokerSet.has(broker)) continue;

      const ticker =
        getRichTextValue(page, PROPERTY.ticker) || getTitleValue(page, info.titleProperty);
      const tradeDate = getDateValue(page, PROPERTY.tradeDate);
      const qty = getNumberValue(page, PROPERTY.qty);
      const fillPrice = getNumberValue(page, PROPERTY.fillPrice);
      const contractKey = getRichTextValue(page, PROPERTY.contractKey);
      if (!ticker || !tradeDate || qty === null || fillPrice === null) continue;

      snapshots.push({
        ticker: ticker.toUpperCase(),
        tradeDate,
        qty,
        fillPrice,
        contractKey: contractKey.toUpperCase(),
        broker
      });
    }

    cursor = response.has_more ? response.next_cursor ?? undefined : undefined;
  } while (cursor);

  return snapshots;
}

export type TradeIdentityIndex = {
  snaptradeIds: Set<string>;
  orderIds: Set<string>;
  pageIdBySnaptradeId: Map<string, string>;
  pageIdByOrderId: Map<string, string>;
};

export async function loadTradeIdentityIndex(): Promise<TradeIdentityIndex> {
  const info = await getJournalInfo();
  const client = getNotionClient();
  const snaptradeIds = new Set<string>();
  const orderIds = new Set<string>();
  const pageIdBySnaptradeId = new Map<string, string>();
  const pageIdByOrderId = new Map<string, string>();
  const filter = info.properties[PROPERTY.rowType]
    ? {
        property: PROPERTY.rowType,
        select: { equals: "Trade" }
      }
    : undefined;

  let cursor: string | undefined;
  do {
    const response = await client.databases.query({
      database_id: info.databaseId,
      start_cursor: cursor,
      ...(filter ? { filter } : {})
    });

    for (const page of response.results as any[]) {
      if (page.archived) continue;
      const snaptradeId = getRichTextValue(page, PROPERTY.snaptradeId);
      const orderId = getRichTextValue(page, PROPERTY.orderId);
      if (snaptradeId) {
        snaptradeIds.add(snaptradeId);
        pageIdBySnaptradeId.set(snaptradeId, page.id);
      }
      if (orderId) {
        orderIds.add(orderId);
        pageIdByOrderId.set(orderId, page.id);
      }
    }

    cursor = response.has_more ? response.next_cursor ?? undefined : undefined;
  } while (cursor);

  return {
    snaptradeIds,
    orderIds,
    pageIdBySnaptradeId,
    pageIdByOrderId
  };
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
  const inferredTradeType =
    inferTradeTypeFromSnapTradeSymbolKey(activity.symbol_key) ??
    inferTradeTypeFromContract(symbol, symbol);

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
  addIfExists(
    PROPERTY.tradeType,
    inferredTradeType ? { select: { name: inferredTradeType } } : { select: null }
  );
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
  side?: "BUY" | "SELL" | null;
  tradeType?: TradeTypeName | null;
  openDate?: string | null;
  openTime?: string | null;
  lastAddDate?: string | null;
  // Manual fields (user-owned). We only set them at creation time.
  strategy?: string | null;
  tags?: string[] | null;
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
  if (params.strategy !== undefined) {
    addIfExists(
      PROPERTY.strategy,
      params.strategy ? { select: { name: params.strategy } } : { select: null }
    );
  }
  if (params.tags !== undefined) {
    addIfExists(
      PROPERTY.tags,
      params.tags && params.tags.length > 0
        ? { multi_select: params.tags.map((name) => ({ name })) }
        : { multi_select: [] }
    );
  }
  addIfExists(
    PROPERTY.side,
    params.side ? { select: { name: params.side } } : { select: null }
  );
  const inferredTradeType =
    params.tradeType ?? inferTradeTypeFromContract(params.contractKey, params.ticker);
  addIfExists(
    PROPERTY.tradeType,
    inferredTradeType ? { select: { name: inferredTradeType } } : { select: null }
  );
  if (params.openDate) {
    addIfExists(PROPERTY.tradeDate, { date: { start: params.openDate } });
  }
  if (params.lastAddDate) {
    addIfExists(PROPERTY.lastAddDate, { date: { start: params.lastAddDate } });
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

type ManualStrategyTags = {
  strategy: string | null;
  tags: string[];
};

function normalizeManualKeyPart(value: string) {
  return value.trim().replace(/\s+/g, "").toUpperCase();
}

export function manualKeyForPosition(account: string, contractKey: string, openDate: string | null) {
  return `${normalizeManualKeyPart(account)}|${normalizeManualKeyPart(contractKey)}|${openDate ?? ""}`;
}

export function manualKeyForPositionLoose(account: string, contractKey: string) {
  return `LOOSE|${normalizeManualKeyPart(account)}|${normalizeManualKeyPart(contractKey)}`;
}

function mergeManual(a: ManualStrategyTags, b: ManualStrategyTags): ManualStrategyTags {
  const tags = Array.from(new Set([...(a.tags ?? []), ...(b.tags ?? [])]));
  return {
    strategy: a.strategy ?? b.strategy ?? null,
    tags
  };
}

export function lookupManualStrategyTags(
  index: Map<string, ManualStrategyTags>,
  account: string,
  contractKey: string,
  openDate: string | null
): ManualStrategyTags | null {
  const exact = index.get(manualKeyForPosition(account, contractKey, openDate));
  if (exact) return exact;
  const loose = index.get(manualKeyForPositionLoose(account, contractKey));
  return loose ?? null;
}

export async function loadManualStrategyTagsIndexForBroker(
  brokerNameExact: string
): Promise<Map<string, ManualStrategyTags>> {
  const info = await getJournalInfo();
  const client = getNotionClient();
  const index = new Map<string, ManualStrategyTags>();
  let cursor: string | undefined;

  const filter: any = {
    and: [
      { property: PROPERTY.broker, select: { equals: brokerNameExact } },
      ...(info.properties[PROPERTY.rowType]
        ? [{ property: PROPERTY.rowType, select: { equals: "Trade" } }]
        : [])
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
      const account = getRichTextValue(page, PROPERTY.account);
      const contractKey = getRichTextValue(page, PROPERTY.contractKey);
      const openDate = getDateValue(page, PROPERTY.tradeDate) || null;
      if (!account || !contractKey) continue;

      const strategyProp = page.properties?.[PROPERTY.strategy];
      const tagsProp = page.properties?.[PROPERTY.tags];
      const strategy =
        strategyProp?.type === "select" ? strategyProp.select?.name ?? null : null;
      const tags =
        tagsProp?.type === "multi_select"
          ? (tagsProp.multi_select ?? []).map((t: any) => t?.name).filter(Boolean)
          : [];

      // Only store if user actually set something.
      if (!strategy && tags.length === 0) continue;

      const value = { strategy, tags } satisfies ManualStrategyTags;

      const exactKey = manualKeyForPosition(account, contractKey, openDate);
      const prevExact = index.get(exactKey);
      index.set(exactKey, prevExact ? mergeManual(prevExact, value) : value);

      // Also store a loose key for cases where the importer can't determine openDate.
      const looseKey = manualKeyForPositionLoose(account, contractKey);
      const prevLoose = index.get(looseKey);
      index.set(looseKey, prevLoose ? mergeManual(prevLoose, value) : value);
    }

    cursor = res.has_more ? res.next_cursor ?? undefined : undefined;
  } while (cursor);

  return index;
}

export async function updatePositionPage(params: {
  pageId: string;
  ticker: string;
  contractKey: string;
  qty: number;
  avgPrice: number;
  side?: "BUY" | "SELL" | null;
  tradeType?: TradeTypeName | null;
  status: "OPEN" | "CLOSED";
  lastAddDate?: string | null;
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
  if (params.status === "OPEN" && params.lastAddDate) {
    addIfExists(PROPERTY.lastAddDate, { date: { start: params.lastAddDate } });
  }
  addIfExists(
    PROPERTY.side,
    params.side ? { select: { name: params.side } } : { select: null }
  );
  const inferredTradeType =
    params.tradeType ?? inferTradeTypeFromContract(params.contractKey, params.ticker);
  addIfExists(
    PROPERTY.tradeType,
    inferredTradeType ? { select: { name: inferredTradeType } } : { select: null }
  );
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
      if (
        rowTypeName === "Trade" &&
        (
          brokerName.startsWith("Fidelity") ||
          brokerName.startsWith("Public (PDF)") ||
          brokerName.startsWith("Public (History)")
        )
      ) {
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
      try {
        await client.pages.update({
          page_id: (page as any).id,
          archived: true
        });
        archived += 1;
      } catch (err: any) {
        // Notion may return rows that are already archived in eventual-consistency windows.
        const msg = String(err?.message ?? "");
        if (msg.includes("already archived") || msg.includes("is archived")) {
          continue;
        }
        throw err;
      }
    }

    cursor = response.has_more ? response.next_cursor ?? undefined : undefined;
  } while (cursor);

  return { archived };
}

export async function archiveTradePagesByExactBroker(brokerNameExact: string) {
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
      if (rowTypeName !== "Trade" || brokerName !== brokerNameExact) {
        continue;
      }
      try {
        await client.pages.update({
          page_id: (page as any).id,
          archived: true
        });
        archived += 1;
      } catch (err: any) {
        const msg = String(err?.message ?? "");
        if (msg.includes("already archived") || msg.includes("is archived")) {
          continue;
        }
        throw err;
      }
    }

    cursor = response.has_more ? response.next_cursor ?? undefined : undefined;
  } while (cursor);

  return { archived };
}

export async function archivePage(pageId: string) {
  const client = getNotionClient();
  try {
    await client.pages.update({
      page_id: pageId,
      archived: true
    });
  } catch (err: any) {
    const msg = String(err?.message ?? "");
    if (msg.includes("already archived") || msg.includes("is archived")) {
      return;
    }
    throw err;
  }
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
  const inferredTradeType =
    inferTradeTypeFromSnapTradeSymbolKey(activity.symbol_key) ??
    inferTradeTypeFromContract(symbol, symbol);

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
  addIfExists(
    PROPERTY.tradeType,
    inferredTradeType ? { select: { name: inferredTradeType } } : { select: null }
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

export async function normalizeFidelityBrokerLabels() {
  const info = await getJournalInfo();
  const client = getNotionClient();
  let cursor: string | undefined;
  let updated = 0;

  const legacyToAccount: Record<string, string> = {
    "Fidelity (Fun)": "Fun",
    "Fidelity (IRA Roth)": "IRA Roth",
    "Fidelity (IRA Trad)": "IRA Trad"
  };

  do {
    const response = await client.databases.query({
      database_id: info.databaseId,
      start_cursor: cursor
    });

    for (const page of response.results as any[]) {
      if (page.archived) continue;
      const broker = getSelectValue(page, PROPERTY.broker);
      const rowType = getSelectValue(page, PROPERTY.rowType);
      if (rowType !== "Trade") continue;

      const mappedAccount = legacyToAccount[broker];
      if (!mappedAccount) continue;

      const existingAccount = getRichTextValue(page, PROPERTY.account);
      const properties: Record<string, any> = {};
      if (info.properties[PROPERTY.broker]) {
        properties[PROPERTY.broker] = { select: { name: "Fidelity" } };
      }
      if (!existingAccount && info.properties[PROPERTY.account]) {
        properties[PROPERTY.account] = { rich_text: [{ text: { content: mappedAccount } }] };
      }

      await client.pages.update({
        page_id: page.id,
        properties
      });
      updated += 1;
    }

    cursor = response.has_more ? response.next_cursor ?? undefined : undefined;
  } while (cursor);

  return { updated };
}

export async function backfillTradeType() {
  const info = await getJournalInfo();
  const client = getNotionClient();
  let cursor: string | undefined;
  let scanned = 0;
  let updated = 0;
  let inferredUnknown = 0;

  do {
    const response = await client.databases.query({
      database_id: info.databaseId,
      start_cursor: cursor
    });

    for (const page of response.results as any[]) {
      if (page.archived) continue;
      scanned += 1;

      const rowType = getSelectValue(page, PROPERTY.rowType);
      if (rowType && rowType !== "Trade") continue;

      const contractKey = getRichTextValue(page, PROPERTY.contractKey);
      const ticker =
        getRichTextValue(page, PROPERTY.ticker) || getTitleValue(page, info.titleProperty);
      const inferred = inferTradeTypeFromContract(contractKey, ticker);
      if (!inferred) {
        inferredUnknown += 1;
        continue;
      }

      const current = getSelectValue(page, PROPERTY.tradeType);
      if (current === inferred) continue;
      if (!info.properties[PROPERTY.tradeType]) continue;

      await client.pages.update({
        page_id: page.id,
        properties: {
          [PROPERTY.tradeType]: { select: { name: inferred } }
        }
      });
      updated += 1;
    }

    cursor = response.has_more ? response.next_cursor ?? undefined : undefined;
  } while (cursor);

  return {
    scannedRows: scanned,
    updatedRows: updated,
    unknownRows: inferredUnknown
  };
}

export async function backfillTradeTypeForBrokerByContractKey(
  brokerName: string,
  typeByContractKey: Map<string, "Stock" | "Call" | "Put">
) {
  const info = await getJournalInfo();
  const client = getNotionClient();
  if (!info.properties[PROPERTY.tradeType]) {
    return {
      broker: brokerName,
      scannedRows: 0,
      matchedRows: 0,
      updatedRows: 0,
      missingTypeRows: 0
    };
  }

  let cursor: string | undefined;
  let scanned = 0;
  let matched = 0;
  let updated = 0;
  let missing = 0;

  const filter = {
    and: [
      {
        property: PROPERTY.broker,
        select: { equals: brokerName }
      },
      ...(info.properties[PROPERTY.rowType]
        ? [
            {
              property: PROPERTY.rowType,
              select: { equals: "Trade" }
            }
          ]
        : [])
    ]
  } as any;

  do {
    const response = await client.databases.query({
      database_id: info.databaseId,
      start_cursor: cursor,
      filter
    });

    for (const page of response.results as any[]) {
      if (page.archived) continue;
      scanned += 1;
      const contractKey = getRichTextValue(page, PROPERTY.contractKey).toUpperCase();
      if (!contractKey) continue;
      const inferred = typeByContractKey.get(contractKey);
      if (!inferred) {
        missing += 1;
        continue;
      }
      matched += 1;
      const current = getSelectValue(page, PROPERTY.tradeType);
      if (current === inferred) continue;

      await client.pages.update({
        page_id: page.id,
        properties: {
          [PROPERTY.tradeType]: { select: { name: inferred } }
        }
      });
      updated += 1;
    }

    cursor = response.has_more ? response.next_cursor ?? undefined : undefined;
  } while (cursor);

  return {
    broker: brokerName,
    scannedRows: scanned,
    matchedRows: matched,
    updatedRows: updated,
    missingTypeRows: missing
  };
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
