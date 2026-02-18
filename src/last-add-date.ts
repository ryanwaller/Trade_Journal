import { getJournalInfo, getNotionClient, PROPERTY } from "./notion.js";

function getSelect(page: any, key: string) {
  const prop = page.properties?.[key];
  if (!prop || prop.type !== "select") return "";
  return prop.select?.name ?? "";
}

function getDateStart(page: any, key: string) {
  const prop = page.properties?.[key];
  if (!prop || prop.type !== "date") return "";
  return prop.date?.start ?? "";
}

export async function runClearLastAddDateEqualsOpen() {
  const info = await getJournalInfo();
  if (!info.properties[PROPERTY.lastAddDate]) {
    return { cleared: 0, reason: "Missing Notion property: Last Add Date" };
  }

  const client = getNotionClient();
  let cursor: string | undefined;
  let cleared = 0;

  const supported = new Set(["Public", "Robinhood", "Fidelity"]);

  do {
    const res = await client.databases.query({
      database_id: info.databaseId,
      start_cursor: cursor,
      filter: {
        and: [
          { property: PROPERTY.status, select: { equals: "OPEN" } },
          { property: PROPERTY.lastAddDate, date: { is_not_empty: true } }
        ]
      } as any
    });

    for (const page of res.results as any[]) {
      if (page.archived) continue;
      const broker = getSelect(page, PROPERTY.broker);
      if (!supported.has(broker)) continue;

      const tradeDate = getDateStart(page, PROPERTY.tradeDate);
      const lastAddDate = getDateStart(page, PROPERTY.lastAddDate);
      if (!tradeDate || !lastAddDate) continue;
      if (tradeDate !== lastAddDate) continue;

      await client.pages.update({
        page_id: page.id,
        properties: {
          [PROPERTY.lastAddDate]: { date: null }
        }
      });
      cleared += 1;
    }

    cursor = res.has_more ? res.next_cursor ?? undefined : undefined;
  } while (cursor);

  return { cleared };
}

