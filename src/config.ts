import dotenv from "dotenv";

dotenv.config();

type RequiredEnv = {
  SNAPTRADE_CLIENT_ID: string;
  SNAPTRADE_CONSUMER_KEY: string;
};

type OptionalEnv = {
  PORT?: string;
  SYNC_TOKEN?: string;
  SNAPTRADE_DAYS?: string;
  SNAPTRADE_START_DATE?: string;
  SNAPTRADE_INCLUDE_ALL?: string;
  SNAPTRADE_FETCH_START_DATE?: string;
  SNAPTRADE_REDIRECT_URI?: string;
  NOTION_TIMEZONE?: string;
  SNAPTRADE_USER_ID?: string;
  SNAPTRADE_USER_SECRET?: string;
  NOTION_TOKEN?: string;
  NOTION_DATABASE_ID?: string;
  NOTION_DATA_SOURCE_ID?: string;
  NOTION_WEEKLY_REVIEWS_PAGE_ID?: string;
};

const requiredKeys: (keyof RequiredEnv)[] = [
  "SNAPTRADE_CLIENT_ID",
  "SNAPTRADE_CONSUMER_KEY"
];

function getEnv() {
  const missing = requiredKeys.filter((key) => !process.env[key]);
  if (missing.length > 0) {
    throw new Error(`Missing required env vars: ${missing.join(", ")}`);
  }

  const clean = (value?: string) => {
    const trimmed = value?.trim();
    return trimmed ? trimmed : undefined;
  };

  const env = {
    SNAPTRADE_CLIENT_ID: process.env.SNAPTRADE_CLIENT_ID!,
    SNAPTRADE_CONSUMER_KEY: process.env.SNAPTRADE_CONSUMER_KEY!,
    SNAPTRADE_USER_ID: clean(process.env.SNAPTRADE_USER_ID),
    SNAPTRADE_USER_SECRET: clean(process.env.SNAPTRADE_USER_SECRET),
    NOTION_TOKEN: clean(process.env.NOTION_TOKEN),
    NOTION_DATABASE_ID: clean(process.env.NOTION_DATABASE_ID),
    NOTION_DATA_SOURCE_ID: clean(process.env.NOTION_DATA_SOURCE_ID),
    NOTION_WEEKLY_REVIEWS_PAGE_ID: clean(process.env.NOTION_WEEKLY_REVIEWS_PAGE_ID),
    PORT: clean(process.env.PORT) ?? "3000",
    SYNC_TOKEN: clean(process.env.SYNC_TOKEN),
    SNAPTRADE_DAYS: clean(process.env.SNAPTRADE_DAYS) ?? "30",
    SNAPTRADE_START_DATE: clean(process.env.SNAPTRADE_START_DATE),
    SNAPTRADE_INCLUDE_ALL: clean(process.env.SNAPTRADE_INCLUDE_ALL),
    SNAPTRADE_FETCH_START_DATE: clean(process.env.SNAPTRADE_FETCH_START_DATE),
    SNAPTRADE_REDIRECT_URI: clean(process.env.SNAPTRADE_REDIRECT_URI),
    NOTION_TIMEZONE: clean(process.env.NOTION_TIMEZONE) ?? "America/New_York"
  } satisfies RequiredEnv & OptionalEnv;

  return env;
}

export const config = getEnv();

export function assertSyncConfig() {
  const missing: string[] = [];
  if (!config.SNAPTRADE_USER_ID) missing.push("SNAPTRADE_USER_ID");
  if (!config.SNAPTRADE_USER_SECRET) missing.push("SNAPTRADE_USER_SECRET");
  if (!config.NOTION_TOKEN) missing.push("NOTION_TOKEN");
  if (!config.NOTION_DATABASE_ID && !config.NOTION_DATA_SOURCE_ID) {
    missing.push("NOTION_DATABASE_ID or NOTION_DATA_SOURCE_ID");
  }

  if (missing.length > 0) {
    throw new Error(`Missing required env vars for sync: ${missing.join(", ")}`);
  }
}

export function assertNotionConfig() {
  const missing: string[] = [];
  if (!config.NOTION_TOKEN) missing.push("NOTION_TOKEN");
  if (!config.NOTION_DATABASE_ID && !config.NOTION_DATA_SOURCE_ID) {
    missing.push("NOTION_DATABASE_ID or NOTION_DATA_SOURCE_ID");
  }

  if (missing.length > 0) {
    throw new Error(`Missing required env vars for Notion: ${missing.join(", ")}`);
  }
}

export function assertWeeklyReviewConfig() {
  const missing: string[] = [];
  if (!config.NOTION_TOKEN) missing.push("NOTION_TOKEN");
  if (!config.NOTION_DATABASE_ID && !config.NOTION_DATA_SOURCE_ID) {
    missing.push("NOTION_DATABASE_ID or NOTION_DATA_SOURCE_ID");
  }
  if (!config.NOTION_WEEKLY_REVIEWS_PAGE_ID) missing.push("NOTION_WEEKLY_REVIEWS_PAGE_ID");

  if (missing.length > 0) {
    throw new Error(
      `Missing required env vars for weekly review: ${missing.join(", ")}`
    );
  }
}
