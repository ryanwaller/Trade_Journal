import { randomUUID } from "crypto";
import { config } from "./config.js";
import { getLoginUrl, registerUser } from "./snaptrade.js";
import { runBackfill, runRebuildPositions, runSync } from "./sync.js";
import { archiveZeroQtyPages, auditJournal, normalizeFidelityBrokerLabels } from "./notion.js";
import { runWeeklyReview } from "./weekly-review.js";
import { runRebuildDailySummary } from "./daily-summary.js";
import { runFreshnessCheck } from "./freshness.js";
import { runImportFidelity } from "./fidelity.js";
import { runImportFidelityPositions } from "./fidelity-positions.js";
import { runSnaptradeReconcileOpen } from "./snaptrade-reconcile.js";
import { runImportPublicPdf } from "./public-pdf.js";
import { runImportPublicHistory } from "./public-history.js";
import { runWatchFidelity } from "./fidelity-watch.js";

const command = process.argv[2];

async function run() {
  if (command === "sync") {
    const result = await runSync();
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  if (command === "backfill") {
    const result = await runBackfill();
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  if (command === "cleanup-zero-qty") {
    const result = await archiveZeroQtyPages();
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  if (command === "audit-journal") {
    const result = await auditJournal();
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  if (command === "normalize-fidelity-broker") {
    const result = await normalizeFidelityBrokerLabels();
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  if (command === "weekly-review") {
    const result = await runWeeklyReview();
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  if (command === "rebuild-daily-summary") {
    const result = await runRebuildDailySummary();
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  if (command === "rebuild-positions") {
    const result = await runRebuildPositions();
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  if (command === "freshness-check") {
    const result = await runFreshnessCheck();
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  if (command === "import-fidelity") {
    const result = await runImportFidelity();
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  if (command === "import-fidelity-positions") {
    const result = await runImportFidelityPositions();
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  if (command === "reconcile-open") {
    const result = await runSnaptradeReconcileOpen();
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  if (command === "import-public-pdf") {
    const result = await runImportPublicPdf();
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  if (command === "import-public-history") {
    const result = await runImportPublicHistory();
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  if (command === "watch-fidelity") {
    await runWatchFidelity();
    return;
  }

  if (command === "connect") {
    const existingUserId = config.SNAPTRADE_USER_ID;
    const existingUserSecret = config.SNAPTRADE_USER_SECRET;

    const userId = existingUserId ?? randomUUID();
    let userSecret = existingUserSecret;

    if (!existingUserSecret) {
      const register = await registerUser(userId);
      userSecret = register.userSecret ?? register.user_secret ?? register.secret;
      if (!userSecret) {
        throw new Error("SnapTrade register did not return a user secret");
      }

      console.log("SnapTrade user created:");
      console.log(`SNAPTRADE_USER_ID=${userId}`);
      console.log(`SNAPTRADE_USER_SECRET=${userSecret}`);
    }

    const login = await getLoginUrl(userId, userSecret!, config.SNAPTRADE_REDIRECT_URI);
    const loginUrl = login.redirectURI ?? login.redirect_uri ?? login;

    console.log("\nConnection Portal URL (open in browser):");
    console.log(loginUrl);
    return;
  }

  console.log(
    "Usage: npm run sync | npm run connect | npm run backfill | npm run cleanup-zero-qty | npm run rebuild-positions | npm run audit-journal | npm run normalize-fidelity-broker | npm run weekly-review | npm run rebuild-daily-summary | npm run freshness-check | npm run import-fidelity | npm run import-fidelity-positions | npm run reconcile-open | npm run import-public-pdf | npm run import-public-history | npm run watch-fidelity"
  );
}

run().catch((error) => {
  console.error(error);
  process.exit(1);
});
