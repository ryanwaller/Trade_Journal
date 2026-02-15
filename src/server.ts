import express from "express";
import { assertSyncConfig, config } from "./config.js";
import { runSync } from "./sync.js";

assertSyncConfig();

const app = express();
app.use(express.json());

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

function isAuthorized(req: express.Request) {
  const token = config.SYNC_TOKEN;
  if (!token) return false;

  const authHeader = req.header("authorization") ?? "";
  const bearer = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : null;
  const headerToken = req.header("x-sync-token");
  const queryToken = typeof req.query.token === "string" ? req.query.token : null;

  return [bearer, headerToken, queryToken].some((t) => t === token);
}

app.post("/sync", async (req, res) => {
  if (!isAuthorized(req)) {
    res.status(401).json({ error: "Unauthorized" });
    return;
  }

  try {
    const dryRun = Boolean(req.body?.dryRun);
    const result = await runSync({ dryRun });
    res.json({ ok: true, ...result });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Sync failed" });
  }
});

app.listen(Number(config.PORT), () => {
  console.log(`Server listening on :${config.PORT}`);
});
