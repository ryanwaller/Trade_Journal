import fs from "node:fs/promises";
import path from "node:path";
import { runImportFidelity } from "./fidelity.js";
import { runImportFidelityPositions } from "./fidelity-positions.js";
import { runRebuildDailySummary } from "./daily-summary.js";

type DirSnapshot = {
  signature: string;
  csvCount: number;
};

async function readCsvSnapshot(dir: string): Promise<DirSnapshot> {
  try {
    const entries = await fs.readdir(dir, { withFileTypes: true });
    const files = entries
      .filter((e) => e.isFile() && e.name.toLowerCase().endsWith(".csv"))
      .map((e) => e.name)
      .sort((a, b) => a.localeCompare(b));

    const parts: string[] = [];
    for (const file of files) {
      const full = path.join(dir, file);
      const stat = await fs.stat(full);
      parts.push(`${file}:${stat.size}:${stat.mtimeMs}`);
    }

    return {
      signature: parts.join("|"),
      csvCount: files.length
    };
  } catch {
    return { signature: "", csvCount: 0 };
  }
}

function nowLocal() {
  return new Date().toLocaleString("en-US", { timeZone: "America/New_York" });
}

export async function runWatchFidelity() {
  const root = process.cwd();
  const rawDir = path.join(root, "imports", "fidelity", "raw");
  const positionsDir = path.join(root, "imports", "fidelity", "positions");
  const intervalMs = Number(process.env.FIDELITY_WATCH_INTERVAL_MS ?? "30000");

  await fs.mkdir(rawDir, { recursive: true });
  await fs.mkdir(positionsDir, { recursive: true });

  let rawState = await readCsvSnapshot(rawDir);
  let posState = await readCsvSnapshot(positionsDir);
  let running = false;
  let pending = false;

  console.log(
    `[${nowLocal()}] Watching Fidelity folders:\n- ${rawDir}\n- ${positionsDir}\nPolling every ${Math.round(intervalMs / 1000)}s`
  );

  const runPipeline = async (txChanged: boolean, posChanged: boolean) => {
    if (running) {
      pending = true;
      return;
    }
    running = true;
    pending = false;

    try {
      if (txChanged) {
        console.log(`[${nowLocal()}] Detected transaction CSV change. Running import-fidelity...`);
        const result = await runImportFidelity();
        console.log(JSON.stringify({ importFidelity: result }, null, 2));
      }

      if (posChanged) {
        console.log(
          `[${nowLocal()}] Detected positions CSV change. Running import-fidelity-positions...`
        );
        const result = await runImportFidelityPositions();
        console.log(JSON.stringify({ importFidelityPositions: result }, null, 2));
      }

      if (txChanged || posChanged) {
        const summary = await runRebuildDailySummary();
        console.log(JSON.stringify({ rebuildDailySummary: summary }, null, 2));
      }
    } catch (error) {
      console.error(`[${nowLocal()}] Fidelity watch pipeline failed`, error);
    } finally {
      rawState = await readCsvSnapshot(rawDir);
      posState = await readCsvSnapshot(positionsDir);
      running = false;
      if (pending) {
        await runPipeline(true, true);
      }
    }
  };

  setInterval(async () => {
    const [newRaw, newPos] = await Promise.all([
      readCsvSnapshot(rawDir),
      readCsvSnapshot(positionsDir)
    ]);

    const txChanged = newRaw.signature !== rawState.signature && newRaw.csvCount > 0;
    const posChanged = newPos.signature !== posState.signature && newPos.csvCount > 0;

    rawState = newRaw;
    posState = newPos;

    if (txChanged || posChanged) {
      await runPipeline(txChanged, posChanged);
    }
  }, intervalMs);
}
