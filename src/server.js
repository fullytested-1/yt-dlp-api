import express from "express";
import { spawn } from "node:child_process";
import { mkdtemp, rm, stat, readdir, copyFile } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";

const app = express();
const PORT = Number(process.env.PORT || 8000);
const MAX_AGE_MS = Number(process.env.MAX_DOWNLOAD_AGE_MS || 30 * 60 * 1000);
const MAX_FILE_BYTES = Number(process.env.MAX_FILE_BYTES || 500 * 1024 * 1024);
const CONCURRENCY = Math.max(1, Number(process.env.DOWNLOAD_CONCURRENCY || 2));
const BASE_URL = (process.env.PUBLIC_BASE_URL || "").replace(/\/$/, "");

let activeDownloads = 0;
const queue = [];
const files = new Map();

app.disable("x-powered-by");

function validUrl(value) {
  try {
    const u = new URL(value);
    return ["http:", "https:"].includes(u.protocol);
  } catch { return false; }
}

const COOKIES_FILE = process.env.YTDLP_COOKIES_FILE || "";
const PROXY_API_URL = "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=json";
const PROXY_POOL_SIZE = 50;

async function getProxyPool() {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 8000);
  try {
    const res = await fetch(PROXY_API_URL, { signal: controller.signal });
    const data = await res.json();
    return (data.proxies || [])
      .filter(p => p.alive && p.proxy)
      .sort((a, b) => (a.timeout ?? Infinity) - (b.timeout ?? Infinity))
      .slice(0, PROXY_POOL_SIZE)
      .map(p => p.proxy);
  } catch {
    return [];
  } finally {
    clearTimeout(timer);
  }
}

function baseYtArgs(url, { client, proxy }) {
  const args = [];
  const isYouTube = /youtu\.?be/i.test(url);
  if (isYouTube) {
    if (client) args.push("--extractor-args", `youtube:player_client=${client}`);
    args.push("--user-agent", "com.google.android.youtube/19.09.37 (Linux; U; Android 11) gzip");
  }
  if (COOKIES_FILE) args.push("--cookies", COOKIES_FILE);
  if (proxy) args.push("--proxy", proxy);
  return args;
}

function spawnYtDlp(args, timeoutMs) {
  let settled = false;
  const child = spawn("yt-dlp", args, { stdio: ["ignore", "pipe", "pipe"] });
  let stdout = "", stderr = "";

  const timer = setTimeout(() => {
    if (settled) return;
    settled = true;
    child.kill("SIGKILL");
  }, timeoutMs);

  const promise = new Promise((resolve, reject) => {
    child.stdout.on("data", d => { stdout += d.toString(); });
    child.stderr.on("data", d => { stderr += d.toString(); });
    child.on("error", err => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(err);
    });
    child.on("close", code => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      code === 0
        ? resolve({ stdout, stderr })
        : reject(new Error(stderr.trim() || `yt-dlp exited with code ${code}`));
    });
  });

  const cancel = () => {
    if (settled) return;
    settled = true;
    clearTimeout(timer);
    try { child.kill("SIGKILL"); } catch {}
  };

  return { promise, cancel };
}

async function runYtDlpWithRotation(url, buildArgs, { maxAttempts = 8, timeoutMs = 60000, batchSize = 3 } = {}) {
  const isYouTube = /youtu\.?be/i.test(url);
  const clients = isYouTube ? ["android", "ios"] : [null];
  const pool = isYouTube ? await getProxyPool() : [];
  const proxyOptions = isYouTube ? [null, ...pool] : [null];

  const candidates = [];
  outer:
  for (const client of clients) {
    for (const proxy of proxyOptions) {
      candidates.push({ client, proxy });
      if (candidates.length >= maxAttempts) break outer;
    }
  }

  let lastError;
  for (let i = 0; i < candidates.length; i += batchSize) {
    const batch = candidates.slice(i, i + batchSize);
    const runners = batch.map(({ client, proxy }) => {
      const bypassArgs = baseYtArgs(url, { client, proxy });
      const runner = spawnYtDlp(buildArgs(bypassArgs), timeoutMs);
      // Catch unhandled rejection when cancelled
      runner.promise.catch(() => {});
      return runner;
    });

    try {
      const result = await Promise.any(runners.map(r => r.promise));
      runners.forEach(r => r.cancel());
      return result;
    } catch (aggErr) {
      lastError = aggErr?.errors?.length ? aggErr.errors[aggErr.errors.length - 1] : aggErr;
      runners.forEach(r => r.cancel());
    }
  }
  throw lastError || new Error("All proxy attempts failed.");
}

function withDownloadSlot(task) {
  return new Promise((resolve, reject) => {
    const run = async () => {
      activeDownloads++;
      try { resolve(await task()); }
      catch (e) { reject(e); }
      finally {
        activeDownloads--;
        const next = queue.shift();
        if (next) next();
      }
    };
    if (activeDownloads < CONCURRENCY) run();
    else queue.push(run);
  });
}

function withOverallTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms / 1000}s`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

async function getInfo(url) {
  const { stdout } = await withOverallTimeout(
    runYtDlpWithRotation(url, (bypassArgs) => [
      "--dump-single-json", "--no-playlist", "--no-warnings",
      ...bypassArgs, url
    ], { timeoutMs: 20000 }),
    30000,
    "info request"
  );
  return JSON.parse(stdout);
}

app.get("/", (_req, res) => res.json({
  status: true,
  name: "yt-dlp-api",
  version: "1.2.1",
  concurrency: CONCURRENCY,
  endpoints: {
    info: "/api/info?url=URL",
    download: "/api/download?url=URL&format=mp4",
    audio: "/api/download?url=URL&format=mp3"
  }
}));

app.get("/health", (_req, res) => res.json({
  status: true,
  service: "yt-dlp-api",
  activeDownloads,
  queuedDownloads: queue.length,
  storedFiles: files.size
}));

app.get("/api/info", async (req, res) => {
  const url = String(req.query.url || "");
  if (!validUrl(url)) return res.status(400).json({
    status:false, error:"A valid http/https URL is required."
  });
  try {
    const info = await getInfo(url);
    res.json({ status:true, result:{
      id:info.id, title:info.title, description:info.description,
      uploader:info.uploader, channel:info.channel, duration:info.duration,
      thumbnail:info.thumbnail, webpage_url:info.webpage_url,
      upload_date:info.upload_date, view_count:info.view_count,
      formats:(info.formats || []).map(f=>({
        format_id:f.format_id, ext:f.ext, resolution:f.resolution,
        width:f.width, height:f.height, fps:f.fps, filesize:f.filesize,
        vcodec:f.vcodec, acodec:f.acodec
      }))
    }});
  } catch (error) {
    res.status(500).json({ status:false, error:error.message });
  }
});

app.get("/api/download", async (req, res) => {
  const url = String(req.query.url || "");
  const format = String(req.query.format || "mp4").toLowerCase();

  if (!validUrl(url)) return res.status(400).json({
    status:false, error:"A valid http/https URL is required."
  });
  if (!["mp4","mp3"].includes(format)) return res.status(400).json({
    status:false, error:"format must be mp4 or mp3"
  });

  try {
    const result = await withDownloadSlot(async () => {
      const workDir = await mkdtemp(join(tmpdir(), "ytdlp-"));
      const output = join(workDir, `${randomUUID()}.%(ext)s`);

      try {
        const args = [
          "--no-playlist", "--no-warnings", "--restrict-filenames",
          "--retries", "3", "--fragment-retries", "3", "-o", output
        ];

        if (format === "mp3") {
          args.push("-x", "--audio-format", "mp3", "--audio-quality", "0");
        } else {
          args.push(
            "-f", "bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/b",
            "--merge-output-format", "mp4"
          );
        }

        await withOverallTimeout(
          runYtDlpWithRotation(url, (bypassArgs) => [...args, ...bypassArgs, url], { timeoutMs: 90000, batchSize: 3 }),
          120000,
          "download request"
        );

        const names = await readdir(workDir);
        const candidates = names.filter(n =>
          !n.endsWith(".part") && !n.endsWith(".ytdl") && !n.endsWith(".json")
        );
        if (!candidates.length) throw new Error("No output file was produced.");

        const filePath = join(workDir, candidates[0]);
        const fileStat = await stat(filePath);
        if (fileStat.size > MAX_FILE_BYTES) {
          throw new Error("Generated file is larger than the configured limit.");
        }

        const id = randomUUID();
        const publicDir = await mkdtemp(join(tmpdir(), "ytdlp-public-"));
        const publicPath = join(publicDir, `download.${format}`);
        await copyFile(filePath, publicPath);
        await rm(workDir, { recursive:true, force:true }).catch(() => {});

        const expiresAt = Date.now() + MAX_AGE_MS;
        files.set(id, { path: publicPath, dir: publicDir, expiresAt, format });

        setTimeout(async () => {
          const item = files.get(id);
          if (!item) return;
          files.delete(id);
          await rm(item.dir, { recursive:true, force:true }).catch(() => {});
        }, MAX_AGE_MS).unref?.();

        return { id, size:fileStat.size, expiresAt };
      } catch (error) {
        await rm(workDir, { recursive:true, force:true }).catch(() => {});
        throw error;
      }
    });

    const host = BASE_URL || `${req.protocol}://${req.get("host")}`;
    res.json({
      status: true,
      format,
      size: result.size,
      expires_at: new Date(result.expiresAt).toISOString(),
      download: `${host}/files/${result.id}`
    });
  } catch (error) {
    res.status(500).json({ status:false, error:error.message });
  }
});

app.get("/files/:id", async (req, res) => {
  const item = files.get(req.params.id);
  if (!item || item.expiresAt <= Date.now()) {
    if (item) {
      files.delete(req.params.id);
      await rm(item.dir, { recursive:true, force:true }).catch(() => {});
    }
    return res.status(404).json({ status:false, error:"Download link expired or not found." });
  }

  try {
    const fileStat = await stat(item.path);
    res.setHeader("Content-Type", item.format === "mp3" ? "audio/mpeg" : "video/mp4");
    res.setHeader("Content-Disposition", `attachment; filename="download.${item.format}"`);
    res.setHeader("Content-Length", fileStat.size);
    res.setHeader("Cache-Control", "no-store");
    createReadStream(item.path).on("error", () => {
      if (!res.headersSent) res.status(500).end();
    }).pipe(res);
  } catch {
    files.delete(req.params.id);
    await rm(item.dir, { recursive:true, force:true }).catch(() => {});
    res.status(404).json({ status:false, error:"File is no longer available." });
  }
});

app.listen(PORT, () =>
  console.log(`yt-dlp API 1.2.1 listening on port ${PORT} (concurrency=${CONCURRENCY})`)
);
