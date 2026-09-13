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

// Cloud/datacenter IPs get flagged by YouTube's bot-check far more than
// residential IPs. Spoofing the Android/iOS client player and routing
// through a residential proxy avoids that check in most cases.
const COOKIES_FILE = process.env.YTDLP_COOKIES_FILE || "";

// Webshare residential proxy pool (ip:port:user:pass). A proxy that gets
// bot-checked or times out is skipped in favor of the next one.
const PROXIES = [
  "31.59.20.176:6754:qszxoitg:5s0blp1oufg9",
  "45.38.107.97:6014:qszxoitg:5s0blp1oufg9",
  "198.105.121.200:6462:qszxoitg:5s0blp1oufg9",
  "64.137.96.74:6641:qszxoitg:5s0blp1oufg9",
  "198.23.243.226:6361:qszxoitg:5s0blp1oufg9",
  "38.154.185.97:6370:qszxoitg:5s0blp1oufg9",
  "84.247.60.125:6095:qszxoitg:5s0blp1oufg9",
  "142.111.67.146:5611:qszxoitg:5s0blp1oufg9",
  "191.96.254.138:6185:qszxoitg:5s0blp1oufg9",
  "31.58.9.4:6077:qszxoitg:5s0blp1oufg9"
].map(line => {
  const [host, port, user, pass] = line.split(":");
  return `http://${user}:${pass}@${host}:${port}`;
});

function shuffledProxies() {
  const arr = [...PROXIES];
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

function baseYtArgs(url, { client, proxy }) {
  const args = [];
  const isYouTube = /youtu\.?be/i.test(url);
  if (isYouTube) {
    args.push("--extractor-args", `youtube:player_client=${client}`);
    args.push("--user-agent", "com.google.android.youtube/19.09.37 (Linux; U; Android 11) gzip");
  }
  if (COOKIES_FILE) args.push("--cookies", COOKIES_FILE);
  if (proxy) args.push("--proxy", proxy);
  return args;
}

// Tries the request across a shuffled proxy pool (and android/ios clients),
// stopping at the first success. Only retries on bot-check-style errors;
// any other error (bad URL, private video, etc.) is thrown immediately.
async function runYtDlpWithRotation(url, buildArgs, { maxAttempts = 5 } = {}) {
  const isYouTube = /youtu\.?be/i.test(url);
  const clients = isYouTube ? ["android", "ios"] : [null];
  const proxyOptions = isYouTube ? [...shuffledProxies(), null] : [null];

  let lastError;
  let attempts = 0;
  for (const client of clients) {
    for (const proxy of proxyOptions) {
      if (attempts >= maxAttempts) break;
      attempts++;
      const bypassArgs = client ? baseYtArgs(url, { client, proxy }) : (proxy ? ["--proxy", proxy] : []);
      try {
        return await runYtDlp(buildArgs(bypassArgs));
      } catch (err) {
        lastError = err;
        const looksLikeBotCheck = /sign in to confirm|not a bot|http error 429/i.test(err.message || "");
        if (!looksLikeBotCheck) throw err; // real error, no point retrying
      }
    }
  }
  throw lastError;
}

function runYtDlp(args) {
  return new Promise((resolve, reject) => {
    const child = spawn("yt-dlp", args, { stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "", stderr = "";
    child.stdout.on("data", d => { stdout += d.toString(); });
    child.stderr.on("data", d => { stderr += d.toString(); });
    child.on("error", reject);
    child.on("close", code => code === 0
      ? resolve({ stdout, stderr })
      : reject(new Error(stderr.trim() || `yt-dlp exited with code ${code}`)));
  });
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

async function getInfo(url) {
  const { stdout } = await runYtDlpWithRotation(url, (bypassArgs) => [
    "--dump-single-json", "--no-playlist", "--no-warnings",
    ...bypassArgs, url
  ]);
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

        await runYtDlpWithRotation(url, (bypassArgs) => [...args, ...bypassArgs, url]);

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

app.listen(PORT, "0.0.0.0", () =>
  console.log(`yt-dlp API 1.2.1 listening on port ${PORT} (concurrency=${CONCURRENCY})`)
);
