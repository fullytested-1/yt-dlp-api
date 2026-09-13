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

// Free public proxy pool (ip:port, scheme varies). These are shared by many
// people and go dead often, so we rotate through a larger list and allow
// more attempts before giving up.
const PROXIES = [
  "http://165.101.230.76:8080",
  "http://66.135.27.9:443",
  "http://208.67.28.27:58090",
  "http://38.191.194.43:999",
  "http://123.138.24.112:8800",
  "http://186.227.196.104:3128",
  "http://23.143.160.193:999",
  "socks5://147.45.136.45:1080",
  "http://38.51.216.56:999",
  "socks5://218.95.39.108:59999",
  "socks4://171.236.89.87:1080",
  "socks5://47.237.110.50:1080",
  "http://45.174.56.21:999",
  "http://103.112.163.131:8080",
  "socks5://178.252.165.226:1080",
  "socks5://51.222.13.193:10084",
  "socks5://168.138.219.12:8081",
  "socks5://91.84.98.74:12546",
  "http://178.104.234.144:8118",
  "http://134.249.86.47:8080",
  "http://200.121.48.195:999",
  "http://120.28.169.31:5050",
  "http://103.97.141.40:8080",
  "socks4://38.190.1.70:1085",
  "socks5://38.76.196.46:9050",
  "socks4://88.204.134.234:1080",
  "socks4://154.88.189.21:5678",
  "socks5://94.23.218.74:10808",
  "http://65.109.186.67:10808",
  "http://2.188.210.83:4443"
];

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
// stopping at the first success. Only retries on bot-check/timeout-style
// errors; any other error (bad URL, private video, etc.) is thrown immediately.
async function runYtDlpWithRotation(url, buildArgs, { maxAttempts = 10, timeoutMs = 20000 } = {}) {
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
        return await runYtDlp(buildArgs(bypassArgs), timeoutMs);
      } catch (err) {
        lastError = err;
        const retryable = /sign in to confirm|not a bot|http error 429|timed out|tunnel connection failed|proxy/i.test(err.message || "");
        if (!retryable) throw err; // real error, no point retrying
      }
    }
  }
  throw lastError;
}

function runYtDlp(args, timeoutMs = 20000) {
  return new Promise((resolve, reject) => {
    const child = spawn("yt-dlp", args, { stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "", stderr = "", settled = false;

    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      child.kill("SIGKILL");
      reject(new Error("proxy timed out (not a bot)"));
    }, timeoutMs);

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

        await runYtDlpWithRotation(url, (bypassArgs) => [...args, ...bypassArgs, url], { timeoutMs: 45000 });

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
