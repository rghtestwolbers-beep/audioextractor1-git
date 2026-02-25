// server.js (ESM)
// Audio extractor for n8n pipeline (Drive -> Cloud Run -> GCS signed URL)
// Updates:
// - safer ffmpeg execution (no multiline shell strings)
// - ffprobe duration + size metadata (useful for debugging/limits)
// - optional output format (ogg/mp3/wav) + bitrate controls
// - robust error handling + clear responses
// - basic cleanup of /tmp files

import express from "express";
import fs from "fs";
import path from "path";
import { execFileSync, execSync } from "child_process";
import { google } from "googleapis";
import { Storage } from "@google-cloud/storage";

const app = express();
app.use(express.json({ limit: "10mb" }));

const PORT = process.env.PORT || 8080;
const BUCKET = process.env.BUCKET;

const storage = new Storage();

app.get("/", (_, res) => res.send("ok"));
app.get("/health", (_, res) => res.json({ ok: true }));

function safeMkdir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function safeUnlink(p) {
  try {
    fs.unlinkSync(p);
  } catch {}
}

function safeRmdir(p) {
  try {
    fs.rmSync(p, { recursive: true, force: true });
  } catch {}
}

function ffprobeJson(filePath) {
  const out = execSync(
    `ffprobe -v error -print_format json -show_format -show_streams "${filePath}"`,
    { encoding: "utf8" }
  );
  return JSON.parse(out);
}

function getDurationSeconds(filePath) {
  try {
    const info = ffprobeJson(filePath);
    const d = Number(info?.format?.duration);
    return Number.isFinite(d) ? d : null;
  } catch {
    return null;
  }
}

// 🔹 Download video from Google Drive
async function downloadDriveFile(fileId, outputPath) {
  const auth = new google.auth.GoogleAuth({
    scopes: ["https://www.googleapis.com/auth/drive.readonly"],
  });

  const drive = google.drive({ version: "v3", auth });

  const response = await drive.files.get(
    { fileId, alt: "media" },
    { responseType: "stream" }
  );

  await new Promise((resolve, reject) => {
    const dest = fs.createWriteStream(outputPath);
    response.data.pipe(dest);
    dest.on("finish", resolve);
    dest.on("error", reject);
  });
}

// 🔹 Upload to GCS + signed URL
async function uploadAndSign(localPath, destination) {
  if (!BUCKET) throw new Error("Missing BUCKET env var");

  await storage.bucket(BUCKET).upload(localPath, {
    destination,
    resumable: false,
    validation: false,
  });

  const [url] = await storage.bucket(BUCKET).file(destination).getSignedUrl({
    action: "read",
    expires: Date.now() + 60 * 60 * 1000, // 1 hour
  });

  return url;
}

/**
 * Extract audio from video.
 * Defaults tuned for speech-to-text (small & accurate).
 *
 * Supported formats:
 * - ogg (opus) [default]
 * - mp3
 * - wav (large, but sometimes useful)
 */
function extractAudio({
  inputPath,
  outputPath,
  format = "ogg",
  sampleRate = 16000,
  channels = 1,
  bitrateK = 32, // kbps (opus/mp3)
}) {
  const args = ["-y", "-i", inputPath, "-vn", "-ac", String(channels), "-ar", String(sampleRate)];

  if (format === "ogg") {
    args.push("-c:a", "libopus", "-b:a", `${bitrateK}k`);
  } else if (format === "mp3") {
    args.push("-c:a", "libmp3lame", "-b:a", `${bitrateK}k`);
  } else if (format === "wav") {
    args.push("-c:a", "pcm_s16le");
  } else {
    throw new Error(`Unsupported format: ${format}`);
  }

  args.push(outputPath);

  // Use execFileSync to avoid shell parsing issues
  execFileSync("ffmpeg", args, { stdio: "ignore" });
}

// 🎯 MAIN ENDPOINT
/**
 * POST /extract-audio
 * Body:
 * {
 *   "fileId": "driveFileId",
 *   "format": "ogg" | "mp3" | "wav",      // optional (default ogg)
 *   "bitrateK": 32,                       // optional (default 32)
 *   "sampleRate": 16000,                  // optional (default 16000)
 *   "channels": 1                         // optional (default 1)
 * }
 */
app.post("/extract-audio", async (req, res) => {
  const startedAt = Date.now();
  const { fileId } = req.body || {};

  // Optional tuning
  const format = (req.body?.format || "ogg").toLowerCase();
  const bitrateK = Number(req.body?.bitrateK ?? 32);
  const sampleRate = Number(req.body?.sampleRate ?? 16000);
  const channels = Number(req.body?.channels ?? 1);

  if (!fileId) return res.status(400).json({ error: "Missing fileId" });
  if (!BUCKET) return res.status(500).json({ error: "Missing BUCKET env var" });

  const workDir = `/tmp/${fileId}-audio`;
  const inputPath = path.join(workDir, "input.mp4");
  const audioExt = format === "mp3" ? "mp3" : format === "wav" ? "wav" : "ogg";
  const audioPath = path.join(workDir, `audio.${audioExt}`);

  try {
    safeMkdir(workDir);

    // 1️⃣ Download video
    await downloadDriveFile(fileId, inputPath);

    // Useful debug metadata
    const inputSize = fs.statSync(inputPath).size;
    const durationSec = getDurationSeconds(inputPath);

    // 2️⃣ Extract audio
    extractAudio({
      inputPath,
      outputPath: audioPath,
      format,
      bitrateK: Number.isFinite(bitrateK) && bitrateK > 0 ? bitrateK : 32,
      sampleRate: Number.isFinite(sampleRate) && sampleRate > 0 ? sampleRate : 16000,
      channels: Number.isFinite(channels) && channels > 0 ? channels : 1,
    });

    const audioSize = fs.statSync(audioPath).size;

    // 3️⃣ Upload + signed URL
    const destination = `audio/${fileId}.${audioExt}`;
    const audioUrl = await uploadAndSign(audioPath, destination);

    return res.json({
      fileId,
      audioUrl,
      meta: {
        format,
        inputBytes: inputSize,
        audioBytes: audioSize,
        durationSec,
        elapsedMs: Date.now() - startedAt,
        bucket: BUCKET,
        destination,
      },
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({
      error: err?.message || String(err),
    });
  } finally {
    // Cleanup temp files
    safeRmdir(workDir);
  }
});

app.listen(PORT, () => console.log(`Server running on ${PORT}`));
