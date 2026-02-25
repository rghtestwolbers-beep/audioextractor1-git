import express from "express";
import fs from "fs";
import { execSync } from "child_process";
import { google } from "googleapis";
import { Storage } from "@google-cloud/storage";

const app = express();
app.use(express.json({ limit: "10mb" }));

const PORT = process.env.PORT || 8080;
const BUCKET = process.env.BUCKET;

const storage = new Storage();

app.get("/", (_, res) => res.send("ok"));
app.get("/health", (_, res) => res.json({ ok: true }));

// 🔹 Download video from Google Drive
async function downloadDriveFile(fileId, outputPath) {
  const auth = new google.auth.GoogleAuth({
    scopes: ["https://www.googleapis.com/auth/drive.readonly"]
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
  await storage.bucket(BUCKET).upload(localPath, { destination });

  const [url] = await storage
    .bucket(BUCKET)
    .file(destination)
    .getSignedUrl({
      action: "read",
      expires: Date.now() + 60 * 60 * 1000
    });

  return url;
}

// 🎯 MAIN ENDPOINT
app.post("/extract-audio", async (req, res) => {
  try {
    const { fileId } = req.body;

    if (!fileId) {
      return res.status(400).json({ error: "Missing fileId" });
    }

    if (!BUCKET) {
      return res.status(500).json({ error: "Missing BUCKET env var" });
    }

    const workDir = `/tmp/${fileId}`;
    fs.mkdirSync(workDir, { recursive: true });

    const inputPath = `${workDir}/input.mp4`;
    const audioPath = `${workDir}/audio.ogg`;

    // 1️⃣ Download video
    await downloadDriveFile(fileId, inputPath);

    // 2️⃣ Extract small speech-optimized audio
    execSync(`
      ffmpeg -y -i "${inputPath}" \
      -vn -ac 1 -ar 16000 \
      -c:a libopus -b:a 32k \
      "${audioPath}"
    `);

    // 3️⃣ Upload to Cloud Storage
    const destination = `audio/${fileId}.ogg`;
    const url = await uploadAndSign(audioPath, destination);

    res.json({
      fileId,
      audioUrl: url
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on ${PORT}`);
});
