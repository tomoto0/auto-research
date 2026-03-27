import "dotenv/config";
import express from "express";
import { createServer } from "http";
import net from "net";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import { createExpressMiddleware } from "@trpc/server/adapters/express";
import { registerOAuthRoutes } from "./oauth";
import { appRouter, subscribeToRun, getRunEvents } from "../routers";
import { createContext } from "./context";
import { serveStatic, setupVite } from "./vite";
import { cleanupStaleRuns } from "../startup-cleanup";
import { getArtifactsForRun, insertDatasetFile } from "../db";
import archiver from "archiver";
import { storagePut, storageGet, storageDownload } from "../storage";
import { nanoid } from "nanoid";

function isPortAvailable(port: number): Promise<boolean> {
  return new Promise(resolve => {
    const server = net.createServer();
    server.listen(port, () => {
      server.close(() => resolve(true));
    });
    server.on("error", () => resolve(false));
  });
}

async function findAvailablePort(startPort: number = 3000): Promise<number> {
  for (let port = startPort; port < startPort + 20; port++) {
    if (await isPortAvailable(port)) {
      return port;
    }
  }
  throw new Error(`No available port found starting from ${startPort}`);
}

async function startServer() {
  const app = express();
  const server = createServer(app);
  app.use(express.json({ limit: "250mb" }));
  app.use(express.urlencoded({ limit: "250mb", extended: true }));

  // OAuth callback
  registerOAuthRoutes(app);

  // ─── Download proxy: forces Content-Disposition attachment ───
  app.get("/api/download/artifact/:artifactId", async (req, res) => {
    try {
      const { artifactId } = req.params;
      const { getDb } = await import("../db");
      const db = await getDb();
      if (!db) { res.status(500).json({ error: "Database unavailable" }); return; }
      const { artifacts: artifactsTable } = await import("../../drizzle/schema");
      const { eq } = await import("drizzle-orm");
      const rows = await db.select().from(artifactsTable).where(eq(artifactsTable.id, parseInt(artifactId))).limit(1);
      if (!rows.length) { res.status(404).json({ error: "Artifact not found" }); return; }
      const artifact = rows[0];
      if (!artifact.fileUrl) { res.status(404).json({ error: "No file URL" }); return; }

      // Fetch the file from S3
      const fileResp = await fetch(artifact.fileUrl);
      if (!fileResp.ok) { res.status(502).json({ error: "Failed to fetch file" }); return; }

      const buffer = Buffer.from(await fileResp.arrayBuffer());
      const fileName = artifact.fileName || "download";
      res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);
      res.setHeader("Content-Type", artifact.mimeType || "application/octet-stream");
      res.setHeader("Content-Length", buffer.length.toString());
      res.send(buffer);
    } catch (err: any) {
      console.error("[Download] Error:", err);
      res.status(500).json({ error: err?.message || "Download failed" });
    }
  });

  // ─── ZIP download: bundle all artifacts for a run ───
  app.get("/api/download/zip/:runId", async (req, res) => {
    try {
      const { runId } = req.params;
      const artifactsList = await getArtifactsForRun(runId);
      if (!artifactsList.length) { res.status(404).json({ error: "No artifacts found" }); return; }

      res.setHeader("Content-Disposition", `attachment; filename="${runId}-artifacts.zip"`);
      res.setHeader("Content-Type", "application/zip");

      const archive = archiver("zip", { zlib: { level: 6 } });
      archive.on("error", (err: any) => { throw err; });
      archive.pipe(res);

      // Fetch each artifact and add to ZIP
      for (const artifact of artifactsList) {
        if (!artifact.fileUrl) continue;
        try {
          const fileResp = await fetch(artifact.fileUrl);
          if (!fileResp.ok) continue;
          const buffer = Buffer.from(await fileResp.arrayBuffer());
          archive.append(buffer, { name: artifact.fileName || `artifact-${artifact.id}` });
        } catch (e) {
          console.warn(`[ZIP] Failed to fetch artifact ${artifact.id}:`, e);
        }
      }

      await archive.finalize();
    } catch (err: any) {
      console.error("[ZIP] Error:", err);
      if (!res.headersSent) {
        res.status(500).json({ error: err?.message || "ZIP generation failed" });
      }
    }
  });

  // ─── Chunked S3 Proxy Upload ───
  // Browser splits file into 8MB chunks, each chunk is uploaded to S3 via server proxy.
  // Server streams each chunk directly to forge API (no memory accumulation).
  // After all chunks uploaded, browser calls /api/upload/register to save metadata.

  // Upload a single chunk to S3 via server proxy (streaming, no buffering)
  app.post("/api/upload/s3chunk", async (req, res) => {
    req.setTimeout(120000); // 2 min per chunk
    res.setTimeout(120000);
    try {
      const fileKey = req.headers["x-file-key"] as string;
      const fileMime = req.headers["x-file-mime"] as string || "application/octet-stream";
      if (!fileKey) {
        res.status(400).json({ error: "Missing x-file-key header" });
        return;
      }

      // Collect chunk data (max ~8MB per chunk, well within proxy limits)
      const chunks: Buffer[] = [];
      let totalSize = 0;
      const MAX_CHUNK_SIZE = 10 * 1024 * 1024; // 10MB safety limit per chunk
      for await (const chunk of req) {
        const buf = typeof chunk === "string" ? Buffer.from(chunk) : chunk;
        totalSize += buf.length;
        if (totalSize > MAX_CHUNK_SIZE) {
          res.status(413).json({ error: "Chunk too large (max 10MB per chunk)" });
          return;
        }
        chunks.push(buf);
      }
      const chunkBuffer = Buffer.concat(chunks);

      // Upload to S3 via forge API using backend key
      const { url } = await storagePut(fileKey, chunkBuffer, fileMime);

      res.json({ success: true, url, fileKey, sizeBytes: chunkBuffer.length });
    } catch (err: any) {
      console.error("[Upload] S3 chunk proxy error:", err);
      res.status(500).json({ error: err?.message || "Chunk upload failed" });
    }
  });

  // Assemble multi-chunk uploads: download parts from S3, concatenate, re-upload as single file
  app.post("/api/upload/assemble", express.json({ limit: "1mb" }), async (req, res) => {
    req.setTimeout(600000); // 10 min for large file assembly
    res.setTimeout(600000);
    const tmpDir = path.join(os.tmpdir(), `assemble-${Date.now()}`);
    try {
      const { uploadId, fileName, fileMime, totalChunks, totalSize } = req.body;
      if (!uploadId || !fileName || !totalChunks) {
        res.status(400).json({ error: "Missing required fields" });
        return;
      }

      console.log(`[Upload] Assembling ${totalChunks} chunks for ${fileName} (${(totalSize / 1024 / 1024).toFixed(1)}MB)`);

      // Create temp directory for streaming assembly
      fs.mkdirSync(tmpDir, { recursive: true });
      const assembledPath = path.join(tmpDir, "assembled");
      const writeStream = fs.createWriteStream(assembledPath);

      // Download each chunk and stream to disk with retry logic and backpressure
      const { Readable } = await import("stream");

      for (let i = 0; i < totalChunks; i++) {
        const partKey = `datasets/${uploadId}/parts/${String(i).padStart(4, "0")}`;
        try {
          // storageDownload includes retry logic and timeout
          const resp = await storageDownload(partKey, { timeoutMs: 120000 });
          if (!resp.body) {
            writeStream.end();
            res.status(500).json({ error: `Empty response body for part ${i}` });
            return;
          }
          const readable = Readable.fromWeb(resp.body as any);
          // Use pipeline for proper backpressure handling (won't overwhelm writeStream)
          await new Promise<void>((resolve, reject) => {
            readable.on("data", (chunk: Buffer) => {
              const canContinue = writeStream.write(chunk);
              if (!canContinue) {
                readable.pause();
                writeStream.once("drain", () => readable.resume());
              }
            });
            readable.on("end", resolve);
            readable.on("error", reject);
          });
        } catch (partErr: any) {
          writeStream.end();
          console.error(`[Upload] Failed to download part ${i}:`, partErr.message);
          res.status(500).json({ error: `Failed to download part ${i}: ${partErr.message}` });
          return;
        }
        console.log(`[Upload] Streamed part ${i + 1}/${totalChunks} to disk`);
      }

      // Close write stream
      await new Promise<void>((resolve) => writeStream.end(resolve));

      const assembledSize = fs.statSync(assembledPath).size;
      console.log(`[Upload] Assembled ${assembledSize} bytes on disk for ${fileName}`);

      // Upload assembled file to S3
      // For files > 100MB, read in chunks to reduce peak memory
      const finalKey = `datasets/${uploadId}/${fileName}`;
      const assembledBuffer = fs.readFileSync(assembledPath);
      const { url: finalUrl } = await storagePut(finalKey, assembledBuffer, fileMime || "application/octet-stream");

      // Clean up temp files
      try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}

      res.json({
        success: true,
        fileUrl: finalUrl,
        fileKey: finalKey,
        sizeBytes: assembledSize,
      });
    } catch (err: any) {
      console.error("[Upload] Assemble error:", err);
      // Clean up temp files on error
      try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}
      res.status(500).json({ error: err?.message || "Assembly failed" });
    }
  });

  // Register endpoint: after all chunks uploaded to S3, browser calls this to
  // save metadata to DB and parse a preview. Only receives a small JSON payload.
  app.post("/api/upload/register", express.json({ limit: "1mb" }), async (req, res) => {
    req.setTimeout(300000); // 5 min for DTA parsing
    res.setTimeout(300000);
    try {
      const { fileName, fileMime, fileKey, fileUrl, sizeBytes, userId } = req.body;
      if (!fileName || !fileKey || !fileUrl) {
        res.status(400).json({ error: "Missing required fields: fileName, fileKey, fileUrl" });
        return;
      }

      const ext = fileName.split(".").pop()?.toLowerCase() || "";
      let fileType: "csv" | "excel" | "dta" | "json" | "tsv" | "other" = "other";
      if (ext === "csv") fileType = "csv";
      else if (["xlsx", "xls"].includes(ext)) fileType = "excel";
      else if (ext === "dta") fileType = "dta";
      else if (ext === "json") fileType = "json";
      else if (ext === "tsv") fileType = "tsv";

      console.log(`[Upload] Registering S3 file: ${fileName} (${fileType}, ${(sizeBytes / 1024 / 1024).toFixed(1)}MB)`);

      // Parse preview by downloading only what we need from S3
      let columnNames: string[] | null = null;
      let rowCount: number | null = null;
      let preview: string | null = null;

      try {
        // For preview parsing, download the file from S3
        // Always get a fresh download URL via storageGet (passed fileUrl may have expired)
        const needsFullFile = fileType === "dta" || fileType === "excel";

        if (fileType === "csv" || fileType === "tsv") {
          // Only fetch first 64KB for CSV/TSV preview
          const resp = await storageDownload(fileKey, {
            timeoutMs: 30000,
            rangeHeader: "bytes=0-65535",
          });
          const previewBuf = Buffer.from(await resp.arrayBuffer());
          let text: string;
          try {
            const iconv = await import("iconv-lite");
            const chardet = await import("chardet");
            const detected = chardet.default.detect(previewBuf);
            const encodingMap: Record<string, string> = {
              "utf8": "utf-8", "ascii": "utf-8", "shiftjis": "Shift_JIS",
              "eucjp": "EUC-JP", "iso2022jp": "ISO-2022-JP", "windows1252": "windows-1252",
            };
            const normalised = (detected || "").toLowerCase().replace(/[^a-z0-9]/g, "");
            const iconvEncoding = encodingMap[normalised] || detected || "utf-8";
            text = iconv.default.decode(previewBuf, iconvEncoding);
            if (text.includes("\uFFFD") && iconvEncoding === "utf-8") {
              text = iconv.default.decode(previewBuf, "Shift_JIS");
            }
          } catch {
            text = previewBuf.toString("utf-8");
          }
          if (text.charCodeAt(0) === 0xFEFF) text = text.slice(1);
          const sep = fileType === "tsv" ? "\t" : ",";
          const lines = text.split("\n").filter(l => l.trim());
          if (lines.length > 0) {
            columnNames = lines[0].split(sep).map(c => c.trim().replace(/^"|"$/g, ""));
            // Estimate row count from file size and average line length
            const avgLineLen = previewBuf.length / Math.max(lines.length, 1);
            rowCount = Math.max(0, Math.round(sizeBytes / avgLineLen) - 1);
            preview = lines.slice(0, 6).join("\n");
          }
        } else if (fileType === "json") {
          // Fetch first 64KB for JSON preview
          const resp = await storageDownload(fileKey, {
            timeoutMs: 30000,
            rangeHeader: "bytes=0-65535",
          });
          const partial = await resp.text();
          try {
            // Try to parse as complete JSON first
            const parsed = JSON.parse(partial);
            if (Array.isArray(parsed) && parsed.length > 0) {
              columnNames = Object.keys(parsed[0]);
              rowCount = parsed.length;
              preview = JSON.stringify(parsed.slice(0, 3), null, 2);
            }
          } catch {
            // Partial JSON - try to extract first few objects
            const match = partial.match(/\[\s*\{/);
            if (match) {
              const objMatches = partial.match(/\{[^{}]+\}/g);
              if (objMatches && objMatches.length > 0) {
                try {
                  const firstObj = JSON.parse(objMatches[0]);
                  columnNames = Object.keys(firstObj);
                  preview = objMatches.slice(0, 3).join(",\n");
                } catch {}
              }
            }
          }
        } else if (needsFullFile) {
          // DTA and Excel need full file download for parsing
          // Stream to temp file to avoid holding entire file in memory
          const tmpPath = path.join(os.tmpdir(), `preview-${Date.now()}-${fileName}`);
          try {
            // Use storageDownload with retry logic and 3-min timeout for large files
            const resp = await storageDownload(fileKey, { timeoutMs: 180000 });
            if (resp.body) {
              const { Readable } = await import("stream");
              const { pipeline: streamPipeline } = await import("stream/promises");
              const tmpStream = fs.createWriteStream(tmpPath);
              const readable = Readable.fromWeb(resp.body as any);
              await streamPipeline(readable, tmpStream);
            } else {
              const buf = Buffer.from(await resp.arrayBuffer());
              fs.writeFileSync(tmpPath, buf);
            }
          } catch (dlErr: any) {
            console.warn("[Upload] Failed to download for preview:", dlErr.message);
            try { fs.unlinkSync(tmpPath); } catch {}
          }
          const fileBuffer = fs.existsSync(tmpPath) ? fs.readFileSync(tmpPath) : null;
          // Clean up temp file after parsing
          const cleanupTmp = () => { try { fs.unlinkSync(tmpPath); } catch {} };

          if (fileBuffer) {
            if (fileType === "excel") {
              try {
                const XLSX = await import("xlsx");
                const workbook = XLSX.read(fileBuffer, { type: "buffer" });
                const sheetName = workbook.SheetNames[0];
                if (sheetName) {
                  const sheet = workbook.Sheets[sheetName];
                  const jsonData = XLSX.utils.sheet_to_json(sheet, { header: 1 }) as any[][];
                  if (jsonData.length > 0) {
                    columnNames = jsonData[0].map((c: any) => String(c ?? "").trim());
                    rowCount = jsonData.length - 1;
                    preview = jsonData.slice(0, 6).map(r => r.join(",")).join("\n");
                  }
                }
              } catch (xlsxErr: any) {
                console.warn("[Upload] Excel parse for preview failed:", xlsxErr.message);
              }
            } else if (fileType === "dta") {
              try {
                const { parseDtaFile } = await import("../dta-parser");
                const dtaResult = parseDtaFile(fileBuffer, { previewRows: 10 });
                if (dtaResult && dtaResult.columns) {
                  columnNames = dtaResult.columns;
                  rowCount = dtaResult.totalRows || dtaResult.data?.length || null;
                  if (dtaResult.data && dtaResult.data.length > 0) {
                    const previewRows = dtaResult.data.slice(0, 5);
                    preview = columnNames.join(",") + "\n" + previewRows.map((r: any) => columnNames!.map(c => r[c] ?? "").join(",")).join("\n");
                  }
                }
              } catch (dtaErr: any) {
                console.warn("[Upload] DTA parse for preview failed:", dtaErr.message);
              }
            }
          }
          cleanupTmp();
        }
      } catch (parseErr: any) {
        console.warn("[Upload] Preview parsing failed (non-fatal):", parseErr.message);
        // Preview parsing failure is non-fatal - file is already in S3
      }

      // Validate parsed data before saving
      const warnings: string[] = [];
      if ((!columnNames || columnNames.length === 0) && (rowCount === 0 || rowCount === null)) {
        res.status(422).json({ error: "File appears to be empty or unparseable: no columns or rows detected" });
        return;
      }
      if (rowCount === 0) {
        res.status(422).json({ error: "File contains column headers but no data rows" });
        return;
      }
      if (rowCount === 1) {
        warnings.push("Dataset contains only a single row — statistical analysis may be limited");
      }
      if (columnNames && columnNames.length > 0) {
        const uniqueNames = new Set(columnNames);
        if (uniqueNames.size < columnNames.length) {
          warnings.push(`Duplicate column names detected (${columnNames.length - uniqueNames.size} duplicates)`);
        }
        const controlCharCols = columnNames.filter(c => /[\x00-\x1f\x7f]/.test(c));
        if (controlCharCols.length > 0) {
          warnings.push(`${controlCharCols.length} column name(s) contain control characters`);
        }
      }
      if (warnings.length > 0) {
        console.warn(`[Upload] Validation warnings for ${fileName}:`, warnings);
      }

      // Save to DB
      const record = await insertDatasetFile({
        userId: userId ? parseInt(userId) : null,
        originalName: fileName,
        fileKey,
        fileUrl,
        mimeType: fileMime || "application/octet-stream",
        sizeBytes: sizeBytes || 0,
        fileType,
        columnNames: columnNames as any,
        rowCount,
        preview,
      });

      console.log(`[Upload] Registered: ${fileName} (id=${record.id}, ${columnNames?.length || 0} cols, ${rowCount ?? "?"} rows)`);

      res.json({
        success: true,
        ...(warnings.length > 0 ? { warnings } : {}),
        file: {
          id: record.id,
          originalName: fileName,
          fileUrl,
          fileType,
          sizeBytes: sizeBytes || 0,
          columnNames,
          rowCount,
          preview,
        },
      });
    } catch (err: any) {
      console.error("[Upload] Register error:", err);
      res.status(500).json({ error: err?.message || "Registration failed" });
    }
  });

  // Legacy single-request upload (for small files < 10MB)
  app.post("/api/upload/dataset", async (req, res) => {
    req.setTimeout(600000);
    res.setTimeout(600000);
    try {
      const fileName = decodeURIComponent(req.headers["x-file-name"] as string || "dataset");
      const fileMime = req.headers["x-file-mime"] as string || "application/octet-stream";
      const userId = req.headers["x-user-id"] as string;

      const chunks: Buffer[] = [];
      let totalSize = 0;
      const MAX_FILE_SIZE = 250 * 1024 * 1024;
      for await (const chunk of req) {
        const buf = typeof chunk === "string" ? Buffer.from(chunk) : chunk;
        totalSize += buf.length;
        if (totalSize > MAX_FILE_SIZE) {
          res.status(413).json({ error: "File too large (max 250MB)" });
          return;
        }
        chunks.push(buf);
      }
      const fileBuffer = Buffer.concat(chunks);

      if (fileBuffer.length === 0) {
        res.status(400).json({ error: "Empty file" });
        return;
      }

      const ext = fileName.split(".").pop()?.toLowerCase() || "";
      let fileType: "csv" | "excel" | "dta" | "json" | "tsv" | "other" = "other";
      if (ext === "csv") fileType = "csv";
      else if (["xlsx", "xls"].includes(ext)) fileType = "excel";
      else if (ext === "dta") fileType = "dta";
      else if (ext === "json") fileType = "json";
      else if (ext === "tsv") fileType = "tsv";

      const fileKey = `datasets/${nanoid(12)}/${fileName}`;
      const { url } = await storagePut(fileKey, fileBuffer, fileMime);

      let columnNames: string[] | null = null;
      let rowCount: number | null = null;
      let preview: string | null = null;

      if (fileType === "csv" || fileType === "tsv") {
        let text: string;
        try {
          const iconv = await import("iconv-lite");
          const chardet = await import("chardet");
          const detected = chardet.default.detect(fileBuffer);
          const encodingMap: Record<string, string> = {
            "utf8": "utf-8", "ascii": "utf-8", "shiftjis": "Shift_JIS",
            "eucjp": "EUC-JP", "iso2022jp": "ISO-2022-JP", "windows1252": "windows-1252",
          };
          const normalised = (detected || "").toLowerCase().replace(/[^a-z0-9]/g, "");
          const iconvEncoding = encodingMap[normalised] || detected || "utf-8";
          text = iconv.default.decode(fileBuffer, iconvEncoding);
          if (text.includes("\uFFFD") && iconvEncoding === "utf-8") {
            text = iconv.default.decode(fileBuffer, "Shift_JIS");
          }
        } catch {
          text = fileBuffer.toString("utf-8");
        }
        if (text.charCodeAt(0) === 0xFEFF) text = text.slice(1);
        const sep = fileType === "tsv" ? "\t" : ",";
        const lines = text.split("\n").filter(l => l.trim());
        if (lines.length > 0) {
          columnNames = lines[0].split(sep).map(c => c.trim().replace(/^"|"$/g, ""));
          rowCount = lines.length - 1;
          preview = lines.slice(0, 6).join("\n");
        }
      } else if (fileType === "json") {
        try {
          const parsed = JSON.parse(fileBuffer.toString("utf-8"));
          if (Array.isArray(parsed) && parsed.length > 0) {
            columnNames = Object.keys(parsed[0]);
            rowCount = parsed.length;
            preview = JSON.stringify(parsed.slice(0, 3), null, 2);
          }
        } catch {}
      } else if (fileType === "excel") {
        try {
          const XLSX = await import("xlsx");
          const workbook = XLSX.read(fileBuffer, { type: "buffer" });
          const sheetName = workbook.SheetNames[0];
          if (sheetName) {
            const sheet = workbook.Sheets[sheetName];
            const jsonData = XLSX.utils.sheet_to_json(sheet, { header: 1 }) as any[][];
            if (jsonData.length > 0) {
              columnNames = jsonData[0].map((c: any) => String(c ?? "").trim());
              rowCount = jsonData.length - 1;
              preview = jsonData.slice(0, 6).map(r => r.join(",")).join("\n");
            }
          }
        } catch (xlsxErr: any) {
          console.warn("[Upload] Excel parse for preview failed:", xlsxErr.message);
        }
      } else if (fileType === "dta") {
        try {
          const { parseDtaFile } = await import("../dta-parser");
          const dtaResult = parseDtaFile(fileBuffer, { previewRows: 10 });
          if (dtaResult && dtaResult.columns) {
            columnNames = dtaResult.columns;
            rowCount = dtaResult.totalRows || dtaResult.data?.length || null;
            if (dtaResult.data && dtaResult.data.length > 0) {
              const previewRows = dtaResult.data.slice(0, 5);
              preview = columnNames.join(",") + "\n" + previewRows.map((r: any) => columnNames!.map(c => r[c] ?? "").join(",")).join("\n");
            }
          }
        } catch (dtaErr: any) {
          console.warn("[Upload] DTA parse for preview failed:", dtaErr.message);
        }
      }

      const record = await insertDatasetFile({
        userId: userId ? parseInt(userId) : null,
        originalName: fileName,
        fileKey,
        fileUrl: url,
        mimeType: fileMime,
        sizeBytes: fileBuffer.length,
        fileType,
        columnNames: columnNames as any,
        rowCount,
        preview,
      });

      res.json({
        success: true,
        file: {
          id: record.id,
          originalName: fileName,
          fileUrl: url,
          fileType,
          sizeBytes: fileBuffer.length,
          columnNames,
          rowCount,
          preview,
        },
      });
    } catch (err: any) {
      console.error("[Upload] Error:", err);
      res.status(500).json({ error: err?.message || "Upload failed" });
    }
  });

  // SSE endpoint for real-time pipeline events
  app.get("/api/pipeline/events/:runId", (req, res) => {
    const { runId } = req.params;
    const since = parseInt(req.query.since as string) || 0;

    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      "Access-Control-Allow-Origin": "*",
    });

    // Send buffered events
    const buffered = getRunEvents(runId, since);
    for (const event of buffered) {
      res.write(`data: ${JSON.stringify(event)}\n\n`);
    }

    // Subscribe to new events
    const unsubscribe = subscribeToRun(runId, (event) => {
      try {
        res.write(`data: ${JSON.stringify(event)}\n\n`);
      } catch {
        unsubscribe();
      }
    });

    // Heartbeat
    const heartbeat = setInterval(() => {
      try { res.write(":heartbeat\n\n"); } catch { clearInterval(heartbeat); }
    }, 15000);

    req.on("close", () => {
      unsubscribe();
      clearInterval(heartbeat);
    });
  });

  // tRPC API
  app.use(
    "/api/trpc",
    createExpressMiddleware({
      router: appRouter,
      createContext,
    })
  );

  // development mode uses Vite, production mode uses static files
  if (process.env.NODE_ENV === "development") {
    await setupVite(app, server);
  } else {
    serveStatic(app);
  }

  const preferredPort = parseInt(process.env.PORT || "3000");
  const port = await findAvailablePort(preferredPort);

  if (port !== preferredPort) {
    console.log(`Port ${preferredPort} is busy, using port ${port} instead`);
  }

  server.listen(port, async () => {
    console.log(`Server running on http://localhost:${port}/`);
    // Cleanup stale pipeline runs from previous server instance
    try {
      const cleaned = await cleanupStaleRuns();
      if (cleaned > 0) console.log(`[Startup] Cleaned up ${cleaned} stale pipeline run(s)`);
    } catch (e) {
      console.warn("[Startup] Cleanup failed:", e);
    }
  });
}

startServer().catch(console.error);
