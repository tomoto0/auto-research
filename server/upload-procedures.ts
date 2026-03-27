import { z } from "zod";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import { TRPCError } from "@trpc/server";
import { publicProcedure, longRunningProcedure } from "./_core/trpc";
import { storagePut, storageDownload } from "./storage";
import { insertDatasetFile } from "./db";

// ─── Helper: parse file preview from downloaded buffer ───

type FileType = "csv" | "excel" | "dta" | "json" | "tsv" | "other";

function detectFileType(fileName: string): FileType {
  const ext = fileName.split(".").pop()?.toLowerCase() || "";
  if (ext === "csv") return "csv";
  if (["xlsx", "xls"].includes(ext)) return "excel";
  if (ext === "dta") return "dta";
  if (ext === "json") return "json";
  if (ext === "tsv") return "tsv";
  return "other";
}

async function parsePreview(
  fileKey: string,
  fileType: FileType,
  fileName: string,
  sizeBytes: number,
): Promise<{ columnNames: string[] | null; rowCount: number | null; preview: string | null }> {
  let columnNames: string[] | null = null;
  let rowCount: number | null = null;
  let preview: string | null = null;

  if (fileType === "csv" || fileType === "tsv") {
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
      const avgLineLen = previewBuf.length / Math.max(lines.length, 1);
      rowCount = Math.max(0, Math.round(sizeBytes / avgLineLen) - 1);
      preview = lines.slice(0, 6).join("\n");
    }
  } else if (fileType === "json") {
    const resp = await storageDownload(fileKey, {
      timeoutMs: 30000,
      rangeHeader: "bytes=0-65535",
    });
    const partial = await resp.text();
    try {
      const parsed = JSON.parse(partial);
      if (Array.isArray(parsed) && parsed.length > 0) {
        columnNames = Object.keys(parsed[0]);
        rowCount = parsed.length;
        preview = JSON.stringify(parsed.slice(0, 3), null, 2);
      }
    } catch {
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
  } else if (fileType === "dta" || fileType === "excel") {
    const tmpPath = path.join(os.tmpdir(), `preview-${Date.now()}-${fileName}`);
    try {
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
          const { parseDtaFile } = await import("./dta-parser");
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

  return { columnNames, rowCount, preview };
}

// ─── tRPC Procedures ───

export const uploadChunkProcedure = publicProcedure
  .input(z.object({
    fileKey: z.string().min(1),
    fileMime: z.string().default("application/octet-stream"),
    chunkBase64: z.string(),
  }))
  .mutation(async ({ input }) => {
    const chunkBuffer = Buffer.from(input.chunkBase64, "base64");
    const MAX_CHUNK_SIZE = 10 * 1024 * 1024; // 10MB safety limit
    if (chunkBuffer.length > MAX_CHUNK_SIZE) {
      throw new TRPCError({ code: "PAYLOAD_TOO_LARGE", message: "Chunk too large (max 10MB per chunk)" });
    }

    try {
      const { url } = await storagePut(input.fileKey, chunkBuffer, input.fileMime);
      return { success: true as const, url, fileKey: input.fileKey, sizeBytes: chunkBuffer.length };
    } catch (err: any) {
      console.error("[Upload] S3 chunk proxy error:", err);
      throw new TRPCError({ code: "INTERNAL_SERVER_ERROR", message: err?.message || "Chunk upload failed" });
    }
  });

export const assembleChunksProcedure = longRunningProcedure
  .input(z.object({
    uploadId: z.string().min(1),
    fileName: z.string().min(1),
    fileMime: z.string().default("application/octet-stream"),
    totalChunks: z.number().int().min(1),
    totalSize: z.number().int().min(0),
  }))
  .mutation(async ({ input }) => {
    const { uploadId, fileName, fileMime, totalChunks, totalSize } = input;
    const tmpDir = path.join(os.tmpdir(), `assemble-${Date.now()}`);
    try {
      console.log(`[Upload] Assembling ${totalChunks} chunks for ${fileName} (${(totalSize / 1024 / 1024).toFixed(1)}MB)`);

      fs.mkdirSync(tmpDir, { recursive: true });
      const assembledPath = path.join(tmpDir, "assembled");
      const writeStream = fs.createWriteStream(assembledPath);
      const { Readable } = await import("stream");

      for (let i = 0; i < totalChunks; i++) {
        const partKey = `datasets/${uploadId}/parts/${String(i).padStart(4, "0")}`;
        try {
          const resp = await storageDownload(partKey, { timeoutMs: 120000 });
          if (!resp.body) {
            writeStream.end();
            throw new TRPCError({ code: "INTERNAL_SERVER_ERROR", message: `Empty response body for part ${i}` });
          }
          const readable = Readable.fromWeb(resp.body as any);
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
          if (partErr instanceof TRPCError) throw partErr;
          console.error(`[Upload] Failed to download part ${i}:`, partErr.message);
          throw new TRPCError({ code: "INTERNAL_SERVER_ERROR", message: `Failed to download part ${i}: ${partErr.message}` });
        }
        console.log(`[Upload] Streamed part ${i + 1}/${totalChunks} to disk`);
      }

      await new Promise<void>((resolve) => writeStream.end(resolve));

      const assembledSize = fs.statSync(assembledPath).size;
      console.log(`[Upload] Assembled ${assembledSize} bytes on disk for ${fileName}`);

      const finalKey = `datasets/${uploadId}/${fileName}`;
      const assembledBuffer = fs.readFileSync(assembledPath);
      const { url: finalUrl } = await storagePut(finalKey, assembledBuffer, fileMime);

      try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}

      return {
        success: true as const,
        fileUrl: finalUrl,
        fileKey: finalKey,
        sizeBytes: assembledSize,
      };
    } catch (err: any) {
      try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}
      if (err instanceof TRPCError) throw err;
      console.error("[Upload] Assemble error:", err);
      throw new TRPCError({ code: "INTERNAL_SERVER_ERROR", message: err?.message || "Assembly failed" });
    }
  });

export const registerFileProcedure = longRunningProcedure
  .input(z.object({
    fileName: z.string().min(1),
    fileMime: z.string().default("application/octet-stream"),
    fileKey: z.string().min(1),
    fileUrl: z.string().min(1),
    sizeBytes: z.number().int().min(0).default(0),
  }))
  .mutation(async ({ input, ctx }) => {
    const { fileName, fileMime, fileKey, fileUrl, sizeBytes } = input;
    const fileType = detectFileType(fileName);

    console.log(`[Upload] Registering S3 file: ${fileName} (${fileType}, ${(sizeBytes / 1024 / 1024).toFixed(1)}MB)`);

    let columnNames: string[] | null = null;
    let rowCount: number | null = null;
    let preview: string | null = null;

    try {
      const parsed = await parsePreview(fileKey, fileType, fileName, sizeBytes);
      columnNames = parsed.columnNames;
      rowCount = parsed.rowCount;
      preview = parsed.preview;
    } catch (parseErr: any) {
      console.warn("[Upload] Preview parsing failed (non-fatal):", parseErr.message);
    }

    // Validate parsed data
    const warnings: string[] = [];
    if ((!columnNames || columnNames.length === 0) && (rowCount === 0 || rowCount === null)) {
      throw new TRPCError({ code: "UNPROCESSABLE_CONTENT", message: "File appears to be empty or unparseable: no columns or rows detected" });
    }
    if (rowCount === 0) {
      throw new TRPCError({ code: "UNPROCESSABLE_CONTENT", message: "File contains column headers but no data rows" });
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

    const record = await insertDatasetFile({
      userId: ctx.user?.id ?? null,
      originalName: fileName,
      fileKey,
      fileUrl,
      mimeType: fileMime,
      sizeBytes,
      fileType,
      columnNames: columnNames as any,
      rowCount,
      preview,
    });

    console.log(`[Upload] Registered: ${fileName} (id=${record.id}, ${columnNames?.length || 0} cols, ${rowCount ?? "?"} rows)`);

    return {
      success: true as const,
      ...(warnings.length > 0 ? { warnings } : {}),
      file: {
        id: record.id,
        originalName: fileName,
        fileUrl,
        fileType,
        sizeBytes,
        columnNames,
        rowCount,
        preview,
      },
    };
  });
