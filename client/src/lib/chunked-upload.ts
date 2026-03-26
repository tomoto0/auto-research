/**
 * Chunked S3 Proxy Upload utility.
 * Splits large files into 8MB chunks and uploads each chunk to S3 via the server proxy.
 * Each chunk goes through /api/upload/s3chunk (server forwards to S3 with backend key).
 * After all chunks are uploaded, calls /api/upload/register to save metadata + parse preview.
 *
 * This approach:
 * - Bypasses reverse proxy body size limits (each chunk < 10MB)
 * - Avoids server memory accumulation (each chunk is processed independently)
 * - Survives server restarts (chunks already in S3 are safe)
 * - Provides real upload progress tracking
 */

const CHUNK_SIZE = 8 * 1024 * 1024; // 8MB per chunk
const MAX_RETRIES = 3;
const RETRY_DELAY = 2000;

export interface ChunkedUploadProgress {
  phase: "initiating" | "uploading" | "completing";
  chunkIndex: number;
  totalChunks: number;
  bytesUploaded: number;
  totalBytes: number;
  percent: number;
}

export interface ChunkedUploadResult {
  success: boolean;
  file?: {
    id: number;
    originalName: string;
    fileUrl: string;
    fileType: string;
    sizeBytes: number;
    columnNames: string[] | null;
    rowCount: number | null;
    preview: string | null;
  };
  error?: string;
}

function generateId(len = 12): string {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let result = "";
  const arr = new Uint8Array(len);
  crypto.getRandomValues(arr);
  for (let i = 0; i < len; i++) result += chars[arr[i] % chars.length];
  return result;
}

async function fetchWithRetry(
  url: string,
  options: RequestInit,
  retries = MAX_RETRIES
): Promise<Response> {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const resp = await fetch(url, options);
      if (resp.ok || resp.status === 413 || resp.status === 400) {
        return resp;
      }
      if (attempt < retries && resp.status >= 500) {
        await new Promise(r => setTimeout(r, RETRY_DELAY * (attempt + 1)));
        continue;
      }
      return resp;
    } catch (err) {
      if (attempt < retries) {
        await new Promise(r => setTimeout(r, RETRY_DELAY * (attempt + 1)));
        continue;
      }
      throw err;
    }
  }
  throw new Error("Max retries exceeded");
}

/**
 * Upload file via chunked S3 proxy, then register with server.
 */
export async function chunkedUpload(
  file: File,
  onProgress?: (progress: ChunkedUploadProgress) => void
): Promise<ChunkedUploadResult> {
  const totalBytes = file.size;
  const MAX_FILE_SIZE = 250 * 1024 * 1024;

  if (totalBytes > MAX_FILE_SIZE) {
    return { success: false, error: "File too large (max 250MB)" };
  }

  const totalChunks = Math.ceil(totalBytes / CHUNK_SIZE);
  const uploadId = generateId();
  const fileMime = file.type || "application/octet-stream";

  // Phase 1: Initiating
  onProgress?.({
    phase: "initiating",
    chunkIndex: 0,
    totalChunks,
    bytesUploaded: 0,
    totalBytes,
    percent: 0,
  });

  // Phase 2: Upload chunks sequentially via server proxy to S3
  let bytesUploaded = 0;
  let lastChunkUrl = "";

  for (let i = 0; i < totalChunks; i++) {
    const start = i * CHUNK_SIZE;
    const end = Math.min(start + CHUNK_SIZE, totalBytes);
    const chunk = file.slice(start, end);

    onProgress?.({
      phase: "uploading",
      chunkIndex: i,
      totalChunks,
      bytesUploaded,
      totalBytes,
      percent: Math.round((bytesUploaded / totalBytes) * 90),
    });

    // Each chunk gets its own S3 key (for multi-chunk files, use part suffix)
    const fileKey = totalChunks === 1
      ? `datasets/${uploadId}/${file.name}`
      : `datasets/${uploadId}/parts/${String(i).padStart(4, "0")}`;

    const chunkResp = await fetchWithRetry("/api/upload/s3chunk", {
      method: "POST",
      headers: {
        "x-file-key": fileKey,
        "x-file-mime": fileMime,
      },
      body: chunk,
    });

    if (!chunkResp.ok) {
      const err = await chunkResp.json().catch(() => ({ error: "Chunk upload failed" }));
      return {
        success: false,
        error: err.error || `Chunk ${i + 1}/${totalChunks} failed (${chunkResp.status})`,
      };
    }

    const chunkResult = await chunkResp.json();
    lastChunkUrl = chunkResult.url;
    bytesUploaded = end;
  }

  // Phase 3: If multi-chunk, we need to assemble on server side
  // For single chunk, the file is already complete in S3
  onProgress?.({
    phase: "completing",
    chunkIndex: totalChunks,
    totalChunks,
    bytesUploaded: totalBytes,
    totalBytes,
    percent: 92,
  });

  let finalFileUrl: string;
  let finalFileKey: string;

  if (totalChunks === 1) {
    // Single chunk - file is already complete in S3
    finalFileUrl = lastChunkUrl;
    finalFileKey = `datasets/${uploadId}/${file.name}`;
  } else {
    // Multi-chunk - call server to assemble parts
    const assembleResp = await fetchWithRetry("/api/upload/assemble", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        uploadId,
        fileName: file.name,
        fileMime,
        totalChunks,
        totalSize: totalBytes,
      }),
    });

    if (!assembleResp.ok) {
      const err = await assembleResp.json().catch(() => ({ error: "Assembly failed" }));
      return { success: false, error: err.error || `Assembly failed (${assembleResp.status})` };
    }

    const assembleResult = await assembleResp.json();
    finalFileUrl = assembleResult.fileUrl;
    finalFileKey = assembleResult.fileKey;
  }

  // Phase 4: Register with server (small JSON, no file data)
  const registerResp = await fetchWithRetry("/api/upload/register", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      fileName: file.name,
      fileMime,
      fileKey: finalFileKey,
      fileUrl: finalFileUrl,
      sizeBytes: totalBytes,
    }),
  });

  if (!registerResp.ok) {
    const err = await registerResp.json().catch(() => ({ error: "Registration failed" }));
    return { success: false, error: err.error || `Registration failed (${registerResp.status})` };
  }

  onProgress?.({
    phase: "completing",
    chunkIndex: totalChunks,
    totalChunks,
    bytesUploaded: totalBytes,
    totalBytes,
    percent: 100,
  });

  const result = await registerResp.json();
  return { success: true, file: result.file };
}
