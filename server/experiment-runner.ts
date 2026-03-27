/**
 * Server-side data analysis and chart generation engine.
 * Uses chartjs-node-canvas for chart rendering (no Chromium/Puppeteer dependency).
 * LLM generates Chart.js configuration and analysis logic,
 * which is executed in a Node.js environment.
 *
 * Key features:
 * - Auto-detect file encoding (UTF-8, Shift-JIS, EUC-JP, CP932, Latin-1)
 * - Detect placeholder/dummy LLM output and fall back to real data analysis
 * - Generate meaningful statistics, charts, and tables from actual data
 */
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import { nanoid } from "nanoid";
import { storagePut } from "./storage";
import { insertExperimentResult, updateExperimentResult } from "./db";
import { parse as csvParse } from "csv-parse/sync";
import * as XLSX from "xlsx";
import { parseDtaFile } from "./dta-parser";
import * as iconv from "iconv-lite";
import chardet from "chardet";
import { invokeLLM } from "./_core/llm";

const EXECUTION_TIMEOUT_MS = 90_000; // 90 seconds max
const MAX_OUTPUT_LENGTH = 50_000;

export interface DatasetInfo {
  originalName: string;
  fileUrl: string;
  fileKey?: string;
  fileType: string;
  columnNames?: string[];
  rowCount?: number;
}

export interface ExperimentOutput {
  success: boolean;
  stdout: string;
  stderr: string;
  exitCode: number;
  executionTimeMs: number;
  charts: { name: string; url: string; description: string }[];
  tables: { name: string; url: string; data: string; description: string }[];
  metrics: Record<string, number | string>;
}

interface MethodFeasibilityContractInput {
  executableNow?: string[];
  requiresMissingData?: string[];
  futureWorkOnly?: string[];
  blockedReasons?: Record<string, string>;
}

function normaliseMethodId(raw: string): string {
  const norm = (raw || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  const aliases: Record<string, string> = {
    descriptive: "descriptive_statistics",
    descriptive_stats: "descriptive_statistics",
    summary_statistics: "descriptive_statistics",
    correlation_analysis: "correlation",
    regression: "linear_regression",
    ols: "linear_regression",
    anova: "group_comparison",
    t_test: "group_comparison",
    time_series_trend: "time_trend",
    text_analysis: "text_feature_analysis",
    nlp: "text_feature_analysis",
    data_visualization: "data_visualisation",
    visualization: "data_visualisation",
    visualisation: "data_visualisation",
    gnn: "graph_modelling",
    computer_vision: "vision_analysis",
    panel_model: "panel_econometrics",
  };
  return aliases[norm] || norm;
}

function buildExecutableMethodSet(
  methodContract?: MethodFeasibilityContractInput | null
): Set<string> | null {
  if (!methodContract) {
    return null;
  }
  const set = new Set<string>();
  for (const methodId of methodContract.executableNow || []) {
    const normalized = normaliseMethodId(methodId);
    if (normalized) set.add(normalized);
  }
  return set;
}

function methodAllowed(executableMethods: Set<string> | null, methodId: string): boolean {
  if (!executableMethods) return true;
  return executableMethods.has(normaliseMethodId(methodId));
}

/* ------------------------------------------------------------------ */
/*  Encoding detection and file reading                                */
/* ------------------------------------------------------------------ */

/**
 * Detect the encoding of a file buffer and decode it to a UTF-8 string.
 * Tries chardet first, then falls back to a series of common Japanese encodings.
 */
function decodeFileBuffer(buffer: Buffer): { text: string; encoding: string } {
  // 1. Try chardet auto-detection
  const detected = chardet.detect(buffer);
  if (detected) {
    const normalised = detected.toLowerCase().replace(/[^a-z0-9]/g, "");
    // Map chardet names to iconv-lite names
    const encodingMap: Record<string, string> = {
      "utf8": "utf-8",
      "ascii": "utf-8",
      "shiftjis": "Shift_JIS",
      "eucjp": "EUC-JP",
      "iso2022jp": "ISO-2022-JP",
      "windows1252": "windows-1252",
      "iso88591": "latin1",
      "big5": "Big5",
      "gb2312": "GB2312",
      "gb18030": "GB18030",
      "euckr": "EUC-KR",
    };
    const iconvEncoding = encodingMap[normalised] || detected;
    try {
      const text = iconv.decode(buffer, iconvEncoding);
      // Verify the decoded text doesn't contain replacement characters
      if (!text.includes("\uFFFD") || normalised === "utf8") {
        return { text, encoding: iconvEncoding };
      }
    } catch {
      // Fall through to manual detection
    }
  }

  // 2. Try UTF-8 first (most common)
  const utf8Text = buffer.toString("utf-8");
  // Check for BOM
  const hasBom = buffer[0] === 0xEF && buffer[1] === 0xBB && buffer[2] === 0xBF;
  if (hasBom) {
    return { text: utf8Text.slice(1), encoding: "utf-8-bom" };
  }
  // Check if UTF-8 decoded cleanly (no replacement characters in first 1000 chars)
  const sample = utf8Text.slice(0, 1000);
  if (!sample.includes("\uFFFD")) {
    // Additional check: if it looks like valid text (has printable chars)
    const printableRatio = sample.replace(/[\x00-\x1f\x7f]/g, "").length / sample.length;
    if (printableRatio > 0.8) {
      return { text: utf8Text, encoding: "utf-8" };
    }
  }

  // 3. Try Japanese encodings in order of likelihood
  const japaneseEncodings = ["Shift_JIS", "CP932", "EUC-JP", "ISO-2022-JP"];
  for (const enc of japaneseEncodings) {
    try {
      const decoded = iconv.decode(buffer, enc);
      // Check quality: should have recognisable characters and no replacement chars
      const decodedSample = decoded.slice(0, 1000);
      if (!decodedSample.includes("\uFFFD")) {
        // Check for Japanese characters (hiragana, katakana, kanji)
        const hasJapanese = /[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]/.test(decodedSample);
        if (hasJapanese) {
          return { text: decoded, encoding: enc };
        }
      }
    } catch {
      continue;
    }
  }

  // 4. Try Latin-1 as last resort (never fails)
  try {
    const latin1Text = iconv.decode(buffer, "latin1");
    return { text: latin1Text, encoding: "latin1" };
  } catch {
    // Absolute fallback
    return { text: utf8Text, encoding: "utf-8" };
  }
}

/* ------------------------------------------------------------------ */
/*  Data file parsing                                                  */
/* ------------------------------------------------------------------ */

async function downloadFile(url: string, destPath: string, fileKey?: string): Promise<void> {
  // Try primary URL first
  let resp = await fetch(url);
  
  // If primary URL fails and we have a fileKey, try storageGet for a fresh URL
  if (!resp.ok && fileKey) {
    console.log(`[Download] Primary URL failed (${resp.status}), trying storageGet for key: ${fileKey}`);
    try {
      const { storageGet } = await import("./storage");
      const { url: freshUrl } = await storageGet(fileKey);
      resp = await fetch(freshUrl);
    } catch (storageErr: any) {
      console.warn(`[Download] storageGet fallback failed: ${storageErr.message}`);
    }
  }
  
  if (!resp.ok) throw new Error(`Failed to download: ${resp.status}`);
  
  // Stream to disk instead of buffering entire file in memory
  if (resp.body) {
    const { Writable } = await import("stream");
    const { pipeline } = await import("stream/promises");
    const fileStream = fs.createWriteStream(destPath);
    // @ts-ignore - Node.js ReadableStream to Node stream
    const nodeReadable = await import("stream");
    const readable = nodeReadable.Readable.fromWeb(resp.body as any);
    await pipeline(readable, fileStream);
  } else {
    // Fallback for environments without streaming
    const buffer = Buffer.from(await resp.arrayBuffer());
    fs.writeFileSync(destPath, buffer);
  }
}

/**
 * Parse a data file into a JSON-serializable array of objects.
 * Supports CSV, TSV, Excel (.xlsx/.xls), Stata (.dta), and JSON.
 * Returns at most 5000 rows to keep memory manageable.
 * Automatically detects file encoding for CSV/TSV files.
 */
function validateParsedData(
  result: { data: Record<string, any>[]; columns: string[]; totalRows: number },
  fileType: string,
): void {
  if (result.columns.length === 0) {
    throw new Error(`Parsed ${fileType} file has no columns — file may be empty or malformed`);
  }
  if (result.totalRows === 0) {
    throw new Error(`Parsed ${fileType} file has columns but zero data rows`);
  }
  // Log all-null columns as warnings (don't throw — analysis can still proceed)
  const sampleSize = Math.min(result.data.length, 50);
  for (const col of result.columns) {
    let allNull = true;
    for (let i = 0; i < sampleSize; i++) {
      const v = result.data[i]?.[col];
      if (v !== null && v !== undefined && v !== "" && v !== "NA" && v !== "NaN" && v !== ".") {
        allNull = false;
        break;
      }
    }
    if (allNull) {
      console.warn(`[DataValidation] Column "${col}" appears to be all-null in sample (${sampleSize} rows)`);
    }
  }
}

function parseDataFile(
  filePath: string,
  fileType: string
): { data: Record<string, any>[]; columns: string[]; totalRows: number; encoding?: string } {
  const MAX_ROWS = 2000;

  if (fileType === "csv" || fileType === "tsv") {
    const rawBuffer = fs.readFileSync(filePath);
    const { text: raw, encoding } = decodeFileBuffer(rawBuffer);
    const delimiter = fileType === "tsv" ? "\t" : ",";

    // Try parsing with auto-detected encoding
    try {
      const records: Record<string, any>[] = csvParse(raw, {
        columns: true,
        skip_empty_lines: true,
        delimiter,
        relax_column_count: true,
        cast: true,
        bom: true,
      });
      const columns = records.length > 0 ? Object.keys(records[0]) : [];

      // Validate columns: check they're not garbled
      const hasGarbledColumns = columns.some(col =>
        col.includes("\uFFFD") || /^[\x00-\x1f]+$/.test(col)
      );

      if (hasGarbledColumns && encoding === "utf-8") {
        // Retry with Shift-JIS
        const sjisText = iconv.decode(rawBuffer, "Shift_JIS");
        const retryRecords: Record<string, any>[] = csvParse(sjisText, {
          columns: true,
          skip_empty_lines: true,
          delimiter,
          relax_column_count: true,
          cast: true,
          bom: true,
        });
        const retryCols = retryRecords.length > 0 ? Object.keys(retryRecords[0]) : [];
        if (retryCols.length > 0 && !retryCols.some(c => c.includes("\uFFFD"))) {
          return {
            data: retryRecords.slice(0, MAX_ROWS),
            columns: retryCols,
            totalRows: retryRecords.length,
            encoding: "Shift_JIS (retry)",
          };
        }
      }

      return {
        data: records.slice(0, MAX_ROWS),
        columns,
        totalRows: records.length,
        encoding,
      };
    } catch (parseError: any) {
      // If CSV parsing fails, try with different encodings
      for (const fallbackEnc of ["Shift_JIS", "CP932", "EUC-JP", "latin1"]) {
        try {
          const fallbackText = iconv.decode(rawBuffer, fallbackEnc);
          const records: Record<string, any>[] = csvParse(fallbackText, {
            columns: true,
            skip_empty_lines: true,
            delimiter,
            relax_column_count: true,
            cast: true,
            bom: true,
          });
          const columns = records.length > 0 ? Object.keys(records[0]) : [];
          if (columns.length > 0) {
            return {
              data: records.slice(0, MAX_ROWS),
              columns,
              totalRows: records.length,
              encoding: fallbackEnc,
            };
          }
        } catch {
          continue;
        }
      }
      throw parseError;
    }
  }

  if (fileType === "dta") {
    const rawBuf = fs.readFileSync(filePath);
    const result = parseDtaFile(rawBuf, { previewRows: MAX_ROWS });
    return {
      data: result.data.slice(0, MAX_ROWS),
      columns: result.columns,
      totalRows: result.totalRows,
    };
  }

  if (fileType === "excel") {
    const workbook = XLSX.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];
    const records: Record<string, any>[] = XLSX.utils.sheet_to_json(sheet);
    const columns = records.length > 0 ? Object.keys(records[0]) : [];
    return {
      data: records.slice(0, MAX_ROWS),
      columns,
      totalRows: records.length,
    };
  }

  if (fileType === "json") {
    const raw = fs.readFileSync(filePath, "utf-8");
    let parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      for (const key of Object.keys(parsed)) {
        if (Array.isArray(parsed[key])) {
          parsed = parsed[key];
          break;
        }
      }
    }
    if (!Array.isArray(parsed)) {
      parsed = [parsed];
    }
    const columns = parsed.length > 0 ? Object.keys(parsed[0]) : [];
    return {
      data: parsed.slice(0, MAX_ROWS),
      columns,
      totalRows: parsed.length,
    };
  }

  throw new Error(`Unsupported file type: ${fileType}`);
}

/** Wrapper that parses and validates a data file */
function parseAndValidateDataFile(
  filePath: string,
  fileType: string
): { data: Record<string, any>[]; columns: string[]; totalRows: number; encoding?: string } {
  const result = parseDataFile(filePath, fileType);
  validateParsedData(result, fileType);
  return result;
}

/* ------------------------------------------------------------------ */
/*  Placeholder / dummy output detection                               */
/* ------------------------------------------------------------------ */

/**
 * Detect whether the LLM-generated analysis output is a placeholder/dummy.
 * Returns true if the output contains placeholder indicators.
 */
function isPlaceholderOutput(parsed: {
  charts?: any[];
  tables?: any[];
  metrics?: Record<string, any>;
}): boolean {
  const placeholderPatterns = [
    /placeholder/i,
    /unknown\s*(column|variable|category)/i,
    /dummy/i,
    /example\s*(data|chart|table)/i,
    /hypothetical/i,
    /simulated\s*(data|result)/i,
    /n\/a/i,
    /category\s*[a-z]/i,  // "Category A", "Category B"
    /group\s*[xyz]/i,     // "Group X", "Group Y"
    /value\s*\d/i,        // "Value 1", "Value 2"
  ];

  // Check chart names and descriptions
  for (const chart of (parsed.charts || [])) {
    const text = `${chart.name || ""} ${chart.description || ""}`;
    if (placeholderPatterns.some(p => p.test(text))) return true;

    // Check chart data labels
    const labels = chart.config?.data?.labels || [];
    const labelStr = labels.join(" ");
    if (placeholderPatterns.some(p => p.test(labelStr))) return true;
  }

  // Check table names, descriptions, and content
  for (const table of (parsed.tables || [])) {
    const text = `${table.name || ""} ${table.description || ""}`;
    if (placeholderPatterns.some(p => p.test(text))) return true;

    // Check for N/A values in rows
    const rows = table.rows || [];
    const naCount = rows.flat().filter((v: any) => v === "N/A" || v === "n/a" || v === null).length;
    const totalCells = rows.flat().length;
    if (totalCells > 0 && naCount / totalCells > 0.3) return true;
  }

  // Check metrics for null/N/A values
  const metricValues = Object.values(parsed.metrics || {});
  const nullMetrics = metricValues.filter(v => v === null || v === "null" || v === "N/A" || v === "n/a").length;
  if (metricValues.length > 0 && nullMetrics / metricValues.length > 0.3) return true;

  return false;
}

/* ------------------------------------------------------------------ */
/*  Chart rendering via chartjs-node-canvas (no Chromium needed)        */
/* ------------------------------------------------------------------ */

/**
 * Convert SVG buffer to PNG buffer using sharp.
 * Returns the original buffer if sharp is unavailable.
 */
async function svgToPng(svgBuffer: Buffer, width: number, height: number): Promise<Buffer> {
  try {
    const sharp = (await import("sharp")).default;
    const pngBuffer = await sharp(svgBuffer)
      .resize(width, height)
      .png()
      .toBuffer();
    return pngBuffer;
  } catch (err: any) {
    console.warn(`[Chart] sharp SVG→PNG conversion failed: ${err.message}`);
    return svgBuffer; // Return SVG as-is if sharp fails
  }
}

/**
 * Render a Chart.js configuration to a PNG buffer.
 * Strategy:
 *   1. Try chartjs-node-canvas (requires native canvas module)
 *   2. Fall back to SVG generation + sharp SVG→PNG conversion
 *   3. Last resort: return SVG buffer directly
 */
async function renderChartToPng(
  chartConfigJs: string,
  width = 900,
  height = 560
): Promise<Buffer> {
  // Strategy 1: Try chartjs-node-canvas
  try {
    const { ChartJSNodeCanvas } = await import("chartjs-node-canvas");
    const chartJSNodeCanvas = new ChartJSNodeCanvas({
      width,
      height,
      backgroundColour: "white",
    });

    let config: any;
    try {
      config = JSON.parse(chartConfigJs);
    } catch {
      config = new Function(`return (${chartConfigJs})`)();
    }

    // Transliterate non-ASCII labels to prevent garbling
    config = transliterateChartConfigSync(config);

    config.options = config.options || {};
    config.options.animation = false;
    config.options.responsive = false;
    config.options.devicePixelRatio = 3;

    const pngBuffer = await chartJSNodeCanvas.renderToBuffer(config);
    console.log(`[Chart] chartjs-node-canvas produced ${pngBuffer.length} bytes PNG`);
    return Buffer.from(pngBuffer);
  } catch (err: any) {
    console.warn(`[Chart] chartjs-node-canvas failed: ${err.message}`);
  }

  // Strategy 2: Generate SVG and convert to PNG via sharp
  console.log(`[Chart] Falling back to SVG + sharp PNG conversion...`);
  const svgBuffer = generateSvgFallbackChart(chartConfigJs, width, height);
  const pngBuffer = await svgToPng(svgBuffer, width, height);

  // Check if we got a real PNG
  if (pngBuffer[0] === 0x89 && pngBuffer[1] === 0x50) {
    console.log(`[Chart] SVG→PNG conversion successful: ${pngBuffer.length} bytes`);
    return pngBuffer;
  }

  // Strategy 3: Return SVG as-is (will be saved with .svg extension)
  console.warn(`[Chart] All PNG strategies failed, returning raw SVG`);
  return svgBuffer;
}

/**
 * Generate an SVG chart as a fallback when chartjs-node-canvas is unavailable.
 * Supports bar/line/scatter/bubble/pie to avoid malformed placeholder visuals.
 * Returns SVG as buffer.
 */
export function generateSvgFallbackChart(
  chartConfigJs: string,
  width: number,
  height: number
): Buffer {
  let config: any;
  try {
    config = JSON.parse(chartConfigJs);
  } catch {
    try {
      config = new Function(`return (${chartConfigJs})`)();
    } catch {
      config = { type: "bar", data: { labels: [], datasets: [] } };
    }
  }

  // Transliterate non-ASCII labels to prevent □□□□ garbling
  config = transliterateChartConfigSync(config);

  const rawTitle = config.options?.plugins?.title?.text;
  const title = Array.isArray(rawTitle)
    ? String(rawTitle.join(" "))
    : String(rawTitle || config.type || "Chart");
  const chartType = String(config.type || "bar").toLowerCase();
  const datasets: any[] = Array.isArray(config.data?.datasets) ? config.data.datasets : [];
  const labels: string[] = Array.isArray(config.data?.labels)
    ? config.data.labels.map((l: any) => String(l))
    : [];

  const asNumber = (value: unknown): number | null => {
    const n = typeof value === "number" ? value : Number(value);
    return Number.isFinite(n) ? n : null;
  };
  const clamp = (value: number, min: number, max: number): number => Math.min(max, Math.max(min, value));
  const normaliseRange = (min: number, max: number): [number, number] => {
    if (!Number.isFinite(min) || !Number.isFinite(max)) return [0, 1];
    if (min === max) {
      const delta = Math.abs(min) > 1 ? Math.abs(min) * 0.1 : 1;
      return [min - delta, max + delta];
    }
    return [min, max];
  };
  const shortLabel = (value: unknown, maxLen = 22): string => {
    const text = String(value ?? "");
    return text.length > maxLen ? `${text.slice(0, maxLen - 3)}...` : text;
  };
  const readRecord = (value: unknown): Record<string, unknown> =>
    value && typeof value === "object" ? (value as Record<string, unknown>) : {};
  const readArray = (value: unknown): unknown[] => (Array.isArray(value) ? value : []);
  type PieSlice = { value: number; label: string; color: string };
  type BubblePoint = { x: number; y: number; r: number };
  type LinePoint = { x: number; y: number };
  type RenderSeries<T> = { label: string; color: string; points: T[] };

  // Use a font stack that includes CJK fonts available on most systems
  const fontFamily = `'Noto Sans JP', 'Noto Sans CJK JP', 'Hiragino Sans', 'Hiragino Kaku Gothic ProN', 'Yu Gothic', 'Meiryo', 'MS Gothic', 'IPAGothic', 'IPAPGothic', 'TakaoPGothic', 'DejaVu Sans', 'Liberation Sans', sans-serif`;

  const padding = { top: 56, right: 36, bottom: 84, left: 64 };
  const plotX = padding.left;
  const plotY = padding.top;
  const plotWidth = Math.max(160, width - padding.left - padding.right);
  const plotHeight = Math.max(140, height - padding.top - padding.bottom);
  const plotBottomY = plotY + plotHeight;

  const palette = [
    "#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f", "#edc949",
    "#af7aa1", "#ff9da7", "#9c755f", "#bab0ab",
  ];
  const pickColor = (dsIndex: number, colorValue: unknown): string => {
    if (typeof colorValue === "string" && colorValue.trim().length > 0) return colorValue;
    if (Array.isArray(colorValue) && typeof colorValue[0] === "string") return colorValue[0];
    return palette[dsIndex % palette.length];
  };
  const mapLinear = (value: number, min: number, max: number, outMin: number, outMax: number): number => {
    const [safeMin, safeMax] = normaliseRange(min, max);
    const t = (value - safeMin) / (safeMax - safeMin);
    return outMin + t * (outMax - outMin);
  };
  const formatTick = (value: number): string => {
    const abs = Math.abs(value);
    if (abs >= 1000) return value.toFixed(0);
    if (abs >= 100) return value.toFixed(1);
    if (abs >= 10) return value.toFixed(2);
    return value.toFixed(3);
  };

  const parts: string[] = [];
  parts.push(`<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">`);
  parts.push(`<style>text { font-family: ${fontFamily}; }</style>`);
  parts.push(`<rect width="${width}" height="${height}" fill="#ffffff"/>`);
  parts.push(`<text x="${width / 2}" y="30" text-anchor="middle" font-size="16" font-weight="bold" fill="#333">${escapeXml(shortLabel(title, 90))}</text>`);

  const drawNoDataMessage = (message: string) => {
    parts.push(`<text x="${width / 2}" y="${plotY + plotHeight / 2}" text-anchor="middle" font-size="12" fill="#999">${escapeXml(message)}</text>`);
  };
  const drawCartesianFrame = (minY: number, maxY: number) => {
    const [safeMinY, safeMaxY] = normaliseRange(minY, maxY);
    for (let i = 0; i <= 4; i++) {
      const t = i / 4;
      const y = plotY + t * plotHeight;
      const value = safeMaxY - t * (safeMaxY - safeMinY);
      parts.push(`<line x1="${plotX}" y1="${y}" x2="${plotX + plotWidth}" y2="${y}" stroke="#ececec" stroke-width="1"/>`);
      parts.push(`<text x="${plotX - 8}" y="${y + 3}" text-anchor="end" font-size="9" fill="#666">${escapeXml(formatTick(value))}</text>`);
    }
    parts.push(`<line x1="${plotX}" y1="${plotBottomY}" x2="${plotX + plotWidth}" y2="${plotBottomY}" stroke="#999" stroke-width="1"/>`);
    parts.push(`<line x1="${plotX}" y1="${plotY}" x2="${plotX}" y2="${plotBottomY}" stroke="#999" stroke-width="1"/>`);
    return [safeMinY, safeMaxY] as [number, number];
  };

  if (chartType === "pie" || chartType === "doughnut") {
    const pieDataset = readRecord(datasets[0]);
    const rawValues = readArray(pieDataset.data);
    const backgroundColors = readArray(pieDataset.backgroundColor);
    const slices: PieSlice[] = rawValues
      .map((value: unknown, i: number) => {
        const n = asNumber(value);
        return {
          value: n === null ? 0 : Math.max(0, n),
          label: shortLabel(labels[i] || `Category ${i + 1}`, 22),
          color: pickColor(i, backgroundColors.length > 0 ? backgroundColors[i] : pieDataset.backgroundColor),
        };
      })
      .filter((s: PieSlice) => s.value > 0);

    const total = slices.reduce((sum: number, s: PieSlice) => sum + s.value, 0);
    if (total <= 0) {
      drawNoDataMessage("No positive values available for pie chart.");
    } else {
      const cx = plotX + plotWidth * 0.35;
      const cy = plotY + plotHeight * 0.5;
      const r = Math.max(40, Math.min(plotWidth * 0.24, plotHeight * 0.42));
      let startAngle = -Math.PI / 2;
      for (const slice of slices) {
        const sweep = (slice.value / total) * Math.PI * 2;
        const endAngle = startAngle + sweep;
        const sx = cx + r * Math.cos(startAngle);
        const sy = cy + r * Math.sin(startAngle);
        const ex = cx + r * Math.cos(endAngle);
        const ey = cy + r * Math.sin(endAngle);
        const largeArc = sweep > Math.PI ? 1 : 0;
        parts.push(
          `<path d="M ${cx} ${cy} L ${sx} ${sy} A ${r} ${r} 0 ${largeArc} 1 ${ex} ${ey} Z" fill="${slice.color}" stroke="#ffffff" stroke-width="1"/>`
        );
        startAngle = endAngle;
      }
      if (chartType === "doughnut") {
        parts.push(`<circle cx="${cx}" cy="${cy}" r="${r * 0.52}" fill="#ffffff"/>`);
      }
      const legendX = plotX + plotWidth * 0.62;
      let legendY = plotY + 12;
      const maxLegend = Math.min(slices.length, 10);
      for (let i = 0; i < maxLegend; i++) {
        const slice = slices[i];
        const pct = ((slice.value / total) * 100).toFixed(1);
        parts.push(`<rect x="${legendX}" y="${legendY - 8}" width="10" height="10" fill="${slice.color}"/>`);
        parts.push(`<text x="${legendX + 14}" y="${legendY}" font-size="10" fill="#666">${escapeXml(`${slice.label} (${pct}%)`)}</text>`);
        legendY += 16;
      }
      if (slices.length > maxLegend) {
        parts.push(`<text x="${legendX}" y="${legendY}" font-size="10" fill="#888">${escapeXml(`... ${slices.length - maxLegend} more categories`)}</text>`);
      }
    }
  } else if (chartType === "scatter" || chartType === "bubble") {
    const allSeries: RenderSeries<BubblePoint>[] = datasets.map((ds, dsIndex) => {
      const dsRecord = readRecord(ds);
      const dsData = readArray(dsRecord.data);
      const points = dsData
        .map((raw: any, idx: number) => {
          if (raw && typeof raw === "object") {
            const pointRecord = raw as Record<string, unknown>;
            const x = asNumber(pointRecord.x);
            const y = asNumber(pointRecord.y);
            const r = asNumber(pointRecord.r);
            if (x === null || y === null) return null;
            return { x, y, r: r === null ? 4 : clamp(r, 2, 16) };
          }
          const y = asNumber(raw);
          if (y === null) return null;
          return { x: idx, y, r: 4 };
        })
        .filter((p): p is { x: number; y: number; r: number } => p !== null);
      return {
        label: shortLabel(dsRecord.label || `Dataset ${dsIndex + 1}`, 28),
        color: pickColor(dsIndex, dsRecord.backgroundColor || dsRecord.borderColor),
        points,
      };
    }).filter((s: RenderSeries<BubblePoint>) => s.points.length > 0);

    if (allSeries.length === 0) {
      drawNoDataMessage("No numeric points available for scatter chart.");
    } else {
      const xs = allSeries.flatMap((s) => s.points.map((p: BubblePoint) => p.x));
      const ys = allSeries.flatMap((s) => s.points.map((p: BubblePoint) => p.y));
      const [minX, maxX] = normaliseRange(Math.min(...xs), Math.max(...xs));
      const [minY, maxY] = drawCartesianFrame(Math.min(...ys), Math.max(...ys));
      const mapX = (x: number): number => mapLinear(x, minX, maxX, plotX, plotX + plotWidth);
      const mapY = (y: number): number => mapLinear(y, minY, maxY, plotBottomY, plotY);

      for (const series of allSeries) {
        for (const point of series.points) {
          const cx = mapX(point.x);
          const cy = mapY(point.y);
          const radius = chartType === "bubble" ? point.r : 3;
          if (!Number.isFinite(cx) || !Number.isFinite(cy)) continue;
          parts.push(`<circle cx="${cx}" cy="${cy}" r="${radius}" fill="${series.color}" fill-opacity="0.65" stroke="${series.color}" stroke-width="1"/>`);
        }
      }
    }
  } else if (chartType === "line") {
    const series: RenderSeries<LinePoint>[] = datasets.map((ds, dsIndex) => {
      const dsRecord = readRecord(ds);
      const dsData = readArray(dsRecord.data);
      const points = dsData
        .map((raw: any, idx: number) => {
          const value = raw && typeof raw === "object" ? asNumber(raw.y) : asNumber(raw);
          if (value === null) return null;
          return { x: idx, y: value };
        })
        .filter((p): p is LinePoint => p !== null);
      return {
        label: shortLabel(dsRecord.label || `Dataset ${dsIndex + 1}`, 28),
        color: pickColor(dsIndex, dsRecord.borderColor || dsRecord.backgroundColor),
        points,
      };
    }).filter((s: RenderSeries<LinePoint>) => s.points.length > 1);

    if (series.length === 0) {
      drawNoDataMessage("No line-series values available.");
    } else {
      const yValues = series.flatMap((s) => s.points.map((p: LinePoint) => p.y));
      const maxPoints = Math.max(...series.map((s) => s.points.length));
      const [minY, maxY] = drawCartesianFrame(Math.min(...yValues), Math.max(...yValues));
      const [minX, maxX] = normaliseRange(0, Math.max(1, maxPoints - 1));
      const mapX = (x: number): number => mapLinear(x, minX, maxX, plotX, plotX + plotWidth);
      const mapY = (y: number): number => mapLinear(y, minY, maxY, plotBottomY, plotY);

      for (const s of series) {
        const path = s.points
          .map((p: LinePoint, i: number) => `${i === 0 ? "M" : "L"} ${mapX(p.x).toFixed(2)} ${mapY(p.y).toFixed(2)}`)
          .join(" ");
        parts.push(`<path d="${path}" fill="none" stroke="${s.color}" stroke-width="2"/>`);
        for (const p of s.points) {
          parts.push(`<circle cx="${mapX(p.x)}" cy="${mapY(p.y)}" r="2.3" fill="${s.color}"/>`);
        }
      }

      const tickStep = Math.max(1, Math.ceil(Math.max(labels.length, maxPoints) / 10));
      for (let i = 0; i < Math.max(labels.length, maxPoints); i += tickStep) {
        const x = mapX(i);
        const text = shortLabel(labels[i] || `${i + 1}`, 10);
        parts.push(`<text x="${x}" y="${plotBottomY + 16}" text-anchor="middle" font-size="9" fill="#666">${escapeXml(text)}</text>`);
      }
    }
  } else {
    // Default to bar-like rendering for bar and unknown cartesian types.
    const categoryCount = labels.length > 0
      ? labels.length
      : Math.max(0, ...datasets.map((d) => Array.isArray(d?.data) ? d.data.length : 0));

    if (categoryCount === 0 || datasets.length === 0) {
      drawNoDataMessage("No categorical values available for bar chart.");
    } else {
      const valueRanges = datasets.map((ds) => Array.from({ length: categoryCount }, (_, i) => {
        const raw = Array.isArray(ds?.data) ? ds.data[i] : null;
        if (Array.isArray(raw) && raw.length >= 2) {
          const low = asNumber(raw[0]);
          const high = asNumber(raw[1]);
          if (low !== null && high !== null) {
            return { low: Math.min(low, high), high: Math.max(low, high) };
          }
          return null;
        }
        const n = raw && typeof raw === "object" ? asNumber(raw.y) : asNumber(raw);
        if (n === null) return null;
        return { low: Math.min(0, n), high: Math.max(0, n) };
      }));
      const allLow = valueRanges.flatMap((row) => row.map((v) => v?.low).filter((v): v is number => typeof v === "number"));
      const allHigh = valueRanges.flatMap((row) => row.map((v) => v?.high).filter((v): v is number => typeof v === "number"));
      if (allLow.length === 0 || allHigh.length === 0) {
        drawNoDataMessage("No numeric bar values available.");
      } else {
        const [minY, maxY] = drawCartesianFrame(Math.min(...allLow), Math.max(...allHigh));
        const mapY = (y: number): number => mapLinear(y, minY, maxY, plotBottomY, plotY);
        const baselineY = mapY(0);
        parts.push(`<line x1="${plotX}" y1="${baselineY}" x2="${plotX + plotWidth}" y2="${baselineY}" stroke="#c9c9c9" stroke-width="1"/>`);

        const groupWidth = plotWidth / categoryCount;
        const innerPadding = Math.min(10, groupWidth * 0.18);
        const barSlotWidth = Math.max(groupWidth - innerPadding, 2);
        const barWidth = Math.max(1.6, Math.min(36, barSlotWidth / Math.max(datasets.length, 1)));

        for (let i = 0; i < categoryCount; i++) {
          const xStart = plotX + i * groupWidth + innerPadding / 2;
          for (let dsIndex = 0; dsIndex < datasets.length; dsIndex++) {
            const range = valueRanges[dsIndex][i];
            if (!range) continue;
            const yTop = mapY(range.high);
            const yBottom = mapY(range.low);
            const rectY = Math.min(yTop, yBottom);
            const rectH = Math.max(1, Math.abs(yBottom - yTop));
            const x = xStart + dsIndex * barWidth;
            const fill = pickColor(dsIndex, datasets[dsIndex]?.backgroundColor);
            parts.push(`<rect x="${x}" y="${rectY}" width="${Math.max(1, barWidth - 1)}" height="${rectH}" fill="${fill}" fill-opacity="0.78" rx="1.4"/>`);
          }
        }

        const tickStep = Math.max(1, Math.ceil(categoryCount / 12));
        for (let i = 0; i < categoryCount; i += tickStep) {
          const x = plotX + i * groupWidth + groupWidth / 2;
          const text = shortLabel(labels[i] || `${i + 1}`, 10);
          parts.push(`<text x="${x}" y="${plotBottomY + 16}" text-anchor="middle" font-size="9" fill="#666">${escapeXml(text)}</text>`);
        }
      }
    }
  }

  // Shared legend for non-pie charts
  if (chartType !== "pie" && chartType !== "doughnut" && datasets.length > 0) {
    const legendCount = Math.min(datasets.length, 6);
    const legendColumns = Math.min(3, legendCount);
    const legendRows = Math.ceil(legendCount / legendColumns);
    const legendStartY = height - 18 - (legendRows - 1) * 14;
    const legendCellW = plotWidth / legendColumns;
    for (let i = 0; i < legendCount; i++) {
      const row = Math.floor(i / legendColumns);
      const col = i % legendColumns;
      const x = plotX + col * legendCellW;
      const y = legendStartY + row * 14;
      const color = pickColor(i, datasets[i]?.backgroundColor || datasets[i]?.borderColor);
      const label = shortLabel(datasets[i]?.label || `Dataset ${i + 1}`, 24);
      parts.push(`<rect x="${x}" y="${y - 8}" width="9" height="9" fill="${color}"/>`);
      parts.push(`<text x="${x + 13}" y="${y}" font-size="9.5" fill="#666">${escapeXml(label)}</text>`);
    }
  }

  parts.push(`</svg>`);
  return Buffer.from(parts.join(""), "utf-8");
}

/* ------------------------------------------------------------------ */
/*  LLM-based translation for non-ASCII labels                          */
/* ------------------------------------------------------------------ */

/** Cache for LLM-translated labels to avoid redundant API calls */
const translationCache = new Map<string, string>();

/**
 * Translate a batch of non-ASCII strings to English using LLM.
 * Results are cached to avoid redundant API calls.
 * Falls back to dictionary-based transliteration if LLM fails.
 */
async function translateLabelsToEnglish(labels: string[]): Promise<Map<string, string>> {
  const results = new Map<string, string>();
  const toTranslate: string[] = [];

  for (const label of labels) {
    if (!label || !/[^\x00-\x7F]/.test(label)) {
      results.set(label, label); // Already ASCII
    } else if (translationCache.has(label)) {
      results.set(label, translationCache.get(label)!);
    } else {
      toTranslate.push(label);
    }
  }

  if (toTranslate.length === 0) return results;

  try {
    const batchStr = toTranslate.map((l, i) => `${i + 1}. ${l}`).join("\n");
    const response = await invokeLLM({
      messages: [
        {
          role: "system",
          content: `You are a translator. Translate the following labels/terms to concise English. These are data column names, chart labels, or category names from a Japanese dataset. Output ONLY a JSON object mapping each original string to its English translation. Keep translations short and suitable for chart labels (max 40 characters). Example: {"男性": "Male", "年齢": "Age", "雇用形態別の経済的不安": "Economic Insecurity by Employment Type"}`
        },
        {
          role: "user",
          content: `Translate these labels to English:\n${batchStr}\n\nOutput ONLY a valid JSON object. No markdown code blocks.`
        }
      ],
      maxTokens: 4096,
    });

    const content = response.choices?.[0]?.message?.content;
    if (typeof content === "string") {
      let cleaned = content.trim();
      cleaned = cleaned.replace(/^```(?:json)?\s*\n?/i, "").replace(/\n?```\s*$/i, "");
      try {
        const translations = JSON.parse(cleaned);
        for (const original of toTranslate) {
          const translated = translations[original];
          if (translated && typeof translated === "string" && translated.trim().length > 0) {
            const finalLabel = translated.trim().slice(0, 60);
            translationCache.set(original, finalLabel);
            results.set(original, finalLabel);
          } else {
            // Fallback to dictionary-based
            const fallback = transliterateLabelSync(original);
            translationCache.set(original, fallback);
            results.set(original, fallback);
          }
        }
      } catch {
        // JSON parse failed, use dictionary fallback
        for (const original of toTranslate) {
          const fallback = transliterateLabelSync(original);
          translationCache.set(original, fallback);
          results.set(original, fallback);
        }
      }
    }
  } catch (err: any) {
    console.warn(`[Chart] LLM translation failed: ${err.message}, using dictionary fallback`);
    for (const original of toTranslate) {
      const fallback = transliterateLabelSync(original);
      translationCache.set(original, fallback);
      results.set(original, fallback);
    }
  }

  return results;
}

/**
 * Synchronous dictionary-based transliteration (fallback when LLM is unavailable).
 * Covers common Japanese academic/survey terms.
 */
export function transliterateLabelSync(str: string): string {
  if (!str) return str;
  if (!/[^\x00-\x7F]/.test(str)) return str;
  const jpMap: Record<string, string> = {
    "男性": "Male", "女性": "Female", "合計": "Total", "平均": "Mean",
    "年齢": "Age", "性別": "Gender", "身長": "Height", "体重": "Weight",
    "収入": "Income", "学歴": "Education", "職業": "Occupation",
    "既婚": "Married", "未婚": "Single", "離婚": "Divorced",
    "はい": "Yes", "いいえ": "No", "その他": "Other",
    "都道府県": "Prefecture", "市区町村": "Municipality",
    "北海道": "Hokkaido", "東京": "Tokyo", "大阪": "Osaka",
    "健康": "Health", "不健康": "Unhealthy", "普通": "Normal",
    "良い": "Good", "悪い": "Bad", "非常に良い": "Very Good",
    "非常に悪い": "Very Bad", "どちらでもない": "Neutral",
    "賛成": "Agree", "反対": "Disagree", "強く賛成": "Strongly Agree",
    "強く反対": "Strongly Disagree", "やや賛成": "Somewhat Agree",
    "やや反対": "Somewhat Disagree",
    "正社員": "Full-time", "パート": "Part-time", "アルバイト": "Part-time",
    "自営業": "Self-employed", "無職": "Unemployed", "学生": "Student",
    "回答": "Response", "質問": "Question", "項目": "Item",
    "度数": "Frequency", "割合": "Proportion", "標準偏差": "Std Dev",
    "中央値": "Median", "最大値": "Max", "最小値": "Min",
    "相関": "Correlation", "有意": "Significant",
    "第1波": "Wave 1", "第2波": "Wave 2", "第3波": "Wave 3",
    "第4波": "Wave 4", "第5波": "Wave 5",
    "波": "Wave", "年": "Year", "月": "Month", "日": "Day",
    "満足": "Satisfaction", "不満": "Dissatisfaction", "幸福": "Happiness",
    "経済": "Economy", "雇用": "Employment", "失業": "Unemployment",
    "世帯": "Household", "家族": "Family", "子供": "Children",
    "結婚": "Marriage", "配偶者": "Spouse", "親": "Parent",
    "父": "Father", "母": "Mother", "兄弟": "Siblings",
    "大学": "University", "高校": "High School", "中学": "Middle School",
    "小学": "Elementary", "卒業": "Graduate", "在学": "Enrolled",
    "正規": "Regular", "非正規": "Non-regular", "派遣": "Temporary",
    "契約": "Contract", "常勤": "Full-time", "非常勤": "Part-time",
    "不安": "Insecurity", "安定": "Stability", "形態": "Type",
    "別": "by", "的": "-type", "の": " ",
  };
  if (jpMap[str]) return jpMap[str];
  let result = str;
  // Sort by length descending to match longer phrases first
  const sortedEntries = Object.entries(jpMap).sort((a, b) => b[0].length - a[0].length);
  for (const [jp, en] of sortedEntries) {
    result = result.replace(new RegExp(jp, "g"), en);
  }
  // Clean up multiple spaces
  result = result.replace(/\s+/g, " ").trim();
  if (/[^\x00-\x7F]/.test(result)) {
    const asciiParts = result.match(/[\x20-\x7E]+/g);
    if (asciiParts && asciiParts.join("").trim().length > 0) {
      return asciiParts.join(" ").trim();
    }
    return `Variable ${str.length}`;
  }
  return result;
}

// Keep the old name as an alias for backward compatibility in tests
const transliterateLabel = transliterateLabelSync;

/**
 * Transliterate all labels in a Chart.js config to ASCII-safe text.
 */
/**
 * Synchronous transliteration of chart config (dictionary-based only).
 * Used as immediate fallback in SVG generation.
 */
function transliterateChartConfigSync(config: any): any {
  if (!config) return config;
  const clone = JSON.parse(JSON.stringify(config));
  if (clone.data?.labels) {
    clone.data.labels = clone.data.labels.map((l: any) => transliterateLabel(String(l)));
  }
  if (clone.data?.datasets) {
    for (const ds of clone.data.datasets) {
      if (ds.label) ds.label = transliterateLabel(String(ds.label));
    }
  }
  if (clone.options?.plugins?.title?.text) {
    clone.options.plugins.title.text = transliterateLabel(String(clone.options.plugins.title.text));
  }
  if (clone.options?.scales) {
    for (const axis of Object.values(clone.options.scales) as any[]) {
      if (axis?.title?.text) {
        axis.title.text = transliterateLabel(String(axis.title.text));
      }
    }
  }
  return clone;
}

/**
 * Async transliteration of chart config using LLM translation.
 * Collects all non-ASCII strings, translates them in one batch, then applies.
 */
async function transliterateChartConfigAsync(config: any): Promise<any> {
  if (!config) return config;
  const clone = JSON.parse(JSON.stringify(config));

  // Collect all non-ASCII strings from the config
  const nonAsciiStrings: string[] = [];
  if (clone.data?.labels) {
    for (const l of clone.data.labels) {
      const s = String(l);
      if (/[^\x00-\x7F]/.test(s)) nonAsciiStrings.push(s);
    }
  }
  if (clone.data?.datasets) {
    for (const ds of clone.data.datasets) {
      if (ds.label && /[^\x00-\x7F]/.test(String(ds.label))) nonAsciiStrings.push(String(ds.label));
    }
  }
  if (clone.options?.plugins?.title?.text) {
    const t = String(clone.options.plugins.title.text);
    if (/[^\x00-\x7F]/.test(t)) nonAsciiStrings.push(t);
  }
  if (clone.options?.scales) {
    for (const axis of Object.values(clone.options.scales) as any[]) {
      if (axis?.title?.text) {
        const t = String(axis.title.text);
        if (/[^\x00-\x7F]/.test(t)) nonAsciiStrings.push(t);
      }
    }
  }

  // If no non-ASCII strings, return as-is
  if (nonAsciiStrings.length === 0) return clone;

  // Translate all non-ASCII strings in one batch
  const translations = await translateLabelsToEnglish(Array.from(new Set(nonAsciiStrings)));

  // Apply translations
  if (clone.data?.labels) {
    clone.data.labels = clone.data.labels.map((l: any) => {
      const s = String(l);
      return translations.get(s) || transliterateLabel(s);
    });
  }
  if (clone.data?.datasets) {
    for (const ds of clone.data.datasets) {
      if (ds.label) {
        const s = String(ds.label);
        ds.label = translations.get(s) || transliterateLabel(s);
      }
    }
  }
  if (clone.options?.plugins?.title?.text) {
    const s = String(clone.options.plugins.title.text);
    clone.options.plugins.title.text = translations.get(s) || transliterateLabel(s);
  }
  if (clone.options?.scales) {
    for (const axis of Object.values(clone.options.scales) as any[]) {
      if (axis?.title?.text) {
        const s = String(axis.title.text);
        axis.title.text = translations.get(s) || transliterateLabel(s);
      }
    }
  }

  return clone;
}

function escapeXml(str: string): string {
  return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

/* ------------------------------------------------------------------ */
/*  Analysis script builder (for LLM prompt)                           */
/* ------------------------------------------------------------------ */

export function buildAnalysisScript(
  dataFiles: { localPath: string; originalName: string; fileType: string }[],
  analysisCode: string,
  outputDir: string
): string {
  return analysisCode;
}

/* ------------------------------------------------------------------ */
/*  Main execution entry point                                         */
/* ------------------------------------------------------------------ */

export async function executePythonExperiment(
  runId: string,
  stageNumber: number,
  analysisCode: string,
  datasets: DatasetInfo[],
  methodContract?: MethodFeasibilityContractInput | null
): Promise<ExperimentOutput> {
  const startTime = Date.now();
  const workDir = path.join(os.tmpdir(), `experiment-${runId}-${nanoid(6)}`);
  const dataDir = path.join(workDir, "data");
  fs.mkdirSync(workDir, { recursive: true });
  fs.mkdirSync(dataDir, { recursive: true });

  // Create DB record
  const dbResult = await insertExperimentResult({
    runId,
    stageNumber,
    executionStatus: "running",
    pythonCode: analysisCode,
  });

  const logs: string[] = [];
  const charts: ExperimentOutput["charts"] = [];
  const tables: ExperimentOutput["tables"] = [];
  const metrics: Record<string, number | string> = {};
  const executableMethods = buildExecutableMethodSet(methodContract);

  try {
    // 1. Download and parse all dataset files
    logs.push("[INFO] Downloading and parsing datasets...");
    const allData: { name: string; data: Record<string, any>[]; columns: string[]; totalRows: number }[] = [];

    for (const ds of datasets) {
      const localPath = path.join(dataDir, ds.originalName);
      await downloadFile(ds.fileUrl, localPath, ds.fileKey);
      logs.push(`[INFO] Downloaded: ${ds.originalName}`);

      try {
        const parsed = parseAndValidateDataFile(localPath, ds.fileType);
        allData.push({ name: ds.originalName, ...parsed });
        logs.push(`[INFO] Parsed ${ds.originalName}: ${parsed.totalRows} rows, ${parsed.columns.length} columns (encoding: ${parsed.encoding || "native"})`);
        // Log first 20 column names for debugging
        const colPreview = parsed.columns.slice(0, 20).join(", ");
        logs.push(`[INFO] Columns: ${colPreview}${parsed.columns.length > 20 ? ` ... (${parsed.columns.length} total)` : ""}`);
        // Log sample data (first 3 rows, first 5 columns)
        if (parsed.data.length > 0) {
          const sampleCols = parsed.columns.slice(0, 5);
          const sampleRows = parsed.data.slice(0, 3).map(row =>
            sampleCols.map(c => String(row[c] ?? "null").slice(0, 30)).join(" | ")
          );
          logs.push(`[INFO] Sample data (first 3 rows, first 5 cols):\n  ${sampleCols.join(" | ")}\n  ${sampleRows.join("\n  ")}`);
        }
        metrics[`${ds.originalName}_rows`] = parsed.totalRows;
        metrics[`${ds.originalName}_columns`] = parsed.columns.length;
      } catch (parseErr: any) {
        logs.push(`[WARN] Failed to parse ${ds.originalName}: ${parseErr.message}`);
      }
    }

    if (allData.length === 0) {
      throw new Error("No datasets could be parsed successfully");
    }

    // 1b. Pre-translate column names to English for chart/table/metric labels
    logs.push("[INFO] Translating column names to English...");
    try {
      const allColNames: string[] = [];
      for (const ds of allData) {
        for (const col of ds.columns) {
          if (/[^\x00-\x7F]/.test(col) && !allColNames.includes(col)) {
            allColNames.push(col);
          }
        }
      }
      if (allColNames.length > 0) {
        const colTranslations = await translateLabelsToEnglish(allColNames);
        // Create column name mapping and rename columns in allData
        for (const ds of allData) {
          const colMap = new Map<string, string>();
          const newColumns: string[] = [];
          for (const col of ds.columns) {
            const translated = colTranslations.get(col) || transliterateLabelSync(col);
            // Ensure unique column names
            let finalName = translated;
            let suffix = 2;
            while (newColumns.includes(finalName) && finalName !== col) {
              finalName = `${translated}_${suffix}`;
              suffix++;
            }
            colMap.set(col, finalName);
            newColumns.push(finalName);
          }
          // Rename columns in data rows IN-PLACE to avoid doubling memory
          ds.columns = newColumns;
          for (const row of ds.data) {
            for (const [oldCol, newCol] of Array.from(colMap.entries())) {
              if (oldCol !== newCol && oldCol in row) {
                row[newCol] = row[oldCol];
                delete row[oldCol];
              }
            }
          }
        }
        logs.push(`[INFO] Translated ${allColNames.length} column names to English`);
      } else {
        logs.push("[INFO] All column names are already ASCII, no translation needed");
      }
    } catch (translateErr: any) {
      logs.push(`[WARN] Column name translation failed: ${translateErr.message}, using original names`);
    }

    // 2. ALWAYS generate charts/tables/metrics from REAL DATA
    // LLM-generated analysis code is NOT trusted for data values (hallucination risk).
    // We always use generateDefaultCharts/Tables/Metrics which compute from actual data.
    logs.push("[INFO] Generating analysis from actual data (bypassing LLM data values to prevent hallucination)...");
    const chartDefinitions = generateDefaultCharts(allData, executableMethods);
    const tableDefinitions = generateDefaultTables(allData, executableMethods);
    const metricsFromCode = generateDefaultMetrics(allData, executableMethods);
    logs.push(`[INFO] Generated ${chartDefinitions.length} charts, ${tableDefinitions.length} tables, ${Object.keys(metricsFromCode).length} metrics from real data`);
    if (executableMethods !== null) {
      logs.push(`[INFO] Enforced method contract executable_now: ${Array.from(executableMethods).join(", ")}`);
      if (methodContract?.requiresMissingData?.length) {
        logs.push(`[INFO] Blocked by missing data: ${methodContract.requiresMissingData.join(", ")}`);
      }
      if (methodContract?.futureWorkOnly?.length) {
        logs.push(`[INFO] Future-work only methods: ${methodContract.futureWorkOnly.join(", ")}`);
      }
    }

    Object.assign(metrics, metricsFromCode);
    const routingDiagnostics = buildRoutingDiagnostics(
      allData,
      metrics,
      chartDefinitions.map(c => ({ name: c.name })),
      tableDefinitions.map(t => ({ name: t.name })),
      executableMethods,
      methodContract
    );
    metrics.execution_blocked_methods = routingDiagnostics.blockedMethods.join(", ");
    metrics.execution_unresolved_prerequisites = routingDiagnostics.unresolvedPrerequisites.join(" | ");
    metrics.execution_skipped_executable_methods = routingDiagnostics.skippedExecutableMethods.join(", ");
    metrics.execution_no_output_reasons = routingDiagnostics.noOutputReasons.join(" | ");
    metrics.analysis_methods_executed = routingDiagnostics.executedMethods.join(", ");
    if (routingDiagnostics.unresolvedPrerequisites.length > 0) {
      logs.push(`[INFO] Unresolved prerequisites: ${routingDiagnostics.unresolvedPrerequisites.join(" | ")}`);
    }
    if (routingDiagnostics.skippedExecutableMethods.length > 0) {
      logs.push(`[WARN] Executable methods with no outputs: ${routingDiagnostics.skippedExecutableMethods.join(", ")}`);
    }

    // 2b. Translate table headers, row values, and metric keys that contain non-ASCII
    logs.push("[INFO] Translating table/metric labels to English...");
    try {
      // Collect all non-ASCII strings from tables (headers + cell values) and metric keys
      const nonAsciiSet = new Set<string>();
      for (const t of tableDefinitions) {
        for (const h of t.headers) {
          if (/[^\x00-\x7F]/.test(String(h))) nonAsciiSet.add(String(h));
        }
        for (const row of t.rows) {
          for (const cell of row) {
            if (typeof cell === "string" && /[^\x00-\x7F]/.test(cell)) nonAsciiSet.add(cell);
          }
        }
        if (/[^\x00-\x7F]/.test(t.description)) nonAsciiSet.add(t.description);
      }
      for (const key of Object.keys(metrics)) {
        if (/[^\x00-\x7F]/.test(key)) nonAsciiSet.add(key);
      }
      for (const cd of chartDefinitions) {
        if (/[^\x00-\x7F]/.test(cd.description)) nonAsciiSet.add(cd.description);
      }

      if (nonAsciiSet.size > 0) {
        const labelTranslations = await translateLabelsToEnglish(Array.from(nonAsciiSet));
        const tr = (s: string) => labelTranslations.get(s) || transliterateLabelSync(s);

        // Apply to tables
        for (const t of tableDefinitions) {
          t.headers = t.headers.map(h => /[^\x00-\x7F]/.test(String(h)) ? tr(String(h)) : String(h));
          t.rows = t.rows.map(row => row.map(cell =>
            typeof cell === "string" && /[^\x00-\x7F]/.test(cell) ? tr(cell) : cell
          ));
          if (/[^\x00-\x7F]/.test(t.description)) t.description = tr(t.description);
        }

        // Apply to metric keys
        const newMetrics: Record<string, number | string> = {};
        for (const [key, val] of Object.entries(metrics)) {
          const newKey = /[^\x00-\x7F]/.test(key) ? tr(key) : key;
          newMetrics[newKey] = val;
        }
        // Replace metrics
        for (const key of Object.keys(metrics)) delete metrics[key];
        Object.assign(metrics, newMetrics);

        // Apply to chart descriptions
        for (const cd of chartDefinitions) {
          if (/[^\x00-\x7F]/.test(cd.description)) cd.description = tr(cd.description);
        }

        logs.push(`[INFO] Translated ${nonAsciiSet.size} non-ASCII labels in tables/metrics/charts`);
      }
    } catch (trErr: any) {
      logs.push(`[WARN] Table/metric label translation failed: ${trErr.message}`);
    }

    // 3. Render each chart via chartjs-node-canvas
    // Pre-translate all chart labels to English using LLM before rendering
    logs.push(`[INFO] Translating chart labels to English...`);
    for (const chartDef of chartDefinitions) {
      try {
        let config: any;
        if (typeof chartDef.config === "string") {
          try { config = JSON.parse(chartDef.config); } catch { config = null; }
        } else {
          config = chartDef.config;
        }
        if (config) {
          const translated = await transliterateChartConfigAsync(config);
          chartDef.config = translated;
        }
      } catch (err: any) {
        logs.push(`[WARN] LLM translation failed for ${chartDef.name}: ${err.message}`);
      }
    }

    logs.push(`[INFO] Rendering ${chartDefinitions.length} charts (server-side, no Chromium)...`);
    for (const chartDef of chartDefinitions) {
      try {
        const configStr = typeof chartDef.config === "string"
          ? chartDef.config
          : JSON.stringify(chartDef.config);

        const pngBuffer = await renderChartToPng(
          configStr,
          (chartDef as any).width || 800,
          (chartDef as any).height || 500
        );

        // Determine content type based on buffer content
        const isSvg = pngBuffer.length > 0 && pngBuffer[0] === 0x3C; // '<' character
        const contentType = isSvg ? "image/svg+xml" : "image/png";
        const ext = isSvg ? "svg" : "png";

        const chartKey = `experiments/${runId}/${chartDef.name}.${ext}`;
        const { url } = await storagePut(chartKey, pngBuffer, contentType);
        charts.push({
          name: chartDef.name,
          url,
          description: chartDef.description || chartDef.name,
        });
        logs.push(`[CHART] Generated: ${chartDef.name} (${(pngBuffer.length / 1024).toFixed(1)} KiB, ${ext})`);
      } catch (chartErr: any) {
        logs.push(`[WARN] Failed to render chart ${chartDef.name}: ${chartErr.message}`);
      }
    }

    // 4. Process tables
    logs.push(`[INFO] Processing ${tableDefinitions.length} tables...`);
    for (const tableDef of tableDefinitions) {
      try {
        const headerRow = tableDef.headers.join(",");
        const dataRows = tableDef.rows.map(r => r.join(",")).join("\n");
        const csvContent = `${headerRow}\n${dataRows}`;

        const tableKey = `experiments/${runId}/${tableDef.name}.csv`;
        const { url } = await storagePut(tableKey, csvContent, "text/csv");
        tables.push({
          name: tableDef.name,
          url,
          description: tableDef.description || tableDef.name,
          data: `${headerRow}\n${dataRows.split("\n").slice(0, 20).join("\n")}`,
        });
        logs.push(`[TABLE] Generated: ${tableDef.name} (${tableDef.rows.length} rows)`);
      } catch (tableErr: any) {
        logs.push(`[WARN] Failed to process table ${tableDef.name}: ${tableErr.message}`);
      }
    }

    const executionTimeMs = Date.now() - startTime;
    const stdout = logs.join("\n");

    const output: ExperimentOutput = {
      success: true,
      stdout: stdout.slice(0, MAX_OUTPUT_LENGTH),
      stderr: "",
      exitCode: 0,
      executionTimeMs,
      charts,
      tables,
      metrics,
    };

    await updateExperimentResult(dbResult.id, {
      executionStatus: "success",
      stdout: output.stdout,
      stderr: "",
      exitCode: 0,
      executionTimeMs,
      generatedCharts: charts as any,
      generatedTables: tables as any,
      metrics: metrics as any,
    });

    try { fs.rmSync(workDir, { recursive: true, force: true }); } catch {}
    return output;

  } catch (err: any) {
    const executionTimeMs = Date.now() - startTime;
    const stderr = err?.message || "Execution failed";
    logs.push(`[ERROR] ${stderr}`);

    const output: ExperimentOutput = {
      success: false,
      stdout: logs.join("\n").slice(0, MAX_OUTPUT_LENGTH),
      stderr,
      exitCode: -1,
      executionTimeMs,
      charts,
      tables,
      metrics,
    };

    await updateExperimentResult(dbResult.id, {
      executionStatus: "error",
      stdout: output.stdout,
      stderr,
      exitCode: -1,
      executionTimeMs,
      generatedCharts: charts as any,
      generatedTables: tables as any,
      metrics: metrics as any,
    });

    try { fs.rmSync(workDir, { recursive: true, force: true }); } catch {}
    return output;
  }
}

/* ------------------------------------------------------------------ */
/*  Default chart/table/metrics generators                             */
/* ------------------------------------------------------------------ */

/**
 * Classify columns into numeric and categorical based on actual data values.
 * Samples multiple rows to avoid misclassification from a single row.
 */
/**
 * Detect whether a numeric column is actually an ID/code column (not meaningful for statistics).
 * ID columns have characteristics like: sequential integers, very high cardinality, or
 * column names suggesting identifiers.
 */
export function isIdOrCodeColumn(col: string, data: Record<string, any>[]): boolean {
  const lowerCol = col.toLowerCase().replace(/[^a-z0-9]/g, "");
  // Name-based detection: common ID/code column patterns
  const idPatterns = [
    "id", "code", "prefecture", "prefcode", "prefecturecode", "regioncode",
    "zipcode", "postalcode", "fips", "iso", "index", "rowid", "recordid",
    "serialno", "caseid", "respondentid", "householdid", "personid",
    "都道府県", "コード", "番号", "識別",
  ];
  if (idPatterns.some(p => lowerCol.includes(p))) return true;

  // Statistical detection: check if values are sequential integers with very high cardinality
  const sampleSize = Math.min(data.length, 200);
  const values = data.slice(0, sampleSize)
    .map(r => Number(r[col]))
    .filter(v => !isNaN(v) && Number.isInteger(v));
  if (values.length < 10) return false;

  // If all values are integers and unique count is very high relative to sample, likely an ID
  const uniqueCount = new Set(values).size;
  if (uniqueCount / values.length > 0.9 && values.length > 20) return true;

  // If values are small integers (1-50) with few unique values, likely a code (e.g., prefecture 1-47)
  const min = Math.min(...values);
  const max = Math.max(...values);
  if (min >= 0 && max <= 100 && uniqueCount <= 50 && uniqueCount === (max - min + 1)) {
    // Looks like a sequential code (e.g., prefecture 1-47)
    // Only flag if the column name doesn't suggest a meaningful measure
    const measurePatterns = ["score", "rating", "level", "grade", "scale", "age", "year",
      "income", "salary", "wage", "price", "cost", "count", "rate", "ratio",
      "percent", "proportion", "frequency", "duration", "time", "hours",
      "スコア", "得点", "評価", "年齢", "収入", "給与"];
    if (!measurePatterns.some(p => lowerCol.includes(p))) return true;
  }

  return false;
}

export function classifyColumns(
  data: Record<string, any>[],
  columns: string[]
): { numericCols: string[]; categoricalCols: string[]; idCols: string[]; nullCols: string[] } {
  const numericCols: string[] = [];
  const categoricalCols: string[] = [];
  const idCols: string[] = [];
  const nullCols: string[] = [];
  const sampleSize = Math.min(data.length, 50);

  for (const col of columns) {
    let numericCount = 0;
    let totalNonNull = 0;

    for (let i = 0; i < sampleSize; i++) {
      const val = data[i][col];
      if (val === null || val === undefined || val === "" || val === "NA" || val === "NaN" || val === ".") continue;
      totalNonNull++;
      if (typeof val === "number" || (typeof val === "string" && !isNaN(Number(val)) && val.trim() !== "")) {
        numericCount++;
      }
    }

    if (totalNonNull === 0) {
      nullCols.push(col);
      continue;
    }

    // A column is numeric if >70% of non-null sampled values are numeric
    if (numericCount / totalNonNull > 0.7) {
      // Check if this is actually an ID/code column
      if (isIdOrCodeColumn(col, data)) {
        idCols.push(col);
      } else {
        numericCols.push(col);
      }
    } else {
      categoricalCols.push(col);
    }
  }

  if (nullCols.length > 0) {
    console.warn(`[classifyColumns] All-null columns skipped: ${nullCols.join(", ")}`);
  }

  return { numericCols, categoricalCols, idCols, nullCols };
}

type ParsedDataset = {
  name: string;
  data: Record<string, any>[];
  columns: string[];
  totalRows: number;
};

function metricKeyPart(input: string, maxLen = 24): string {
  const ascii = transliterateLabelSync(input || "var");
  const normalized = ascii
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, maxLen);
  return normalized || "var";
}

function parseNumericPairs(ds: ParsedDataset, xCol: string, yCol: string): [number, number][] {
  const pairs: [number, number][] = [];
  for (const row of ds.data) {
    const x = Number(row[xCol]);
    const y = Number(row[yCol]);
    if (!isNaN(x) && !isNaN(y)) pairs.push([x, y]);
  }
  return pairs;
}

/**
 * Approximate two-tailed p-value for Pearson correlation using t-distribution.
 * Uses Abramowitz & Stegun rational approximation for the normal CDF when df > 30,
 * and discrete thresholds for small samples.
 */
function approximateCorrelationPValue(r: number, n: number): number {
  if (n <= 3) return 1;
  const absR = Math.abs(r);
  if (absR >= 1) return 0;
  const df = n - 2;
  const t = absR * Math.sqrt(df / (1 - absR * absR + 1e-12));
  return approxTwoTailPValue(t, df);
}

/**
 * Approximate two-tailed p-value for a t-statistic with given degrees of freedom.
 * For df > 30, uses a normal approximation via the Abramowitz & Stegun formula.
 * For smaller df, uses conservative threshold-based estimation.
 */
function approxTwoTailPValue(t: number, df: number): number {
  const absT = Math.abs(t);
  if (df > 30) {
    // Normal approximation: P(Z > |t|) using A&S 26.2.17
    const z = absT;
    const p = 0.2316419;
    const b1 = 0.319381530, b2 = -0.356563782, b3 = 1.781477937, b4 = -1.821255978, b5 = 1.330274429;
    const tVal = 1 / (1 + p * z);
    const phi = Math.exp(-z * z / 2) / Math.sqrt(2 * Math.PI);
    const oneTail = phi * (b1 * tVal + b2 * tVal ** 2 + b3 * tVal ** 3 + b4 * tVal ** 4 + b5 * tVal ** 5);
    return Math.min(1, Math.max(0, 2 * oneTail));
  }
  // Small sample: conservative thresholds
  if (absT > 3.291) return 0.005;
  if (absT > 2.576) return 0.01;
  if (absT > 1.96) return 0.05;
  if (absT > 1.645) return 0.1;
  if (absT > 1.282) return 0.2;
  return 0.5;
}

function regressionStatsFromPairs(pairs: [number, number][]): {
  slope: number;
  intercept: number;
  r2: number;
  n: number;
} | null {
  if (pairs.length < 10) return null;
  const n = pairs.length;
  const meanX = pairs.reduce((a, p) => a + p[0], 0) / n;
  const meanY = pairs.reduce((a, p) => a + p[1], 0) / n;

  let cov = 0;
  let varX = 0;
  let ssTot = 0;
  for (const [x, y] of pairs) {
    cov += (x - meanX) * (y - meanY);
    varX += (x - meanX) ** 2;
    ssTot += (y - meanY) ** 2;
  }
  if (varX <= 0) return null;

  const slope = cov / varX;
  const intercept = meanY - slope * meanX;
  let ssRes = 0;
  for (const [x, y] of pairs) {
    const pred = intercept + slope * x;
    ssRes += (y - pred) ** 2;
  }
  const r2 = ssTot > 0 ? Math.max(0, 1 - ssRes / ssTot) : 0;
  return { slope, intercept, r2, n };
}

function parseTimeValue(raw: any): number | null {
  if (raw === null || raw === undefined || raw === "") return null;
  if (typeof raw === "number" && isFinite(raw)) return raw;
  const asNum = Number(raw);
  if (!isNaN(asNum) && isFinite(asNum)) return asNum;

  if (typeof raw === "string") {
    const date = new Date(raw);
    if (!isNaN(date.getTime())) {
      return date.getTime() / 86400000; // days
    }
  }
  return null;
}

function detectTextColumns(ds: ParsedDataset): string[] {
  const textCols: string[] = [];
  for (const col of ds.columns) {
    const sample = ds.data.slice(0, 300).map(r => r[col]).filter(v => v !== null && v !== undefined && v !== "");
    if (sample.length < 20) continue;
    const stringVals = sample.filter(v => typeof v === "string") as string[];
    if (stringVals.length / sample.length < 0.6) continue;
    const avgLen = stringVals.reduce((a, s) => a + s.length, 0) / stringVals.length;
    const nameHintsText = /(text|comment|abstract|title|description|review|note|content|summary)/i.test(col);
    if (avgLen >= 20 || nameHintsText) textCols.push(col);
  }
  return textCols;
}

function topTerms(texts: string[], topN = 10): Array<[string, number]> {
  const stop = new Set(["the", "and", "for", "with", "that", "this", "from", "are", "was", "were", "have", "has", "had", "not", "but", "you", "your", "our", "their", "its", "into", "than", "also", "can", "could", "would", "should", "about", "between", "within", "using"]);
  const freq = new Map<string, number>();
  for (const text of texts) {
    const tokens = text.toLowerCase().match(/[a-z][a-z0-9_-]{2,}/g) || [];
    for (const token of tokens) {
      if (stop.has(token)) continue;
      freq.set(token, (freq.get(token) || 0) + 1);
    }
  }
  return Array.from(freq.entries()).sort((a, b) => b[1] - a[1]).slice(0, topN);
}

function getPrimaryDataset(allData: ParsedDataset[]): ParsedDataset | null {
  if (allData.length === 0) return null;
  let best: ParsedDataset | null = null;
  let bestScore = -Infinity;
  for (const ds of allData) {
    if (!ds || ds.data.length === 0) continue;
    const { numericCols, categoricalCols, idCols } = classifyColumns(ds.data, ds.columns);
    const meaningfulNumeric = numericCols.filter(c => !idCols.includes(c)).length;
    const score = ds.totalRows + meaningfulNumeric * 300 + categoricalCols.length * 50;
    if (score > bestScore) {
      bestScore = score;
      best = ds;
    }
  }
  return best || allData[0];
}

function generateDefaultCharts(
  allData: { name: string; data: Record<string, any>[]; columns: string[]; totalRows: number }[],
  executableMethods: Set<string> | null
): { name: string; description: string; config: any }[] {
  const charts: { name: string; description: string; config: any }[] = [];
  const ds = getPrimaryDataset(allData);
  if (!ds || ds.data.length === 0) return charts;

  const { numericCols: rawNumericCols, categoricalCols, idCols } = classifyColumns(ds.data, ds.columns);
  // Exclude ID/code columns from analysis - they are not meaningful for statistics
  const numericCols = rawNumericCols.filter(c => !idCols.includes(c));

  // Chart 1: Distribution of first numeric column (histogram-like bar chart)
  if (numericCols.length > 0 && methodAllowed(executableMethods, "descriptive_statistics")) {
    const col = numericCols[0];
    const values = ds.data.map(r => Number(r[col])).filter(v => !isNaN(v));
    if (values.length > 0) {
      const min = Math.min(...values);
      const max = Math.max(...values);
      const binCount = Math.min(20, Math.ceil(Math.sqrt(values.length)));
      const binWidth = (max - min) / binCount || 1;
      const bins = Array(binCount).fill(0);
      const labels: string[] = [];

      for (let i = 0; i < binCount; i++) {
        const lo = min + i * binWidth;
        const hi = lo + binWidth;
        labels.push(`${lo.toFixed(1)}-${hi.toFixed(1)}`);
      }
      for (const v of values) {
        const idx = Math.min(Math.floor((v - min) / binWidth), binCount - 1);
        bins[idx]++;
      }

      // Truncate column name for display
      const displayCol = col.length > 40 ? col.slice(0, 37) + "..." : col;

      charts.push({
        name: "distribution_histogram",
        description: `Distribution of ${displayCol} (n=${values.length})`,
        config: {
          type: "bar",
          data: {
            labels,
            datasets: [{
              label: displayCol,
              data: bins,
              backgroundColor: "rgba(78, 121, 167, 0.7)",
              borderColor: "rgba(78, 121, 167, 1)",
              borderWidth: 1,
            }],
          },
          options: {
            plugins: { title: { display: true, text: `Distribution of ${displayCol}`, font: { size: 16 } } },
            scales: { y: { title: { display: true, text: "Frequency" } }, x: { title: { display: true, text: displayCol } } },
          },
        },
      });
    }
  }

  // Chart 2: Scatter plot of best-correlated numeric pair (with regression line if applicable)
  if (numericCols.length >= 2 && methodAllowed(executableMethods, "correlation")) {
    // Find the pair with highest absolute correlation (scan up to 15 pairs)
    let bestAbsCorr = -1;
    let xCol = numericCols[0];
    let yCol = numericCols[1];
    const pairLimit = Math.min(numericCols.length, 6); // up to C(6,2)=15 pairs
    for (let ai = 0; ai < pairLimit; ai++) {
      for (let bi = ai + 1; bi < pairLimit; bi++) {
        const pairs = parseNumericPairs(ds, numericCols[ai], numericCols[bi]);
        if (pairs.length < 5) continue;
        const reg = regressionStatsFromPairs(pairs);
        const absCorr = reg ? Math.sqrt(reg.r2) : 0;
        if (absCorr > bestAbsCorr) {
          bestAbsCorr = absCorr;
          xCol = numericCols[ai];
          yCol = numericCols[bi];
        }
      }
    }
    const points = ds.data
      .map(r => ({ x: Number(r[xCol]), y: Number(r[yCol]) }))
      .filter(p => !isNaN(p.x) && !isNaN(p.y) && isFinite(p.x) && isFinite(p.y))
      .slice(0, 500);

    if (points.length >= 5) {
      const displayXCol = xCol.length > 30 ? xCol.slice(0, 27) + "..." : xCol;
      const displayYCol = yCol.length > 30 ? yCol.slice(0, 27) + "..." : yCol;

      const datasets: any[] = [{
        label: `${displayXCol} vs ${displayYCol}`,
        data: points,
        backgroundColor: "rgba(242, 142, 43, 0.6)",
        borderColor: "rgba(242, 142, 43, 1)",
        pointRadius: 3,
        type: "scatter",
      }];

      // Add regression line if linear_regression is executable
      let titleSuffix = "";
      if (methodAllowed(executableMethods, "linear_regression") && points.length >= 10) {
        const pairs: [number, number][] = points.map(p => [p.x, p.y]);
        const reg = regressionStatsFromPairs(pairs);
        if (reg && reg.r2 > 0) {
          const xMin = Math.min(...points.map(p => p.x));
          const xMax = Math.max(...points.map(p => p.x));
          const linePoints = [
            { x: xMin, y: reg.intercept + reg.slope * xMin },
            { x: xMax, y: reg.intercept + reg.slope * xMax },
          ];
          datasets.push({
            label: `y = ${reg.slope.toFixed(3)}x + ${reg.intercept.toFixed(3)} (R²=${reg.r2.toFixed(3)})`,
            data: linePoints,
            type: "scatter",
            showLine: true,
            pointRadius: 0,
            borderColor: "rgba(225, 87, 89, 1)",
            borderWidth: 2,
            borderDash: [6, 3],
            fill: false,
          });
          titleSuffix = ` (R²=${reg.r2.toFixed(3)})`;
        }
      }

      charts.push({
        name: "scatter_plot",
        description: `Scatter plot: ${displayXCol} vs ${displayYCol}${titleSuffix}`,
        config: {
          type: "scatter",
          data: { datasets },
          options: {
            plugins: { title: { display: true, text: `${displayXCol} vs ${displayYCol}${titleSuffix}`, font: { size: 16 } } },
            scales: {
              x: { title: { display: true, text: displayXCol } },
              y: { title: { display: true, text: displayYCol } },
            },
          },
        },
      });
    }
  }

  // Chart 3: Bar chart by categorical column (if available)
  if (categoricalCols.length > 0 && numericCols.length > 0 && methodAllowed(executableMethods, "group_comparison")) {
    const catCol = categoricalCols[0];
    const numCol = numericCols[0];
    const groups: Record<string, number[]> = {};
    for (const row of ds.data) {
      const key = String(row[catCol] ?? "N/A").slice(0, 30);
      if (!groups[key]) groups[key] = [];
      groups[key].push(Number(row[numCol]));
    }
    const sortedKeys = Object.keys(groups).sort().slice(0, 20);
    const means = sortedKeys.map(k => {
      const vals = groups[k].filter(v => !isNaN(v));
      return vals.length > 0 ? vals.reduce((a, b) => a + b, 0) / vals.length : 0;
    });

    const displayCatCol = catCol.length > 30 ? catCol.slice(0, 27) + "..." : catCol;
    const displayNumCol = numCol.length > 30 ? numCol.slice(0, 27) + "..." : numCol;

    charts.push({
      name: "category_comparison",
      description: `Mean ${displayNumCol} by ${displayCatCol}`,
      config: {
        type: "bar",
        data: {
          labels: sortedKeys,
          datasets: [{
            label: `Mean ${displayNumCol}`,
            data: means.map(m => Math.round(m * 100) / 100),
            backgroundColor: "rgba(225, 87, 89, 0.7)",
            borderColor: "rgba(225, 87, 89, 1)",
            borderWidth: 1,
          }],
        },
        options: {
          plugins: { title: { display: true, text: `Mean ${displayNumCol} by ${displayCatCol}`, font: { size: 16 } } },
          scales: { y: { title: { display: true, text: `Mean ${displayNumCol}` } } },
        },
      },
    });
  }

  // Chart 4: Correlation bubble heatmap (if multiple numeric cols)
  if (numericCols.length >= 3 && methodAllowed(executableMethods, "correlation")) {
    const cols = numericCols.slice(0, 8);
    const displayCols = cols.map(c => c.length > 15 ? c.slice(0, 12) + "..." : c);
    const bubbleData: { x: number; y: number; r: number; corr: number }[] = [];
    for (let ci = 0; ci < cols.length; ci++) {
      for (let cj = 0; cj < cols.length; cj++) {
        const pairs = parseNumericPairs(ds, cols[ci], cols[cj]);
        const n = pairs.length;
        let corr = ci === cj ? 1 : 0;
        if (ci !== cj && n >= 3) {
          const m1 = pairs.reduce((s, p) => s + p[0], 0) / n;
          const m2 = pairs.reduce((s, p) => s + p[1], 0) / n;
          let num = 0, d1 = 0, d2 = 0;
          for (const [a, b] of pairs) {
            num += (a - m1) * (b - m2);
            d1 += (a - m1) ** 2;
            d2 += (b - m2) ** 2;
          }
          corr = d1 > 0 && d2 > 0 ? num / Math.sqrt(d1 * d2) : 0;
        }
        corr = Math.round(corr * 100) / 100;
        bubbleData.push({ x: ci, y: cj, r: Math.max(3, Math.abs(corr) * 18), corr });
      }
    }

    // Separate positive and negative correlations into two datasets
    const positivePoints = bubbleData.filter(d => d.corr >= 0);
    const negativePoints = bubbleData.filter(d => d.corr < 0);

    charts.push({
      name: "correlation_matrix",
      description: `Correlation bubble heatmap of numeric variables`,
      config: {
        type: "bubble",
        data: {
          datasets: [
            {
              label: "Positive correlation",
              data: positivePoints,
              backgroundColor: "rgba(78, 121, 167, 0.6)",
              borderColor: "rgba(78, 121, 167, 1)",
              borderWidth: 1,
            },
            {
              label: "Negative correlation",
              data: negativePoints,
              backgroundColor: "rgba(225, 87, 89, 0.6)",
              borderColor: "rgba(225, 87, 89, 1)",
              borderWidth: 1,
            },
          ],
        },
        options: {
          plugins: {
            title: { display: true, text: "Correlation Matrix (bubble size = |r|)", font: { size: 16 } },
            tooltip: {
              callbacks: {
                label: (ctx: any) => {
                  const d = ctx.raw;
                  return `r = ${d.corr}`;
                },
              },
            },
          },
          scales: {
            x: {
              type: "linear",
              min: -0.5,
              max: cols.length - 0.5,
              ticks: {
                stepSize: 1,
                callback: (val: number) => displayCols[val] || "",
              },
              title: { display: false },
            },
            y: {
              type: "linear",
              min: -0.5,
              max: cols.length - 0.5,
              ticks: {
                stepSize: 1,
                callback: (val: number) => displayCols[val] || "",
              },
              title: { display: false },
              reverse: true,
            },
          },
        },
      },
    });
  }

  // Chart 5: Pie chart of first categorical column (if available)
  if (categoricalCols.length > 0 && methodAllowed(executableMethods, "descriptive_statistics")) {
    const catCol = categoricalCols[0];
    const counts: Record<string, number> = {};
    for (const row of ds.data) {
      const key = String(row[catCol] ?? "N/A").slice(0, 30);
      counts[key] = (counts[key] || 0) + 1;
    }
    const sorted = Object.entries(counts).sort((a, b) => b[1] - a[1]);
    const topN = sorted.slice(0, 10);
    const otherCount = sorted.slice(10).reduce((sum, [, c]) => sum + c, 0);
    if (otherCount > 0) topN.push(["Other", otherCount]);

    const displayCatCol = catCol.length > 30 ? catCol.slice(0, 27) + "..." : catCol;
    const pieColors = ["#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f", "#edc949", "#af7aa1", "#ff9da7", "#9c755f", "#bab0ab", "#86bcb6"];

    charts.push({
      name: "category_distribution",
      description: `Distribution of ${displayCatCol}`,
      config: {
        type: "pie",
        data: {
          labels: topN.map(([k]) => k),
          datasets: [{
            data: topN.map(([, v]) => v),
            backgroundColor: pieColors.slice(0, topN.length),
          }],
        },
        options: {
          plugins: { title: { display: true, text: `Distribution of ${displayCatCol}`, font: { size: 16 } } },
        },
      },
    });
  }

  // Chart 6: Time trend line chart (if time-like columns exist)
  const timeCols = ds.columns.filter(c => /(year|month|date|time|wave|period|quarter)/i.test(c));
  if (timeCols.length > 0 && numericCols.length > 0 && methodAllowed(executableMethods, "time_trend")) {
    const timeCol = timeCols[0];
    const numCol = numericCols[0];

    // Aggregate by time value (mean of numeric col per time point)
    const timeGroups: Record<string, number[]> = {};
    for (const row of ds.data) {
      const tRaw = row[timeCol];
      const val = Number(row[numCol]);
      if (tRaw === null || tRaw === undefined || tRaw === "" || isNaN(val)) continue;
      const tKey = String(tRaw);
      if (!timeGroups[tKey]) timeGroups[tKey] = [];
      timeGroups[tKey].push(val);
    }
    const sortedTimeKeys = Object.keys(timeGroups).sort((a, b) => {
      const na = Number(a), nb = Number(b);
      if (!isNaN(na) && !isNaN(nb)) return na - nb;
      return a.localeCompare(b);
    }).slice(0, 50);
    const timeMeans = sortedTimeKeys.map(k => {
      const vals = timeGroups[k];
      return Math.round((vals.reduce((a, b) => a + b, 0) / vals.length) * 1000) / 1000;
    });

    if (sortedTimeKeys.length >= 3) {
      const displayTimeCol = timeCol.length > 30 ? timeCol.slice(0, 27) + "..." : timeCol;
      const displayNumCol = numCol.length > 30 ? numCol.slice(0, 27) + "..." : numCol;

      // Compute linear trend line
      const trendDatasets: any[] = [{
        label: `Mean ${displayNumCol}`,
        data: timeMeans,
        borderColor: "#4e79a7",
        backgroundColor: "rgba(78, 121, 167, 0.1)",
        tension: 0.1,
        fill: true,
      }];

      // Add linear trend line
      if (sortedTimeKeys.length >= 5) {
        const xVals = sortedTimeKeys.map((_, i) => i);
        const trendPairs: [number, number][] = xVals.map((x, i) => [x, timeMeans[i]]);
        const trendReg = regressionStatsFromPairs(trendPairs);
        if (trendReg) {
          const trendLine = xVals.map(x => Math.round((trendReg.intercept + trendReg.slope * x) * 1000) / 1000);
          trendDatasets.push({
            label: `Trend (slope=${trendReg.slope.toFixed(3)})`,
            data: trendLine,
            borderColor: "rgba(225, 87, 89, 0.8)",
            borderWidth: 2,
            borderDash: [6, 3],
            pointRadius: 0,
            fill: false,
          });
        }
      }

      charts.push({
        name: "time_trend",
        description: `Trend of ${displayNumCol} over ${displayTimeCol}`,
        config: {
          type: "line",
          data: {
            labels: sortedTimeKeys,
            datasets: trendDatasets,
          },
          options: {
            plugins: { title: { display: true, text: `Trend of ${displayNumCol} over ${displayTimeCol}`, font: { size: 16 } } },
            scales: {
              x: { title: { display: true, text: displayTimeCol } },
              y: { title: { display: true, text: `Mean ${displayNumCol}` } },
            },
          },
        },
      });
    }
  }

  // Chart 7: Box plot approximation (floating bar showing Q1–Q3 with median marker)
  if (categoricalCols.length > 0 && numericCols.length > 0 && methodAllowed(executableMethods, "group_comparison")) {
    const catCol = categoricalCols[0];
    const numCol = numericCols[0];
    const groups: Record<string, number[]> = {};
    for (const row of ds.data) {
      const key = String(row[catCol] ?? "N/A").slice(0, 30);
      const val = Number(row[numCol]);
      if (isNaN(val)) continue;
      if (!groups[key]) groups[key] = [];
      groups[key].push(val);
    }
    const sortedKeys = Object.keys(groups).sort().slice(0, 15);
    const boxStats = sortedKeys.map(k => {
      const vals = groups[k].sort((a, b) => a - b);
      const q1Idx = Math.floor(vals.length * 0.25);
      const medIdx = Math.floor(vals.length * 0.5);
      const q3Idx = Math.floor(vals.length * 0.75);
      return {
        q1: vals[q1Idx] ?? 0,
        median: vals[medIdx] ?? 0,
        q3: vals[q3Idx] ?? 0,
        min: vals[0] ?? 0,
        max: vals[vals.length - 1] ?? 0,
      };
    });

    if (boxStats.length >= 2) {
      const displayCatCol = catCol.length > 30 ? catCol.slice(0, 27) + "..." : catCol;
      const displayNumCol = numCol.length > 30 ? numCol.slice(0, 27) + "..." : numCol;

      charts.push({
        name: "box_plot_approx",
        description: `Box plot of ${displayNumCol} by ${displayCatCol}`,
        config: {
          type: "bar",
          data: {
            labels: sortedKeys,
            datasets: [
              {
                label: "Q1–Q3 range",
                data: boxStats.map(s => [s.q1, s.q3]),
                backgroundColor: "rgba(78, 121, 167, 0.5)",
                borderColor: "rgba(78, 121, 167, 1)",
                borderWidth: 1,
                borderSkipped: false,
              },
              {
                label: "Median",
                data: boxStats.map(s => s.median),
                type: "line",
                borderColor: "rgba(225, 87, 89, 1)",
                backgroundColor: "rgba(225, 87, 89, 0.8)",
                pointRadius: 5,
                pointStyle: "rectRot",
                showLine: false,
              },
            ],
          },
          options: {
            plugins: { title: { display: true, text: `Box Plot: ${displayNumCol} by ${displayCatCol}`, font: { size: 16 } } },
            scales: {
              y: { title: { display: true, text: displayNumCol } },
              x: { title: { display: true, text: displayCatCol } },
            },
          },
        },
      });
    }
  }

  // Chart 8: Grouped bar chart (multiple numeric variables by category)
  if (categoricalCols.length > 0 && numericCols.length >= 2 && methodAllowed(executableMethods, "group_comparison")) {
    const catCol = categoricalCols[0];
    const useCols = numericCols.slice(0, 4); // Up to 4 numeric vars
    const groups: Record<string, Record<string, number[]>> = {};
    for (const row of ds.data) {
      const key = String(row[catCol] ?? "N/A").slice(0, 30);
      if (!groups[key]) groups[key] = {};
      for (const nc of useCols) {
        if (!groups[key][nc]) groups[key][nc] = [];
        const val = Number(row[nc]);
        if (!isNaN(val)) groups[key][nc].push(val);
      }
    }
    const sortedKeys = Object.keys(groups).sort().slice(0, 15);
    const barColors = ["rgba(78, 121, 167, 0.7)", "rgba(242, 142, 43, 0.7)", "rgba(225, 87, 89, 0.7)", "rgba(118, 183, 178, 0.7)"];
    const borderColors = ["rgba(78, 121, 167, 1)", "rgba(242, 142, 43, 1)", "rgba(225, 87, 89, 1)", "rgba(118, 183, 178, 1)"];

    if (sortedKeys.length >= 2) {
      const displayCatCol = catCol.length > 30 ? catCol.slice(0, 27) + "..." : catCol;

      charts.push({
        name: "grouped_bar_multivar",
        description: `Grouped comparison of ${useCols.length} variables by ${displayCatCol}`,
        config: {
          type: "bar",
          data: {
            labels: sortedKeys,
            datasets: useCols.map((nc, idx) => {
              const displayNc = nc.length > 20 ? nc.slice(0, 17) + "..." : nc;
              return {
                label: `Mean ${displayNc}`,
                data: sortedKeys.map(k => {
                  const vals = (groups[k]?.[nc] || []).filter(v => !isNaN(v));
                  return vals.length > 0 ? Math.round((vals.reduce((a, b) => a + b, 0) / vals.length) * 100) / 100 : 0;
                }),
                backgroundColor: barColors[idx % barColors.length],
                borderColor: borderColors[idx % borderColors.length],
                borderWidth: 1,
              };
            }),
          },
          options: {
            plugins: { title: { display: true, text: `Comparison by ${displayCatCol}`, font: { size: 16 } } },
            scales: { y: { title: { display: true, text: "Mean value" } }, x: { title: { display: true, text: displayCatCol } } },
          },
        },
      });
    }
  }

  // Chart 9: Density plot (KDE approximation) for first numeric column
  if (numericCols.length > 0 && methodAllowed(executableMethods, "descriptive_statistics")) {
    const col = numericCols[0];
    const values = ds.data.map(r => Number(r[col])).filter(v => !isNaN(v));
    if (values.length >= 20) {
      const sorted = [...values].sort((a, b) => a - b);
      const min = sorted[0];
      const max = sorted[sorted.length - 1];
      const range = max - min || 1;
      // Silverman's rule of thumb for bandwidth
      const std = Math.sqrt(values.reduce((s, v) => s + (v - values.reduce((a, b) => a + b, 0) / values.length) ** 2, 0) / values.length);
      const bandwidth = 1.06 * std * Math.pow(values.length, -0.2) || range / 20;
      const gridSize = 50;
      const step = range / gridSize;
      const xPoints: number[] = [];
      const yPoints: number[] = [];
      for (let i = 0; i <= gridSize; i++) {
        const x = min + i * step;
        xPoints.push(Math.round(x * 100) / 100);
        // Gaussian KDE
        let density = 0;
        for (const v of values) {
          const u = (x - v) / bandwidth;
          density += Math.exp(-0.5 * u * u) / (bandwidth * Math.sqrt(2 * Math.PI));
        }
        density /= values.length;
        yPoints.push(Math.round(density * 10000) / 10000);
      }

      const displayCol = col.length > 40 ? col.slice(0, 37) + "..." : col;
      charts.push({
        name: "density_plot",
        description: `Density estimate of ${displayCol} (KDE, n=${values.length})`,
        config: {
          type: "line",
          data: {
            labels: xPoints.map(String),
            datasets: [{
              label: `Density of ${displayCol}`,
              data: yPoints,
              borderColor: "rgba(118, 183, 178, 1)",
              backgroundColor: "rgba(118, 183, 178, 0.3)",
              fill: true,
              tension: 0.4,
              pointRadius: 0,
            }],
          },
          options: {
            plugins: { title: { display: true, text: `Density Estimate: ${displayCol}`, font: { size: 16 } } },
            scales: {
              x: { title: { display: true, text: displayCol } },
              y: { title: { display: true, text: "Density" } },
            },
          },
        },
      });
    }
  }

  // Chart 10: Pairwise scatter matrix (top correlated pairs beyond the main scatter)
  if (numericCols.length >= 3 && methodAllowed(executableMethods, "correlation")) {
    // Collect all pairwise correlations
    const pairCorrs: { a: string; b: string; absCorr: number; corr: number }[] = [];
    const pairLimit = Math.min(numericCols.length, 8);
    for (let ai = 0; ai < pairLimit; ai++) {
      for (let bi = ai + 1; bi < pairLimit; bi++) {
        const pairs = parseNumericPairs(ds, numericCols[ai], numericCols[bi]);
        if (pairs.length < 5) continue;
        const reg = regressionStatsFromPairs(pairs);
        if (!reg) continue;
        const sign = reg.slope >= 0 ? 1 : -1;
        const corr = sign * Math.sqrt(reg.r2);
        pairCorrs.push({ a: numericCols[ai], b: numericCols[bi], absCorr: Math.abs(corr), corr: Math.round(corr * 100) / 100 });
      }
    }
    // Sort by absolute correlation, take top 4 (skip first since Chart 2 already shows it)
    pairCorrs.sort((a, b) => b.absCorr - a.absCorr);
    const topPairs = pairCorrs.slice(1, 5); // skip the #1 pair already shown in Chart 2

    if (topPairs.length >= 2) {
      // Create a multi-scatter with up to 4 sub-series
      const scatterColors = ["rgba(78, 121, 167, 0.5)", "rgba(242, 142, 43, 0.5)", "rgba(225, 87, 89, 0.5)", "rgba(118, 183, 178, 0.5)"];
      const scatterBorders = ["rgba(78, 121, 167, 1)", "rgba(242, 142, 43, 1)", "rgba(225, 87, 89, 1)", "rgba(118, 183, 178, 1)"];
      const scatterDatasets = topPairs.map((pc, idx) => {
        const points = parseNumericPairs(ds, pc.a, pc.b).slice(0, 200).map(([x, y]) => ({ x, y }));
        const da = pc.a.length > 12 ? pc.a.slice(0, 10) + ".." : pc.a;
        const db = pc.b.length > 12 ? pc.b.slice(0, 10) + ".." : pc.b;
        return {
          label: `${da} vs ${db} (r=${pc.corr})`,
          data: points,
          backgroundColor: scatterColors[idx],
          borderColor: scatterBorders[idx],
          pointRadius: 2,
        };
      });

      charts.push({
        name: "pairwise_scatter",
        description: `Pairwise scatter of top correlated variable pairs`,
        config: {
          type: "scatter",
          data: { datasets: scatterDatasets },
          options: {
            plugins: { title: { display: true, text: "Pairwise Scatter (Top Correlations)", font: { size: 16 } } },
          },
        },
      });
    }
  }

  return charts;
}

function buildRoutingDiagnostics(
  datasets: ParsedDataset[],
  metrics: Record<string, number | string>,
  charts: { name: string }[],
  tables: { name: string }[],
  executableMethods: Set<string> | null,
  methodContract?: MethodFeasibilityContractInput | null
): {
  executedMethods: string[];
  blockedMethods: string[];
  unresolvedPrerequisites: string[];
  skippedExecutableMethods: string[];
  noOutputReasons: string[];
} {
  const executedMethods: string[] = [];
  const metricKeys = Object.keys(metrics);
  if (metricKeys.some(k => k.startsWith("mean_") || k.startsWith("std_") || k.startsWith("median_"))) executedMethods.push("descriptive_statistics");
  if (metricKeys.some(k => k.startsWith("strongest_correlation_"))) executedMethods.push("correlation");
  if (metricKeys.some(k => k.startsWith("regression_"))) executedMethods.push("linear_regression");
  if (metricKeys.some(k => k.startsWith("anova_"))) executedMethods.push("group_comparison");
  if (metricKeys.some(k => k.startsWith("time_trend_"))) executedMethods.push("time_trend");
  if (metricKeys.some(k => k.startsWith("text_") || k.startsWith("top_term_"))) executedMethods.push("text_feature_analysis");
  if (charts.length > 0) executedMethods.push("data_visualisation");

  const executedSet = new Set(executedMethods.map(normaliseMethodId));
  const blockedMethods = Array.from(
    new Set([
      ...(methodContract?.requiresMissingData || []),
      ...(methodContract?.futureWorkOnly || []),
    ].map(normaliseMethodId).filter(Boolean))
  );

  const unresolvedPrerequisites: string[] = [];
  const allCols = datasets.flatMap(ds => ds.columns || []).map(c => c.toLowerCase());
  const hasTimeLike = allCols.some(c => /(year|month|date|time|wave|period|quarter)/i.test(c));
  const hasTextLike = allCols.some(c => /(text|comment|abstract|title|description|review|note|content|summary)/i.test(c));
  const hasGraphLike = allCols.some(c => /(node|edge|source|target|network|graph)/i.test(c));
  const hasImageLike = allCols.some(c => /(image|img|pixel|vision|frame|video|path)/i.test(c));
  const hasPanelLike = hasTimeLike && allCols.some(c => /(id|code|entity|respondent|household|firm|user|patient)/i.test(c));

  if (blockedMethods.includes("advanced_nlp") && !hasTextLike) unresolvedPrerequisites.push("advanced_nlp: text columns not detected");
  if (blockedMethods.includes("advanced_time_series") && !hasTimeLike) unresolvedPrerequisites.push("advanced_time_series: time index not detected");
  if (blockedMethods.includes("panel_econometrics") && !hasPanelLike) unresolvedPrerequisites.push("panel_econometrics: panel identifiers/time pairing not detected");
  if (blockedMethods.includes("graph_modelling") && !hasGraphLike) unresolvedPrerequisites.push("graph_modelling: graph edge/node structure not detected");
  if (blockedMethods.includes("vision_analysis") && !hasImageLike) unresolvedPrerequisites.push("vision_analysis: image/path features not detected");
  if (blockedMethods.includes("causal_inference")) unresolvedPrerequisites.push("causal_inference: identification assumptions not met");

  const requested = executableMethods ? Array.from(executableMethods) : [];
  const skippedExecutableMethods = requested.filter(m => !executedSet.has(normaliseMethodId(m)));
  const noOutputReasons: string[] = [];
  if (skippedExecutableMethods.length > 0) {
    noOutputReasons.push(`Executable methods without observed outputs: ${skippedExecutableMethods.join(", ")}`);
  }
  if (charts.length === 0) noOutputReasons.push("No charts were generated from executable methods.");
  if (tables.length === 0) noOutputReasons.push("No tables were generated from executable methods.");
  if (metricKeys.length === 0) noOutputReasons.push("No metrics were generated from executable methods.");

  return {
    executedMethods: Array.from(new Set(executedMethods.map(normaliseMethodId))).filter(Boolean),
    blockedMethods,
    unresolvedPrerequisites,
    skippedExecutableMethods,
    noOutputReasons,
  };
}

function generateDefaultTables(
  allData: { name: string; data: Record<string, any>[]; columns: string[]; totalRows: number }[],
  executableMethods: Set<string> | null
): { name: string; description: string; headers: string[]; rows: (string | number)[][] }[] {
  const tables: { name: string; description: string; headers: string[]; rows: (string | number)[][] }[] = [];
  const ds = getPrimaryDataset(allData);
  if (!ds || ds.data.length === 0) return tables;

  const { numericCols: rawNumericCols, categoricalCols, idCols: _idCols2 } = classifyColumns(ds.data, ds.columns);
  // Exclude ID/code columns from tables - they are not meaningful for descriptive statistics
  const numericCols = rawNumericCols.filter(c => !_idCols2.includes(c));

  // Table 1: Descriptive statistics of numeric variables
  if (numericCols.length > 0 && methodAllowed(executableMethods, "descriptive_statistics")) {
    const headers = ["Variable", "N", "Mean", "Std Dev", "Min", "Q1", "Median", "Q3", "Max", "Skewness"];
    const rows: (string | number)[][] = [];

    for (const col of numericCols.slice(0, 20)) {
      const values = ds.data.map(r => Number(r[col])).filter(v => !isNaN(v));
      if (values.length === 0) continue;
      const sorted = [...values].sort((a, b) => a - b);
      const n = values.length;
      const mean = values.reduce((a, b) => a + b, 0) / n;
      const variance = values.reduce((a, b) => a + (b - mean) ** 2, 0) / (n - 1 || 1);
      const std = Math.sqrt(variance);
      const median = n % 2 === 0 ? (sorted[n / 2 - 1] + sorted[n / 2]) / 2 : sorted[Math.floor(n / 2)];
      const q1 = sorted[Math.floor(n * 0.25)];
      const q3 = sorted[Math.floor(n * 0.75)];

      // Skewness
      let skewness = 0;
      if (std > 0 && n > 2) {
        const m3 = values.reduce((a, b) => a + ((b - mean) / std) ** 3, 0) / n;
        skewness = m3;
      }

      const displayCol = col.length > 30 ? col.slice(0, 27) + "..." : col;

      rows.push([
        displayCol,
        n,
        Math.round(mean * 1000) / 1000,
        Math.round(std * 1000) / 1000,
        Math.round(sorted[0] * 1000) / 1000,
        Math.round(q1 * 1000) / 1000,
        Math.round(median * 1000) / 1000,
        Math.round(q3 * 1000) / 1000,
        Math.round(sorted[n - 1] * 1000) / 1000,
        Math.round(skewness * 1000) / 1000,
      ]);
    }

    if (rows.length > 0) {
      tables.push({
        name: "descriptive_statistics",
        description: "Descriptive statistics of numeric variables",
        headers,
        rows,
      });
    }
  }

  // Table 2: Cross-tabulation / frequency table of categorical columns
  if (categoricalCols.length > 0 && methodAllowed(executableMethods, "group_comparison")) {
    const catCol = categoricalCols[0];
    const counts: Record<string, number> = {};
    for (const row of ds.data) {
      const key = String(row[catCol] ?? "N/A").slice(0, 50);
      counts[key] = (counts[key] || 0) + 1;
    }
    const sortedKeys = Object.entries(counts).sort((a, b) => b[1] - a[1]).slice(0, 30);
    const total = ds.data.length;

    const displayCatCol = catCol.length > 30 ? catCol.slice(0, 27) + "..." : catCol;

    tables.push({
      name: "frequency_table",
      description: `Frequency distribution of ${displayCatCol}`,
      headers: [displayCatCol, "Count", "Percentage", "Cumulative %"],
      rows: (() => {
        let cumPct = 0;
        return sortedKeys.map(([key, count]) => {
          const pct = (count / total) * 100;
          cumPct += pct;
          return [
            key,
            count,
            `${pct.toFixed(1)}%`,
            `${cumPct.toFixed(1)}%`,
          ];
        });
      })(),
    });
  }

  // Table 3: Correlation matrix (if enough numeric columns)
  if (numericCols.length >= 2 && methodAllowed(executableMethods, "correlation")) {
    const cols = numericCols.slice(0, 10);
    const displayCols = cols.map(c => c.length > 15 ? c.slice(0, 12) + "..." : c);
    const headers = ["Variable", ...displayCols];
    const rows: (string | number)[][] = [];

    for (let i = 0; i < cols.length; i++) {
      const row: (string | number)[] = [displayCols[i]];
      for (let j = 0; j < cols.length; j++) {
        const v1 = ds.data.map(r => Number(r[cols[i]])).filter(v => !isNaN(v));
        const v2 = ds.data.map(r => Number(r[cols[j]])).filter(v => !isNaN(v));
        const n = Math.min(v1.length, v2.length);
        if (n < 3) { row.push(0); continue; }
        const m1 = v1.slice(0, n).reduce((a, b) => a + b, 0) / n;
        const m2 = v2.slice(0, n).reduce((a, b) => a + b, 0) / n;
        let num = 0, d1 = 0, d2 = 0;
        for (let k = 0; k < n; k++) {
          num += (v1[k] - m1) * (v2[k] - m2);
          d1 += (v1[k] - m1) ** 2;
          d2 += (v2[k] - m2) ** 2;
        }
        const corr = d1 > 0 && d2 > 0 ? num / Math.sqrt(d1 * d2) : 0;
        row.push(Math.round(corr * 1000) / 1000);
      }
      rows.push(row);
    }

    tables.push({
      name: "correlation_matrix",
      description: "Pearson correlation matrix of numeric variables",
      headers,
      rows,
    });
  }

  // Table 4: Regression results (if linear_regression is executable)
  if (numericCols.length >= 2 && methodAllowed(executableMethods, "linear_regression")) {
    const regressionHeaders = ["Dep. Var", "Indep. Var", "Coeff (beta)", "Std. Error", "t-stat", "p-value", "R²", "Adj. R²", "N"];
    const regressionRows: (string | number)[][] = [];

    const pairsEvaluated: { xCol: string; yCol: string; reg: { slope: number; intercept: number; r2: number; n: number }; se: number; tStat: number; pValue: number }[] = [];

    for (let i = 0; i < Math.min(numericCols.length, 6); i++) {
      for (let j = i + 1; j < Math.min(numericCols.length, 6); j++) {
        const xCol = numericCols[i];
        const yCol = numericCols[j];
        const pairs = parseNumericPairs(ds, xCol, yCol);
        const reg = regressionStatsFromPairs(pairs);
        if (!reg || reg.n < 10) continue;

        // Compute standard error, t-statistic, p-value
        let ssRes = 0;
        let ssX = 0;
        const meanX = pairs.reduce((a, p) => a + p[0], 0) / reg.n;
        for (const [x, y] of pairs) {
          const pred = reg.intercept + reg.slope * x;
          ssRes += (y - pred) ** 2;
          ssX += (x - meanX) ** 2;
        }
        const mse = ssRes / (reg.n - 2);
        const se = ssX > 0 ? Math.sqrt(mse / ssX) : 0;
        const tStat = se > 0 ? reg.slope / se : 0;
        const pValue = approximateCorrelationPValue(Math.sqrt(reg.r2) * Math.sign(reg.slope), reg.n);

        pairsEvaluated.push({ xCol, yCol, reg, se, tStat, pValue });
      }
    }

    // Sort by R² descending, take top 5
    pairsEvaluated.sort((a, b) => b.reg.r2 - a.reg.r2);
    for (const pe of pairsEvaluated.slice(0, 5)) {
      const displayX = pe.xCol.length > 15 ? pe.xCol.slice(0, 12) + "..." : pe.xCol;
      const displayY = pe.yCol.length > 15 ? pe.yCol.slice(0, 12) + "..." : pe.yCol;
      const adjR2 = 1 - (1 - pe.reg.r2) * (pe.reg.n - 1) / (pe.reg.n - 2);
      const pStr = pe.pValue < 0.001 ? "<0.001" : pe.pValue.toFixed(4);

      regressionRows.push([
        displayY,
        displayX,
        Math.round(pe.reg.slope * 10000) / 10000,
        Math.round(pe.se * 10000) / 10000,
        Math.round(pe.tStat * 1000) / 1000,
        pStr,
        Math.round(pe.reg.r2 * 1000) / 1000,
        Math.round(adjR2 * 1000) / 1000,
        pe.reg.n,
      ]);
    }

    if (regressionRows.length > 0) {
      tables.push({
        name: "regression_results",
        description: "OLS regression results (top models by R²)",
        headers: regressionHeaders,
        rows: regressionRows,
      });
    }
  }

  // Table 5: Group comparison with significance (if group_comparison is executable)
  if (categoricalCols.length > 0 && numericCols.length > 0 && methodAllowed(executableMethods, "group_comparison")) {
    const catCol = categoricalCols[0];
    const numCol = numericCols[0];

    const groups: Record<string, number[]> = {};
    for (const row of ds.data) {
      const key = String(row[catCol] ?? "").trim();
      const val = Number(row[numCol]);
      if (!key || isNaN(val)) continue;
      if (!groups[key]) groups[key] = [];
      groups[key].push(val);
    }

    const validGroupEntries = Object.entries(groups).filter(([, v]) => v.length >= 3).sort((a, b) => b[1].length - a[1].length).slice(0, 15);

    if (validGroupEntries.length >= 2) {
      const displayCatCol = catCol.length > 20 ? catCol.slice(0, 17) + "..." : catCol;
      const displayNumCol = numCol.length > 20 ? numCol.slice(0, 17) + "..." : numCol;

      const compHeaders = [displayCatCol, "N", `Mean ${displayNumCol}`, "Std Dev", "Min", "Max"];
      const compRows: (string | number)[][] = [];

      // Compute grand stats
      const allVals = validGroupEntries.flatMap(([, v]) => v);
      const grandMean = allVals.reduce((a, b) => a + b, 0) / allVals.length;

      for (const [key, vals] of validGroupEntries) {
        const n = vals.length;
        const mean = vals.reduce((a, b) => a + b, 0) / n;
        const std = Math.sqrt(vals.reduce((a, b) => a + (b - mean) ** 2, 0) / (n - 1 || 1));
        const sorted = [...vals].sort((a, b) => a - b);
        const displayKey = key.length > 20 ? key.slice(0, 17) + "..." : key;

        compRows.push([
          displayKey,
          n,
          Math.round(mean * 1000) / 1000,
          Math.round(std * 1000) / 1000,
          Math.round(sorted[0] * 1000) / 1000,
          Math.round(sorted[n - 1] * 1000) / 1000,
        ]);
      }

      // Add ANOVA F-test summary row
      let ssBetween = 0;
      let ssWithin = 0;
      const totalN = allVals.length;
      const k = validGroupEntries.length;
      for (const [, vals] of validGroupEntries) {
        const groupMean = vals.reduce((a, b) => a + b, 0) / vals.length;
        ssBetween += vals.length * (groupMean - grandMean) ** 2;
        for (const v of vals) ssWithin += (v - groupMean) ** 2;
      }
      const dfBetween = k - 1;
      const dfWithin = totalN - k;
      const fStat = dfWithin > 0 && dfBetween > 0 ? (ssBetween / dfBetween) / (ssWithin / dfWithin) : 0;
      const eta2 = ssBetween / (ssBetween + ssWithin);

      // Approximate F-test p-value
      const fPValue = fStat > 6.63 ? "<0.001" : fStat > 3.84 ? "<0.05" : fStat > 2.71 ? "<0.10" : ">0.10";

      compRows.push([
        `F(${dfBetween},${dfWithin})=${Math.round(fStat * 100) / 100}, p${fPValue}, eta²=${Math.round(eta2 * 1000) / 1000}`,
        totalN, "", "", "", "",
      ]);

      tables.push({
        name: "group_comparison",
        description: `Group comparison: ${displayNumCol} by ${displayCatCol} (ANOVA)`,
        headers: compHeaders,
        rows: compRows,
      });
    }
  }

  // Table 6: Missing data summary
  {
    const headers = ["Variable", "N Total", "N Missing", "Missing %", "N Valid", "Data Type"];
    const rows: (string | number)[][] = [];

    for (const col of ds.columns.slice(0, 30)) {
      let missing = 0;
      let numericCount = 0;
      let totalNonNull = 0;

      for (const row of ds.data) {
        const val = row[col];
        if (val === null || val === undefined || val === "" || val === "NA" || val === "NaN" || val === ".") {
          missing++;
        } else {
          totalNonNull++;
          if (typeof val === "number" || (typeof val === "string" && !isNaN(Number(val)) && val.trim() !== "")) {
            numericCount++;
          }
        }
      }

      const dataType = totalNonNull > 0 && numericCount / totalNonNull > 0.7 ? "Numeric" : "Categorical";
      const displayCol = col.length > 30 ? col.slice(0, 27) + "..." : col;

      rows.push([
        displayCol,
        ds.data.length,
        missing,
        `${((missing / ds.data.length) * 100).toFixed(1)}%`,
        ds.data.length - missing,
        dataType,
      ]);
    }

    tables.push({
      name: "missing_data_summary",
      description: "Missing data summary by variable",
      headers,
      rows,
    });
  }

  return tables;
}

function generateDefaultMetrics(
  allData: { name: string; data: Record<string, any>[]; columns: string[]; totalRows: number }[],
  executableMethods: Set<string> | null
): Record<string, number | string> {
  const metrics: Record<string, number | string> = {};
  const ds = getPrimaryDataset(allData);
  if (!ds) return metrics;

  metrics.total_observations = ds.totalRows;
  metrics.total_variables = ds.columns.length;
  metrics.datasets_loaded = allData.length;
  metrics.primary_dataset = ds.name;

  const { numericCols, categoricalCols, idCols } = classifyColumns(ds.data, ds.columns);
  metrics.numeric_variables = numericCols.length;
  metrics.categorical_variables = categoricalCols.length;
  if (idCols.length > 0) {
    metrics.id_code_variables_excluded = idCols.length;
    metrics.id_code_columns = idCols.join(", ");
  }

  // Count missing values
  let totalMissing = 0;
  for (const row of ds.data) {
    for (const col of ds.columns) {
      if (row[col] === null || row[col] === undefined || row[col] === "" || row[col] === "NA" || row[col] === "NaN" || row[col] === ".") {
        totalMissing++;
      }
    }
  }
  metrics.missing_values = totalMissing;
  metrics.missing_rate = `${((totalMissing / (ds.data.length * ds.columns.length)) * 100).toFixed(2)}%`;

  // Descriptive stats for meaningful numeric columns only (exclude ID/code columns)
  const meaningfulNumericCols = numericCols.filter(c => !idCols.includes(c));
  if (methodAllowed(executableMethods, "descriptive_statistics")) {
    for (const col of meaningfulNumericCols.slice(0, 5)) {
      const values = ds.data.map(r => Number(r[col])).filter(v => !isNaN(v));
      if (values.length === 0) continue;
      const mean = values.reduce((a, b) => a + b, 0) / values.length;
      const variance = values.reduce((a, b) => a + (b - mean) ** 2, 0) / (values.length - 1 || 1);
      const std = Math.sqrt(variance);
    const sorted = [...values].sort((a, b) => a - b);
    const median = sorted.length % 2 === 0
      ? (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2
      : sorted[Math.floor(sorted.length / 2)];
    const displayCol = col.length > 20 ? col.slice(0, 17) + "..." : col;
      metrics[`mean_${displayCol}`] = Math.round(mean * 1000) / 1000;
      metrics[`std_${displayCol}`] = Math.round(std * 1000) / 1000;
      metrics[`median_${displayCol}`] = Math.round(median * 1000) / 1000;
      metrics[`min_${displayCol}`] = Math.round(sorted[0] * 1000) / 1000;
      metrics[`max_${displayCol}`] = Math.round(sorted[sorted.length - 1] * 1000) / 1000;
    }
  }

  // Correlation analysis: find the MOST MEANINGFUL pair of numeric columns
  // Skip ID/code columns and find columns that are conceptually related
  if (meaningfulNumericCols.length >= 2 && methodAllowed(executableMethods, "correlation")) {
    // Try multiple pairs and pick the one with highest absolute correlation
    // (but only report if the correlation is statistically meaningful)
    let bestCorr = 0;
    let bestPair: [string, string] | null = null;
    let bestN = 0;
    let bestPValue = 1;

    const pairsToTry = Math.min(10, meaningfulNumericCols.length * (meaningfulNumericCols.length - 1) / 2);
    let pairsTried = 0;

    for (let i = 0; i < meaningfulNumericCols.length && pairsTried < pairsToTry; i++) {
      for (let j = i + 1; j < meaningfulNumericCols.length && pairsTried < pairsToTry; j++) {
        pairsTried++;
        const col1 = meaningfulNumericCols[i];
        const col2 = meaningfulNumericCols[j];
        const pairs: [number, number][] = [];
        for (const row of ds.data) {
          const v1 = Number(row[col1]);
          const v2 = Number(row[col2]);
          if (!isNaN(v1) && !isNaN(v2)) pairs.push([v1, v2]);
        }
        if (pairs.length < 10) continue;

        const n = pairs.length;
        const m1 = pairs.reduce((a, p) => a + p[0], 0) / n;
        const m2 = pairs.reduce((a, p) => a + p[1], 0) / n;
        let num = 0, d1 = 0, d2 = 0;
        for (const [v1, v2] of pairs) {
          num += (v1 - m1) * (v2 - m2);
          d1 += (v1 - m1) ** 2;
          d2 += (v2 - m2) ** 2;
        }
        const corr = d1 > 0 && d2 > 0 ? num / Math.sqrt(d1 * d2) : 0;

        // Approximate p-value
        const t = corr * Math.sqrt((n - 2) / (1 - corr * corr + 1e-10));
        const absT = Math.abs(t);
        let pApprox: number;
        if (n > 30) {
          pApprox = Math.min(1, Math.exp(-0.717 * absT - 0.416 * absT * absT) * 2);
        } else {
          pApprox = absT > 2.576 ? 0.01 : absT > 1.96 ? 0.05 : absT > 1.645 ? 0.1 : 0.5;
        }

        if (Math.abs(corr) > Math.abs(bestCorr)) {
          bestCorr = corr;
          bestPair = [col1, col2];
          bestN = n;
          bestPValue = pApprox;
        }
      }
    }

    if (bestPair) {
      const displayCol1 = bestPair[0].length > 15 ? bestPair[0].slice(0, 12) + "..." : bestPair[0];
      const displayCol2 = bestPair[1].length > 15 ? bestPair[1].slice(0, 12) + "..." : bestPair[1];
      metrics[`strongest_correlation_${displayCol1}_vs_${displayCol2}`] = Math.round(bestCorr * 1000) / 1000;
      metrics[`p_value_${displayCol1}_vs_${displayCol2}`] = Math.round(bestPValue * 10000) / 10000;
      metrics[`correlation_sample_size`] = bestN;
      // Add interpretation
      const absCorr = Math.abs(bestCorr);
      const strength = absCorr > 0.7 ? "strong" : absCorr > 0.4 ? "moderate" : absCorr > 0.2 ? "weak" : "negligible";
      const direction = bestCorr > 0 ? "positive" : "negative";
      metrics[`correlation_interpretation`] = `${strength} ${direction} (r=${bestCorr.toFixed(3)}, p=${bestPValue < 0.001 ? "<0.001" : bestPValue.toFixed(4)}, n=${bestN})`;
    }
  }

  // Linear regression on the best numeric pair
  if (meaningfulNumericCols.length >= 2 && methodAllowed(executableMethods, "linear_regression")) {
    let bestModel: { xCol: string; yCol: string; r2: number; slope: number; intercept: number; n: number } | null = null;
    const maxPairs = Math.min(15, (meaningfulNumericCols.length * (meaningfulNumericCols.length - 1)) / 2);
    let evaluated = 0;
    for (let i = 0; i < meaningfulNumericCols.length && evaluated < maxPairs; i++) {
      for (let j = i + 1; j < meaningfulNumericCols.length && evaluated < maxPairs; j++) {
        evaluated++;
        const xCol = meaningfulNumericCols[i];
        const yCol = meaningfulNumericCols[j];
        const pairs = parseNumericPairs(ds, xCol, yCol);
        const reg = regressionStatsFromPairs(pairs);
        if (!reg) continue;
        if (!bestModel || reg.r2 > bestModel.r2) {
          bestModel = { xCol, yCol, r2: reg.r2, slope: reg.slope, intercept: reg.intercept, n: reg.n };
        }
      }
    }
    if (bestModel) {
      const xKey = metricKeyPart(bestModel.xCol, 16);
      const yKey = metricKeyPart(bestModel.yCol, 16);
      metrics[`regression_slope_${yKey}_on_${xKey}`] = Math.round(bestModel.slope * 1000) / 1000;
      metrics[`regression_intercept_${yKey}_on_${xKey}`] = Math.round(bestModel.intercept * 1000) / 1000;
      metrics[`regression_r2_${yKey}_on_${xKey}`] = Math.round(bestModel.r2 * 1000) / 1000;
      const adjR2 = 1 - (1 - bestModel.r2) * (bestModel.n - 1) / (bestModel.n - 2);
      metrics[`regression_adj_r2_${yKey}_on_${xKey}`] = Math.round(adjR2 * 1000) / 1000;
      // F-statistic for simple regression: F = (R²/1) / ((1-R²)/(n-2))
      const fStat = bestModel.r2 > 0 ? (bestModel.r2 * (bestModel.n - 2)) / (1 - bestModel.r2 + 1e-10) : 0;
      metrics[`regression_f_stat_${yKey}_on_${xKey}`] = Math.round(fStat * 100) / 100;
      const regrPValue = approxTwoTailPValue(Math.sqrt(fStat), bestModel.n - 2);
      metrics[`regression_p_value_${yKey}_on_${xKey}`] = regrPValue < 0.001 ? "<0.001" : (Math.round(regrPValue * 10000) / 10000).toString();
      metrics[`regression_sample_size_${yKey}_on_${xKey}`] = bestModel.n;
    }
  }

  // Group comparison (ANOVA-like between/within decomposition)
  if (categoricalCols.length > 0 && meaningfulNumericCols.length > 0 && methodAllowed(executableMethods, "group_comparison")) {
    let bestAnova: {
      catCol: string;
      numCol: string;
      groups: number;
      n: number;
      eta2: number;
    } | null = null;

    for (const catCol of categoricalCols.slice(0, 8)) {
      for (const numCol of meaningfulNumericCols.slice(0, 8)) {
        const groups = new Map<string, number[]>();
        for (const row of ds.data) {
          const cat = String(row[catCol] ?? "").trim();
          const value = Number(row[numCol]);
          if (!cat || isNaN(value)) continue;
          const bucket = groups.get(cat) || [];
          bucket.push(value);
          groups.set(cat, bucket);
        }
        const validGroups = Array.from(groups.values()).filter(v => v.length >= 3);
        const n = validGroups.reduce((sum, v) => sum + v.length, 0);
        if (validGroups.length < 2 || n < 20) continue;

        const allValues = validGroups.flat();
        const grandMean = allValues.reduce((a, b) => a + b, 0) / allValues.length;
        let ssBetween = 0;
        let ssTotal = 0;
        for (const groupVals of validGroups) {
          const mean = groupVals.reduce((a, b) => a + b, 0) / groupVals.length;
          ssBetween += groupVals.length * (mean - grandMean) ** 2;
        }
        for (const v of allValues) {
          ssTotal += (v - grandMean) ** 2;
        }
        if (ssTotal <= 0) continue;
        const eta2 = ssBetween / ssTotal;
        if (!bestAnova || eta2 > bestAnova.eta2) {
          bestAnova = { catCol, numCol, groups: validGroups.length, n, eta2 };
        }
      }
    }

    if (bestAnova) {
      const catKey = metricKeyPart(bestAnova.catCol, 16);
      const numKey = metricKeyPart(bestAnova.numCol, 16);
      metrics[`anova_eta2_${numKey}_by_${catKey}`] = Math.round(bestAnova.eta2 * 1000) / 1000;
      metrics[`anova_groups_${numKey}_by_${catKey}`] = bestAnova.groups;
      metrics[`anova_sample_size_${numKey}_by_${catKey}`] = bestAnova.n;
      // Add F-statistic and p-value
      const dfBetween = bestAnova.groups - 1;
      const dfWithin = bestAnova.n - bestAnova.groups;
      const fStat = dfWithin > 0 && dfBetween > 0 && bestAnova.eta2 < 1
        ? (bestAnova.eta2 / dfBetween) / ((1 - bestAnova.eta2) / dfWithin)
        : 0;
      metrics[`anova_f_stat_${numKey}_by_${catKey}`] = Math.round(fStat * 100) / 100;
      const fPValue = fStat > 6.63 ? "<0.001" : fStat > 3.84 ? "<0.05" : fStat > 2.71 ? "<0.10" : ">0.10";
      metrics[`anova_p_value_${numKey}_by_${catKey}`] = fPValue;
    }
  }

  // Time trend analysis
  const timeCols = ds.columns.filter(c => /(year|month|date|time|wave|period|quarter)/i.test(c));
  if (timeCols.length > 0 && meaningfulNumericCols.length > 0 && methodAllowed(executableMethods, "time_trend")) {
    let bestTrend: { timeCol: string; numCol: string; slope: number; r: number; p: number; n: number } | null = null;
    for (const timeCol of timeCols.slice(0, 4)) {
      for (const numCol of meaningfulNumericCols.slice(0, 8)) {
        const pairs: [number, number][] = [];
        for (const row of ds.data) {
          const t = parseTimeValue(row[timeCol]);
          const y = Number(row[numCol]);
          if (t === null || isNaN(y)) continue;
          pairs.push([t, y]);
        }
        if (pairs.length < 15) continue;
        const reg = regressionStatsFromPairs(pairs);
        if (!reg) continue;

        const n = pairs.length;
        const meanX = pairs.reduce((a, p) => a + p[0], 0) / n;
        const meanY = pairs.reduce((a, p) => a + p[1], 0) / n;
        let num = 0;
        let d1 = 0;
        let d2 = 0;
        for (const [x, y] of pairs) {
          num += (x - meanX) * (y - meanY);
          d1 += (x - meanX) ** 2;
          d2 += (y - meanY) ** 2;
        }
        const r = d1 > 0 && d2 > 0 ? num / Math.sqrt(d1 * d2) : 0;
        const p = approximateCorrelationPValue(r, n);

        if (!bestTrend || reg.r2 > (bestTrend.r * bestTrend.r)) {
          bestTrend = { timeCol, numCol, slope: reg.slope, r, p, n };
        }
      }
    }
    if (bestTrend) {
      const tKey = metricKeyPart(bestTrend.timeCol, 14);
      const yKey = metricKeyPart(bestTrend.numCol, 14);
      metrics[`time_trend_slope_${yKey}_over_${tKey}`] = Math.round(bestTrend.slope * 10000) / 10000;
      metrics[`time_trend_corr_${yKey}_over_${tKey}`] = Math.round(bestTrend.r * 1000) / 1000;
      metrics[`time_trend_p_value_${yKey}_over_${tKey}`] = Math.round(bestTrend.p * 10000) / 10000;
      metrics[`time_trend_sample_size_${yKey}_over_${tKey}`] = bestTrend.n;
    }
  }

  // Text feature analysis
  const textCols = detectTextColumns(ds);
  if (textCols.length > 0 && methodAllowed(executableMethods, "text_feature_analysis")) {
    const textCol = textCols[0];
    const texts = ds.data
      .map(r => (typeof r[textCol] === "string" ? String(r[textCol]).trim() : ""))
      .filter(s => s.length > 0)
      .slice(0, 4000);
    if (texts.length > 0) {
      const lengths = texts.map(t => t.length);
      const avgLen = lengths.reduce((a, b) => a + b, 0) / lengths.length;
      const sorted = [...lengths].sort((a, b) => a - b);
      const medianLen = sorted.length % 2 === 0
        ? (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2
        : sorted[Math.floor(sorted.length / 2)];

      metrics.text_documents_analysed = texts.length;
      metrics.text_mean_length_chars = Math.round(avgLen * 10) / 10;
      metrics.text_median_length_chars = Math.round(medianLen * 10) / 10;
      metrics.text_source_column = textCol;

      const top = topTerms(texts, 8);
      for (let i = 0; i < top.length; i++) {
        metrics[`top_term_${i + 1}`] = `${top[i][0]} (${top[i][1]})`;
      }
    }
  }

  return metrics;
}
