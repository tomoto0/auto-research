/**
 * Pure JavaScript Stata .dta file parser.
 * Supports DTA format versions 114, 115 (Stata 10-12) and 117-119 (Stata 13-16).
 * No native dependencies required.
 *
 * Based on the Stata DTA file format specification and StataDtaJS reference implementation.
 */

export interface DtaResult {
  data: Record<string, any>[];
  columns: string[];
  totalRows: number;
  variableLabels?: Record<string, string>;
  valueLabels?: Record<string, Record<string, string>>;
}

export interface DtaParseOptions {
  /** Only parse header + first N rows for preview (skip remaining data). Default: parse all (up to 10000). */
  previewRows?: number;
}

/* ------------------------------------------------------------------ */
/*  Helper: read null-terminated string from buffer                    */
/* ------------------------------------------------------------------ */

function readString(buf: Buffer, offset: number, maxLen: number): string {
  let end = offset;
  const limit = offset + maxLen;
  while (end < limit && buf[end] !== 0) end++;
  return buf.subarray(offset, end).toString("utf-8");
}

function readFixedString(buf: Buffer, offset: number, len: number): string {
  return readString(buf, offset, len);
}

/* ------------------------------------------------------------------ */
/*  Missing value detection                                            */
/* ------------------------------------------------------------------ */

// Stata missing values for numeric types
const MISSING_BYTE_MIN = 101;
const MISSING_INT16_MIN = 32741;
const MISSING_INT32_MIN = 2147483621;

function isMissingFloat32(buf: Buffer, offset: number, le: boolean): boolean {
  const byte0 = le ? buf[offset + 3] : buf[offset];
  // Missing float32 values start with 0x7F800000 pattern (byte 0x7F or higher)
  return (byte0 & 0x7f) === 0x7f && (buf[le ? offset + 2 : offset + 1] & 0x80) !== 0;
}

function isMissingFloat64(buf: Buffer, offset: number, le: boolean): boolean {
  const byte0 = le ? buf[offset + 7] : buf[offset];
  // Missing float64 values start with 0x7FF pattern
  return (byte0 & 0x7f) === 0x7f && (buf[le ? offset + 6 : offset + 1] & 0xf0) >= 0xe0;
}

/* ------------------------------------------------------------------ */
/*  Format 114/115 parser (Stata 10-12)                                */
/* ------------------------------------------------------------------ */

function parseOldFormat(buf: Buffer, previewMaxRows?: number): DtaResult {
  let offset = 0;

  // Header
  const dsFormat = buf.readUInt8(offset); offset += 1;
  const byteOrder = buf.readUInt8(offset); offset += 1;
  const littleEndian = byteOrder === 2; // 1 = big-endian (HILO), 2 = little-endian (LOHI)

  // Skip filetype (1 byte) and unused (1 byte)
  offset += 2;

  const nvar = littleEndian ? buf.readUInt16LE(offset) : buf.readUInt16BE(offset);
  offset += 2;
  const nobs = littleEndian ? buf.readInt32LE(offset) : buf.readInt32BE(offset);
  offset += 4;

  // Data label (81 bytes) and timestamp (18 bytes)
  offset += 81 + 18;

  // Type list: nvar bytes, each byte is a type code
  const typlist: number[] = [];
  for (let i = 0; i < nvar; i++) {
    typlist.push(buf.readUInt8(offset));
    offset += 1;
  }

  // Variable names: nvar * 33 bytes
  const varlist: string[] = [];
  for (let i = 0; i < nvar; i++) {
    varlist.push(readFixedString(buf, offset, 33));
    offset += 33;
  }

  // Sort list: (nvar + 1) * 2 bytes
  offset += (nvar + 1) * 2;

  // Format list: nvar * 49 bytes
  offset += nvar * 49;

  // Value label names: nvar * 33 bytes
  offset += nvar * 33;

  // Variable labels: nvar * 81 bytes
  const variableLabels: Record<string, string> = {};
  for (let i = 0; i < nvar; i++) {
    const label = readFixedString(buf, offset, 81);
    if (label) variableLabels[varlist[i]] = label;
    offset += 81;
  }

  // Expansion fields (skip)
  while (offset + 5 <= buf.length) {
    const dataType = buf.readUInt8(offset); offset += 1;
    const dataLen = littleEndian ? buf.readInt32LE(offset) : buf.readInt32BE(offset);
    offset += 4;
    if (dataType === 0 && dataLen === 0) break;
    offset += dataLen;
  }

  // Data section
  const data: Record<string, any>[] = [];
  const maxRows = previewMaxRows != null ? Math.min(nobs, previewMaxRows) : Math.min(nobs, 10000);
  // For preview mode, only iterate through the rows we need (+ skip the rest via offset calc)
  const iterRows = previewMaxRows != null ? Math.min(nobs, maxRows) : nobs;

  // Calculate row size for skipping remaining rows in preview mode
  let rowSize = 0;
  if (previewMaxRows != null && iterRows < nobs) {
    for (let j = 0; j < nvar; j++) {
      const typ = typlist[j];
      if (typ <= 244) rowSize += typ;
      else if (typ === 251) rowSize += 1;
      else if (typ === 252) rowSize += 2;
      else if (typ === 253) rowSize += 4;
      else if (typ === 254) rowSize += 4;
      else if (typ === 255) rowSize += 8;
      else rowSize += 1;
    }
  }

  for (let i = 0; i < iterRows; i++) {
    const row: Record<string, any> = {};
    for (let j = 0; j < nvar; j++) {
      const typ = typlist[j];
      if (typ <= 244) {
        // Fixed-length string (str1 to str244)
        row[varlist[j]] = readFixedString(buf, offset, typ);
        offset += typ;
      } else if (typ === 251) {
        // byte (int8)
        const val = buf.readInt8(offset); offset += 1;
        row[varlist[j]] = val >= MISSING_BYTE_MIN ? null : val;
      } else if (typ === 252) {
        // int (int16)
        const val = littleEndian ? buf.readInt16LE(offset) : buf.readInt16BE(offset);
        offset += 2;
        row[varlist[j]] = val >= MISSING_INT16_MIN ? null : val;
      } else if (typ === 253) {
        // long (int32)
        const val = littleEndian ? buf.readInt32LE(offset) : buf.readInt32BE(offset);
        offset += 4;
        row[varlist[j]] = val >= MISSING_INT32_MIN ? null : val;
      } else if (typ === 254) {
        // float (float32)
        if (isMissingFloat32(buf, offset, littleEndian)) {
          row[varlist[j]] = null;
        } else {
          row[varlist[j]] = littleEndian ? buf.readFloatLE(offset) : buf.readFloatBE(offset);
        }
        offset += 4;
      } else if (typ === 255) {
        // double (float64)
        if (isMissingFloat64(buf, offset, littleEndian)) {
          row[varlist[j]] = null;
        } else {
          row[varlist[j]] = littleEndian ? buf.readDoubleLE(offset) : buf.readDoubleBE(offset);
        }
        offset += 8;
      } else {
        // Unknown type, skip 1 byte
        row[varlist[j]] = null;
        offset += 1;
      }
    }
    data.push(row);
  }

  return {
    data,
    columns: varlist,
    totalRows: nobs,
    variableLabels,
  };
}

/* ------------------------------------------------------------------ */
/*  Format 117-119 parser (Stata 13-16, XML-tagged)                    */
/* ------------------------------------------------------------------ */

function findTag(buf: Buffer, tag: string, startFrom: number): number {
  const tagBuf = Buffer.from(tag, "utf-8");
  const idx = buf.indexOf(tagBuf, startFrom);
  return idx;
}

function parseNewFormat(buf: Buffer, previewMaxRows?: number): DtaResult {
  let offset = 0;

  // <stata_dta>
  const openTag = readFixedString(buf, 0, 11);
  if (openTag !== "<stata_dta>") {
    throw new Error(`Not a valid DTA 117+ file: expected <stata_dta>, got "${openTag}"`);
  }
  offset += 11;

  // <header>
  const headerIdx = findTag(buf, "<header>", offset);
  if (headerIdx < 0) throw new Error("Missing <header> tag");
  offset = headerIdx + 8;

  // <release>NNN</release>
  const releaseIdx = findTag(buf, "<release>", offset);
  if (releaseIdx < 0) throw new Error("Missing <release> tag");
  offset = releaseIdx + 9;
  const releaseStr = readFixedString(buf, offset, 3);
  const release = parseInt(releaseStr, 10);
  offset += 3;
  // skip </release>
  offset = findTag(buf, "</release>", offset) + 10;

  // <byteorder>LSF or MSF</byteorder>
  const boIdx = findTag(buf, "<byteorder>", offset);
  offset = boIdx + 11;
  const boStr = readFixedString(buf, offset, 3);
  const littleEndian = boStr === "LSF";
  offset += 3;
  offset = findTag(buf, "</byteorder>", offset) + 12;

  // <K>nvar</K>
  const kIdx = findTag(buf, "<K>", offset);
  offset = kIdx + 3;
  const nvar = littleEndian ? buf.readUInt16LE(offset) : buf.readUInt16BE(offset);
  offset += 2;
  offset = findTag(buf, "</K>", offset) + 4;

  // <N>nobs</N>  (4 bytes for 117, 8 bytes for 118+)
  const nIdx = findTag(buf, "<N>", offset);
  offset = nIdx + 3;
  let nobs: number;
  if (release >= 118) {
    // 8-byte observation count (read as uint32 low + high, assuming < 2^32)
    if (littleEndian) {
      nobs = buf.readUInt32LE(offset);
      // high 32 bits at offset+4, ignore for practical purposes
    } else {
      nobs = buf.readUInt32BE(offset + 4);
    }
    offset += 8;
  } else {
    nobs = littleEndian ? buf.readUInt32LE(offset) : buf.readUInt32BE(offset);
    offset += 4;
  }
  offset = findTag(buf, "</N>", offset) + 4;

  // Skip label and timestamp
  offset = findTag(buf, "</header>", offset) + 9;

  // Skip <map>
  const mapIdx = findTag(buf, "<map>", offset);
  offset = findTag(buf, "</map>", mapIdx) + 6;

  // <variable_types>
  const vtIdx = findTag(buf, "<variable_types>", offset);
  offset = vtIdx + 16;
  const typlist: number[] = [];
  for (let i = 0; i < nvar; i++) {
    typlist.push(littleEndian ? buf.readUInt16LE(offset) : buf.readUInt16BE(offset));
    offset += 2;
  }
  offset = findTag(buf, "</variable_types>", offset) + 17;

  // <varnames>
  const vnIdx = findTag(buf, "<varnames>", offset);
  offset = vnIdx + 10;
  const varlist: string[] = [];
  // Variable name length: 33 bytes for 117, 129 bytes for 118+
  const varNameLen = release >= 118 ? 129 : 33;
  for (let i = 0; i < nvar; i++) {
    varlist.push(readFixedString(buf, offset, varNameLen));
    offset += varNameLen;
  }
  offset = findTag(buf, "</varnames>", offset) + 11;

  // Skip sortlist, formats, value_label_names
  offset = findTag(buf, "</sortlist>", offset) + 11;
  offset = findTag(buf, "</formats>", offset) + 10;
  offset = findTag(buf, "</value_label_names>", offset) + 20;

  // <variable_labels>
  const vlIdx = findTag(buf, "<variable_labels>", offset);
  offset = vlIdx + 17;
  const variableLabels: Record<string, string> = {};
  // Label length: 81 bytes for 117, 321 bytes for 118+
  const varLabelLen = release >= 118 ? 321 : 81;
  for (let i = 0; i < nvar; i++) {
    const label = readFixedString(buf, offset, varLabelLen);
    if (label) variableLabels[varlist[i]] = label;
    offset += varLabelLen;
  }
  offset = findTag(buf, "</variable_labels>", offset) + 18;

  // Skip characteristics
  offset = findTag(buf, "</characteristics>", offset) + 18;

  // <data>
  const dataIdx = findTag(buf, "<data>", offset);
  offset = dataIdx + 6;

  const data: Record<string, any>[] = [];
  const maxRows = previewMaxRows != null ? Math.min(nobs, previewMaxRows) : Math.min(nobs, 10000);
  // For preview mode, only iterate the rows we need
  const iterRows = previewMaxRows != null ? Math.min(nobs, maxRows) : nobs;

  // Type codes for format 117+:
  // 1-2045: fixed-length string (str1 to str2045)
  // 32768: strL (long string reference, 8 bytes: v=uint32, o=uint32)
  // 65530: byte (int8)
  // 65529: int (int16)
  // 65528: long (int32)
  // 65527: float (float32)
  // 65526: double (float64)

  // strL references to resolve later
  const strlRefs: { rowIdx: number; varIdx: number; v: number; o: number }[] = [];

  for (let i = 0; i < iterRows; i++) {
    // Safety: stop if we're about to read beyond the buffer
    if (offset + 1 > buf.length) break;
    const row: Record<string, any> = {};
    let rowOk = true;
    for (let j = 0; j < nvar; j++) {
      const typ = typlist[j];
      const bytesNeeded = typ >= 1 && typ <= 2045 ? typ : typ === 32768 ? 8 : typ === 65530 ? 1 : typ === 65529 ? 2 : typ === 65528 ? 4 : typ === 65527 ? 4 : typ === 65526 ? 8 : 1;
      if (offset + bytesNeeded > buf.length) { rowOk = false; break; }

      if (typ >= 1 && typ <= 2045) {
        // Fixed-length string
        row[varlist[j]] = readFixedString(buf, offset, typ);
        offset += typ;
      } else if (typ === 32768) {
        // strL reference (v, o) - 8 bytes
        const v = littleEndian ? buf.readUInt32LE(offset) : buf.readUInt32BE(offset);
        const o = littleEndian ? buf.readUInt32LE(offset + 4) : buf.readUInt32BE(offset + 4);
        offset += 8;
        if (v === 0 && o === 0) {
          row[varlist[j]] = "";
        } else {
          row[varlist[j]] = null; // placeholder, resolved later
          strlRefs.push({ rowIdx: i, varIdx: j, v, o });
        }
      } else if (typ === 65530) {
        // byte (int8)
        const val = buf.readInt8(offset); offset += 1;
        row[varlist[j]] = val >= MISSING_BYTE_MIN ? null : val;
      } else if (typ === 65529) {
        // int (int16)
        const val = littleEndian ? buf.readInt16LE(offset) : buf.readInt16BE(offset);
        offset += 2;
        row[varlist[j]] = val >= MISSING_INT16_MIN ? null : val;
      } else if (typ === 65528) {
        // long (int32)
        const val = littleEndian ? buf.readInt32LE(offset) : buf.readInt32BE(offset);
        offset += 4;
        row[varlist[j]] = val >= MISSING_INT32_MIN ? null : val;
      } else if (typ === 65527) {
        // float (float32)
        if (isMissingFloat32(buf, offset, littleEndian)) {
          row[varlist[j]] = null;
        } else {
          row[varlist[j]] = littleEndian ? buf.readFloatLE(offset) : buf.readFloatBE(offset);
        }
        offset += 4;
      } else if (typ === 65526) {
        // double (float64)
        if (isMissingFloat64(buf, offset, littleEndian)) {
          row[varlist[j]] = null;
        } else {
          row[varlist[j]] = littleEndian ? buf.readDoubleLE(offset) : buf.readDoubleBE(offset);
        }
        offset += 8;
      } else {
        row[varlist[j]] = null;
        offset += 1;
      }
    }
    if (!rowOk) break; // Reached end of loaded buffer
    data.push(row);
  }

  // Resolve strL references — but only if the <strls> tag is reachable within
  // the loaded buffer.  For very large files in preview mode we may have loaded
  // only the first portion of the file, so the strls section may be beyond our
  // buffer.  In that case we leave the placeholder values as empty strings
  // rather than crashing or scanning hundreds of MB.
  if (strlRefs.length > 0) {
    const strlsIdx = findTag(buf, "<strls>", offset);
    if (strlsIdx >= 0) {
      // Build a set of (v,o) pairs we actually need so we can stop early
      const neededKeys = new Set<string>();
      for (const ref of strlRefs) {
        neededKeys.add(`${ref.v}:${ref.o}`);
      }

      // Parse GSO entries
      const strls: Record<string, Record<string, string>> = {};
      let pos = strlsIdx + 7;
      let resolvedCount = 0;

      while (pos + 3 <= buf.length) {
        const marker = readFixedString(buf, pos, 3);
        if (marker !== "GSO") break;
        pos += 3;

        const v = littleEndian ? buf.readUInt32LE(pos) : buf.readUInt32BE(pos);
        pos += 4;
        let o: number;
        if (release >= 118) {
          // 8-byte observation number for 118+
          o = littleEndian ? buf.readUInt32LE(pos) : buf.readUInt32BE(pos);
          pos += 8;
        } else {
          o = littleEndian ? buf.readUInt32LE(pos) : buf.readUInt32BE(pos);
          pos += 4;
        }
        const t = buf.readUInt8(pos); pos += 1;
        const len = littleEndian ? buf.readUInt32LE(pos) : buf.readUInt32BE(pos);
        pos += 4;

        if (neededKeys.has(`${v}:${o}`)) {
          if (!strls[v]) strls[v] = {};
          if (t === 130) {
            strls[v][o] = readFixedString(buf, pos, len);
          } else {
            strls[v][o] = `[binary ${len} bytes]`;
          }
          resolvedCount++;
          // Stop early once all needed refs are resolved
          if (resolvedCount >= neededKeys.size) {
            pos += len;
            break;
          }
        }
        pos += len;
      }

      // Apply strL values to data rows
      for (const ref of strlRefs) {
        if (ref.rowIdx < data.length && strls[ref.v] && strls[ref.v][ref.o] !== undefined) {
          data[ref.rowIdx][varlist[ref.varIdx]] = strls[ref.v][ref.o];
        } else if (ref.rowIdx < data.length) {
          // Could not resolve — set to empty string instead of null
          data[ref.rowIdx][varlist[ref.varIdx]] = "";
        }
      }
    } else {
      // <strls> tag not found in buffer — set all strL refs to empty string
      for (const ref of strlRefs) {
        if (ref.rowIdx < data.length) {
          data[ref.rowIdx][varlist[ref.varIdx]] = "";
        }
      }
    }
  }

  return {
    data,
    columns: varlist,
    totalRows: nobs,
    variableLabels,
  };
}

/* ------------------------------------------------------------------ */
/*  Public API                                                         */
/* ------------------------------------------------------------------ */

/**
 * Parse a Stata .dta file from a Buffer.
 * Automatically detects the format version (114/115 or 117+).
 */
export function parseDtaFile(buf: Buffer, options?: DtaParseOptions): DtaResult {
  if (buf.length < 20) {
    throw new Error("File too small to be a valid DTA file");
  }

  // Detect format version
  const firstByte = buf.readUInt8(0);

  const maxRows = options?.previewRows;

  // Format 117+ starts with "<stata_dta>" (0x3C = '<')
  if (firstByte === 0x3c) {
    const tag = readFixedString(buf, 0, 11);
    if (tag === "<stata_dta>") {
      return parseNewFormat(buf, maxRows);
    }
  }

  // Format 114/115: first byte is the version number
  if (firstByte >= 102 && firstByte <= 115) {
    return parseOldFormat(buf, maxRows);
  }

  // Format 118/119 might also start with <stata_dta> but with BOM
  // Try searching for the tag in the first 20 bytes
  const headerStr = buf.subarray(0, 20).toString("utf-8");
  if (headerStr.includes("<stata_dta>")) {
    const tagOffset = headerStr.indexOf("<stata_dta>");
    // Re-parse from the tag offset
    return parseNewFormat(buf.subarray(tagOffset), maxRows);
  }

  throw new Error(
    `Unsupported DTA format. First byte: 0x${firstByte.toString(16)}. ` +
    `Supported: Stata 10-16 (format versions 102-119).`
  );
}
