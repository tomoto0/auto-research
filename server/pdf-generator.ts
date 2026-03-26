/**
 * PDF Generator - Converts research paper content to PDF
 * Uses PDFKit (pure Node.js, no Chromium dependency) for PDF generation.
 *
 * Strategy:
 *  1. Parse LaTeX or Markdown content into structured sections
 *  2. Render to PDF using PDFKit with academic formatting
 *  3. Embed chart images directly from URLs
 */
import PDFDocument from "pdfkit";
import { marked } from "marked";
import * as https from "https";
import * as http from "http";
import * as fsNode from "fs";
import { execSync } from "child_process";

/* ------------------------------------------------------------------ */
/*  Types                                                             */
/* ------------------------------------------------------------------ */

export interface ChartImage {
  /** Unique key used in \includegraphics{<key>} */
  key: string;
  /** Remote URL of the chart image (S3 / CDN) */
  url: string;
  /** Human-readable name for the figure caption */
  name: string;
  /** Description for the figure caption */
  description: string;
}

/* ------------------------------------------------------------------ */
/*  Image fetching helper                                              */
/* ------------------------------------------------------------------ */

async function fetchImageBuffer(url: string): Promise<Buffer | null> {
  return new Promise((resolve) => {
    const timeout = setTimeout(() => resolve(null), 15000);
    const protocol = url.startsWith("https") ? https : http;
    protocol.get(url, { timeout: 10000 }, (res) => {
      // Follow redirects
      if (res.statusCode && res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        clearTimeout(timeout);
        fetchImageBuffer(res.headers.location).then(resolve);
        return;
      }
      if (res.statusCode !== 200) {
        clearTimeout(timeout);
        resolve(null);
        return;
      }
      const chunks: Buffer[] = [];
      res.on("data", (chunk: Buffer) => chunks.push(chunk));
      res.on("end", () => {
        clearTimeout(timeout);
        resolve(Buffer.concat(chunks));
      });
      res.on("error", () => {
        clearTimeout(timeout);
        resolve(null);
      });
    }).on("error", () => {
      clearTimeout(timeout);
      resolve(null);
    });
  });
}

/* ------------------------------------------------------------------ */
/*  LaTeX / Markdown parser → structured content                       */
/* ------------------------------------------------------------------ */

interface PaperSection {
  type: "title" | "conference" | "abstract" | "heading" | "subheading" | "subsubheading" |
        "paragraph" | "figure" | "table" | "equation" | "list" | "bibliography" | "separator";
  text?: string;
  level?: number;
  items?: string[];
  imageUrl?: string;
  caption?: string;
  headers?: string[];
  rows?: string[][];
  tableCaption?: string;
}

function parseLatexToSections(
  latexSource: string,
  title: string,
  conference: string,
  chartImages: ChartImage[] = []
): PaperSection[] {
  const sections: PaperSection[] = [];
  let tex = latexSource;

  // Strip code-block fences
  tex = tex.replace(/^```(?:latex|tex|\w*)\s*\n?/i, "");
  tex = tex.replace(/\n?```\s*$/i, "");

  // Build chart URL map
  const chartUrlMap = new Map<string, ChartImage>();
  chartImages.forEach((c, i) => {
    chartUrlMap.set(c.key, c);
    chartUrlMap.set(`figure_${i + 1}`, c);
    chartUrlMap.set(`figure_${i + 1}.png`, c);
  });

  // Extract title
  const titleMatch = tex.match(/\\title\{([^}]+)\}/);
  const paperTitle = titleMatch ? cleanLatexInline(titleMatch[1]) : title;
  sections.push({ type: "title", text: paperTitle });
  sections.push({ type: "conference", text: conference || "NeurIPS 2025" });

  // Extract abstract
  const abstractMatch = tex.match(/\\begin\{abstract\}([\s\S]*?)\\end\{abstract\}/);
  if (abstractMatch) {
    sections.push({ type: "abstract", text: cleanLatexInline(abstractMatch[1].trim()) });
  }

  // Extract body
  const bodyMatch = tex.match(/\\begin\{document\}([\s\S]*?)\\end\{document\}/);
  let body = bodyMatch ? bodyMatch[1] : tex;

  // Remove preamble commands
  body = body.replace(/\\maketitle\s*/g, "");
  body = body.replace(/\\title\{[^}]*\}\s*/g, "");
  body = body.replace(/\\author\{[\s\S]*?\}\s*/g, "");
  body = body.replace(/\\date\{[^}]*\}\s*/g, "");

  // Remove abstract environment from body (already extracted above)
  body = body.replace(/\\begin\{abstract\}[\s\S]*?\\end\{abstract\}\s*/g, "");

  // Remove preamble-style commands that may appear in body
  body = body.replace(/\\pretitle\{[\s\S]*?\}\s*/g, "");
  body = body.replace(/\\posttitle\{[\s\S]*?\}\s*/g, "");
  body = body.replace(/\\preauthor\{[\s\S]*?\}\s*/g, "");
  body = body.replace(/\\postauthor\{[\s\S]*?\}\s*/g, "");
  body = body.replace(/\\predate\{[\s\S]*?\}\s*/g, "");
  body = body.replace(/\\postdate\{[\s\S]*?\}\s*/g, "");
  body = body.replace(/\\hypersetup\{[\s\S]*?\}\s*/g, "");
  body = body.replace(/\\sisetup\{[\s\S]*?\}\s*/g, "");

  // Strip LaTeX comments from body (lines starting with % or inline % not escaped)
  body = body.split("\n").map(line => {
    // Remove full-line comments
    if (line.trim().startsWith("%")) return "";
    // Remove inline comments (% not preceded by \)
    return line.replace(/(?<!\\)%.*$/, "");
  }).join("\n");

  // Process display math environments (equation, align, gather, etc.) → markers
  let eqCounter = 0;
  const equationMap = new Map<string, PaperSection>();

  // Named equation environments: equation, align, gather, multline, eqnarray
  body = body.replace(
    /\\begin\{(equation|align|gather|multline|eqnarray)\*?\}([\s\S]*?)\\end\{\1\*?\}/g,
    (_match, _env, content) => {
      eqCounter++;
      const marker = `__EQUATION_${eqCounter}__`;
      // Clean the math content
      let mathContent = content.trim();
      // Remove \label{...}
      mathContent = mathContent.replace(/\\label\{[^}]*\}/g, "");
      // Remove \nonumber and \notag
      mathContent = mathContent.replace(/\\(?:nonumber|notag)/g, "");
      // For align/gather: split by \\ and join with newlines
      const lines = mathContent.split("\\\\").map((l: string) => l.trim()).filter((l: string) => l);
      const converted = lines.map((l: string) => {
        // Remove alignment markers &
        const cleaned = l.replace(/&/g, " ").trim();
        return latexMathToUnicode(cleaned);
      }).join("\n");
      equationMap.set(marker, { type: "equation", text: converted });
      return `\n${marker}\n`;
    }
  );

  // Display math: $$...$$ (must come before inline $ processing)
  body = body.replace(
    /\$\$([\s\S]*?)\$\$/g,
    (_match, content) => {
      eqCounter++;
      const marker = `__EQUATION_${eqCounter}__`;
      const converted = latexMathToUnicode(content.trim());
      equationMap.set(marker, { type: "equation", text: converted });
      return `\n${marker}\n`;
    }
  );

  // \[ ... \] display math
  body = body.replace(
    /\\\[([\s\S]*?)\\\]/g,
    (_match, content) => {
      eqCounter++;
      const marker = `__EQUATION_${eqCounter}__`;
      const converted = latexMathToUnicode(content.trim());
      equationMap.set(marker, { type: "equation", text: converted });
      return `\n${marker}\n`;
    }
  );

  // Process figure environments first (extract and replace with markers)
  let figureCounter = 0;
  const figureMap = new Map<string, PaperSection>();
  body = body.replace(
    /\\begin\{figure\}(?:\[([^\]]*)\])?([\s\S]*?)\\end\{figure\}/g,
    (_match, _placement, content) => {
      figureCounter++;
      const marker = `__FIGURE_${figureCounter}__`;

      // Extract includegraphics
      const imgMatch = content.match(/\\includegraphics(?:\[([^\]]*)\])?\{([^}]+)\}/);
      let imageUrl = "";
      if (imgMatch) {
        const src = imgMatch[2];
        const chart = chartUrlMap.get(src) || chartUrlMap.get(src.replace(/\.[^.]+$/, ""));
        if (chart) imageUrl = chart.url;
        else if (src.startsWith("http")) imageUrl = src;
      }

      // Extract caption
      const capMatch = content.match(/\\caption\{([^}]*)\}/);
      const caption = capMatch ? cleanLatexInline(capMatch[1]) : `Figure ${figureCounter}`;

      figureMap.set(marker, {
        type: "figure",
        imageUrl,
        caption: `Figure ${figureCounter}: ${caption}`,
      });
      return `\n${marker}\n`;
    }
  );

  // Process table environments
  let tableCounter = 0;
  const tableMap = new Map<string, PaperSection>();
  body = body.replace(
    /\\begin\{table\}(?:\[([^\]]*)\])?([\s\S]*?)\\end\{table\}/g,
    (_match, _placement, content) => {
      tableCounter++;
      const marker = `__TABLE_${tableCounter}__`;

      // Extract caption
      const capMatch = content.match(/\\caption\{([^}]*)\}/);
      const tableCaption = capMatch ? cleanLatexInline(capMatch[1]) : `Table ${tableCounter}`;

      // Remove non-tabular content
      let cleanContent = content
        .replace(/\\centering\s*/g, "")
        .replace(/\\caption\*?\{[^}]*\}\s*/g, "")
        .replace(/\\label\{[^}]*\}\s*/g, "")
        .replace(/\\sisetup\{[^}]*\}\s*/g, "");

      // Extract tabular content
      const tabMatch = cleanContent.match(/\\begin\{(?:tabular|tabularx)\}(?:\{[^}]*\})?\{([^}]*)\}([\s\S]*?)\\end\{(?:tabular|tabularx)\}/);
      // Also handle resizebox wrapping
      const resizeMatch = cleanContent.match(/\\resizebox\{[^}]*\}\{[^}]*\}\{[\s\S]*?\\begin\{(?:tabular|tabularx)\}(?:\{[^}]*\})?\{([^}]*)\}([\s\S]*?)\\end\{(?:tabular|tabularx)\}[\s\S]*?\}/);
      // Also handle adjustbox wrapping
      const adjustMatch = cleanContent.match(/\\begin\{adjustbox\}\{[^}]*\}[\s\S]*?\\begin\{(?:tabular|tabularx)\}(?:\{[^}]*\})?\{([^}]*)\}([\s\S]*?)\\end\{(?:tabular|tabularx)\}[\s\S]*?\\end\{adjustbox\}/);

      const actualMatch = resizeMatch || adjustMatch || tabMatch;

      if (actualMatch) {
        const tabContent = actualMatch[2];
        const { headers, rows } = parseTabularContent(tabContent);
        tableMap.set(marker, {
          type: "table",
          headers,
          rows,
          tableCaption: `Table ${tableCounter}: ${tableCaption}`,
        });
      } else {
        tableMap.set(marker, {
          type: "table",
          headers: [],
          rows: [],
          tableCaption: `Table ${tableCounter}: ${tableCaption}`,
        });
      }
      return `\n${marker}\n`;
    }
  );

  // Process bibliography
  const bibMatch = body.match(/\\begin\{thebibliography\}\{[^}]*\}([\s\S]*?)\\end\{thebibliography\}/);
  let bibItems: string[] = [];
  if (bibMatch) {
    const bibContent = bibMatch[1];
    body = body.replace(/\\begin\{thebibliography\}\{[^}]*\}[\s\S]*?\\end\{thebibliography\}/, "__BIBLIOGRAPHY__");
    const items = bibContent.split(/\\bibitem(?:\{[^}]*\}|\[[^\]]*\]\{[^}]*\})/);
    bibItems = items.map(item => cleanLatexInline(item.trim())).filter(item => item.length > 0);
  }

  // Now process the body line by line
  const lines = body.split("\n");
  let currentParagraph = "";

  function flushParagraph() {
    const text = cleanLatexInline(currentParagraph.trim());
    if (text.length > 0) {
      sections.push({ type: "paragraph", text });
    }
    currentParagraph = "";
  }

  let inList = false;
  let listItems: string[] = [];
  let listType: "ul" | "ol" = "ul";

  for (const line of lines) {
    const trimmed = line.trim();

    // Check for figure/table markers
    const figMarkerMatch = trimmed.match(/^__FIGURE_(\d+)__$/);
    if (figMarkerMatch) {
      flushParagraph();
      const fig = figureMap.get(trimmed);
      if (fig) sections.push(fig);
      continue;
    }

    const tabMarkerMatch = trimmed.match(/^__TABLE_(\d+)__$/);
    if (tabMarkerMatch) {
      flushParagraph();
      const tab = tableMap.get(trimmed);
      if (tab) sections.push(tab);
      continue;
    }

    // Check for equation markers
    const eqMarkerMatch = trimmed.match(/^__EQUATION_(\d+)__$/);
    if (eqMarkerMatch) {
      flushParagraph();
      const eq = equationMap.get(trimmed);
      if (eq) sections.push(eq);
      continue;
    }

    if (trimmed === "__BIBLIOGRAPHY__") {
      flushParagraph();
      if (bibItems.length > 0) {
        sections.push({ type: "bibliography", items: bibItems });
      }
      continue;
    }

    // Sections
    const sectionMatch = trimmed.match(/\\section\*?\{([^}]+)\}/);
    if (sectionMatch) {
      flushParagraph();
      sections.push({ type: "heading", text: cleanLatexInline(sectionMatch[1]) });
      continue;
    }

    const subsectionMatch = trimmed.match(/\\subsection\*?\{([^}]+)\}/);
    if (subsectionMatch) {
      flushParagraph();
      sections.push({ type: "subheading", text: cleanLatexInline(subsectionMatch[1]) });
      continue;
    }

    const subsubsectionMatch = trimmed.match(/\\subsubsection\*?\{([^}]+)\}/);
    if (subsubsectionMatch) {
      flushParagraph();
      sections.push({ type: "subsubheading", text: cleanLatexInline(subsubsectionMatch[1]) });
      continue;
    }

    // Lists
    if (trimmed === "\\begin{itemize}" || trimmed === "\\begin{enumerate}") {
      flushParagraph();
      inList = true;
      listItems = [];
      listType = trimmed.includes("enumerate") ? "ol" : "ul";
      continue;
    }
    if (trimmed === "\\end{itemize}" || trimmed === "\\end{enumerate}") {
      if (listItems.length > 0) {
        sections.push({ type: "list", items: listItems });
      }
      inList = false;
      listItems = [];
      continue;
    }
    if (inList && trimmed.startsWith("\\item")) {
      listItems.push(cleanLatexInline(trimmed.replace(/^\\item\s*/, "")));
      continue;
    }

    // Skip LaTeX commands
    if (/^\\(?:documentclass|usepackage|geometry|begin\{document\}|end\{document\}|newcommand|renewcommand|setlength|pagestyle|thispagestyle|pretitle|posttitle|preauthor|postauthor|predate|postdate|hypersetup|sisetup|bibliographystyle|bibliography|input|include|graphicspath|DeclareGraphicsExtensions)/.test(trimmed)) {
      continue;
    }
    if (/^\\(?:vspace|hspace|vfill|hfill|newpage|clearpage|pagebreak|noindent|bigskip|medskip|smallskip|centering|raggedright|raggedleft)\b/.test(trimmed)) {
      continue;
    }
    // Skip \begin{center}/\end{center} and similar environments
    if (/^\\(?:begin|end)\{(?:center|flushleft|flushright)\}/.test(trimmed)) {
      continue;
    }
    // Skip standalone \LARGE, \bfseries, etc.
    if (/^\\(?:LARGE|Large|large|bfseries|itshape|normalfont|normalsize|par)\b\s*$/.test(trimmed)) {
      continue;
    }
    // Skip caption* (note: already removed in table processing, but catch stragglers)
    if (/^\\caption\*?\{/.test(trimmed)) {
      continue;
    }

    // Empty line = paragraph break
    if (trimmed === "") {
      flushParagraph();
      continue;
    }

    // Accumulate paragraph text
    currentParagraph += (currentParagraph ? " " : "") + trimmed;
  }
  flushParagraph();

  return sections;
}

function parseTabularContent(content: string): { headers: string[]; rows: string[][] } {
  const rawRows = content.split("\\\\").map(r => r.trim()).filter(r => r.length > 0);
  const headers: string[] = [];
  const rows: string[][] = [];

  for (let i = 0; i < rawRows.length; i++) {
    let row = rawRows[i];
    // Remove rule commands
    row = row.replace(/\\(?:hline|toprule|midrule|bottomrule)\s*/g, "").trim();
    row = row.replace(/\\cline\{[^}]*\}\s*/g, "").trim();
    if (!row) continue;

    const cells = row.split("&").map(c => cleanLatexInline(c.trim()));
    if (headers.length === 0) {
      headers.push(...cells);
    } else {
      rows.push(cells);
    }
  }
  return { headers, rows };
}

/* ------------------------------------------------------------------ */
/*  LaTeX Math → Unicode converter                                     */
/* ------------------------------------------------------------------ */

/**
 * Convert LaTeX math expressions to readable Unicode text.
 * Handles Greek letters, operators, fractions, subscripts, superscripts, etc.
 */
/**
 * Convert LaTeX math to ASCII-safe readable text for PDFKit rendering.
 * Outputs only characters in the Basic Latin + Latin-1 Supplement range
 * that PDFKit's built-in fonts (Times-Roman, Courier) can render.
 * No Unicode Greek, no combining marks, no exotic symbols.
 */
export function latexMathToUnicode(math: string): string {
  let m = math.trim();

  // --- Greek letters → ASCII names ---
  const greekMap: Record<string, string> = {
    "\\alpha": "alpha", "\\beta": "beta", "\\gamma": "gamma", "\\delta": "delta",
    "\\epsilon": "epsilon", "\\varepsilon": "epsilon", "\\zeta": "zeta", "\\eta": "eta",
    "\\theta": "theta", "\\vartheta": "theta", "\\iota": "iota", "\\kappa": "kappa",
    "\\lambda": "lambda", "\\mu": "mu", "\\nu": "nu", "\\xi": "xi",
    "\\pi": "pi", "\\varpi": "pi", "\\rho": "rho", "\\varrho": "rho",
    "\\sigma": "sigma", "\\varsigma": "sigma", "\\tau": "tau", "\\upsilon": "upsilon",
    "\\phi": "phi", "\\varphi": "phi", "\\chi": "chi", "\\psi": "psi",
    "\\omega": "omega",
    "\\Gamma": "Gamma", "\\Delta": "Delta", "\\Theta": "Theta", "\\Lambda": "Lambda",
    "\\Xi": "Xi", "\\Pi": "Pi", "\\Sigma": "Sigma", "\\Upsilon": "Upsilon",
    "\\Phi": "Phi", "\\Psi": "Psi", "\\Omega": "Omega",
  };
  for (const [cmd, ch] of Object.entries(greekMap)) {
    m = m.replace(new RegExp(cmd.replace(/\\/g, "\\\\") + "(?![a-zA-Z])", "g"), ch);
  }

  // --- Math operators & symbols → ASCII ---
  const symbolMap: Record<string, string> = {
    "\\times": "*", "\\cdot": "*", "\\div": "/",
    "\\pm": "+/-", "\\mp": "-/+",
    "\\leq": "<=", "\\le": "<=", "\\geq": ">=", "\\ge": ">=",
    "\\neq": "!=", "\\ne": "!=",
    "\\approx": "~=", "\\sim": "~", "\\simeq": "~=",
    "\\equiv": "===", "\\propto": "proportional to",
    "\\infty": "inf", "\\partial": "d", "\\nabla": "grad",
    "\\forall": "for all", "\\exists": "exists", "\\nexists": "not exists",
    "\\in": "in", "\\notin": "not in", "\\subset": "subset", "\\supset": "superset",
    "\\subseteq": "subset=", "\\supseteq": "superset=",
    "\\cup": "union", "\\cap": "intersect", "\\emptyset": "{}",
    "\\to": "->", "\\rightarrow": "->", "\\leftarrow": "<-",
    "\\Rightarrow": "=>", "\\Leftarrow": "<=", "\\Leftrightarrow": "<=>",
    "\\mapsto": "|->",
    "\\ldots": "...", "\\cdots": "...", "\\vdots": "...", "\\ddots": "...",
    "\\langle": "<", "\\rangle": ">",
    "\\lceil": "[", "\\rceil": "]", "\\lfloor": "[", "\\rfloor": "]",
    "\\neg": "not", "\\land": "and", "\\lor": "or",
    "\\circ": "o", "\\bullet": "*", "\\star": "*",
    "\\dagger": "+", "\\ddagger": "++",
    "\\ell": "l", "\\hbar": "h", "\\Re": "Re", "\\Im": "Im",
    "\\aleph": "aleph",
    "\\prime": "'",
  };
  for (const [cmd, ch] of Object.entries(symbolMap)) {
    m = m.replace(new RegExp(cmd.replace(/\\/g, "\\\\") + "(?![a-zA-Z])", "g"), ch);
  }

  // --- Big operators → ASCII with sub/superscripts ---
  m = m.replace(/\\sum(?:_\{([^}]*)\})?(?:\^\{([^}]*)\})?/g, (_match, sub, sup) => {
    let s = "SUM";
    if (sub && sup) s += `(${latexMathToUnicode(sub)}..${latexMathToUnicode(sup)})`;
    else if (sub) s += `_(${latexMathToUnicode(sub)})`;
    return s;
  });
  m = m.replace(/\\prod(?:_\{([^}]*)\})?(?:\^\{([^}]*)\})?/g, (_match, sub, sup) => {
    let s = "PROD";
    if (sub && sup) s += `(${latexMathToUnicode(sub)}..${latexMathToUnicode(sup)})`;
    else if (sub) s += `_(${latexMathToUnicode(sub)})`;
    return s;
  });
  m = m.replace(/\\int(?:_\{([^}]*)\})?(?:\^\{([^}]*)\})?/g, (_match, sub, sup) => {
    let s = "INT";
    if (sub && sup) s += `(${latexMathToUnicode(sub)}..${latexMathToUnicode(sup)})`;
    else if (sub) s += `_(${latexMathToUnicode(sub)})`;
    return s;
  });
  const bigOpNames = ["lim", "max", "min", "sup", "inf"];
  for (const op of bigOpNames) {
    m = m.replace(new RegExp(`\\\\${op}(?:_\\{([^}]*)\\})?`, "g"), (_match: string, sub: string) => {
      let s = op;
      if (sub) s += `_(${latexMathToUnicode(sub)})`;
      return s;
    });
  }

  // --- Fractions: \frac{a}{b} → (a)/(b) ---
  m = m.replace(/\\frac\{([^}]*)\}\{([^}]*)\}/g, (_match, num, den) => {
    const n = latexMathToUnicode(num);
    const d = latexMathToUnicode(den);
    if (n.length <= 2 && d.length <= 2) return `${n}/${d}`;
    return `(${n})/(${d})`;
  });

  // --- Square root: \sqrt{x} → sqrt(x) ---
  m = m.replace(/\\sqrt\[([^\]]*)\]\{([^}]*)\}/g, (_match, n, content) => {
    return `root(${latexMathToUnicode(n)}, ${latexMathToUnicode(content)})`;
  });
  m = m.replace(/\\sqrt\{([^}]*)\}/g, (_match, content) => {
    return `sqrt(${latexMathToUnicode(content)})`;
  });

  // --- Overline/hat/bar/tilde → ASCII notation ---
  m = m.replace(/\\(?:overline|bar)\{([^}]*)\}/g, (_match, content) => `bar(${latexMathToUnicode(content)})`);
  m = m.replace(/\\hat\{([^}]*)\}/g, (_match, content) => `hat(${latexMathToUnicode(content)})`);
  m = m.replace(/\\tilde\{([^}]*)\}/g, (_match, content) => `tilde(${latexMathToUnicode(content)})`);
  m = m.replace(/\\dot\{([^}]*)\}/g, (_match, content) => `${latexMathToUnicode(content)}'`);
  m = m.replace(/\\ddot\{([^}]*)\}/g, (_match, content) => `${latexMathToUnicode(content)}''`);
  m = m.replace(/\\vec\{([^}]*)\}/g, (_match, content) => `vec(${latexMathToUnicode(content)})`);

  // --- Subscripts: _{...} → _(...) ---
  m = m.replace(/_\{([^}]*)\}/g, (_match, content) => `_(${latexMathToUnicode(content)})`);
  // Single char subscript: _x stays as _x
  m = m.replace(/_([a-zA-Z0-9])/g, "_$1");

  // --- Superscripts: ^{...} → ^(...) ---
  m = m.replace(/\^\{([^}]*)\}/g, (_match, content) => `^(${latexMathToUnicode(content)})`);
  // Single char superscript: ^x stays as ^x
  m = m.replace(/\^([a-zA-Z0-9])/g, "^$1");

  // --- Math functions ---
  const funcNames = ["sin", "cos", "tan", "log", "ln", "exp", "det", "dim", "ker", "deg",
    "arg", "gcd", "hom", "Pr", "sec", "csc", "cot", "sinh", "cosh", "tanh"];
  for (const fn of funcNames) {
    m = m.replace(new RegExp(`\\\\${fn}(?![a-zA-Z])`, "g"), fn);
  }

  // --- Text commands inside math ---
  m = m.replace(/\\(?:text|mathrm|textrm|mathit|textit|mathbf|textbf|mathsf|textsf|mathtt|texttt|operatorname|operatorname\*)\{([^}]*)\}/g, "$1");
  m = m.replace(/\\(?:boldsymbol|bm)\{([^}]*)\}/g, "$1");

  // --- Spacing commands ---
  m = m.replace(/\\(?:quad|qquad)/g, "  ");
  m = m.replace(/\\[,;:!]/g, " ");
  m = m.replace(/\\(?:hspace|mspace)\{[^}]*\}/g, " ");

  // --- Delimiters ---
  m = m.replace(/\\(?:left|right|big|Big|bigg|Bigg)\s*/g, "");
  m = m.replace(/\\\{/g, "{");
  m = m.replace(/\\\}/g, "}");
  m = m.replace(/\\\|/g, "||");

  // --- Matrix environments → bracket notation ---
  m = m.replace(/\\begin\{(?:pmatrix|bmatrix|vmatrix|Vmatrix|matrix|Bmatrix)\}([\s\S]*?)\\end\{(?:pmatrix|bmatrix|vmatrix|Vmatrix|matrix|Bmatrix)\}/g, (_match, content) => {
    const rows = content.split("\\\\").map((r: string) => r.trim()).filter((r: string) => r);
    const formatted = rows.map((r: string) => r.split("&").map((c: string) => latexMathToUnicode(c.trim())).join(", ")).join("; ");
    return `[${formatted}]`;
  });

  // --- Cases environment ---
  m = m.replace(/\\begin\{cases\}([\s\S]*?)\\end\{cases\}/g, (_match, content) => {
    const rows = content.split("\\\\").map((r: string) => r.trim()).filter((r: string) => r);
    return "{ " + rows.map((r: string) => {
      const parts = r.split("&").map((c: string) => latexMathToUnicode(c.trim()));
      return parts.join(", if ");
    }).join(" | ") + " }";
  });

  // --- Cleanup remaining LaTeX commands ---
  m = m.replace(/\\[a-zA-Z]+\{([^}]*)\}/g, "$1");
  m = m.replace(/\\[a-zA-Z]+/g, "");
  m = m.replace(/[{}]/g, "");
  m = m.replace(/\s+/g, " ").trim();

  return m;
}

/**
 * Sanitize math text for PDFKit rendering.
 * Since latexMathToUnicode now outputs ASCII-safe text, this only needs
 * to handle edge cases where non-ASCII characters slip through.
 */
function sanitizePdfMathText(text: string): string {
  if (!text) return text;
  // Replace any remaining non-ASCII characters with safe fallbacks
  let safe = text.replace(/[^\x20-\x7E]/g, (ch) => {
    // Keep Latin-1 supplement chars that Times-Roman supports
    if (ch.charCodeAt(0) >= 0xA0 && ch.charCodeAt(0) <= 0xFF) return ch;
    return "";
  });
  return safe.replace(/\s+/g, " ").trim();
}

/**
 * Convert text to Unicode superscript characters where possible.
 */
function toSuperscript(text: string): string {
  const supMap: Record<string, string> = {
    "0": "\u2070", "1": "\u00B9", "2": "\u00B2", "3": "\u00B3",
    "4": "\u2074", "5": "\u2075", "6": "\u2076", "7": "\u2077",
    "8": "\u2078", "9": "\u2079",
    "+": "\u207A", "-": "\u207B", "=": "\u207C",
    "(": "\u207D", ")": "\u207E",
    "n": "\u207F", "i": "\u2071",
    "a": "\u1D43", "b": "\u1D47", "c": "\u1D9C", "d": "\u1D48",
    "e": "\u1D49", "f": "\u1DA0", "g": "\u1D4D", "h": "\u02B0",
    "j": "\u02B2", "k": "\u1D4F", "l": "\u02E1", "m": "\u1D50",
    "o": "\u1D52", "p": "\u1D56", "r": "\u02B3", "s": "\u02E2",
    "t": "\u1D57", "u": "\u1D58", "v": "\u1D5B", "w": "\u02B7",
    "x": "\u02E3", "y": "\u02B8", "z": "\u1DBB",
    "T": "\u1D40",
    "*": "\u204E",
  };
  return Array.from(text).map(ch => supMap[ch] || ch).join("");
}

/**
 * Convert text to Unicode subscript characters where possible.
 */
function toSubscript(text: string): string {
  const subMap: Record<string, string> = {
    "0": "\u2080", "1": "\u2081", "2": "\u2082", "3": "\u2083",
    "4": "\u2084", "5": "\u2085", "6": "\u2086", "7": "\u2087",
    "8": "\u2088", "9": "\u2089",
    "+": "\u208A", "-": "\u208B", "=": "\u208C",
    "(": "\u208D", ")": "\u208E",
    "a": "\u2090", "e": "\u2091", "h": "\u2095",
    "i": "\u1D62", "j": "\u2C7C", "k": "\u2096", "l": "\u2097",
    "m": "\u2098", "n": "\u2099", "o": "\u2092", "p": "\u209A",
    "r": "\u1D63", "s": "\u209B", "t": "\u209C", "u": "\u1D64",
    "v": "\u1D65", "x": "\u2093",
  };
  return Array.from(text).map(ch => subMap[ch] || ch).join("");
}

function cleanLatexInline(text: string): string {
  let result = text;

  // Strip LaTeX comments: lines starting with % and inline % (not escaped \%)
  result = result.replace(/^%.*$/gm, "");
  result = result.replace(/(?<!\\)%.*$/gm, "");

  // Remove \resizebox, \adjustbox wrappers (handle nested braces)
  result = result.replace(/\\resizebox\{[^}]*\}\{[^}]*\}\{/g, "");
  result = result.replace(/\\adjustbox\{[^}]*\}\{/g, "");

  // Remove environment markers that might leak through
  result = result.replace(/\\begin\{(?:abstract|center|flushleft|flushright|quote|quotation|verse|minipage|adjustbox)\}(?:\{[^}]*\})?/g, "");
  result = result.replace(/\\end\{(?:abstract|center|flushleft|flushright|quote|quotation|verse|minipage|adjustbox)\}/g, "");

  // Text formatting - extract content from braces
  result = result.replace(/\\textbf\{([^}]+)\}/g, "$1");
  result = result.replace(/\\textit\{([^}]+)\}/g, "$1");
  result = result.replace(/\\emph\{([^}]+)\}/g, "$1");
  result = result.replace(/\\texttt\{([^}]+)\}/g, "$1");
  result = result.replace(/\\underline\{([^}]+)\}/g, "$1");
  result = result.replace(/\\textsc\{([^}]+)\}/g, "$1");
  result = result.replace(/\\textrm\{([^}]+)\}/g, "$1");
  result = result.replace(/\\textsf\{([^}]+)\}/g, "$1");
  result = result.replace(/\\mbox\{([^}]+)\}/g, "$1");
  result = result.replace(/\\text\{([^}]+)\}/g, "$1");
  result = result.replace(/\\mathrm\{([^}]+)\}/g, "$1");
  result = result.replace(/\\mathbf\{([^}]+)\}/g, "$1");
  result = result.replace(/\\mathit\{([^}]+)\}/g, "$1");
  result = result.replace(/\\boldsymbol\{([^}]+)\}/g, "$1");

  // Citations and references
  result = result.replace(/\\cite(?:p|t|author|year)?\{([^}]+)\}/g, "[$1]");
  // Resolve \ref{} to numbered references (Figure X, Table X, Section X)
  result = result.replace(/\\ref\{fig:([^}]+)\}/g, (_m, label) => {
    // Try to extract number from label like "figure_1" or "fig1"
    const numMatch = label.match(/(\d+)/);
    return numMatch ? `Figure ${numMatch[1]}` : `Figure`;
  });
  result = result.replace(/\\ref\{tab:([^}]+)\}/g, (_m: string, label: string) => {
    const numMatch = label.match(/(\d+)/);
    return numMatch ? `Table ${numMatch[1]}` : `Table`;
  });
  result = result.replace(/\\ref\{sec:([^}]+)\}/g, (_m: string, label: string) => {
    // Convert section label to readable form
    return label.replace(/_/g, " ").replace(/^\w/, (c: string) => c.toUpperCase());
  });
  result = result.replace(/\\ref\{([^}]+)\}/g, (_m: string, label: string) => {
    const numMatch = label.match(/(\d+)/);
    if (numMatch) return numMatch[1];
    return label.replace(/_/g, " ");
  });
  result = result.replace(/\\eqref\{([^}]+)\}/g, "(Eq. $1)");
  result = result.replace(/\\autoref\{([^}]+)\}/g, (_m: string, label: string) => {
    if (label.startsWith("fig:")) return `Figure ${(label.match(/(\d+)/) || ["?"])[0]}`;
    if (label.startsWith("tab:")) return `Table ${(label.match(/(\d+)/) || ["?"])[0]}`;
    if (label.startsWith("sec:")) return label.replace(/^sec:/, "").replace(/_/g, " ");
    return label.replace(/_/g, " ");
  });
  result = result.replace(/\\url\{([^}]+)\}/g, "$1");
  result = result.replace(/\\href\{[^}]*\}\{([^}]+)\}/g, "$1");

  // Math: inline $...$ → convert to Unicode math
  result = result.replace(/\$([^$]+)\$/g, (_m, math) => latexMathToUnicode(math));

  // Escaped special characters
  result = result.replace(/\\\$/g, "$");
  result = result.replace(/\\%/g, "%");
  result = result.replace(/\\&/g, "&");
  result = result.replace(/\\_/g, "_");
  result = result.replace(/\\#/g, "#");
  result = result.replace(/\\~/g, "~");
  result = result.replace(/\\\{/g, "{");
  result = result.replace(/\\\}/g, "}");

  // Remove size commands
  result = result.replace(/\\(?:footnotesize|scriptsize|tiny|small|normalsize|large|Large|LARGE|huge|Huge|bfseries|itshape|normalfont|rmfamily|sffamily|ttfamily)\b\s*/g, "");

  // Remove various commands with arguments
  result = result.replace(/\\footnote\{[^}]*\}/g, "");
  result = result.replace(/\\label\{[^}]*\}/g, "");
  result = result.replace(/\\caption\*?\{[^}]*\}/g, "");
  result = result.replace(/\\includegraphics(?:\[[^\]]*\])?\{[^}]*\}/g, "[Figure]");
  result = result.replace(/\\sisetup\{[^}]*\}/g, "");
  result = result.replace(/\\hypersetup\{[^}]*\}/g, "");
  result = result.replace(/\\color\{[^}]*\}/g, "");
  result = result.replace(/\\colorbox\{[^}]*\}\{([^}]+)\}/g, "$1");
  result = result.replace(/\\fcolorbox\{[^}]*\}\{[^}]*\}\{([^}]+)\}/g, "$1");

  // Remove spacing commands
  result = result.replace(/\\(?:hspace|vspace)\*?\{[^}]*\}/g, "");
  result = result.replace(/\\(?:quad|qquad|enspace|thinspace|negthinspace|,|;|!)/g, " ");

  // Remove \noindent, \par, etc.
  result = result.replace(/\\(?:noindent|par|centering|raggedright|raggedleft)\b\s*/g, "");

  // Line breaks
  result = result.replace(/\\\\/g, " ");
  result = result.replace(/\\newline\b/g, " ");

  // Remove any remaining \command{} patterns (catch-all for unknown commands)
  // Only remove if the command is a simple word (no nested braces)
  result = result.replace(/\\[a-zA-Z]+\{([^{}]*)\}/g, "$1");

  // Remove any remaining \command without arguments
  result = result.replace(/\\[a-zA-Z]+\b/g, "");

  // Clean up orphaned braces
  result = result.replace(/[{}]/g, "");

  // Clean up extra whitespace
  result = result.replace(/\s+/g, " ").trim();
  return result;
}

function parseMarkdownToSections(
  markdownContent: string,
  title: string,
  conference: string,
  chartImages: ChartImage[] = []
): PaperSection[] {
  const sections: PaperSection[] = [];
  sections.push({ type: "title", text: title });
  sections.push({ type: "conference", text: conference || "NeurIPS 2025" });

  const lines = markdownContent.split("\n");
  let currentParagraph = "";
  let inAbstract = false;

  function flushParagraph() {
    const text = currentParagraph.trim();
    if (text.length > 0) {
      if (inAbstract) {
        sections.push({ type: "abstract", text });
        inAbstract = false;
      } else {
        sections.push({ type: "paragraph", text });
      }
    }
    currentParagraph = "";
  }

  for (const line of lines) {
    const trimmed = line.trim();

    if (trimmed.startsWith("# ")) {
      flushParagraph();
      sections.push({ type: "heading", text: trimmed.slice(2) });
      continue;
    }
    if (trimmed.startsWith("## ")) {
      flushParagraph();
      const heading = trimmed.slice(3);
      if (/abstract/i.test(heading)) {
        inAbstract = true;
      } else {
        sections.push({ type: "heading", text: heading });
      }
      continue;
    }
    if (trimmed.startsWith("### ")) {
      flushParagraph();
      sections.push({ type: "subheading", text: trimmed.slice(4) });
      continue;
    }
    if (trimmed.startsWith("#### ")) {
      flushParagraph();
      sections.push({ type: "subsubheading", text: trimmed.slice(5) });
      continue;
    }
    if (trimmed === "---") {
      flushParagraph();
      sections.push({ type: "separator" });
      continue;
    }
    if (trimmed.startsWith("- ") || trimmed.startsWith("* ")) {
      flushParagraph();
      // Collect list items
      const items: string[] = [trimmed.slice(2)];
      // Note: this only captures the current line; multi-line lists are handled by subsequent iterations
      sections.push({ type: "list", items });
      continue;
    }
    if (trimmed === "") {
      flushParagraph();
      continue;
    }
    currentParagraph += (currentParagraph ? " " : "") + trimmed;
  }
  flushParagraph();

  // Append chart images as figures
  if (chartImages.length > 0) {
    sections.push({ type: "heading", text: "Figures" });
    chartImages.forEach((chart, i) => {
      sections.push({
        type: "figure",
        imageUrl: chart.url,
        caption: `Figure ${i + 1}: ${chart.name} — ${chart.description}`,
      });
    });
  }

  return sections;
}

/* ------------------------------------------------------------------ */
/*  PDFKit renderer                                                    */
/* ------------------------------------------------------------------ */

// A4 dimensions in points (72 points per inch)
const PAGE_WIDTH = 595.28;
const PAGE_HEIGHT = 841.89;
const MARGIN_TOP = 72;
const MARGIN_BOTTOM = 72;
const MARGIN_LEFT = 56;
const MARGIN_RIGHT = 56;
const CONTENT_WIDTH = PAGE_WIDTH - MARGIN_LEFT - MARGIN_RIGHT;

async function renderSectionsToPdf(
  sections: PaperSection[],
  conference: string
): Promise<Buffer> {
  return new Promise(async (resolve, reject) => {
    try {
      const doc = new PDFDocument({
        size: "A4",
        margins: {
          top: MARGIN_TOP,
          bottom: MARGIN_BOTTOM,
          left: MARGIN_LEFT,
          right: MARGIN_RIGHT,
        },
        info: {
          Title: sections.find(s => s.type === "title")?.text || "Research Paper",
          Author: "Auto Research",
          Creator: "Auto Research Pipeline",
        },
        bufferPages: true,
      });

      const chunks: Buffer[] = [];
      doc.on("data", (chunk: Buffer) => chunks.push(chunk));
      doc.on("end", () => resolve(Buffer.concat(chunks)));
      doc.on("error", reject);

      // Register fonts (use built-in fonts)
      const FONT_SERIF = "Times-Roman";
      const FONT_SERIF_BOLD = "Times-Bold";
      const FONT_SERIF_ITALIC = "Times-Italic";
      const FONT_MONO = "Courier";

      // Helper: check if we need a new page
      const ensureSpace = (needed: number) => {
        const available = PAGE_HEIGHT - MARGIN_BOTTOM - doc.y;
        // Only add a new page if the needed space exceeds what's available
        // AND we've actually used some space on the current page (prevent empty pages)
        if (needed > available && doc.y > MARGIN_TOP + 20) {
          doc.addPage();
        }
      };

      // Process each section
      for (const section of sections) {
        switch (section.type) {
          case "title": {
            doc.font(FONT_SERIF_BOLD).fontSize(18);
            const titleText = section.text || "";
            doc.text(titleText, MARGIN_LEFT, doc.y, {
              width: CONTENT_WIDTH,
              align: "center",
              lineGap: 4,
            });
            doc.moveDown(0.3);
            break;
          }

          case "conference": {
            doc.font(FONT_SERIF).fontSize(9).fillColor("#666666");
            doc.text(section.text || "", { width: CONTENT_WIDTH, align: "center" });
            doc.fillColor("#000000");
            doc.moveDown(0.2);
            doc.font(FONT_SERIF).fontSize(8).fillColor("#888888");
            doc.text("Generated by Auto Research — Autonomous Research Pipeline", {
              width: CONTENT_WIDTH,
              align: "center",
            });
            doc.fillColor("#000000");
            doc.moveDown(0.5);
            // Draw separator line
            doc.moveTo(MARGIN_LEFT, doc.y).lineTo(PAGE_WIDTH - MARGIN_RIGHT, doc.y).strokeColor("#cccccc").lineWidth(0.5).stroke();
            doc.moveDown(0.5);
            break;
          }

          case "abstract": {
            ensureSpace(80);
            // Draw abstract box
            const absStartY = doc.y;
            doc.font(FONT_SERIF_BOLD).fontSize(10);
            doc.text("Abstract", MARGIN_LEFT + 20, doc.y, { width: CONTENT_WIDTH - 40 });
            doc.moveDown(0.3);
            doc.font(FONT_SERIF).fontSize(9.5);
            doc.text(section.text || "", MARGIN_LEFT + 20, doc.y, {
              width: CONTENT_WIDTH - 40,
              align: "justify",
              lineGap: 2,
            });
            const absEndY = doc.y + 10;
            // Draw left border
            doc.moveTo(MARGIN_LEFT + 15, absStartY - 5).lineTo(MARGIN_LEFT + 15, absEndY).strokeColor("#2563eb").lineWidth(2).stroke();
            // Draw background
            doc.rect(MARGIN_LEFT + 18, absStartY - 5, CONTENT_WIDTH - 36, absEndY - absStartY + 10).fillOpacity(0.03).fill("#2563eb");
            doc.fillOpacity(1).fillColor("#000000");
            doc.y = absEndY + 5;
            doc.moveDown(0.5);
            break;
          }

          case "heading": {
            ensureSpace(40);
            doc.moveDown(0.8);
            doc.font(FONT_SERIF_BOLD).fontSize(13);
            doc.text(section.text || "", MARGIN_LEFT, doc.y, { width: CONTENT_WIDTH });
            doc.moveDown(0.15);
            // Subtle underline
            doc.moveTo(MARGIN_LEFT, doc.y).lineTo(PAGE_WIDTH - MARGIN_RIGHT, doc.y).strokeColor("#eeeeee").lineWidth(0.5).stroke();
            doc.moveDown(0.35);
            break;
          }

          case "subheading": {
            ensureSpace(30);
            doc.moveDown(0.5);
            doc.font(FONT_SERIF_BOLD).fontSize(11.5);
            doc.text(section.text || "", MARGIN_LEFT, doc.y, { width: CONTENT_WIDTH });
            doc.moveDown(0.25);
            break;
          }

          case "subsubheading": {
            ensureSpace(20);
            doc.moveDown(0.2);
            doc.font(FONT_SERIF_ITALIC).fontSize(11);
            doc.text(section.text || "", MARGIN_LEFT, doc.y, { width: CONTENT_WIDTH });
            doc.moveDown(0.2);
            break;
          }

          case "paragraph": {
            ensureSpace(20);
            doc.font(FONT_SERIF).fontSize(10.5);
            doc.text(section.text || "", MARGIN_LEFT, doc.y, {
              width: CONTENT_WIDTH,
              align: "justify",
              lineGap: 2,
              indent: 0,
            });
            doc.moveDown(0.3);
            break;
          }

          case "figure": {
            // Add spacing before figure
            doc.moveDown(0.8);

            if (section.imageUrl) {
              try {
                const imgBuffer = await fetchImageBuffer(section.imageUrl);
                if (imgBuffer && imgBuffer.length > 100) {
                  // Calculate image dimensions to fit within content width
                  const maxImgWidth = CONTENT_WIDTH * 0.85;
                  const maxImgHeight = 280;

                  // Estimate actual rendered height for page-break calculation
                  // PDFKit's image with fit returns the actual dimensions used
                  const captionHeight = section.caption ? 30 : 0;
                  const totalFigureHeight = maxImgHeight + captionHeight + 40; // image + caption + margins
                  ensureSpace(totalFigureHeight);

                  // Draw light border around figure area
                  const figStartY = doc.y;

                  // Center the image
                  const imgX = MARGIN_LEFT + (CONTENT_WIDTH - maxImgWidth) / 2;
                  doc.image(imgBuffer, imgX, doc.y, {
                    fit: [maxImgWidth, maxImgHeight],
                    align: "center",
                  });

                  // PDFKit does not auto-advance y after image with fit; manually advance
                  // Calculate actual image height from aspect ratio
                  try {
                    const sizeOf = (await import("image-size")).default;
                    const dims = sizeOf(imgBuffer);
                    if (dims.width && dims.height) {
                      const scale = Math.min(maxImgWidth / dims.width, maxImgHeight / dims.height);
                      const renderedHeight = dims.height * scale;
                      doc.y = figStartY + renderedHeight + 8;
                    } else {
                      doc.y = figStartY + maxImgHeight + 8;
                    }
                  } catch {
                    // If image-size fails, use maxImgHeight as fallback
                    doc.y = figStartY + maxImgHeight + 8;
                  }
                }
              } catch (imgErr: any) {
                console.warn(`[PDF] Failed to embed image: ${imgErr.message}`);
                doc.font(FONT_SERIF_ITALIC).fontSize(9).fillColor("#999999");
                doc.text("[Image could not be loaded]", { width: CONTENT_WIDTH, align: "center" });
                doc.fillColor("#000000");
                doc.moveDown(0.3);
              }
            }
            if (section.caption) {
              ensureSpace(25);
              doc.font(FONT_SERIF_ITALIC).fontSize(9);
              doc.text(section.caption, MARGIN_LEFT + 20, doc.y, {
                width: CONTENT_WIDTH - 40,
                align: "center",
              });
              doc.moveDown(0.3);
            }
            // Add spacing after figure
            doc.moveDown(0.8);
            break;
          }

          case "table": {
            if (!section.headers || section.headers.length === 0) break;

            // Add spacing before table
            doc.moveDown(0.6);

            // Estimate total table height for page-break decision
            const estRowHeight = (section.headers.length > 5 ? 7 : section.headers.length > 3 ? 8 : 9) + 6 + 2;
            const estTableHeight = estRowHeight * ((section.rows?.length || 0) + 1) + 40; // rows + header + margins
            ensureSpace(Math.min(estTableHeight, PAGE_HEIGHT - MARGIN_TOP - MARGIN_BOTTOM - 40));

            // Table caption
            if (section.tableCaption) {
              doc.font(FONT_SERIF_BOLD).fontSize(9);
              doc.text(section.tableCaption, MARGIN_LEFT, doc.y, { width: CONTENT_WIDTH });
              doc.moveDown(0.3);
            }

            const numCols = section.headers.length;
            // Calculate column widths to fit within content width
            const colWidth = Math.min(CONTENT_WIDTH / numCols, 120);
            const tableWidth = colWidth * numCols;
            const tableStartX = MARGIN_LEFT + Math.max(0, (CONTENT_WIDTH - tableWidth) / 2);
            const cellPadding = 3;
            const fontSize = numCols > 5 ? 7 : numCols > 3 ? 8 : 9;

            // Draw header row
            const headerY = doc.y;
            doc.font(FONT_SERIF_BOLD).fontSize(fontSize);

            // Header background
            doc.rect(tableStartX, headerY - 2, tableWidth, fontSize + cellPadding * 2 + 2)
              .fillOpacity(0.06).fill("#000000");
            doc.fillOpacity(1).fillColor("#000000");

            // Header text
            for (let i = 0; i < numCols; i++) {
              const cellX = tableStartX + i * colWidth + cellPadding;
              doc.font(FONT_SERIF_BOLD).fontSize(fontSize);
              doc.text(
                (section.headers[i] || "").substring(0, Math.floor(colWidth / (fontSize * 0.5))),
                cellX,
                headerY + cellPadding,
                { width: colWidth - cellPadding * 2, align: "left", lineBreak: false }
              );
            }

            // Top rule
            doc.moveTo(tableStartX, headerY - 2).lineTo(tableStartX + tableWidth, headerY - 2)
              .strokeColor("#000000").lineWidth(1).stroke();

            let currentY = headerY + fontSize + cellPadding * 2 + 2;

            // Mid rule
            doc.moveTo(tableStartX, currentY).lineTo(tableStartX + tableWidth, currentY)
              .strokeColor("#000000").lineWidth(0.5).stroke();
            currentY += 2;

            // Data rows
            doc.font(FONT_SERIF).fontSize(fontSize);
            for (const row of (section.rows || [])) {
              const rowHeight = fontSize + cellPadding * 2 + 2;
              if (currentY + rowHeight > PAGE_HEIGHT - MARGIN_BOTTOM - 4) {
                // Continue table on next page with repeated header
                doc.addPage();
                const continuedHeaderY = doc.y;
                doc.rect(tableStartX, continuedHeaderY - 2, tableWidth, fontSize + cellPadding * 2 + 2)
                  .fillOpacity(0.06).fill("#000000");
                doc.fillOpacity(1).fillColor("#000000");
                for (let i = 0; i < numCols; i++) {
                  const cellX = tableStartX + i * colWidth + cellPadding;
                  doc.font(FONT_SERIF_BOLD).fontSize(fontSize);
                  doc.text(
                    (section.headers[i] || "").substring(0, Math.floor(colWidth / (fontSize * 0.5))),
                    cellX,
                    continuedHeaderY + cellPadding,
                    { width: colWidth - cellPadding * 2, align: "left", lineBreak: false }
                  );
                }
                doc.moveTo(tableStartX, continuedHeaderY - 2).lineTo(tableStartX + tableWidth, continuedHeaderY - 2)
                  .strokeColor("#000000").lineWidth(1).stroke();
                currentY = continuedHeaderY + fontSize + cellPadding * 2 + 2;
                doc.moveTo(tableStartX, currentY).lineTo(tableStartX + tableWidth, currentY)
                  .strokeColor("#000000").lineWidth(0.5).stroke();
                currentY += 2;
                doc.font(FONT_SERIF).fontSize(fontSize);
              }
              for (let i = 0; i < numCols; i++) {
                const cellX = tableStartX + i * colWidth + cellPadding;
                const cellText = (row[i] || "").substring(0, Math.floor(colWidth / (fontSize * 0.45)));
                doc.text(cellText, cellX, currentY + cellPadding, {
                  width: colWidth - cellPadding * 2,
                  align: "left",
                  lineBreak: false,
                });
              }
              currentY += fontSize + cellPadding * 2;
            }

            // Bottom rule
            doc.moveTo(tableStartX, currentY).lineTo(tableStartX + tableWidth, currentY)
              .strokeColor("#000000").lineWidth(1).stroke();

            doc.y = currentY + 8;
            // Add spacing after table
            doc.moveDown(0.8);
            break;
          }

          case "equation": {
            // Display math equation - centered with subtle background
            const eqText = section.text || "";
            const eqLines = eqText
              .split("\n")
              .map((l: string) => sanitizePdfMathText(l))
              .filter((l: string) => l.trim());
            const lineHeight = 14;
            const eqTotalHeight = eqLines.length * lineHeight + 24; // padding top/bottom
            ensureSpace(eqTotalHeight + 20);

            doc.moveDown(0.4);
            const eqStartY = doc.y;

            // Draw subtle background
            doc.rect(
              MARGIN_LEFT + 30,
              eqStartY - 4,
              CONTENT_WIDTH - 60,
              eqTotalHeight
            ).fillOpacity(0.025).fill("#1e40af");
            doc.fillOpacity(1).fillColor("#000000");

            // Draw left accent bar
            doc.moveTo(MARGIN_LEFT + 30, eqStartY - 4)
              .lineTo(MARGIN_LEFT + 30, eqStartY - 4 + eqTotalHeight)
              .strokeColor("#6366f1").lineWidth(1.5).stroke();

            // Render each line of the equation
            doc.font(FONT_SERIF_ITALIC).fontSize(10.5);
            let eqY = eqStartY + 8;
            for (const eqLine of eqLines) {
              doc.text(eqLine, MARGIN_LEFT + 40, eqY, {
                width: CONTENT_WIDTH - 80,
                align: "center",
                lineBreak: false,
              });
              eqY += lineHeight;
            }

            doc.y = eqStartY + eqTotalHeight + 4;
            doc.moveDown(0.4);
            break;
          }

          case "list": {
            ensureSpace(20);
            doc.font(FONT_SERIF).fontSize(10.5);
            for (const item of (section.items || [])) {
              ensureSpace(15);
              doc.text(`  •  ${item}`, MARGIN_LEFT + 10, doc.y, {
                width: CONTENT_WIDTH - 20,
                lineGap: 2,
              });
              doc.moveDown(0.1);
            }
            doc.moveDown(0.2);
            break;
          }

          case "bibliography": {
            ensureSpace(30);
            doc.moveDown(0.5);
            doc.font(FONT_SERIF_BOLD).fontSize(13);
            doc.text("References", MARGIN_LEFT, doc.y, { width: CONTENT_WIDTH });
            doc.moveDown(0.1);
            doc.moveTo(MARGIN_LEFT, doc.y).lineTo(PAGE_WIDTH - MARGIN_RIGHT, doc.y).strokeColor("#eeeeee").lineWidth(0.5).stroke();
            doc.moveDown(0.3);

            doc.font(FONT_SERIF).fontSize(9.5);
            (section.items || []).forEach((item, i) => {
              ensureSpace(15);
              doc.text(`[${i + 1}] ${item}`, MARGIN_LEFT + 5, doc.y, {
                width: CONTENT_WIDTH - 10,
                lineGap: 1.5,
              });
              doc.moveDown(0.15);
            });
            break;
          }

          case "separator": {
            doc.moveDown(0.3);
            doc.moveTo(MARGIN_LEFT, doc.y).lineTo(PAGE_WIDTH - MARGIN_RIGHT, doc.y).strokeColor("#dddddd").lineWidth(0.5).stroke();
            doc.moveDown(0.3);
            break;
          }
        }
      }

      const range = doc.bufferedPageRange();
      const totalPages = range.count;

      // Add page numbers and header to all pages
      for (let i = 0; i < totalPages; i++) {
        doc.switchToPage(i);
        // Footer: page number
        doc.font(FONT_SERIF).fontSize(8).fillColor("#999999");
        doc.text(
          `${i + 1} / ${totalPages}`,
          0,
          PAGE_HEIGHT - 40,
          { width: PAGE_WIDTH, align: "center" }
        );
        // Header: conference name (skip first page)
        if (i > 0) {
          doc.text(
            conference || "NeurIPS 2025",
            0,
            25,
            { width: PAGE_WIDTH, align: "center" }
          );
        }
        doc.fillColor("#000000");
      }

      // Footer on last page
      doc.switchToPage(totalPages - 1);
      doc.font(FONT_SERIF).fontSize(7).fillColor("#999999");
      doc.text(
        `Generated by Auto Research • ${new Date().toISOString().split("T")[0]}`,
        0,
        PAGE_HEIGHT - 28,
        { width: PAGE_WIDTH, align: "center" }
      );

      doc.end();
    } catch (err) {
      reject(err);
    }
  });
}

/* ------------------------------------------------------------------ */
/*  Public API                                                        */
/* ------------------------------------------------------------------ */

/**
 * Generate PDF from paper content using PDFKit (no Chromium required).
 *
 * Strategy:
 *  1. If latexSource is provided, parse LaTeX → structured sections → PDF
 *  2. Otherwise, parse Markdown → structured sections → PDF
 *  3. Chart images are fetched and embedded directly
 */
export async function generatePaperPdf(
  markdownContent: string,
  title: string,
  conference?: string,
  latexSource?: string,
  chartImages: ChartImage[] = []
): Promise<Buffer> {
  const confLabel = conference || "NeurIPS 2025";

  // Primary path: LaTeX → sections → PDF
  if (latexSource && latexSource.trim().length > 100) {
    console.log(`[PDF] Parsing LaTeX → structured sections (${chartImages.length} chart images)...`);
    try {
      const sections = parseLatexToSections(latexSource, title, confLabel, chartImages);
      console.log(`[PDF] Parsed ${sections.length} sections, rendering to PDF...`);
      const pdfBuffer = await renderSectionsToPdf(sections, confLabel);
      if (pdfBuffer && pdfBuffer.length > 100) {
        console.log(`[PDF] LaTeX → PDFKit produced ${(pdfBuffer.length / 1024).toFixed(1)} KiB`);
        return pdfBuffer;
      }
    } catch (err: any) {
      console.warn(`[PDF] LaTeX → PDFKit failed: ${err.message}, falling back to Markdown`);
    }
  }

  // Fallback path: Markdown → sections → PDF
  if (markdownContent && markdownContent.trim().length > 0) {
    console.log(`[PDF] Parsing Markdown → structured sections (${chartImages.length} chart images)...`);
    const sections = parseMarkdownToSections(markdownContent, title, confLabel, chartImages);
    console.log(`[PDF] Parsed ${sections.length} sections, rendering to PDF...`);
    const pdfBuffer = await renderSectionsToPdf(sections, confLabel);
    console.log(`[PDF] Markdown → PDFKit produced ${(pdfBuffer.length / 1024).toFixed(1)} KiB`);
    return pdfBuffer;
  }

  throw new Error("No content available for PDF generation (both LaTeX and Markdown are empty)");
}

/**
 * Generate PDF from Markdown content.
 */
export async function generatePaperPdfFromMarkdown(
  markdownContent: string,
  title: string,
  conference?: string,
  chartImages: ChartImage[] = []
): Promise<Buffer> {
  return generatePaperPdf(markdownContent, title, conference, undefined, chartImages);
}

/**
 * @deprecated Use generatePaperPdf instead. Kept for backward compatibility.
 */
export async function compileLatexToPdf(
  latexSource: string,
  chartImages: ChartImage[] = [],
  _timeoutMs = 180_000
): Promise<Buffer | null> {
  try {
    return await generatePaperPdf("", "Research Paper", "Conference", latexSource, chartImages);
  } catch {
    return null;
  }
}

/**
 * Find Chromium - kept for backward compatibility with experiment-runner.
 * Returns empty string if not found (no longer throws).
 */
export function findChromium(): string {
  const paths = [
    "/usr/bin/chromium-browser",
    "/usr/bin/chromium",
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
  ];
  for (const p of paths) {
    try {
      if (fsNode.existsSync(p)) return p;
    } catch {}
  }
  // Try 'which' command
  for (const cmd of ["chromium-browser", "chromium", "google-chrome"]) {
    try {
      const result = execSync(`which ${cmd} 2>/dev/null`, { encoding: "utf-8" }).trim();
      if (result) return result;
    } catch {}
  }
  return ""; // Return empty instead of throwing
}
