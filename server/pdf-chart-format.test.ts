import { describe, expect, it } from "vitest";
import { generatePaperPdf } from "./pdf-generator";
import { generateSvgFallbackChart } from "./experiment-runner";

function countPdfPages(pdfBuffer: Buffer): number {
  const raw = pdfBuffer.toString("latin1");
  return (raw.match(/\/Type\s*\/Page\b/g) || []).length;
}

describe("PDF formatting regressions", () => {
  it("does not append extra blank pages on short papers", async () => {
    const latex = `\\documentclass{article}
\\begin{document}
\\title{Blank Page Regression}
\\maketitle
\\begin{abstract}
Short abstract for page counting.
\\end{abstract}
\\section{Introduction}
This paragraph should fit on one page and must not trigger extra blank pages.
\\section{Conclusion}
The document ends here.
\\end{document}`;

    const pdf = await generatePaperPdf("", "Blank Page Regression", "TestConf 2026", latex, []);
    expect(pdf.slice(0, 5).toString()).toBe("%PDF-");

    const pageCount = countPdfPages(pdf);
    // Previously this produced 3 pages due footer drawing outside the printable region.
    expect(pageCount).toBeLessThanOrEqual(2);
    expect(pageCount).toBeGreaterThanOrEqual(1);
  });
});

describe("Chart fallback rendering regressions", () => {
  it("renders bubble charts with circles and without malformed coordinates", () => {
    const bubbleConfig = JSON.stringify({
      type: "bubble",
      data: {
        datasets: [
          {
            label: "Positive correlation",
            data: [
              { x: 0, y: 0, r: 8 },
              { x: 1, y: 1, r: 12 },
              { x: 2, y: 1.8, r: 10 },
            ],
          },
          {
            label: "Negative correlation",
            data: [
              { x: 0, y: 2, r: 6 },
              { x: 1, y: 1, r: 7 },
            ],
          },
        ],
      },
      options: {
        plugins: {
          title: { display: true, text: "Correlation Matrix" },
        },
      },
    });

    const svg = generateSvgFallbackChart(bubbleConfig, 900, 560).toString("utf-8");
    expect(svg.startsWith("<svg")).toBe(true);
    expect(svg).toContain("<circle");
    expect(svg).not.toContain("NaN");
    expect(svg).not.toContain("undefined");
  });

  it("renders pie charts as arc paths (not bar placeholders)", () => {
    const pieConfig = JSON.stringify({
      type: "pie",
      data: {
        labels: ["A", "B", "C", "D"],
        datasets: [
          {
            data: [40, 25, 20, 15],
          },
        ],
      },
      options: {
        plugins: {
          title: { display: true, text: "Category Distribution" },
        },
      },
    });

    const svg = generateSvgFallbackChart(pieConfig, 900, 560).toString("utf-8");
    expect(svg.startsWith("<svg")).toBe(true);
    expect(svg).toContain("<path d=\"M");
    expect(svg).not.toContain("NaN");
    expect(svg).not.toContain("undefined");
  });
});
