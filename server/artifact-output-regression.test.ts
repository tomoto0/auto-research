import { describe, expect, it } from "vitest";
import { generateSvgFallbackChart } from "./experiment-runner";
import { describeExperimentArtifact } from "./pipeline-engine";

describe("Artifact output regressions", () => {
  it("stores deterministic dataset analysis plans as JSON artifacts", () => {
    const descriptor = describeExperimentArtifact(JSON.stringify({
      methods: ["descriptive_statistics", "correlation"],
      blockedMethods: ["advanced_nlp"],
      topic: "Mental Health and Labour Market in UK",
    }));

    expect(descriptor.fileName).toBe("analysis_plan.json");
    expect(descriptor.storageFileName).toBe("analysis-plan.json");
    expect(descriptor.mimeType).toBe("application/json");
  });

  it("keeps free-form experiment code as Python artifacts", () => {
    const descriptor = describeExperimentArtifact("import pandas as pd\nprint('ok')\n");

    expect(descriptor.fileName).toBe("experiment.py");
    expect(descriptor.storageFileName).toBe("experiment.py");
    expect(descriptor.mimeType).toBe("text/x-python");
  });

  it("sanitises non-ASCII chart labels before SVG rendering", () => {
    const svg = generateSvgFallbackChart(JSON.stringify({
      type: "line",
      data: {
        labels: ["2021年", "2022年", "2023年"],
        datasets: [
          {
            label: "メンタルヘルス指標",
            data: [1.1, 1.4, 1.2],
          },
        ],
      },
      options: {
        plugins: {
          title: { display: true, text: "雇用とメンタルヘルスの推移" },
        },
        scales: {
          x: { title: { display: true, text: "年" } },
          y: { title: { display: true, text: "平均スコア" } },
        },
      },
    }), 900, 560).toString("utf-8");

    expect(svg).not.toMatch(/[^\x00-\x7F]/);
    expect(svg).toContain("<svg");
    expect(svg).toContain("</svg>");
  });
});
