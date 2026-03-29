import { describe, expect, it } from "vitest";
import { generateDefaultCharts, generateSvgFallbackChart } from "./experiment-runner";
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

  it("prioritises topic-relevant variables over age for descriptive figures", () => {
    const charts = generateDefaultCharts([
      {
        name: "panel.csv",
        totalRows: 6,
        columns: ["pidp", "wave", "age", "ghq_score", "job_hours", "employment_shock"],
        data: [
          { pidp: 1, wave: 1, age: 28, ghq_score: 11, job_hours: 40, employment_shock: 0 },
          { pidp: 1, wave: 2, age: 29, ghq_score: 13, job_hours: 35, employment_shock: 1 },
          { pidp: 2, wave: 1, age: 41, ghq_score: 6, job_hours: 38, employment_shock: 0 },
          { pidp: 2, wave: 2, age: 42, ghq_score: 8, job_hours: 37, employment_shock: 0 },
          { pidp: 3, wave: 1, age: 36, ghq_score: 10, job_hours: 20, employment_shock: 1 },
          { pidp: 3, wave: 2, age: 37, ghq_score: 12, job_hours: 18, employment_shock: 1 },
        ],
      },
    ], null, "Mental Health and Labour Market in UK");

    const firstChart = charts.find(chart => chart.name === "distribution_histogram");
    expect(firstChart).toBeTruthy();
    expect(firstChart?.description.toLowerCase()).toContain("ghq");
    expect(firstChart?.description.toLowerCase()).not.toContain("age");
  });
});
