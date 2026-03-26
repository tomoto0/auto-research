/**
 * 23-Stage Research Pipeline Engine
 * Orchestrates the full autonomous research pipeline using LLM and literature search
 * Supports manual approval mode: pauses after each stage for user review
 * Supports dataset-driven experiments: uploads data files, generates analysis code, executes Python
 */
import { invokeLLM } from "./_core/llm";
import { PIPELINE_STAGES, RunConfig, PipelineEvent } from "../shared/pipeline";
import * as db from "./db";
import { unifiedSearch, type LiteratureResult } from "./literature";
import { storagePut, storageGet } from "./storage";
import { nanoid } from "nanoid";
import { generatePaperPdf, type ChartImage } from "./pdf-generator";
import { executePythonExperiment, type DatasetInfo, type ExperimentOutput } from "./experiment-runner";

export type EventEmitter = (event: PipelineEvent) => void;

interface PipelineContext {
  runId: string;
  topic: string;
  config: RunConfig;
  emit: EventEmitter;
  papers: LiteratureResult[];
  hypothesis: string;
  methodology: string;
  methodValidation: string;
  experimentCode: string;
  experimentResults: string;
  statisticalAnalysis: string;
  figures: string[];
  tables: string[];
  outline: string;
  abstract: string;
  paperBody: string;
  references: string;
  latex: string;
  reviewReport: string;
  revision: string;
  finalPaper: string;
  // Dataset-driven experiment fields
  datasetFiles: DatasetInfo[];
  experimentOutput: ExperimentOutput | null;
  evidenceProfile: ResearchEvidenceProfile | null;
  methodContract: MethodFeasibilityContract | null;
  executionDiagnostics: ExecutionDiagnostics | null;
  methodIntegrityNote: string;
  claimVerificationReport: string;
}

interface ResearchEvidenceProfile {
  datasetSummary: {
    datasetCount: number;
    totalRowsHint: number;
    hasTimeLike: boolean;
    hasTextLike: boolean;
    hasPanelLike: boolean;
    hasGraphLike: boolean;
    hasImageLike: boolean;
  };
  literatureSummary: {
    paperCount: number;
    topMethodSignals: Record<string, number>;
  };
  recommendedExecutableMethods: string[];
  constrainedMethods: string[];
}

interface MethodFeasibilityContract {
  executableNow: string[];
  requiresMissingData: string[];
  futureWorkOnly: string[];
  blockedReasons: Record<string, string>;
  evidenceNotes: string[];
}

interface ExecutionDiagnostics {
  executionStatus: "success" | "partial" | "failed";
  executableRequested: string[];
  executedMethods: string[];
  missingRequested: string[];
  analyticalMetricCount: number;
  chartCount: number;
  tableCount: number;
  failureReasons: string[];
}

interface ClaimVerificationResult {
  report: string;
  flaggedClaims: string[];
}

// ─── Approval tracking for manual mode ───
// Maps runId -> { resolve, reject } for the currently awaiting approval
const approvalWaiters = new Map<string, {
  resolve: (editedOutput?: string) => void;
  reject: (reason?: string) => void;
}>();

/** Approve a stage and optionally provide edited output */
export function approveStage(runId: string, editedOutput?: string): boolean {
  const waiter = approvalWaiters.get(runId);
  if (!waiter) return false;
  waiter.resolve(editedOutput);
  approvalWaiters.delete(runId);
  return true;
}

/** Reject a stage and stop the pipeline */
export function rejectStage(runId: string, reason?: string): boolean {
  const waiter = approvalWaiters.get(runId);
  if (!waiter) return false;
  waiter.reject(reason || "Stage rejected by user");
  approvalWaiters.delete(runId);
  return true;
}

/** Check if a run is currently awaiting approval */
export function isAwaitingApproval(runId: string): boolean {
  return approvalWaiters.has(runId);
}

async function callLLM(systemPrompt: string, userPrompt: string, maxTokens?: number): Promise<string> {
  const result = await invokeLLM({
    messages: [
      { role: "system", content: systemPrompt },
      { role: "user", content: userPrompt },
    ],
    maxTokens: maxTokens || 16384,
  });
  const content = result.choices?.[0]?.message?.content;
  return typeof content === "string" ? content : JSON.stringify(content);
}

/**
 * Strip markdown code block markers from LLM output.
 * Handles ```latex, ```python, ```tex, ```bibtex, and generic ``` markers.
 */
function stripCodeBlockMarkers(text: string): string {
  if (!text) return text;
  // Remove opening ```lang markers and closing ``` markers
  let cleaned = text.trim();
  // Match opening code fence with optional language tag
  cleaned = cleaned.replace(/^```(?:latex|tex|python|bibtex|json|\w*)\s*\n?/i, "");
  // Match closing code fence at the end
  cleaned = cleaned.replace(/\n?```\s*$/i, "");
  return cleaned.trim();
}

// ─── Helper: Build dataset description for LLM prompts ───
function buildDatasetDescription(datasets: DatasetInfo[]): string {
  if (datasets.length === 0) return "";
  return datasets.map((ds, i) => {
    const cols = ds.columnNames?.join(", ") || "unknown columns";
    let desc = `Dataset ${i + 1}: "${ds.originalName}" (${ds.fileType}, ${ds.rowCount ?? "?"} rows)\n  Columns: ${cols}`;
    if (!ds.columnNames || ds.columnNames.length === 0) {
      desc += "\n  NOTE: Column names could not be extracted at upload time. The analysis engine will parse the file at runtime and use actual column names.";
    }
    return desc;
  }).join("\n");
}

function buildDatasetCapabilityHints(datasets: DatasetInfo[]): string {
  if (datasets.length === 0) return "";
  return datasets.map((ds, i) => {
    const cols = ds.columnNames || [];
    const colText = cols.length > 0 ? cols.join(", ") : "unknown";
    const hasTimeLike = cols.some(c => /(year|month|date|time|wave|period|quarter)/i.test(c));
    const hasTextLike = cols.some(c => /(text|comment|abstract|title|description|review|note)/i.test(c));
    return `Dataset ${i + 1}: ${ds.originalName}\n- Rows: ${ds.rowCount ?? "unknown"}\n- Columns: ${cols.length || "unknown"}\n- Time-like columns detected: ${hasTimeLike ? "yes" : "no"}\n- Text-like columns detected: ${hasTextLike ? "yes" : "no"}\n- Column names: ${colText}`;
  }).join("\n\n");
}

function normaliseMethodId(raw: string): string {
  const norm = (raw || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  const aliases: Record<string, string> = {
    descriptive: "descriptive_statistics",
    descriptive_stats: "descriptive_statistics",
    descriptive_statistics: "descriptive_statistics",
    summary_statistics: "descriptive_statistics",
    correlation_analysis: "correlation",
    pearson_correlation: "correlation",
    correlation: "correlation",
    linear_regression: "linear_regression",
    regression: "linear_regression",
    ols: "linear_regression",
    group_comparison: "group_comparison",
    anova: "group_comparison",
    t_test: "group_comparison",
    time_trend: "time_trend",
    time_series_trend: "time_trend",
    text_analysis: "text_feature_analysis",
    text_feature_analysis: "text_feature_analysis",
    nlp: "text_feature_analysis",
    data_visualization: "data_visualisation",
    data_visualisation: "data_visualisation",
    visualization: "data_visualisation",
    visualisation: "data_visualisation",
    causal_inference: "causal_inference",
    graph_neural_network: "graph_modelling",
    gnn: "graph_modelling",
    graph_modelling: "graph_modelling",
    computer_vision: "vision_analysis",
    vision_analysis: "vision_analysis",
    panel_model: "panel_econometrics",
    panel_econometrics: "panel_econometrics",
    structural_equation_modelling: "panel_econometrics",
    advanced_time_series: "advanced_time_series",
    arima_var_lstm: "advanced_time_series",
    advanced_nlp_deep_learning: "advanced_nlp",
    advanced_nlp: "advanced_nlp",
  };
  return aliases[norm] || norm;
}

function uniqMethodIds(items: string[]): string[] {
  const set = new Set<string>();
  for (const item of items) {
    const m = normaliseMethodId(item);
    if (m) set.add(m);
  }
  return Array.from(set);
}

function buildLiteratureMethodSignals(papers: LiteratureResult[]): Record<string, number> {
  const signals: Record<string, number> = {
    descriptive_statistics: 0,
    correlation: 0,
    linear_regression: 0,
    group_comparison: 0,
    time_trend: 0,
    text_feature_analysis: 0,
    causal_inference: 0,
    graph_modelling: 0,
    vision_analysis: 0,
    panel_econometrics: 0,
    advanced_time_series: 0,
    advanced_nlp: 0,
  };
  for (const p of papers) {
    const text = `${p.title || ""} ${p.abstract || ""}`.toLowerCase();
    if (/(descriptive|summary statistics|distribution)/i.test(text)) signals.descriptive_statistics++;
    if (/(correlation|association|pearson|spearman)/i.test(text)) signals.correlation++;
    if (/(regression|ols|glm|logit|probit)/i.test(text)) signals.linear_regression++;
    if (/(anova|t-test|between-group|group comparison|treatment group)/i.test(text)) signals.group_comparison++;
    if (/(time trend|temporal trend|panel year|longitudinal trend)/i.test(text)) signals.time_trend++;
    if (/(text mining|nlp|sentiment|topic model|keyword extraction)/i.test(text)) signals.text_feature_analysis++;
    if (/(causal|difference-in-differences|instrumental variable|propensity score|rdd|synthetic control)/i.test(text)) signals.causal_inference++;
    if (/(graph neural|gnn|network model|graph convolution|edge|node)/i.test(text)) signals.graph_modelling++;
    if (/(computer vision|image classification|visual recognition|cnn)/i.test(text)) signals.vision_analysis++;
    if (/(panel model|fixed effects|random effects|sem|structural equation|gmm)/i.test(text)) signals.panel_econometrics++;
    if (/(arima|var\b|state space model|lstm|forecasting)/i.test(text)) signals.advanced_time_series++;
    if (/(transformer|bert|deep learning|neural network|embedding model|large language model)/i.test(text)) signals.advanced_nlp++;
  }
  return signals;
}

function buildResearchEvidenceProfile(topic: string, datasets: DatasetInfo[], papers: LiteratureResult[]): ResearchEvidenceProfile {
  const allCols = datasets.flatMap(ds => ds.columnNames || []).map(c => c.toLowerCase());
  const hasTimeLike = allCols.some(c => /(year|month|date|time|wave|period|quarter)/i.test(c));
  const hasTextLike = allCols.some(c => /(text|comment|abstract|title|description|review|note|content)/i.test(c));
  const hasGraphLike = allCols.some(c => /(node|edge|source|target|network|graph)/i.test(c));
  const hasImageLike = allCols.some(c => /(image|img|pixel|vision|frame|video|path)/i.test(c));
  const hasPanelLike = hasTimeLike && allCols.some(c => /(id|code|entity|respondent|household|firm|user|patient)/i.test(c));
  const totalRowsHint = datasets.reduce((sum, ds) => sum + (ds.rowCount || 0), 0);
  const methodSignals = buildLiteratureMethodSignals(papers);

  const recommendedExecutableMethods = uniqMethodIds([
    "descriptive_statistics",
    "correlation",
    totalRowsHint >= 30 ? "linear_regression" : "",
    hasTimeLike ? "time_trend" : "",
    hasTextLike ? "text_feature_analysis" : "",
    datasets.length > 0 ? "data_visualisation" : "",
    hasPanelLike ? "group_comparison" : "",
  ].filter(Boolean));

  const constrainedMethods = uniqMethodIds([
    !hasTextLike ? "advanced_nlp" : "",
    !hasGraphLike ? "graph_modelling" : "",
    !hasImageLike ? "vision_analysis" : "",
    !hasPanelLike ? "panel_econometrics" : "",
    !hasTimeLike ? "advanced_time_series" : "",
    "causal_inference",
  ].filter(Boolean));

  return {
    datasetSummary: {
      datasetCount: datasets.length,
      totalRowsHint,
      hasTimeLike,
      hasTextLike,
      hasPanelLike,
      hasGraphLike,
      hasImageLike,
    },
    literatureSummary: {
      paperCount: papers.length,
      topMethodSignals: methodSignals,
    },
    recommendedExecutableMethods,
    constrainedMethods,
  };
}

function ensureEvidenceProfile(ctx: PipelineContext): ResearchEvidenceProfile {
  if (!ctx.evidenceProfile) {
    ctx.evidenceProfile = buildResearchEvidenceProfile(ctx.topic, ctx.datasetFiles, ctx.papers);
  }
  return ctx.evidenceProfile;
}

function extractJsonBetweenTags(text: string, tagName: string): string | null {
  const regex = new RegExp(`\\[${tagName}\\]([\\s\\S]*?)\\[\\/${tagName}\\]`, "i");
  const m = text.match(regex);
  if (!m) return null;
  return m[1].trim();
}

function deriveFallbackMethodContract(methodologyText: string, profile: ResearchEvidenceProfile): MethodFeasibilityContract {
  const lower = (methodologyText || "").toLowerCase();
  const executable = new Set<string>(profile.recommendedExecutableMethods);
  const requiresMissing = new Set<string>();
  const futureOnly = new Set<string>(profile.constrainedMethods);
  const blockedReasons: Record<string, string> = {};

  if (/(regression|ols|logit|probit)/i.test(lower)) executable.add("linear_regression");
  if (/(correlation|association|pearson|spearman)/i.test(lower)) executable.add("correlation");
  if (/(anova|t-test|group comparison|between-group)/i.test(lower)) executable.add("group_comparison");
  if (/(time trend|time-series|arima|var\b|lstm)/i.test(lower)) {
    if (profile.datasetSummary.hasTimeLike) executable.add("time_trend");
    else {
      requiresMissing.add("advanced_time_series");
      blockedReasons.advanced_time_series = "No reliable temporal fields detected in uploaded datasets.";
    }
  }
  if (/(nlp|text mining|sentiment|topic model|transformer|bert|embedding)/i.test(lower)) {
    if (profile.datasetSummary.hasTextLike) executable.add("text_feature_analysis");
    else {
      requiresMissing.add("advanced_nlp");
      blockedReasons.advanced_nlp = "No text-like columns were detected in uploaded datasets.";
    }
  }
  if (/(graph neural|gnn|network embedding|graph convolution)/i.test(lower)) {
    requiresMissing.add("graph_modelling");
    blockedReasons.graph_modelling = "No graph structure (node/edge columns) was detected in uploaded datasets.";
  }
  if (/(computer vision|image model|cnn|visual analysis)/i.test(lower)) {
    requiresMissing.add("vision_analysis");
    blockedReasons.vision_analysis = "No image paths/pixel features were detected in uploaded datasets.";
  }
  if (/(causal|difference-in-differences|instrumental variable|propensity score|rdd|synthetic control)/i.test(lower)) {
    requiresMissing.add("causal_inference");
    blockedReasons.causal_inference = "Current data/identification assumptions are insufficient for robust causal claims.";
  }

  const executableNow = uniqMethodIds(Array.from(executable).filter(m =>
    !profile.constrainedMethods.includes(m) && !requiresMissing.has(m)
  ));
  const requiresMissingData = uniqMethodIds(Array.from(requiresMissing));
  const futureWorkOnly = uniqMethodIds(Array.from(new Set([...Array.from(futureOnly), ...requiresMissingData])));

  return {
    executableNow,
    requiresMissingData,
    futureWorkOnly,
    blockedReasons,
    evidenceNotes: [
      `Datasets: ${profile.datasetSummary.datasetCount}, rows (hint): ${profile.datasetSummary.totalRowsHint || "unknown"}`,
      `Capabilities — time:${profile.datasetSummary.hasTimeLike ? "yes" : "no"}, text:${profile.datasetSummary.hasTextLike ? "yes" : "no"}, panel:${profile.datasetSummary.hasPanelLike ? "yes" : "no"}, graph:${profile.datasetSummary.hasGraphLike ? "yes" : "no"}, image:${profile.datasetSummary.hasImageLike ? "yes" : "no"}`,
      `Literature papers considered: ${profile.literatureSummary.paperCount}`,
    ],
  };
}

function parseMethodContract(validationText: string, methodologyText: string, profile: ResearchEvidenceProfile): MethodFeasibilityContract {
  const fallback = deriveFallbackMethodContract(methodologyText, profile);
  const jsonBlock = extractJsonBetweenTags(validationText, "METHOD_CONTRACT_JSON");
  if (!jsonBlock) return fallback;
  try {
    const parsed = JSON.parse(jsonBlock) as {
      executable_now?: string[];
      requires_missing_data?: string[];
      future_work_only?: string[];
      blocked_reasons?: Record<string, string>;
      evidence_notes?: string[];
    };
    const executableNow = uniqMethodIds(parsed.executable_now || []);
    const requiresMissingData = uniqMethodIds(parsed.requires_missing_data || []);
    const futureWorkOnly = uniqMethodIds(parsed.future_work_only || []);
    const blockedReasons: Record<string, string> = {};
    for (const [k, v] of Object.entries(parsed.blocked_reasons || {})) {
      blockedReasons[normaliseMethodId(k)] = String(v);
    }
    const mergedFuture = uniqMethodIds([...futureWorkOnly, ...requiresMissingData, ...profile.constrainedMethods]);
    const safeExecutableNow = executableNow.filter(m =>
      !mergedFuture.includes(m) && !profile.constrainedMethods.includes(m)
    );
    return {
      executableNow: safeExecutableNow.length > 0 ? safeExecutableNow : fallback.executableNow,
      requiresMissingData: requiresMissingData.length > 0 ? requiresMissingData : fallback.requiresMissingData,
      futureWorkOnly: mergedFuture.length > 0 ? mergedFuture : fallback.futureWorkOnly,
      blockedReasons: Object.keys(blockedReasons).length > 0 ? blockedReasons : fallback.blockedReasons,
      evidenceNotes: (parsed.evidence_notes || []).map(s => String(s)).filter(Boolean),
    };
  } catch {
    return fallback;
  }
}

function formatMethodContract(contract: MethodFeasibilityContract | null): string {
  if (!contract) return "No method feasibility contract available.";
  const reasons = Object.entries(contract.blockedReasons)
    .map(([k, v]) => `- ${k}: ${v}`)
    .join("\n");
  return [
    `Executable now: ${contract.executableNow.length ? contract.executableNow.join(", ") : "none"}`,
    `Requires missing data: ${contract.requiresMissingData.length ? contract.requiresMissingData.join(", ") : "none"}`,
    `Future-work only: ${contract.futureWorkOnly.length ? contract.futureWorkOnly.join(", ") : "none"}`,
    reasons ? `Blocked reasons:\n${reasons}` : "",
    contract.evidenceNotes.length ? `Evidence notes:\n${contract.evidenceNotes.map(n => `- ${n}`).join("\n")}` : "",
  ].filter(Boolean).join("\n");
}

function collectExecutedMethodIds(experimentOutput: ExperimentOutput | null): string[] {
  if (!experimentOutput) return [];
  const byMetrics = String(experimentOutput.metrics?.analysis_methods_executed || "")
    .split(",")
    .map(s => normaliseMethodId(s.trim()))
    .filter(Boolean);
  if (byMetrics.length > 0) return uniqMethodIds(byMetrics);
  const keys = Object.keys(experimentOutput.metrics || {});
  const inferred: string[] = [];
  if (keys.some(k => k.startsWith("mean_") || k.startsWith("std_") || k.startsWith("median_"))) inferred.push("descriptive_statistics");
  if (keys.some(k => k.startsWith("strongest_correlation_"))) inferred.push("correlation");
  if (keys.some(k => k.startsWith("regression_"))) inferred.push("linear_regression");
  if (keys.some(k => k.startsWith("anova_"))) inferred.push("group_comparison");
  if (keys.some(k => k.startsWith("time_trend_"))) inferred.push("time_trend");
  if (keys.some(k => k.startsWith("text_") || k.startsWith("top_term_"))) inferred.push("text_feature_analysis");
  if ((experimentOutput.charts || []).length > 0) inferred.push("data_visualisation");
  return uniqMethodIds(inferred);
}

function buildExecutionDiagnostics(
  contract: MethodFeasibilityContract | null,
  output: ExperimentOutput,
  analyticalMetricCount: number
): ExecutionDiagnostics {
  const executableRequested = contract?.executableNow || [];
  const executedMethods = collectExecutedMethodIds(output);
  const missingRequested = executableRequested.filter(m => !executedMethods.includes(m));
  const failureReasons: string[] = [];
  if (output.stderr) failureReasons.push(output.stderr.substring(0, 500));
  if (missingRequested.length > 0) failureReasons.push(`Requested executable methods not observed: ${missingRequested.join(", ")}`);
  if (analyticalMetricCount === 0) failureReasons.push("No analytical metrics were produced.");
  const unresolvedPrereq = String(output.metrics?.execution_unresolved_prerequisites || "").trim();
  const skippedExecutable = String(output.metrics?.execution_skipped_executable_methods || "").trim();
  const noOutputReasons = String(output.metrics?.execution_no_output_reasons || "").trim();
  if (unresolvedPrereq) failureReasons.push(`Unresolved prerequisites: ${unresolvedPrereq}`);
  if (skippedExecutable) failureReasons.push(`Executable methods skipped in runner: ${skippedExecutable}`);
  if (noOutputReasons) failureReasons.push(`Runner output notes: ${noOutputReasons}`);
  const executionStatus: ExecutionDiagnostics["executionStatus"] =
    !output.success && analyticalMetricCount === 0
      ? "failed"
      : (!output.success || missingRequested.length > 0 || analyticalMetricCount === 0 ? "partial" : "success");
  return {
    executionStatus,
    executableRequested,
    executedMethods,
    missingRequested,
    analyticalMetricCount,
    chartCount: output.charts.length,
    tableCount: output.tables.length,
    failureReasons,
  };
}

function formatExecutionDiagnostics(diag: ExecutionDiagnostics | null): string {
  if (!diag) return "No execution diagnostics available.";
  return [
    `Execution status: ${diag.executionStatus}`,
    `Executable methods requested: ${diag.executableRequested.length ? diag.executableRequested.join(", ") : "none"}`,
    `Methods evidenced by outputs: ${diag.executedMethods.length ? diag.executedMethods.join(", ") : "none"}`,
    `Missing requested methods: ${diag.missingRequested.length ? diag.missingRequested.join(", ") : "none"}`,
    `Analytical metrics count: ${diag.analyticalMetricCount}`,
    `Charts: ${diag.chartCount}, Tables: ${diag.tableCount}`,
    diag.failureReasons.length ? `Failure/limitation reasons:\n${diag.failureReasons.map(r => `- ${r}`).join("\n")}` : "",
  ].filter(Boolean).join("\n");
}

function buildClaimVerificationReport(ctx: PipelineContext, bodyText: string): ClaimVerificationResult {
  const flaggedClaims: string[] = [];
  const executed = new Set<string>((ctx.executionDiagnostics?.executedMethods || []).map(normaliseMethodId));
  const unsupportedMethods = new Set<string>([
    ...(ctx.methodContract?.futureWorkOnly || []),
    ...(ctx.methodContract?.requiresMissingData || []),
  ].map(normaliseMethodId));
  const claimSentences = bodyText
    .replace(/\n+/g, " ")
    .split(/(?<=[.!?])\s+/)
    .map(s => s.trim())
    .filter(s => s.length > 0);

  const assertive = /\b(we|our|this study)\b.*\b(use|used|apply|applied|implement|implemented|train|trained|estimate|estimated|demonstrate|demonstrated|find|found|show|shows)\b/i;
  const methodPatterns: Array<[string, RegExp]> = [
    ["causal_inference", /(causal|difference-in-differences|instrumental variable|propensity score|synthetic control|rdd)/i],
    ["graph_modelling", /(graph neural|gnn|graph convolution|network embedding)/i],
    ["vision_analysis", /(computer vision|image model|cnn|visual analysis)/i],
    ["advanced_nlp", /(transformer|bert|topic model|deep learning|embedding model|llm)/i],
    ["panel_econometrics", /(fixed effects|random effects|gmm|sem|structural equation)/i],
    ["advanced_time_series", /(arima|var\b|lstm|state space)/i],
  ];

  for (const sentence of claimSentences) {
    if (!assertive.test(sentence)) continue;
    for (const [methodId, pattern] of methodPatterns) {
      if (pattern.test(sentence) && (unsupportedMethods.has(methodId) || !executed.has(methodId))) {
        flaggedClaims.push(sentence.slice(0, 220));
        break;
      }
    }
  }

  const fabricatedEvidencePattern = /\b(figure|table)\s*\d+\s*(shows|demonstrates|proves)\b/i;
  const hasCharts = (ctx.experimentOutput?.charts?.length || 0) > 0;
  const hasTables = (ctx.experimentOutput?.tables?.length || 0) > 0;
  for (const sentence of claimSentences) {
    if (!fabricatedEvidencePattern.test(sentence)) continue;
    const referencesFigure = /figure\s*\d+/i.test(sentence);
    const referencesTable = /table\s*\d+/i.test(sentence);
    if ((referencesFigure && !hasCharts) || (referencesTable && !hasTables)) {
      flaggedClaims.push(sentence.slice(0, 220));
    }
  }

  const hasRealMetrics = getAnalyticalMetricEntries(ctx.experimentOutput).length > 0;
  if (!hasRealMetrics && /\b(p\s*[<=>]\s*0?\.\d+|r\s*=\s*-?\d+\.\d+|r\^?2\s*=\s*0?\.\d+|accuracy\s*=\s*0?\.\d+)/i.test(bodyText)) {
    flaggedClaims.push("Quantitative claim syntax detected despite no analytical metrics being available.");
  }

  const blockedPhrase = /\b(successfully (applied|implemented|validated)|outperformed|significantly improved|state-of-the-art)\b/i;
  if (blockedPhrase.test(bodyText) && (ctx.executionDiagnostics?.executionStatus === "failed" || !hasRealMetrics)) {
    flaggedClaims.push("Strong success/performance phrasing detected despite missing or failed empirical evidence.");
  }

  const reportLines = [
    `Claim verifier summary: ${flaggedClaims.length} potential unsupported claim(s) detected.`,
    `Executed methods: ${(ctx.executionDiagnostics?.executedMethods || []).join(", ") || "none"}`,
    `Contract future/missing methods: ${uniqMethodIds([...(ctx.methodContract?.futureWorkOnly || []), ...(ctx.methodContract?.requiresMissingData || [])]).join(", ") || "none"}`,
    flaggedClaims.length > 0
      ? `Flagged claims:\n${flaggedClaims.slice(0, 8).map(c => `- ${c}`).join("\n")}`
      : "No unsupported method-claim sentence was detected by heuristic verifier.",
  ];
  return { report: reportLines.join("\n"), flaggedClaims };
}

async function persistStageAudit(ctx: PipelineContext, stageNumber: number, metrics: Record<string, unknown>): Promise<void> {
  try {
    await db.updateStageLog(ctx.runId, stageNumber, { metrics: metrics as any });
  } catch (err: any) {
    console.warn(`[Pipeline] Failed to persist stage ${stageNumber} audit:`, err?.message);
  }
}

function getAnalyticalMetricEntries(experimentOutput: ExperimentOutput | null): [string, number | string][] {
  if (!experimentOutput) return [];
  const entries = Object.entries(experimentOutput.metrics || {});
  if (entries.length === 0) return [];

  const analyticalPrefixes = [
    "mean_", "std_", "median_", "min_", "max_",
    "strongest_correlation_", "p_value_", "regression_",
    "anova_", "time_trend_", "text_", "top_",
  ];
  const analyticalExact = new Set([
    "correlation_sample_size",
    "correlation_interpretation",
    "analysis_methods_executed",
  ]);

  return entries.filter(([k]) => {
    if (analyticalExact.has(k)) return true;
    return analyticalPrefixes.some(prefix => k.startsWith(prefix));
  });
}

function collectExecutedMethods(experimentOutput: ExperimentOutput | null): string[] {
  if (!experimentOutput) return [];
  const metricKeys = Object.keys(experimentOutput.metrics || {});
  const methods = new Set<string>();

  if (metricKeys.some(k => k.startsWith("mean_") || k.startsWith("std_") || k.startsWith("median_"))) {
    methods.add("descriptive statistics");
  }
  if (metricKeys.some(k => k.startsWith("strongest_correlation_") || k === "correlation_interpretation")) {
    methods.add("correlation analysis");
  }
  if (metricKeys.some(k => k.startsWith("regression_"))) {
    methods.add("linear regression");
  }
  if (metricKeys.some(k => k.startsWith("anova_") || k.startsWith("group_"))) {
    methods.add("group comparison analysis");
  }
  if (metricKeys.some(k => k.startsWith("time_trend_"))) {
    methods.add("time trend analysis");
  }
  if (metricKeys.some(k => k.startsWith("text_") || k.startsWith("top_"))) {
    methods.add("text feature analysis");
  }
  if (experimentOutput.charts.length > 0) {
    methods.add("data visualisation");
  }

  return Array.from(methods);
}

function detectLikelyOverclaims(methodologyText: string, executedMethods: string[]): string[] {
  if (!methodologyText) return [];
  const lower = methodologyText.toLowerCase();
  const hasMethod = (needle: string) => executedMethods.some(m => m.toLowerCase().includes(needle));
  const overclaims: string[] = [];

  if (/(causal|difference-in-differences|instrumental variable|propensity score|rdd|synthetic control)/i.test(lower)) {
    overclaims.push("causal inference");
  }
  if (/(transformer|bert|llm|deep learning|neural network|topic model|embedding)/i.test(lower) && !hasMethod("text feature")) {
    overclaims.push("advanced NLP/deep learning");
  }
  if (/(graph neural|gnn|network embedding|graph convolution)/i.test(lower)) {
    overclaims.push("graph modelling");
  }
  if (/(arima|var\b|vector autoregression|lstm|time-series)/i.test(lower) && !hasMethod("time trend")) {
    overclaims.push("advanced time-series modelling");
  }
  if (/(panel model|fixed effects|random effects|gmm|sem|structural equation)/i.test(lower)) {
    overclaims.push("panel/structural econometric modelling");
  }

  return overclaims;
}

function buildMethodIntegrityNote(ctx: PipelineContext): string {
  const output = ctx.experimentOutput;
  if (!output) {
    return "No executed analysis output is available. The paper must remain methodological and avoid empirical claims.";
  }

  const analyticalMetrics = getAnalyticalMetricEntries(output);
  const executedMethods = collectExecutedMethods(output);
  const overclaims = detectLikelyOverclaims(ctx.methodology, executedMethods);

  const lines: string[] = [];
  lines.push(`Datasets analysed: ${ctx.datasetFiles.length}`);
  lines.push(`Computed analytical metrics: ${analyticalMetrics.length}`);
  lines.push(`Executed methods: ${executedMethods.length > 0 ? executedMethods.join(", ") : "none"}`);
  if (overclaims.length > 0) {
    lines.push(`Methods mentioned but not executed: ${Array.from(new Set(overclaims)).join(", ")}`);
    lines.push("Rule: treat non-executed methods as future work, not as completed experiments.");
  } else {
    lines.push("Method-claim alignment check: no obvious overclaim detected from methodology keywords.");
  }

  return lines.join("\n");
}

// ─── Stage Implementations ───

async function stage1_topicAnalysis(ctx: PipelineContext): Promise<string> {
  const datasetInfo = ctx.datasetFiles.length > 0
    ? `\n\nThe researcher has uploaded the following dataset(s) for analysis:\n${buildDatasetDescription(ctx.datasetFiles)}\nIncorporate these datasets into the analysis plan.`
    : "";
  return callLLM(
    "You are a research topic analyst. Analyze the given research topic and extract key concepts, research questions, and relevant keywords for literature search.",
    `Analyze this research topic in depth:\n\n"${ctx.topic}"${datasetInfo}\n\nProvide:\n1. Key research questions (3-5)\n2. Core concepts and definitions\n3. Related fields and subfields\n4. Search keywords for literature databases (10-15 keywords/phrases)\n5. Potential research directions`
  );
}

async function stage2_literatureSearch(ctx: PipelineContext): Promise<string> {
  const searchResults = await unifiedSearch(ctx.topic, {
    maxPerSource: 8,
    semanticScholarApiKey: process.env.SEMANTIC_SCHOLAR_API_KEY,
    springerApiKey: process.env.SPRINGER_API_KEY,
    sources: ctx.config.dataSources,
  });
  ctx.papers = searchResults;

  if (searchResults.length > 0) {
    await db.insertPapers(searchResults.map(p => ({
      runId: ctx.runId,
      paperId: p.paperId,
      title: p.title,
      authors: p.authors,
      year: p.year,
      abstract: p.abstract,
      venue: p.venue,
      citationCount: p.citationCount,
      doi: p.doi,
      arxivId: p.arxivId,
      url: p.url,
      source: p.source,
      bibtex: p.bibtex,
    })));
  }

  const sourceCounts: Record<string, number> = {};
  for (const p of searchResults) {
    sourceCounts[p.source] = (sourceCounts[p.source] || 0) + 1;
  }
  ctx.evidenceProfile = buildResearchEvidenceProfile(ctx.topic, ctx.datasetFiles, searchResults);
  const queryTerms = ctx.topic
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter(t => t.length >= 3)
    .slice(0, 10);
  const topPapers = searchResults.slice(0, 20);
  const matchCounts = topPapers.map((p) => {
    const text = `${p.title || ""} ${p.abstract || ""}`.toLowerCase();
    const matched = queryTerms.filter((term) => text.includes(term)).length;
    return matched;
  });
  const avgQueryTermCoverage = matchCounts.length
    ? matchCounts.reduce((sum, n) => sum + n, 0) / matchCounts.length
    : 0;
  const withAbstractRatio = topPapers.length
    ? topPapers.filter((p) => (p.abstract || "").trim().length > 80).length / topPapers.length
    : 0;
  const withDoiRatio = topPapers.length
    ? topPapers.filter((p) => !!(p.doi || "").trim()).length / topPapers.length
    : 0;
  const meanYear = topPapers.length
    ? topPapers.reduce((sum, p) => sum + (p.year || 0), 0) / topPapers.length
    : 0;
  await persistStageAudit(ctx, 2, {
    papersFound: searchResults.length,
    sourceCounts,
    evidenceProfile: ctx.evidenceProfile,
    retrievalQuality: {
      avgQueryTermCoverage: Math.round(avgQueryTermCoverage * 100) / 100,
      withAbstractRatio: Math.round(withAbstractRatio * 1000) / 1000,
      withDoiRatio: Math.round(withDoiRatio * 1000) / 1000,
      meanYear: Math.round(meanYear),
    },
  });

  const topSignals = Object.entries(ctx.evidenceProfile.literatureSummary.topMethodSignals)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 6)
    .map(([k, v]) => `${k}(${v})`)
    .join(", ");

  return `Found ${searchResults.length} papers from ${Object.entries(ctx.config.dataSources).filter(([,v]) => v).map(([k]) => k).join(", ")}.\nSource distribution: ${Object.entries(sourceCounts).map(([k, v]) => `${k}:${v}`).join(", ") || "none"}.\nLiterature method signals: ${topSignals || "none"}.\nRetrieval quality: avg query-term coverage=${avgQueryTermCoverage.toFixed(2)}, abstracts present=${(withAbstractRatio * 100).toFixed(1)}%, DOI present=${(withDoiRatio * 100).toFixed(1)}%.\n\nTop papers:\n${searchResults.slice(0, 10).map((p, i) => `${i + 1}. [${p.source}] ${p.title} (${p.year}) - Citations: ${p.citationCount}`).join("\n")}`;
}

async function stage3_paperScreening(ctx: PipelineContext): Promise<string> {
  const paperList = ctx.papers.slice(0, 20).map((p, i) => `${i + 1}. "${p.title}" (${p.year}) [${p.source}]\nAbstract: ${p.abstract?.substring(0, 200)}...`).join("\n\n");
  return callLLM(
    "You are a research paper screener. Evaluate papers for relevance to the research topic.",
    `Research topic: "${ctx.topic}"\n\nScreen these papers for relevance:\n\n${paperList}\n\nFor each paper, provide:\n- Relevance score (1-10)\n- Key contribution\n- Relevance justification\n- Include/Exclude recommendation`
  );
}

async function stage4_deepAnalysis(ctx: PipelineContext): Promise<string> {
  const topPapers = ctx.papers.slice(0, 10).map((p, i) => `${i + 1}. "${p.title}"\nAuthors: ${p.authors}\nAbstract: ${p.abstract}`).join("\n\n");
  return callLLM(
    "You are a research analyst performing deep analysis of academic papers.",
    `Research topic: "${ctx.topic}"\n\nPerform deep analysis of these key papers:\n\n${topPapers}\n\nProvide:\n1. Methodology comparison across papers\n2. Key findings synthesis\n3. Contradictions or debates in the field\n4. Common limitations\n5. Emerging trends`
  );
}

async function stage5_gapIdentification(ctx: PipelineContext): Promise<string> {
  return callLLM(
    "You are a research gap analyst. Identify unexplored areas and opportunities.",
    `Research topic: "${ctx.topic}"\n\nBased on the literature analysis, identify:\n1. Unexplored research gaps (3-5)\n2. Methodological gaps\n3. Data/empirical gaps\n4. Theoretical gaps\n5. Prioritized gap ranking with justification\n6. Potential impact of addressing each gap`
  );
}

async function stage6_hypothesisGeneration(ctx: PipelineContext): Promise<string> {
  const evidence = ensureEvidenceProfile(ctx);
  const hypothesisConstraint = `\n\nDataset/literature feasibility profile:\n- Dataset count: ${evidence.datasetSummary.datasetCount}\n- Capabilities: time=${evidence.datasetSummary.hasTimeLike ? "yes" : "no"}, text=${evidence.datasetSummary.hasTextLike ? "yes" : "no"}, panel=${evidence.datasetSummary.hasPanelLike ? "yes" : "no"}, graph=${evidence.datasetSummary.hasGraphLike ? "yes" : "no"}, image=${evidence.datasetSummary.hasImageLike ? "yes" : "no"}\n- Executable method hints: ${evidence.recommendedExecutableMethods.join(", ") || "none"}\n- Constrained method hints: ${evidence.constrainedMethods.join(", ") || "none"}`;
  const result = await callLLM(
    "You are a research hypothesis generator. Create testable hypotheses from identified gaps.",
    `Research topic: "${ctx.topic}"${hypothesisConstraint}\n\nBased on identified research gaps, generate:\n1. Primary hypothesis (clear, testable, specific)\n2. Secondary hypotheses (2-3)\n3. Null hypotheses\n4. Expected outcomes\n5. Variables (independent, dependent, control)\n6. Theoretical framework\n7. Operationalisation table (construct -> dataset column(s) -> expected sign/direction)\n8. Falsification criteria and rejection conditions for each core hypothesis\n\nRules:\n- Keep hypotheses executable with available data modalities.\n- If a hypothesis needs unavailable modalities, explicitly mark it as future-work only.`
  );
  ctx.hypothesis = result;
  await persistStageAudit(ctx, 6, {
    hypothesisLength: result.length,
    evidenceProfile: evidence,
  });
  return result;
}

async function stage7_methodDesign(ctx: PipelineContext): Promise<string> {
  const evidence = ensureEvidenceProfile(ctx);
  const hasDatasets = ctx.datasetFiles.length > 0;
  const datasetInfo = hasDatasets
    ? `\n\nAvailable datasets for analysis:\n${buildDatasetDescription(ctx.datasetFiles)}`
    : "";
  const capabilityHints = hasDatasets
    ? `\n\nDataset capability profile (derived from metadata; use this to avoid over-claiming):\n${buildDatasetCapabilityHints(ctx.datasetFiles)}`
    : "";
  const evidenceHints = `\n\nResearch evidence profile:\n- Dataset count: ${evidence.datasetSummary.datasetCount}\n- Dataset rows (hint): ${evidence.datasetSummary.totalRowsHint || "unknown"}\n- Capabilities: time=${evidence.datasetSummary.hasTimeLike ? "yes" : "no"}, text=${evidence.datasetSummary.hasTextLike ? "yes" : "no"}, panel=${evidence.datasetSummary.hasPanelLike ? "yes" : "no"}, graph=${evidence.datasetSummary.hasGraphLike ? "yes" : "no"}, image=${evidence.datasetSummary.hasImageLike ? "yes" : "no"}\n- Literature signals: ${Object.entries(evidence.literatureSummary.topMethodSignals).sort((a,b)=>b[1]-a[1]).slice(0,8).map(([k,v])=>`${k}:${v}`).join(", ")}\n- Recommended executable methods now: ${evidence.recommendedExecutableMethods.join(", ") || "none"}\n- Constrained methods (must not be claimed as executed): ${evidence.constrainedMethods.join(", ") || "none"}`;

  const dataConstraint = hasDatasets
    ? `\n\nCRITICAL METHODOLOGY CONSTRAINTS — ABSOLUTE RULES (VIOLATION = INVALID PAPER):

ALLOWED METHODS (whitelist — you MUST select your primary analysis methods ONLY from this list):
${evidence.recommendedExecutableMethods.map(m => `  - ${m}`).join("\n")}

BLOCKED METHODS (these MUST NOT appear in your main Methodology section):
${evidence.constrainedMethods.map(m => `  - ${m}`).join("\n")}

RULES:
1. Your main Methodology section MUST use ONLY methods from the ALLOWED list above. Any method NOT on the allowed list MUST be placed in a separate "Future Work / Aspirational Methods" subsection and MUST NOT appear in the main analytical plan.
2. The methodology title and framing MUST reflect what is actually executable. For example:
   - If only descriptive_statistics and correlation are allowed → title: "Descriptive and Correlational Analysis of ..."
   - If linear_regression is allowed → title: "Regression Analysis of ..."
   - Do NOT title it "Causal Inference Framework" or "Deep Learning Approach" if those methods are blocked.
3. DO NOT propose transformers, GNNs, deep learning, NLP, causal inference (DiD, IV, RDD, propensity score), or any advanced method unless it explicitly appears in the ALLOWED list above.
4. BE HONEST: if the data only supports basic statistics, design a rigorous descriptive/correlational study. A well-executed simple analysis is better than an unexecutable sophisticated one.
5. The methodology section should describe:
   a. What specific statistical tests or models will be applied to which columns
   b. How variables will be operationalised (which columns map to which concepts)
   c. What preprocessing steps are needed (handling missing values, encoding categoricals, normalisation)
   d. What the expected output of each analysis step is
6. DO NOT promise results you cannot deliver. Every claim in the methodology about "we will show" or "we will demonstrate" must be achievable with the allowed methods and available data.`
    : "";

  const result = await callLLM(
    `You are a research methodology designer. You must design methods that are REALISTIC and EXECUTABLE with the available data. Do NOT propose methods that sound impressive but cannot actually be implemented with the given dataset.`,
    `Research topic: "${ctx.topic}"\nHypothesis: ${ctx.hypothesis}${datasetInfo}${capabilityHints}${evidenceHints}${dataConstraint}\n\nDesign a complete experimental methodology:\n1. Research design (experimental/quasi-experimental/observational) — choose based on what the DATA actually supports\n2. Data description and variable operationalisation — map dataset columns to research variables\n3. Data preprocessing steps — handling missing values, encoding, normalisation\n4. Statistical analysis plan — specific tests/models matched to data type and research questions\n5. Evaluation approach — how will you assess the quality of the analysis?\n6. Baseline comparisons — what simple benchmarks will you compare against?\n7. Limitations — what CAN'T be answered with this data?\n8. Potential confounding variables and how to address them\n9. Hypothesis-to-test alignment matrix (hypothesis -> executable test -> decision rule -> expected falsification outcome)\n\nAt the end, add two sections:\n- "Executable Analyses" with concrete analyses runnable now.\n- "Blocked Analyses" with methods that require missing data/modalities and why.\n\nAlso add a final short section named "Empirical Readiness Classification" with one label:\n- empirical (if executable analyses can produce evidence-backed quantitative claims)\n- methodological_protocol (if evidence is mainly design/protocol and quantitative claims are not yet supported).`
  );
  ctx.methodology = result;
  await persistStageAudit(ctx, 7, {
    evidenceProfile: evidence,
    methodologyLength: result.length,
  });
  return result;
}

async function stage8_methodValidation(ctx: PipelineContext): Promise<string> {
  const evidence = ensureEvidenceProfile(ctx);
  const datasetInfo = ctx.datasetFiles.length > 0
    ? `\n\nAvailable datasets:\n${buildDatasetDescription(ctx.datasetFiles)}`
    : "";
  const capabilityHints = ctx.datasetFiles.length > 0
    ? `\n\nDataset capability profile:\n${buildDatasetCapabilityHints(ctx.datasetFiles)}`
    : "";
  const evidenceHints = `\n\nResearch evidence profile:\n- Recommended executable methods: ${evidence.recommendedExecutableMethods.join(", ") || "none"}\n- Constrained methods: ${evidence.constrainedMethods.join(", ") || "none"}\n- Literature method signals: ${Object.entries(evidence.literatureSummary.topMethodSignals).sort((a,b)=>b[1]-a[1]).slice(0,8).map(([k,v])=>`${k}:${v}`).join(", ")}`;

  const result = await callLLM(
    `You are a methodology reviewer and data science expert. You must critically evaluate whether the proposed methodology is ACTUALLY FEASIBLE with the available data. Be harsh and honest. Your job is to REJECT over-ambitious methodologies and force them to match reality.`,
    `Research topic: "${ctx.topic}"\nMethodology:\n${ctx.methodology}${datasetInfo}${capabilityHints}${evidenceHints}\n\nCritically validate:\n1. DATA-METHOD ALIGNMENT: Does the proposed method match the actual data? For example:\n   - If the data is a simple CSV with 10 columns, can you really train a graph neural network on it? NO.\n   - If the data has no temporal ordering, can you do time-series analysis? NO.\n   - If the data is aggregate statistics, can you do individual-level causal inference? NO.\n   Flag ANY method that cannot be executed with the available data.\n\n2. STATISTICAL VALIDITY: Are the proposed statistical tests appropriate for the data types?\n   - Correlation on categorical ID columns (prefecture codes, year codes) is MEANINGLESS.\n   - t-tests require proper group definitions, not arbitrary splits.\n   - Regression requires meaningful dependent and independent variables.\n\n3. FEASIBILITY: Can this methodology actually be implemented in Python with standard libraries?\n\n4. HONESTY CHECK: Does the methodology over-promise? If so, recommend simpler, honest alternatives.\n\n5. Recommendations: What methodology SHOULD be used given the actual data?\n\n6. Overall feasibility score (1-10) — be strict. A methodology that proposes deep learning on a 4000-row CSV should score 2-3.\n\nCRITICAL VALIDATION RULE:\nIf the proposed methodology contains ANY method not in the recommended executable list (${evidence.recommendedExecutableMethods.join(", ")}) as a PRIMARY analysis method (not future work), your validation MUST:\n- Assign a feasibility score of 3 or below\n- Explicitly list each non-executable method and explain why it cannot be run\n- Provide a REWRITTEN methodology that uses ONLY executable methods\n- Move all non-executable methods to the "future_work_only" list in the contract\n\nAfter the narrative review, output a machine-readable block using EXACTLY this format:\n[METHOD_CONTRACT_JSON]\n{\"executable_now\":[...],\"requires_missing_data\":[...],\"future_work_only\":[...],\"blocked_reasons\":{\"method\":\"reason\"},\"evidence_notes\":[...]}\n[/METHOD_CONTRACT_JSON]\n\nUse concise snake_case method ids (e.g., descriptive_statistics, correlation, linear_regression, group_comparison, time_trend, text_feature_analysis, causal_inference, graph_modelling, vision_analysis, panel_econometrics, advanced_time_series, advanced_nlp).\n\nIMPORTANT: The "executable_now" list MUST be a strict subset of: ${evidence.recommendedExecutableMethods.join(", ")}. Do NOT add methods to "executable_now" that are not in this list.`
  );
  ctx.methodValidation = result;
  const contract = parseMethodContract(result, ctx.methodology, evidence);
  ctx.methodContract = contract;
  await persistStageAudit(ctx, 8, {
    methodContract: contract,
    methodValidationLength: result.length,
  });
  return result;
}

async function stage9_codeGeneration(ctx: PipelineContext): Promise<string> {
  const hasDatasets = ctx.datasetFiles.length > 0;
  const contract = ctx.methodContract || deriveFallbackMethodContract(ctx.methodology, ensureEvidenceProfile(ctx));
  ctx.methodContract = contract;
  const contractBlock = `\n\nMethod feasibility contract (strictly enforce this):\n${formatMethodContract(contract)}\n\nRULES:\n- Generate analysis specs ONLY for methods listed under "Executable now".\n- DO NOT generate charts/tables/metrics for methods in "Requires missing data" or "Future-work only".\n- If executable methods are very limited, produce fewer but faithful analyses rather than broad fabricated coverage.`;

  if (hasDatasets) {
    // Generate a deterministic analysis plan from the method contract
    // (The experiment runner computes all charts/tables/metrics from real data,
    //  so we don't need an LLM to generate a Chart.js JSON spec that would be ignored.)
    const analysisPlan = {
      methods: contract.executableNow,
      blockedMethods: [...(contract.requiresMissingData || []), ...(contract.futureWorkOnly || [])],
      datasets: ctx.datasetFiles.map(d => ({
        name: d.originalName,
        columns: d.columnNames || [],
        rows: d.rowCount || 0,
        fileType: d.fileType,
      })),
      topic: ctx.topic,
    };
    ctx.experimentCode = JSON.stringify(analysisPlan, null, 2);
    await persistStageAudit(ctx, 9, {
      methodContract: contract,
      experimentCodeLength: ctx.experimentCode.length,
      deterministicPlan: true,
    });
    return ctx.experimentCode;
  } else {
    // Original simulated code generation
    const result = await callLLM(
      "You are an expert programmer generating experiment code. Write clean, well-documented Python code. Output raw Python code only - do NOT wrap in markdown code blocks.",
      `Research topic: "${ctx.topic}"\nMethodology:\n${ctx.methodology}\n\nGenerate complete Python experiment code including:\n1. Data loading/generation\n2. Model implementation\n3. Training/evaluation loop\n4. Metrics computation\n5. Result saving\n6. Visualization code\n\nUse standard libraries (numpy, pandas, scikit-learn, matplotlib). Include proper error handling and logging.\n\nIMPORTANT: Output raw Python code only. Do NOT wrap in \`\`\`python code blocks.`
    );
    // Strip any code block markers the LLM might have added
    ctx.experimentCode = stripCodeBlockMarkers(result);
    await persistStageAudit(ctx, 9, {
      methodContract: contract,
      experimentCodeLength: ctx.experimentCode.length,
      noDatasetMode: true,
    });
    return ctx.experimentCode;
  }
}

async function stage10_codeReview(ctx: PipelineContext): Promise<string> {
  return callLLM(
    "You are a senior code reviewer. Review experiment code for correctness, efficiency, and best practices.",
    `Review this experiment code:\n\n${ctx.experimentCode}\n\nCheck for:\n1. Logical errors\n2. Statistical correctness\n3. Edge cases\n4. Performance issues\n5. Code quality and documentation\n6. Reproducibility\n7. Suggested improvements\n\nProvide a corrected version if needed.`
  );
}

async function stage11_experimentExecution(ctx: PipelineContext): Promise<string> {
  const hasDatasets = ctx.datasetFiles.length > 0;

  if (hasDatasets) {
    // Server-side data analysis with Chart.js rendering
    ctx.emit({
      type: "log", runId: ctx.runId, stageNumber: 11,
      message: "Executing data analysis and chart generation...",
      timestamp: Date.now(),
    });

    try {
      // Strip any remaining code block markers
      let codeBody = stripCodeBlockMarkers(ctx.experimentCode);

      const output = await executePythonExperiment(
        ctx.runId,
        11,
        codeBody,
        ctx.datasetFiles,
        ctx.methodContract
      );

      ctx.experimentOutput = output;
      const analyticalMetricEntries = getAnalyticalMetricEntries(output);
      ctx.executionDiagnostics = buildExecutionDiagnostics(ctx.methodContract, output, analyticalMetricEntries.length);
      ctx.methodIntegrityNote = buildMethodIntegrityNote(ctx);
      await persistStageAudit(ctx, 11, {
        methodContract: ctx.methodContract,
        executionDiagnostics: ctx.executionDiagnostics,
        analyticalMetricCount: analyticalMetricEntries.length,
      });

      // Build result summary
      const chartSummary = output.charts.length > 0
        ? `\n\nGenerated Charts:\n${output.charts.map((c, i) => `  ${i + 1}. ${c.name}: ${c.description}`).join("\n")}`
        : "";
      const tableSummary = output.tables.length > 0
        ? `\n\nGenerated Tables:\n${output.tables.map((t, i) => `  ${i + 1}. ${t.name}: ${t.description}`).join("\n")}`
        : "";
      const metricsSummary = Object.keys(output.metrics).length > 0
        ? `\n\nKey Metrics:\n${Object.entries(output.metrics).map(([k, v]) => `  ${k}: ${v}`).join("\n")}`
        : "";

      // Upload charts as artifacts
      for (const chart of output.charts) {
        await db.insertArtifact({
          runId: ctx.runId,
          stageNumber: 11,
          artifactType: "experiment_chart",
          fileName: `${chart.name}.png`,
          fileUrl: chart.url,
          fileKey: `experiments/${ctx.runId}/${chart.name}.png`,
          mimeType: "image/png",
        });
      }

      const resultText = `Python experiment executed ${output.success ? "successfully" : "with errors"} in ${(output.executionTimeMs / 1000).toFixed(1)}s.\n\nExit code: ${output.exitCode}${chartSummary}${tableSummary}${metricsSummary}\n\nMETHOD FEASIBILITY CONTRACT:\n${formatMethodContract(ctx.methodContract)}\n\nEXECUTION DIAGNOSTICS:\n${formatExecutionDiagnostics(ctx.executionDiagnostics)}\n\nMETHOD EXECUTION INTEGRITY CHECK:\n${ctx.methodIntegrityNote}\n\nStdout:\n${output.stdout.substring(0, 3000)}${output.stderr ? `\n\nStderr:\n${output.stderr.substring(0, 1000)}` : ""}`;

      ctx.experimentResults = resultText;
      return resultText;
    } catch (err: any) {
      console.warn("[Pipeline] Data analysis failed, using non-fabricated failure mode:", err?.message);
      ctx.emit({
        type: "log", runId: ctx.runId, stageNumber: 11,
        message: `Data analysis failed: ${err?.message}. Proceeding without fabricated empirical results.`,
        timestamp: Date.now(),
      });
      // Fall through to explicit failure reporting below
    }
  }

  // No real data analysis available — record this fact clearly
  // DO NOT ask LLM to "simulate" or "generate realistic" numbers — this causes hallucination
  if (hasDatasets) {
    // Data analysis was attempted but failed
    ctx.experimentResults = `DATA ANALYSIS STATUS: FAILED\n\nThe uploaded dataset(s) could not be fully analysed due to technical errors. No empirical metrics, correlations, or p-values were computed from the actual data. The paper should acknowledge this limitation and present the methodology and analytical framework without fabricated numerical results.\n\nDatasets uploaded: ${ctx.datasetFiles.map(d => d.originalName).join(", ")}\nColumns available: ${ctx.datasetFiles.map(d => d.columnNames?.join(", ") || "unknown").join("; ")}`;
    const blocked = uniqMethodIds([
      ...(ctx.methodContract?.requiresMissingData || []),
      ...(ctx.methodContract?.futureWorkOnly || []),
    ]);
    const blockedReasonLines = Object.entries(ctx.methodContract?.blockedReasons || {})
      .map(([method, reason]) => `- ${method}: ${reason}`)
      .join("\n");
    const blockedSummary = blocked.length > 0
      ? `\nBlocked/unsupported methods from contract: ${blocked.join(", ")}${blockedReasonLines ? `\nBlocked reason details:\n${blockedReasonLines}` : ""}`
      : "";
    const missingExec = (ctx.methodContract?.executableNow || []).length > 0
      ? `\nExecutable methods that could not be evidenced: ${(ctx.methodContract?.executableNow || []).join(", ")}`
      : "";
    ctx.experimentResults += `${missingExec}${blockedSummary}`;
    ctx.executionDiagnostics = {
      executionStatus: "failed",
      executableRequested: ctx.methodContract?.executableNow || [],
      executedMethods: [],
      missingRequested: ctx.methodContract?.executableNow || [],
      analyticalMetricCount: 0,
      chartCount: 0,
      tableCount: 0,
      failureReasons: [
        "Execution crashed before analytical outputs were produced.",
        ...(blockedReasonLines ? [blockedReasonLines] : []),
      ],
    };
    ctx.methodIntegrityNote = "Execution failed; no methods can be claimed as empirically executed.";
    await persistStageAudit(ctx, 11, {
      methodContract: ctx.methodContract,
      executionDiagnostics: ctx.executionDiagnostics,
      analyticalMetricCount: 0,
      failureMode: "dataset_execution_failed",
    });
  } else {
    // No datasets at all — purely theoretical paper
    ctx.experimentResults = `DATA ANALYSIS STATUS: NO DATASETS\n\nNo empirical datasets were provided. This is a theoretical/conceptual paper. All numerical results in the paper should be clearly marked as hypothetical examples or derived from cited literature, NOT presented as original empirical findings.`;
    ctx.executionDiagnostics = {
      executionStatus: "failed",
      executableRequested: [],
      executedMethods: [],
      missingRequested: [],
      analyticalMetricCount: 0,
      chartCount: 0,
      tableCount: 0,
      failureReasons: ["No datasets provided."],
    };
    ctx.methodIntegrityNote = "No datasets provided; empirical method claims are not allowed.";
    await persistStageAudit(ctx, 11, {
      methodContract: ctx.methodContract,
      executionDiagnostics: ctx.executionDiagnostics,
      analyticalMetricCount: 0,
      failureMode: "no_dataset",
    });
  }
  return ctx.experimentResults;
}

async function stage12_resultCollection(ctx: PipelineContext): Promise<string> {
  const analyticalMetrics = getAnalyticalMetricEntries(ctx.experimentOutput);
  const hasRealData = analyticalMetrics.length > 0;
  const experimentChartInfo = ctx.experimentOutput?.charts.length
    ? `\n\nActual generated charts from data analysis:\n${ctx.experimentOutput.charts.map((c, i) => `${i + 1}. ${c.name}: ${c.description}`).join("\n")}`
    : "";
  const experimentMetrics = hasRealData
    ? `\n\nActual computed analytical metrics (these are the ONLY valid numerical results):\n${analyticalMetrics.map(([k, v]) => `${k}: ${v}`).join("\n")}`
    : "";
  const experimentTables = ctx.experimentOutput?.tables.length
    ? `\n\nActual computed tables:\n${ctx.experimentOutput.tables.map((t, i) => `Table ${i + 1}: ${t.name}\n${t.data?.substring(0, 800)}`).join("\n\n")}`
    : "";
  const contractBlock = `\n\nMethod feasibility contract:\n${formatMethodContract(ctx.methodContract)}`;
  const executionBlock = `\n\nExecution diagnostics:\n${formatExecutionDiagnostics(ctx.executionDiagnostics)}`;

  const result = await callLLM(
    `You are a data analyst organizing experimental results. CRITICAL ANTI-HALLUCINATION RULES:\n1. You may ONLY report numerical values that appear in the "Actual computed metrics" or "Actual computed tables" sections below.\n2. Do NOT invent, estimate, or extrapolate any numbers not explicitly provided.\n3. If no actual metrics are provided, state: "Empirical analysis could not be completed. No numerical results are available."\n4. Never write phrases like "results show" or "we found" followed by numbers you generated yourself.`,
    `Organize these experiment results into structured format:\n\n${ctx.experimentResults}${experimentChartInfo}${experimentMetrics}${experimentTables}${contractBlock}${executionBlock}\n\nProvide:\n1. Summary of what data was actually analysed (based ONLY on the information above)\n2. List of actual computed metrics (copy them verbatim from above, do NOT add new ones)\n3. Description of generated charts and tables\n4. Data quality assessment\n5. Limitations of the analysis\n\n${!hasRealData ? "IMPORTANT: No empirical metrics were computed. State this clearly. Do NOT fabricate any numbers." : ""}`
  );
  await persistStageAudit(ctx, 12, {
    hasRealData,
    analyticalMetricCount: analyticalMetrics.length,
    methodContract: ctx.methodContract,
    executionDiagnostics: ctx.executionDiagnostics,
  });
  return result;
}

async function stage13_statisticalAnalysis(ctx: PipelineContext): Promise<string> {
  const analyticalMetrics = getAnalyticalMetricEntries(ctx.experimentOutput);
  const hasRealMetrics = analyticalMetrics.length > 0;
  const experimentMetrics = hasRealMetrics
    ? `\n\nActual computed analytical metrics from data analysis (ONLY these numbers are valid):\n${analyticalMetrics.map(([k, v]) => `${k}: ${v}`).join("\n")}`
    : "";
  const experimentTables = ctx.experimentOutput?.tables.length
    ? `\n\nActual computed tables from data analysis:\n${ctx.experimentOutput.tables.map((t, i) => `Table ${i + 1}: ${t.name}\n${t.data?.substring(0, 800)}`).join("\n\n")}`
    : "";
  const contractBlock = `\n\nMethod feasibility contract:\n${formatMethodContract(ctx.methodContract)}`;
  const executionBlock = `\n\nExecution diagnostics:\n${formatExecutionDiagnostics(ctx.executionDiagnostics)}`;

  const result = await callLLM(
    `You are a statistician. CRITICAL ANTI-HALLUCINATION RULES:\n1. You may ONLY discuss and interpret numerical values that appear in the "Actual computed metrics" or "Actual computed tables" sections.\n2. Do NOT invent p-values, confidence intervals, effect sizes, or any other statistics not explicitly computed.\n3. If no actual metrics are provided, describe WHAT statistical tests SHOULD be performed and WHY, but do NOT report any numerical results.\n4. Clearly distinguish between "computed results" and "recommended analyses".`,
    `${hasRealMetrics ? "Interpret and discuss" : "Describe the statistical analysis plan for"} these results:\n\n${ctx.experimentResults}${experimentMetrics}${experimentTables}${contractBlock}${executionBlock}\n\n${hasRealMetrics ? `Interpret the actual computed metrics above:\n1. What do these descriptive statistics tell us?\n2. What patterns or trends are visible?\n3. What additional statistical tests would strengthen the analysis?\n4. What are the limitations of the current analysis?\n\nIMPORTANT: Only discuss the numbers provided above. Do NOT generate new statistics.` : `No empirical metrics were computed from the data. Describe:\n1. What statistical tests SHOULD be performed (but do NOT report results)\n2. What descriptive statistics would be informative\n3. Recommended hypothesis tests and their rationale\n4. Required assumptions and how to validate them\n5. Suggested sample size and power analysis approach\n\nIMPORTANT: Do NOT fabricate any numerical results. Only describe the analytical plan.`}`
  );
  ctx.statisticalAnalysis = result;
  await persistStageAudit(ctx, 13, {
    hasRealMetrics,
    analyticalMetricCount: analyticalMetrics.length,
    methodContract: ctx.methodContract,
    executionDiagnostics: ctx.executionDiagnostics,
  });
  return result;
}

async function stage14_figureGeneration(ctx: PipelineContext): Promise<string> {
  // If we have real charts from Python execution, reference them
  if (ctx.experimentOutput?.charts.length) {
    const chartList = ctx.experimentOutput.charts.map((c, i) =>
      `Figure ${i + 1}: ${c.name} - ${c.description} (URL: ${c.url})`
    ).join("\n");

    const result = await callLLM(
      "You are a scientific visualization expert. Describe the generated figures for inclusion in the paper.",
      `The following figures were generated from actual data analysis:\n\n${chartList}\n\nBased on these results:\n${ctx.experimentResults?.substring(0, 3000)}\n\nFor each figure, provide:\n1. A detailed caption suitable for a research paper\n2. Description of what the figure shows\n3. Key observations from the visualization\n4. How it supports the research hypothesis\n\nAlso suggest any additional figures that would strengthen the paper.`
    );
    ctx.figures = [result];
    return result;
  }

  const noFigureMessage = "No chart artifacts were generated from executed analysis. Figures are intentionally omitted to avoid simulated or fabricated visual evidence.";
  ctx.figures = [noFigureMessage];
  return noFigureMessage;
}

async function stage15_tableGeneration(ctx: PipelineContext): Promise<string> {
  const hasRealTables = ctx.experimentOutput?.tables && ctx.experimentOutput.tables.length > 0;
  const experimentTables = hasRealTables
    ? `\n\nActual computed tables from data analysis (use ONLY these values):\n${ctx.experimentOutput!.tables.map((t, i) => `Table ${i + 1}: ${t.name}\n${t.data?.substring(0, 1000)}`).join("\n\n")}`
    : "";
  const analyticalMetrics = getAnalyticalMetricEntries(ctx.experimentOutput);
  const hasRealMetrics = analyticalMetrics.length > 0;
  const experimentMetrics = hasRealMetrics
    ? `\n\nActual computed analytical metrics:\n${analyticalMetrics.map(([k, v]) => `${k}: ${v}`).join("\n")}`
    : "";
  const contractBlock = `\n\nMethod feasibility contract:\n${formatMethodContract(ctx.methodContract)}`;
  const executionBlock = `\n\nExecution diagnostics:\n${formatExecutionDiagnostics(ctx.executionDiagnostics)}`;

  const result = await callLLM(
    `You are a scientific table designer. Create publication-quality LaTeX tables.\n\nCRITICAL ANTI-HALLUCINATION RULES:\n1. Tables MUST contain ONLY values from the "Actual computed tables" or "Actual computed metrics" sections.\n2. Do NOT invent, estimate, or fabricate any numerical values.\n3. If no actual data is provided, create tables showing the STRUCTURE only (column headers, row labels) with "—" or "N/A" in data cells, and add a note explaining that empirical values are pending.\n4. Every number in every cell must be traceable to the provided data.`,
    `${hasRealTables || hasRealMetrics ? "Format the following actual data into publication-quality LaTeX tables" : "Create table STRUCTURES (without fabricated data) for"}:\n\n${ctx.experimentResults}\n${ctx.statisticalAnalysis?.substring(0, 2000)}${experimentTables}${experimentMetrics}${contractBlock}${executionBlock}\n\n${hasRealTables ? `Convert the actual computed tables above into LaTeX format using booktabs. Preserve ALL original values exactly as computed. Do NOT round, adjust, or add values.` : `No empirical data tables were computed. Create table STRUCTURES showing:\n1. What a main results table WOULD contain (headers and row labels only, data cells = "—")\n2. What a descriptive statistics table WOULD contain (headers and row labels only, data cells = "—")\nAdd \\caption*{Note: Empirical values pending data analysis.} to each table.`}\n\nTable formatting rules:\n- Use \\resizebox{\\textwidth}{!}{...} for tables with 4+ columns\n- Use booktabs (\\toprule, \\midrule, \\bottomrule)\n- Do NOT use sisetup or S column type\n- Keep column headers SHORT (abbreviate if needed)`
  );
  ctx.tables = [result];
  await persistStageAudit(ctx, 15, {
    hasRealTables: !!hasRealTables,
    hasRealMetrics,
    methodContract: ctx.methodContract,
    executionDiagnostics: ctx.executionDiagnostics,
  });
  return result;
}

async function stage16_outlineGeneration(ctx: PipelineContext): Promise<string> {
  const result = await callLLM(
    `You are an academic paper outline generator targeting ${ctx.config.targetConference} conference.`,
    `Research topic: "${ctx.topic}"\nMethod contract:\n${formatMethodContract(ctx.methodContract)}\nExecution diagnostics:\n${formatExecutionDiagnostics(ctx.executionDiagnostics)}\n\nGenerate a detailed paper outline for ${ctx.config.targetConference}:\n1. Title (compelling, specific)\n2. Abstract outline (key points)\n3. Introduction structure (motivation, contributions)\n4. Related Work organization\n5. Methodology section structure\n6. Experiments section structure\n7. Results and Discussion\n8. Conclusion and Future Work\n9. Appendix items\n\nInclude explicit subsection placeholders for:\n- Construct operationalisation and falsification logic\n- Evidence-backed findings only\n- Execution limitations and unmet data prerequisites`
  );
  ctx.outline = result;
  return result;
}

async function stage17_abstractWriting(ctx: PipelineContext): Promise<string> {
  const analyticalMetrics = getAnalyticalMetricEntries(ctx.experimentOutput);
  const hasRealMetrics = analyticalMetrics.length > 0;
  const metricsForAbstract = hasRealMetrics
    ? `\n\nActual computed analytical metrics (you may cite ONLY these specific numbers):\n${analyticalMetrics.map(([k, v]) => `${k}: ${v}`).join("\n")}`
    : "";
  const methodIntegrityBlock = ctx.methodIntegrityNote
    ? `\n\nMethod execution integrity note:\n${ctx.methodIntegrityNote}`
    : "";
  const contractBlock = `\n\nMethod feasibility contract:\n${formatMethodContract(ctx.methodContract)}`;
  const executionBlock = `\n\nExecution diagnostics:\n${formatExecutionDiagnostics(ctx.executionDiagnostics)}`;

  const result = await callLLM(
    `You are an expert academic writer. Write a compelling abstract for a ${ctx.config.targetConference} paper. Use British English spelling and academic tone.\n\nCRITICAL ANTI-HALLUCINATION RULES:\n1. You may ONLY cite specific numbers that appear in the "Actual computed analytical metrics" section below.\n2. If no actual analytical metrics are provided, write the abstract WITHOUT specific numerical claims. Use qualitative descriptions instead (e.g., "we analyse", "we propose", "our framework examines").\n3. Do NOT invent percentages, p-values, effect sizes, or any other statistics.\n4. If methodology mentions techniques not executed, frame them as planned/future work, never as completed evidence.\n\nMETHODOLOGY ALIGNMENT RULES:\n5. The abstract MUST NOT mention unexecuted methods (NLP, causal inference, deep learning, graph neural networks, etc.) as contributions or methods of this paper.\n6. Only describe the methods that were actually executed (see execution diagnostics below).\n7. Unexecuted methods may be mentioned ONLY in a single sentence about future directions at the end of the abstract.\n8. The abstract should accurately reflect what the paper delivers, not what it aspires to deliver.`,
    `Research topic: "${ctx.topic}"\nOutline:\n${ctx.outline}\nStatistical analysis:\n${ctx.statisticalAnalysis?.substring(0, 2000)}${metricsForAbstract}${methodIntegrityBlock}${contractBlock}${executionBlock}\n\nWrite a 150-250 word abstract that:\n1. States the problem clearly\n2. Describes the approach and methodology\n3. ${hasRealMetrics ? "Highlights key results using ONLY the actual computed analytical metrics above" : "Describes the analytical framework and expected contributions WITHOUT fabricating numerical results"}\n4. States the main contribution\n\n${!hasRealMetrics ? "IMPORTANT: No empirical analytical metrics are available. Write the abstract focusing on the research question, methodology, and analytical framework. Do NOT include any specific numbers, percentages, or statistical values." : ""}`
  );
  ctx.abstract = result;
  await persistStageAudit(ctx, 17, {
    hasRealMetrics,
    methodContract: ctx.methodContract,
    executionDiagnostics: ctx.executionDiagnostics,
  });
  return result;
}

async function stage18_bodyWriting(ctx: PipelineContext): Promise<string> {
  const numberedRefs = ctx.papers.slice(0, 20).map((p, i) => {
    const authors = p.authors || "Unknown";
    const year = p.year || "n.d.";
    const venue = p.venue ? `, ${p.venue}` : "";
    return `[${i + 1}] ${authors}. "${p.title}". ${year}${venue}.`;
  }).join("\n");

  // Build data analysis results section for the prompt
  let dataAnalysisSection = "";
  if (ctx.experimentOutput) {
    const chartRefs = ctx.experimentOutput.charts.map((c, i) =>
      `Figure ${i + 1}: ${c.name} - ${c.description}`
    ).join("\n");
    const tableRefs = ctx.experimentOutput.tables.map((t, i) =>
      `Table ${i + 1}: ${t.name} - ${t.description}`
    ).join("\n");
    const metricsText = getAnalyticalMetricEntries(ctx.experimentOutput)
      .map(([k, v]) => `${k}: ${v}`).join("\n");

    dataAnalysisSection = `\n\n## Data Analysis Results (from actual dataset analysis)\nCharts generated:\n${chartRefs}\n\nTables generated:\n${tableRefs}\n\nAnalytical metrics:\n${metricsText || "None"}\n\nMethod feasibility contract:\n${formatMethodContract(ctx.methodContract)}\n\nExecution diagnostics:\n${formatExecutionDiagnostics(ctx.executionDiagnostics)}\n\nMethod integrity note:\n${ctx.methodIntegrityNote || "No integrity note available."}\n\nIMPORTANT: Reference these actual figures and tables in the paper body. Use "Figure 1", "Table 1" etc. to refer to them. The results section should discuss these actual analysis outputs only.`;
  }

  const hasRealMetrics = getAnalyticalMetricEntries(ctx.experimentOutput).length > 0;
  const hasRealCharts = ctx.experimentOutput?.charts && ctx.experimentOutput.charts.length > 0;
  const hasRealTables = ctx.experimentOutput?.tables && ctx.experimentOutput.tables.length > 0;

  const executedMethodsList = ctx.executionDiagnostics?.executedMethods?.join(", ") || "none";
  const blockedMethodsList = [
    ...(ctx.methodContract?.requiresMissingData || []),
    ...(ctx.methodContract?.futureWorkOnly || []),
  ].join(", ") || "none";

  let antiHallucinationRules = "";
  if (hasRealMetrics || hasRealCharts || hasRealTables) {
    antiHallucinationRules = `\n\nANTI-HALLUCINATION RULES:\n1. In the Results section, you may ONLY report numerical values from the "Data Analysis Results" section above.\n2. When discussing figures and tables, describe what they show based on the provided descriptions.\n3. Do NOT invent additional statistics, p-values, or effect sizes beyond what is provided.\n4. If you need to discuss implications, use hedged language ("suggests", "indicates", "is consistent with").\n5. Any methodology component not confirmed by the method integrity note must be framed as unexecuted/future work.\n\nMETHODOLOGY-RESULTS ALIGNMENT (CRITICAL):\n- Actually executed methods: ${executedMethodsList}\n- Blocked/unexecuted methods: ${blockedMethodsList}\n- The Methodology section MUST describe ONLY the analyses that were actually executed.\n- Methods listed as blocked/unexecuted MUST appear ONLY in a "Limitations and Future Work" subsection, clearly marked as "not yet implemented" or "planned for future work".\n- The paper title MUST NOT reference unexecuted methods as if they are the paper's contribution.\n- Do NOT describe NLP, causal inference, deep learning, or any blocked method as something "we apply" or "we implement" — only as "future work".`;
  } else {
    antiHallucinationRules = `\n\nCRITICAL ANTI-HALLUCINATION RULES:\n1. No empirical results were computed from the data. The Results section MUST acknowledge this.\n2. Do NOT fabricate any numerical results, p-values, correlations, means, standard deviations, or effect sizes.\n3. Instead, describe the analytical FRAMEWORK: what analyses WOULD be performed, what metrics WOULD be computed, and what patterns WOULD be examined.\n4. Use conditional language throughout: "would", "is expected to", "the analysis aims to".\n5. The Experiments section should describe the planned methodology, not fabricated outcomes.\n6. If the abstract contains no specific numbers, the body should not introduce any either.\n\nMETHODOLOGY-RESULTS ALIGNMENT (CRITICAL):\n- Blocked/unexecuted methods: ${blockedMethodsList}\n- The paper MUST NOT claim to have executed any analysis. Frame the entire paper as a methodological protocol or research proposal.\n- Do NOT use past tense ("we found", "we demonstrated") for unexecuted analyses. Use future/conditional tense only.`;
  }

  const firstPass = await callLLM(
    `You are an expert academic writer producing a full research paper for ${ctx.config.targetConference}. Use British English spelling. You MUST cite the provided references throughout the paper body using numbered citations like [1], [2], [3], etc. Every claim derived from prior work must include a citation. The Related Work section must cite at least 8 references. The Introduction should cite at least 3-5 references to motivate the research.${antiHallucinationRules}

WRITING QUALITY REQUIREMENTS:
- Each section must be substantive (at least 3-4 paragraphs for major sections like Methodology, Results).
- Avoid single-sentence subsections. Every subsection must have at least 2 paragraphs of detailed content.
- Use formal academic prose, not bullet points, for the main body text.
- Methodology must include: (a) formal problem definition with mathematical notation where appropriate, (b) detailed description of the analytical framework or model, (c) data preprocessing steps, (d) variable operationalisation.
- Experiments must include: (a) dataset description (source, size, time period, key variables), (b) experimental setup and implementation details, (c) evaluation metrics with definitions, (d) baseline methods for comparison.
- Results must include: (a) main findings with reference to specific tables and figures, (b) comparison with baselines or prior work, (c) discussion of statistical significance where applicable, (d) limitations and potential confounds.`,
    `Write the complete paper body in Markdown format.\n\nTopic: "${ctx.topic}"\nAbstract: ${ctx.abstract}\nOutline: ${ctx.outline}\nHypothesis package: ${ctx.hypothesis}\nMethodology: ${ctx.methodology}\nResults: ${ctx.experimentResults?.substring(0, 3000)}\nStatistical Analysis: ${ctx.statisticalAnalysis?.substring(0, 2000)}${dataAnalysisSection}\n\n## Available References (use these numbered citations in the text):\n${numberedRefs}\n\nWrite complete sections with the following MINIMUM requirements:\n\n1. **Introduction** (at least 4 paragraphs) — Motivate the research problem with real-world significance, cite relevant prior work using [1], [2] etc., identify the specific research gap, and clearly state contributions (as a numbered list at the end of the Introduction).\n\n2. **Related Work** (at least 3 subsections) — Thoroughly review the literature. Cite each referenced paper by its number [1]-[${ctx.papers.slice(0, 20).length}]. Group related works thematically into subsections (e.g., "2.1 Prior Work on X", "2.2 Approaches to Y", "2.3 Gap Analysis"). Each subsection must have at least 2 paragraphs.\n\n3. **Methodology** (at least 3 subsections) — This is a CRITICAL section that must be detailed:\n   3.1 **Problem Formulation** — Formally define the research problem. Use mathematical notation where appropriate (e.g., define variables, objective functions, hypotheses).\n   3.2 **Analytical Framework / Model Description** — Describe the proposed approach step by step. Include data preprocessing, feature engineering, or variable operationalisation. Explain WHY each methodological choice was made.\n   3.3 **Implementation Details** — Describe tools, libraries, parameters, and computational environment used.\n   3.4 **Operationalisation and Falsification Logic** — explicitly map each core hypothesis to operational variables, executable tests, and falsification criteria.\n\n4. **Experiments** (at least 3 subsections) —\n   4.1 **Dataset Description** — Source, collection method, time period, sample size, key variables with descriptive statistics.\n   4.2 **Experimental Setup** — ${hasRealMetrics ? "Describe the exact configuration, hyperparameters, cross-validation strategy, and reproducibility measures." : "Describe the planned experimental configuration and reproducibility measures. Acknowledge that full empirical results are pending."}\n   4.3 **Evaluation Metrics** — Define each metric mathematically (e.g., accuracy = TP+TN/N, RMSE = sqrt(1/n * sum(yi - y_hat_i)^2)). Explain why these metrics are appropriate.\n   4.4 **Baselines** — Describe comparison methods and why they were chosen.\n\n5. **Results and Discussion** (at least 4 paragraphs) — ${hasRealMetrics ? "Present findings using ONLY the actual computed metrics. Structure as: (a) Main results with table/figure references, (b) Comparison with baselines, (c) Ablation or sensitivity analysis discussion, (d) Limitations and threats to validity." : "Describe the analytical framework and what the results WOULD show. Do NOT fabricate any numbers. Use conditional language. Discuss expected patterns, potential limitations, and how results would be interpreted."}\n\n6. **Conclusion and Future Work** (at least 2 paragraphs) — Summarise contributions (matching the numbered list from Introduction), discuss broader implications, and outline concrete future research directions.\n\n7. **Research Classification** — Add one explicit label and one short justification:\n   - empirical (if supported by executed evidence), or\n   - methodological_protocol (if empirical evidence is incomplete).\n\n8. **References** — List all cited references in the format:\n   [1] Authors. "Title". Year, Venue.\n   [2] Authors. "Title". Year, Venue.\n   ... (include ALL references from the list above that were cited in the text)\n\nIMPORTANT: You MUST include inline citations [1], [2], etc. throughout the text. The References section at the end MUST list every cited paper. Each major section must be substantive — no single-sentence sections or subsections.`,
    32768
  );
  const claimCheck = buildClaimVerificationReport(ctx, firstPass);
  ctx.claimVerificationReport = claimCheck.report;
  await persistStageAudit(ctx, 18, {
    claimVerificationReport: ctx.claimVerificationReport,
    flaggedClaimsCount: claimCheck.flaggedClaims.length,
    methodContract: ctx.methodContract,
    executionDiagnostics: ctx.executionDiagnostics,
  });

  let result = firstPass;
  if (claimCheck.flaggedClaims.length > 0) {
    result = await callLLM(
      `You are an expert academic editor. Rewrite the paper body to remove unsupported empirical/method claims while preserving quality and structure.`,
      `Original body:\n${firstPass}\n\nClaim verifier report:\n${ctx.claimVerificationReport}\n\nMethod feasibility contract:\n${formatMethodContract(ctx.methodContract)}\n\nExecution diagnostics:\n${formatExecutionDiagnostics(ctx.executionDiagnostics)}\n\nRewrite requirements:\n1. Remove or reframe all flagged unsupported claims.\n2. Any method in requires_missing_data/future_work_only MUST be written as planned/future work only.\n3. Keep only evidence-backed empirical claims.\n4. Preserve section structure and citations.\n5. Do not add new quantitative values unless already in analytical metrics/tables.`
    );
  }
  const secondCheck = buildClaimVerificationReport(ctx, result);
  if (secondCheck.flaggedClaims.length > 0) {
    result = await callLLM(
      `You are an academic compliance editor. Perform a strict final pass to eliminate unsupported claims.`,
      `Draft body:\n${result}\n\nSecond-pass claim verifier report:\n${secondCheck.report}\n\nMethod feasibility contract:\n${formatMethodContract(ctx.methodContract)}\n\nExecution diagnostics:\n${formatExecutionDiagnostics(ctx.executionDiagnostics)}\n\nApply strict edits:\n1. Remove unsupported assertive claims.\n2. Replace unsupported completed-method wording with "planned/future work" wording.\n3. Preserve section structure and citation markers.\n4. Keep only evidence-backed numbers and references to actually generated figures/tables.`
    );
    ctx.claimVerificationReport = buildClaimVerificationReport(ctx, result).report;
  }
  // Completeness check: ensure the paper has all required sections
  const requiredSections = ["Introduction", "Related Work", "Methodology", "Experiment", "Result", "Conclusion"];
  const missingSections = requiredSections.filter(sec => {
    const pattern = new RegExp(`(?:^|\\n)#+\\s*\\d*\\.?\\s*${sec}`, "i");
    return !pattern.test(result) && !result.toLowerCase().includes(sec.toLowerCase());
  });

  if (missingSections.length > 2) {
    console.warn(`[Pipeline] Paper body missing ${missingSections.length} sections: ${missingSections.join(", ")}. The paper may be incomplete.`);
    // Append a note about missing sections so downstream stages are aware
    ctx.paperBody = result + `\n\n<!-- WARNING: The following sections may be incomplete or missing: ${missingSections.join(", ")} -->\n`;
  } else {
    ctx.paperBody = result;
  }
  return result;
}

async function stage19_referenceFormatting(ctx: PipelineContext): Promise<string> {
  const bibtexEntries = ctx.papers.slice(0, 20).map((p, i) => {
    if (p.bibtex && p.bibtex.trim().length > 10) return p.bibtex;
    const firstAuthor = (p.authors || "Unknown").split(",")[0].split(" ").pop() || "unknown";
    const key = `${firstAuthor.toLowerCase()}${p.year || "nd"}_${i + 1}`;
    return `@article{${key},\n  title = {${p.title}},\n  author = {${p.authors || "Unknown"}},\n  year = {${p.year || "n.d."}},\n  journal = {${p.venue || "Unknown"}},\n  doi = {${p.doi || ""}},\n  url = {${p.url || ""}}\n}`;
  }).join("\n\n");
  ctx.references = bibtexEntries;
  return `Generated BibTeX file with ${ctx.papers.slice(0, 20).length} references.\n\n${bibtexEntries}`;
}

async function stage20_latexCompilation(ctx: PipelineContext): Promise<string> {
  // Build figure instructions if we have experiment-generated charts
  let figureInstructions = "";
  if (ctx.experimentOutput?.charts.length) {
    const figureList = ctx.experimentOutput.charts.map((c, i) => {
      const figKey = `figure_${i + 1}`;
      return `  - Figure ${i + 1}: key="${figKey}", caption="${c.name}: ${c.description}"`;
    }).join("\n");

    figureInstructions = `\n\n## IMPORTANT: Embed Data Analysis Figures\nThe following figures were generated from actual data analysis and MUST be embedded in the LaTeX document using \\includegraphics.\nFor each figure, use this exact pattern:\n\n\\begin{figure}[htbp]\n  \\centering\n  \\includegraphics[width=0.85\\textwidth]{<figure_key>}\n  \\caption{<caption text>}\n  \\label{fig:<label>}\n\\end{figure}\n\nAvailable figures:\n${figureList}\n\nPlace each figure in the most appropriate section (typically Results or Experiments). Use the exact figure key (e.g., figure_1, figure_2) as the argument to \\includegraphics. The graphicx package must be included in the preamble. Reference each figure in the text using \\ref{fig:<label>}.`;
  }

  // Build a strict data manifest: only these numbers may appear in the paper
  const analyticalMetrics = getAnalyticalMetricEntries(ctx.experimentOutput);
  const hasRealMetrics = analyticalMetrics.length > 0;
  const hasRealTables = ctx.experimentOutput?.tables && ctx.experimentOutput.tables.length > 0;
  let dataManifest = "";
  if (hasRealMetrics || hasRealTables) {
    dataManifest = `\n\n## ANTI-HALLUCINATION DATA MANIFEST\nThe following is the COMPLETE set of numerical results computed from the actual dataset.\nYou MUST use ONLY these values in the Results section. Do NOT invent, extrapolate, or add ANY numbers not listed here.\n`;
    if (hasRealMetrics) {
      dataManifest += `\nComputed Analytical Metrics:\n${analyticalMetrics.map(([k, v]) => `  ${k} = ${v}`).join("\n")}\n`;
    }
    if (hasRealTables) {
      dataManifest += `\nComputed Tables (use these exact values in LaTeX tables):\n${ctx.experimentOutput!.tables.map((t, i) => `  Table ${i + 1}: ${t.name}\n${t.data?.substring(0, 1200)}`).join("\n\n")}\n`;
    }
    dataManifest += `\nMethod feasibility contract:\n${formatMethodContract(ctx.methodContract)}\n`;
    dataManifest += `\nExecution diagnostics:\n${formatExecutionDiagnostics(ctx.executionDiagnostics)}\n`;
    dataManifest += `\nMethod execution integrity:\n${ctx.methodIntegrityNote || "No integrity note available."}\n`;
    if (ctx.claimVerificationReport) {
      dataManifest += `\nClaim verification report:\n${ctx.claimVerificationReport}\n`;
    }
    dataManifest += `\nRULES:\n- Every number in the Results/Discussion sections MUST come from the list above.\n- If a statistic is not listed above, do NOT include it.\n- Do NOT add p-values, effect sizes, confidence intervals, or any other statistics unless they appear above.\n- Tables must reproduce the exact values from "Computed Tables" above.\n- If the data is insufficient, state the limitation rather than fabricating values.\n- Any method not explicitly supported by the method execution integrity note must be presented as planned/future work only.`;
  } else {
    dataManifest = `\n\n## ANTI-HALLUCINATION NOTICE\nNo empirical metrics were computed from the data. The Results section MUST:\n- Acknowledge that empirical analysis could not be completed\n- Describe the analytical framework without fabricating any numbers\n- Use conditional language ("would", "is expected to")\n- NOT contain any specific numerical results, p-values, correlations, means, or standard deviations`;
  }

  const result = await callLLM(
    `You are a LaTeX expert. Convert the paper to a professional academic LaTeX document styled for ${ctx.config.targetConference}.

CRITICAL DOCUMENT CLASS RULE:
- You MUST use \\documentclass[11pt,a4paper]{article} as the document class.
- Do NOT use any conference-specific class files (e.g., neurips_2024, neurips_2025, icml2025, iclr2025_conference). These .cls files are NOT available and will cause compilation errors.
- Instead, replicate the conference style using ONLY standard LaTeX packages (geometry, titling, amsmath, graphicx, booktabs, hyperref, tabularx, adjustbox, etc.).

CRITICAL ANTI-HALLUCINATION RULE:
- You MUST NOT invent, fabricate, or generate ANY numerical values (means, standard deviations, p-values, correlations, effect sizes, percentages, sample sizes) that are not explicitly provided in the data manifest below.
- If a number does not appear in the data manifest, it MUST NOT appear in the paper.
- Copy numerical values EXACTLY as provided — do not round, adjust, or "improve" them.
- If no data manifest is provided, the Results section must state that analysis is pending.

CRITICAL REFERENCE INTEGRITY RULE:
- You MUST ONLY cite references that appear in the BibTeX references provided below.
- Do NOT invent, fabricate, or hallucinate ANY references, authors, titles, or publication years.
- Do NOT add references with future publication years (e.g., 2026 or later).
- Every \cite{key} command MUST have a corresponding \bibitem{key} in the bibliography.
- Every \bibitem MUST correspond to a real paper from the provided BibTeX entries.
- If you need more references than provided, state the limitation rather than fabricating citations.

Output ONLY the raw LaTeX source code. Do NOT wrap it in markdown code blocks (no \`\`\`latex or \`\`\`). Start directly with \\documentclass and end with \\end{document}.
You MUST include \\usepackage{graphicx}, \\usepackage{float}, \\usepackage{tabularx}, and \\usepackage{adjustbox} in the preamble.
ALL content — including figures, tables, and equations — MUST fit within A4 portrait page margins (\\textwidth). Never allow any element to overflow the page width.`,
    `Convert this paper to complete LaTeX format styled for ${ctx.config.targetConference}:\n\nAbstract: ${ctx.abstract}\n\nBody (with numbered citations [1], [2], etc.):\n${ctx.paperBody || ""}\n\nTables:\n${ctx.tables.join("\n")}\n\nBibTeX references:\n${ctx.references || ""}${figureInstructions}${dataManifest}\n\nGenerate complete LaTeX source with:\n1. \\documentclass[11pt,a4paper]{article} — NEVER use conference-specific .cls files\n2. \\usepackage[a4paper, margin=2.5cm]{geometry} for A4 layout\n3. \\usepackage{graphicx}, \\usepackage{float}, \\usepackage{amsmath}, \\usepackage{amssymb}, \\usepackage{booktabs}, \\usepackage{hyperref}, \\usepackage{tabularx}, \\usepackage{adjustbox} in preamble\n4. Professional title formatting with \\title{}, \\author{}, \\date{}, \\maketitle\n5. All sections with proper \\cite{} commands for every reference\n6. Actual \\includegraphics commands for each data analysis figure (using the exact keys provided above)\n7. Table environments using booktabs (\\toprule, \\midrule, \\bottomrule)\n8. \\begin{thebibliography}{99} section at the end with all cited \\bibitem entries\n\n## TABLE WIDTH CONSTRAINTS (ABSOLUTELY CRITICAL — MUST FOLLOW):\nEvery table MUST fit within the A4 page width (\\textwidth = approximately 16cm with 2.5cm margins).\nFor tables with 4 or more columns, you MUST use one of these approaches:\n\nApproach 1 (PREFERRED): Wrap the entire tabular in \\resizebox:\n\\begin{table}[H]\n  \\centering\n  \\caption{...}\n  \\resizebox{\\textwidth}{!}{%\n    \\begin{tabular}{lcccc}\n      ...\n    \\end{tabular}%\n  }\n\\end{table}\n\nApproach 2: Use tabularx with X columns that auto-wrap:\n\\begin{table}[H]\n  \\centering\n  \\caption{...}\n  \\begin{tabularx}{\\textwidth}{lXXXX}\n    ...\n  \\end{tabularx}\n\\end{table}\n\nApproach 3: Use adjustbox:\n\\begin{table}[H]\n  \\centering\n  \\caption{...}\n  \\begin{adjustbox}{max width=\\textwidth}\n    \\begin{tabular}{lcccc}\n      ...\n    \\end{tabular}\n  \\end{adjustbox}\n\\end{table}\n\nRULES:\n- NEVER use plain \\begin{tabular} without \\resizebox, tabularx, or adjustbox for tables with 4+ columns\n- NEVER use sisetup or S column type (these cause width issues)\n- Use SHORT column headers (abbreviate long names, e.g., \"Female Mgmt Ratio\" instead of \"Female Management Ratio\")\n- Use \\footnotesize inside tables if needed for extra space\n- For correlation matrices and wide data tables, ALWAYS use \\resizebox{\\textwidth}{!}{...}\n\nA4 PAGE WIDTH CONSTRAINTS (OTHER ELEMENTS):\n- ALL figures MUST use width=\\textwidth or smaller (e.g., width=0.85\\textwidth) in \\includegraphics. Never use absolute widths.\n- ALL equations MUST fit within \\textwidth. For long equations, use split, multline, or aligned environments.\n- Never use \\hspace or manual spacing that pushes content beyond margins.\n\nIMPORTANT:\n- Convert all [1], [2] style citations to \\cite{key} commands with corresponding \\bibitem entries.\n- Output ONLY raw LaTeX code. Start with \\documentclass[11pt,a4paper]{article} and end with \\end{document}.\n- Each figure MUST use \\includegraphics with the exact key provided (e.g., figure_1, figure_2). Do NOT use placeholder paths or URLs.\n- Do NOT use \\usepackage{natbib} — use \\begin{thebibliography} with \\bibitem instead.\n- Do NOT use \\usepackage{siunitx} or S column type — they cause width overflow issues.`,
    32768
  );
  // Strip any code block markers the LLM might have added
  let latex = stripCodeBlockMarkers(result);
  if (ctx.executionDiagnostics?.executionStatus !== "success") {
    const failureReasons = ctx.executionDiagnostics?.failureReasons || [];
    const limitationSection = [
      "\\section*{Execution Limitations}",
      "The empirical execution did not complete all planned analyses.",
      "\\begin{itemize}",
      ...failureReasons.slice(0, 8).map((reason) => `  \\item ${reason.replace(/[_%$#&{}]/g, " ")}`),
      "\\end{itemize}",
      "Methods without evidence are treated as planned future work.",
    ].join("\n");
    if (latex.includes("\\end{document}") && !latex.includes("\\section*{Execution Limitations}")) {
      latex = latex.replace("\\end{document}", `${limitationSection}\n\n\\end{document}`);
    }
  }

  // Ensure bibliography is present in LaTeX output
  if (!latex.includes("\\begin{thebibliography}") && ctx.references) {
    console.log("[Pipeline] LaTeX missing bibliography, injecting from BibTeX references...");
    // Convert BibTeX entries to \bibitem format
    const bibEntries = ctx.references.split(/\n\n+/).filter(e => e.trim().startsWith("@"));
    if (bibEntries.length > 0) {
      const bibItems = bibEntries.map((entry, i) => {
        const keyMatch = entry.match(/@\w+\{([^,]+),/);
        const titleMatch = entry.match(/title\s*=\s*\{([^}]+)\}/i);
        const authorMatch = entry.match(/author\s*=\s*\{([^}]+)\}/i);
        const yearMatch = entry.match(/year\s*=\s*\{([^}]+)\}/i);
        const journalMatch = entry.match(/(?:journal|booktitle|venue)\s*=\s*\{([^}]+)\}/i);
        const key = keyMatch ? keyMatch[1].trim() : `ref${i + 1}`;
        const title = titleMatch ? titleMatch[1] : "Unknown title";
        const author = authorMatch ? authorMatch[1] : "Unknown";
        const year = yearMatch ? yearMatch[1] : "n.d.";
        const journal = journalMatch ? journalMatch[1] : "";
        return `\\bibitem{${key}} ${author}. \\textit{${title}}. ${journal}${journal ? ", " : ""}${year}.`;
      }).join("\n\n");

      const bibliography = `\n\n\\begin{thebibliography}{${bibEntries.length}}\n${bibItems}\n\\end{thebibliography}`;

      // Insert before \end{document}
      if (latex.includes("\\end{document}")) {
        latex = latex.replace("\\end{document}", `${bibliography}\n\n\\end{document}`);
      } else {
        latex += bibliography;
      }
      console.log(`[Pipeline] Injected ${bibEntries.length} bibliography entries into LaTeX`);
    }
  }

  // Ensure \end{document} is present
  if (!latex.includes("\\end{document}")) {
    latex += "\n\n\\end{document}";
  }

  ctx.latex = latex;
  await persistStageAudit(ctx, 20, {
    hasRealMetrics,
    hasRealTables: !!hasRealTables,
    methodContract: ctx.methodContract,
    executionDiagnostics: ctx.executionDiagnostics,
    claimVerificationReport: ctx.claimVerificationReport || "",
    latexLength: ctx.latex.length,
  });
  return ctx.latex;
}

async function stage21_peerReview(ctx: PipelineContext): Promise<string> {
  const result = await callLLM(
    "You are a panel of 3 expert peer reviewers for a top-tier ML/AI conference. Provide detailed, constructive reviews.",
    `Review this paper submission:\n\nTitle: ${ctx.topic}\nAbstract: ${ctx.abstract}\nBody: ${ctx.paperBody || ""}\n\nProvide 3 independent reviews, each containing:\n1. Summary (2-3 sentences)\n2. Strengths (3-5 points)\n3. Weaknesses (3-5 points)\n4. Questions for authors\n5. Minor issues\n6. Overall score (1-10)\n7. Confidence (1-5)\n8. Recommendation (Accept/Weak Accept/Borderline/Weak Reject/Reject)\n\nAlso provide a meta-review summarizing the consensus.`
  );
  ctx.reviewReport = result;
  return result;
}

async function stage22_revision(ctx: PipelineContext): Promise<string> {
  const result = await callLLM(
    "You are the paper author revising based on peer review feedback.",
    `Revise the paper based on these reviews:\n\nReviews:\n${ctx.reviewReport?.substring(0, 8000)}\n\nOriginal paper:\n${ctx.paperBody || ""}\n\nProvide:\n1. Point-by-point response to reviewers\n2. Revised sections (showing changes)\n3. Additional experiments or analysis if requested\n4. Summary of all changes made`
  );
  ctx.revision = result;
  return result;
}

async function stage23_finalCompilation(ctx: PipelineContext): Promise<string> {
  const suffix = nanoid(8);

  try {
    if (ctx.latex) {
      const { url } = await storagePut(`runs/${ctx.runId}/paper-${suffix}.tex`, ctx.latex, "text/x-tex");
      await db.insertArtifact({ runId: ctx.runId, stageNumber: 23, artifactType: "paper_tex", fileName: "paper.tex", fileUrl: url, fileKey: `runs/${ctx.runId}/paper-${suffix}.tex`, mimeType: "text/x-tex" });
    }
    if (ctx.references) {
      const { url } = await storagePut(`runs/${ctx.runId}/references-${suffix}.bib`, ctx.references, "text/plain");
      await db.insertArtifact({ runId: ctx.runId, stageNumber: 23, artifactType: "references_bib", fileName: "references.bib", fileUrl: url, fileKey: `runs/${ctx.runId}/references-${suffix}.bib`, mimeType: "text/plain" });
    }
    if (ctx.experimentCode) {
      const { url } = await storagePut(`runs/${ctx.runId}/experiment-${suffix}.py`, ctx.experimentCode, "text/x-python");
      await db.insertArtifact({ runId: ctx.runId, stageNumber: 23, artifactType: "experiment_code", fileName: "experiment.py", fileUrl: url, fileKey: `runs/${ctx.runId}/experiment-${suffix}.py`, mimeType: "text/x-python" });
    }
    if (ctx.reviewReport) {
      const { url } = await storagePut(`runs/${ctx.runId}/review-${suffix}.md`, ctx.reviewReport, "text/markdown");
      await db.insertArtifact({ runId: ctx.runId, stageNumber: 23, artifactType: "review_report", fileName: "review_report.md", fileUrl: url, fileKey: `runs/${ctx.runId}/review-${suffix}.md`, mimeType: "text/markdown" });
    }

    // Refresh chart URLs (S3 pre-signed URLs may have expired since stage 11)
    const charts = ctx.experimentOutput?.charts || [];
    for (const chart of charts) {
      try {
        const refreshed = await storageGet(`experiments/${ctx.runId}/${chart.name}.png`);
        chart.url = refreshed.url;
      } catch {
        try {
          const refreshed = await storageGet(`experiments/${ctx.runId}/${chart.name}.svg`);
          chart.url = refreshed.url;
        } catch {
          console.warn(`[Pipeline] Could not refresh URL for chart ${chart.name}, using original`);
        }
      }
    }
    if (charts.length > 0) {
      console.log(`[Pipeline] Refreshed ${charts.length} chart URLs for PDF generation`);
    }

    // Build chart images array for PDF embedding
    const chartImages: ChartImage[] = charts.map((c, i) => ({
      key: `figure_${i + 1}`,
      url: c.url,
      name: c.name,
      description: c.description,
    }));

    // Ensure chart images are saved as individual artifacts (even if stage11 already did this,
    // we do it again in stage23 to guarantee they appear in the Artifacts tab)
    for (const chart of (ctx.experimentOutput?.charts || [])) {
      try {
        // Check if this chart already exists as an artifact (avoid duplicates)
        const existingArtifacts = await db.getArtifactsForRun(ctx.runId);
        const alreadyExists = existingArtifacts.some(
          a => a.artifactType === "experiment_chart" && a.fileUrl === chart.url
        );
        if (!alreadyExists) {
          await db.insertArtifact({
            runId: ctx.runId,
            stageNumber: 23,
            artifactType: "experiment_chart",
            fileName: `${chart.name}.png`,
            fileUrl: chart.url,
            fileKey: `experiments/${ctx.runId}/${chart.name}.png`,
            mimeType: "image/png",
          });
          console.log(`[Pipeline] Chart artifact saved: ${chart.name}`);
        }
      } catch (chartErr: any) {
        console.warn(`[Pipeline] Failed to save chart artifact ${chart.name}:`, chartErr?.message);
      }
    }

    if (chartImages.length > 0) {
      console.log(`[Pipeline] ${chartImages.length} chart images will be embedded in PDF`);
    }

    // Generate PDF – prefer LaTeX→HTML→PDF, fall back to Markdown→HTML→PDF
    const paperMdForPdf = `# ${ctx.topic}\n\n## Abstract\n${ctx.abstract}\n\n${ctx.paperBody}`;
    try {
      console.log("[Pipeline] Generating PDF...");
      const pdfBuffer = await generatePaperPdf(
        paperMdForPdf,
        ctx.topic,
        ctx.config.targetConference,
        ctx.latex || undefined,
        chartImages
      );
      const { url: pdfUrl } = await storagePut(`runs/${ctx.runId}/paper-${suffix}.pdf`, pdfBuffer, "application/pdf");
      await db.insertArtifact({ runId: ctx.runId, stageNumber: 23, artifactType: "paper_pdf", fileName: "paper.pdf", fileUrl: pdfUrl, fileKey: `runs/${ctx.runId}/paper-${suffix}.pdf`, mimeType: "application/pdf" });
      console.log(`[Pipeline] PDF generated successfully (${(pdfBuffer.length / 1024).toFixed(1)} KiB)`);
    } catch (pdfErr: any) {
      // Log the full error for debugging
      console.error("[Pipeline] PDF generation FAILED:", pdfErr);
      console.error("[Pipeline] PDF error stack:", pdfErr?.stack);
      // Save an error artifact so the user knows PDF generation failed
      try {
        const errorMsg = `PDF generation failed: ${pdfErr?.message || "Unknown error"}\n\nStack trace:\n${pdfErr?.stack || "N/A"}\n\nChromium paths tried: /usr/bin/chromium-browser, /usr/bin/chromium, /usr/bin/google-chrome, etc.\nTimestamp: ${new Date().toISOString()}`;
        const { url: errUrl } = await storagePut(`runs/${ctx.runId}/pdf-error-${suffix}.txt`, errorMsg, "text/plain");
        await db.insertArtifact({ runId: ctx.runId, stageNumber: 23, artifactType: "pdf_error_log", fileName: "pdf-error.txt", fileUrl: errUrl, fileKey: `runs/${ctx.runId}/pdf-error-${suffix}.txt`, mimeType: "text/plain" });
      } catch {}
    }
  } catch (e) {
    console.warn("[Pipeline] Failed to upload some artifacts:", e);
  }

  const paperMd = `# ${ctx.topic}\n\n## Abstract\n${ctx.abstract}\n\n${ctx.paperBody}`;
  await db.updatePipelineRun(ctx.runId, {
    paperMarkdown: paperMd,
    paperLatex: ctx.latex,
    referencesBib: ctx.references,
    experimentCode: ctx.experimentCode,
    reviewReport: ctx.reviewReport,
  });

  ctx.finalPaper = paperMd;
  return `Final compilation complete. Generated artifacts:\n- Paper (Markdown)\n- Paper (PDF)\n- LaTeX source\n- BibTeX references\n- Experiment code\n- Review report${ctx.experimentOutput?.charts.length ? `\n- ${ctx.experimentOutput.charts.length} data analysis charts` : ""}`;
}

const STAGE_HANDLERS: Record<number, (ctx: PipelineContext) => Promise<string>> = {
  1: stage1_topicAnalysis, 2: stage2_literatureSearch, 3: stage3_paperScreening,
  4: stage4_deepAnalysis, 5: stage5_gapIdentification, 6: stage6_hypothesisGeneration,
  7: stage7_methodDesign, 8: stage8_methodValidation, 9: stage9_codeGeneration,
  10: stage10_codeReview, 11: stage11_experimentExecution, 12: stage12_resultCollection,
  13: stage13_statisticalAnalysis, 14: stage14_figureGeneration, 15: stage15_tableGeneration,
  16: stage16_outlineGeneration, 17: stage17_abstractWriting, 18: stage18_bodyWriting,
  19: stage19_referenceFormatting, 20: stage20_latexCompilation, 21: stage21_peerReview,
  22: stage22_revision, 23: stage23_finalCompilation,
};

/**
 * Wait for user approval in manual mode.
 * Returns the (possibly edited) output string.
 */
async function waitForApproval(runId: string, stageNumber: number, output: string, emit: EventEmitter): Promise<string> {
  // Update DB to awaiting_approval
  await db.updatePipelineRun(runId, { status: "awaiting_approval" });
  await db.updateStageLog(runId, stageNumber, { status: "blocked_approval" });

  emit({
    type: "stage_awaiting_approval",
    runId,
    stageNumber,
    stageName: PIPELINE_STAGES[stageNumber - 1]?.name,
    message: `Stage ${stageNumber} complete. Awaiting your approval to proceed.`,
    data: { output: output.substring(0, 2000) },
    timestamp: Date.now(),
  });

  return new Promise<string>((resolve, reject) => {
    approvalWaiters.set(runId, {
      resolve: (editedOutput?: string) => {
        resolve(editedOutput || output);
      },
      reject: (reason?: string) => {
        reject(new Error(reason || "Stage rejected by user"));
      },
    });
  });
}

export async function executePipeline(
  runId: string,
  topic: string,
  config: RunConfig,
  emit: EventEmitter,
  startStage = 1
): Promise<void> {
  // Load dataset files if any were assigned
  const datasetFiles: DatasetInfo[] = [];
  if (config.datasetFileIds && config.datasetFileIds.length > 0) {
    const dbFiles = await db.getDatasetFilesForRun(runId);
    for (const f of dbFiles) {
      datasetFiles.push({
        originalName: f.originalName,
        fileUrl: f.fileUrl,
        fileKey: f.fileKey,
        fileType: f.fileType,
        columnNames: f.columnNames as string[] | undefined,
        rowCount: f.rowCount ?? undefined,
      });
    }
    if (datasetFiles.length > 0) {
      emit({
        type: "log", runId, message: `Loaded ${datasetFiles.length} dataset file(s) for analysis`,
        timestamp: Date.now(),
      });
    }
  }

  const ctx: PipelineContext = {
    runId, topic, config, emit,
    papers: [], hypothesis: "", methodology: "", methodValidation: "", experimentCode: "",
    experimentResults: "", statisticalAnalysis: "", figures: [], tables: [],
    outline: "", abstract: "", paperBody: "", references: "",
    latex: "", reviewReport: "", revision: "", finalPaper: "",
    datasetFiles,
    experimentOutput: null,
    evidenceProfile: null,
    methodContract: null,
    executionDiagnostics: null,
    methodIntegrityNote: "",
    claimVerificationReport: "",
  };

  await db.updatePipelineRun(runId, { status: "running", currentStage: startStage });

  for (let i = startStage; i <= 23; i++) {
    const stageDef = PIPELINE_STAGES[i - 1];
    const handler = STAGE_HANDLERS[i];
    if (!handler) continue;

    // Check if pipeline was stopped
    const currentRun = await db.getPipelineRun(runId);
    if (currentRun?.status === "stopped") {
      emit({ type: "run_fail", runId, message: "Pipeline stopped by user", timestamp: Date.now() });
      return;
    }

    await db.createStageLog({
      runId, stageNumber: i, stageName: stageDef.name, phaseName: stageDef.phase,
      status: "running", startedAt: new Date(),
    });

    emit({ type: "stage_start", runId, stageNumber: i, stageName: stageDef.name, message: `Starting: ${stageDef.description}`, timestamp: Date.now() });

    const startTime = Date.now();
    let retries = 0;

    while (retries <= config.maxRetries) {
      try {
        let output = await handler(ctx);
        const duration = Date.now() - startTime;

        // In manual mode (autoApprove=false), pause for user approval after each stage
        if (!config.autoApprove && i < 23) {
          await db.updateStageLog(runId, i, {
            status: "done", output: output?.substring(0, 60000), durationMs: duration, completedAt: new Date(),
          });

          try {
            const approvedOutput = await waitForApproval(runId, i, output, emit);

            // If user edited the output, update the stage log and context
            if (approvedOutput !== output) {
              output = approvedOutput;
              await db.updateStageLog(runId, i, { output: approvedOutput?.substring(0, 60000) });
              // Update context fields based on stage
              updateContextFromEdit(ctx, i, approvedOutput);
            }

            emit({ type: "stage_approved", runId, stageNumber: i, stageName: stageDef.name, message: `Stage ${i} approved`, timestamp: Date.now() });
            await db.updatePipelineRun(runId, { status: "running", currentStage: i, stagesDone: i });
          } catch (rejectError: any) {
            // User rejected the stage
            await db.updateStageLog(runId, i, { status: "failed", errorMessage: rejectError?.message });
            await db.updatePipelineRun(runId, { status: "failed", errorMessage: `Stage ${i} rejected: ${rejectError?.message}` });
            emit({ type: "stage_rejected", runId, stageNumber: i, stageName: stageDef.name, message: `Stage ${i} rejected: ${rejectError?.message}`, timestamp: Date.now() });
            emit({ type: "run_fail", runId, message: `Pipeline stopped: Stage ${i} rejected`, timestamp: Date.now() });
            return;
          }
        } else {
          // Auto-approve mode: proceed immediately
          await db.updateStageLog(runId, i, {
            status: "done", output: output?.substring(0, 60000), durationMs: duration, completedAt: new Date(),
          });
          await db.updatePipelineRun(runId, { currentStage: i, stagesDone: i });
          emit({ type: "stage_complete", runId, stageNumber: i, stageName: stageDef.name, message: `Completed: ${stageDef.description}`, data: { durationMs: duration }, timestamp: Date.now() });
        }

        break; // success
      } catch (error: any) {
        retries++;
        if (retries > config.maxRetries) {
          const errMsg = error?.message || String(error);
          await db.updateStageLog(runId, i, {
            status: "failed", errorMessage: errMsg, durationMs: Date.now() - startTime, completedAt: new Date(),
          });
          await db.updatePipelineRun(runId, {
            status: "failed", currentStage: i, errorMessage: `Stage ${i} (${stageDef.name}) failed: ${errMsg}`,
            stagesFailed: (await db.getPipelineRun(runId))?.stagesFailed ? (await db.getPipelineRun(runId))!.stagesFailed + 1 : 1,
          });
          emit({ type: "stage_fail", runId, stageNumber: i, stageName: stageDef.name, message: `Failed: ${errMsg}`, timestamp: Date.now() });
          emit({ type: "run_fail", runId, message: `Pipeline failed at stage ${i}: ${stageDef.name}`, timestamp: Date.now() });
          return;
        }
        emit({ type: "log", runId, stageNumber: i, message: `Retry ${retries}/${config.maxRetries}: ${error?.message}`, timestamp: Date.now() });
        await new Promise(r => setTimeout(r, 2000 * retries));
      }
    }
  }

  await db.updatePipelineRun(runId, { status: "completed", completedAt: new Date(), stagesDone: 23 });
  emit({ type: "run_complete", runId, message: "Pipeline completed successfully!", timestamp: Date.now() });
}

/** Update pipeline context when user edits stage output in manual mode */
function updateContextFromEdit(ctx: PipelineContext, stageNumber: number, editedOutput: string) {
  switch (stageNumber) {
    case 6: ctx.hypothesis = editedOutput; break;
    case 7: ctx.methodology = editedOutput; break;
    case 8:
      ctx.methodValidation = editedOutput;
      ctx.methodContract = parseMethodContract(editedOutput, ctx.methodology, ensureEvidenceProfile(ctx));
      break;
    case 9: ctx.experimentCode = editedOutput; break;
    case 11:
      ctx.experimentResults = editedOutput;
      if (!ctx.executionDiagnostics) {
        ctx.executionDiagnostics = {
          executionStatus: "partial",
          executableRequested: ctx.methodContract?.executableNow || [],
          executedMethods: [],
          missingRequested: ctx.methodContract?.executableNow || [],
          analyticalMetricCount: 0,
          chartCount: 0,
          tableCount: 0,
          failureReasons: ["Stage 11 output was manually edited."],
        };
      }
      break;
    case 13: ctx.statisticalAnalysis = editedOutput; break;
    case 16: ctx.outline = editedOutput; break;
    case 17: ctx.abstract = editedOutput; break;
    case 18:
      ctx.paperBody = editedOutput;
      ctx.claimVerificationReport = buildClaimVerificationReport(ctx, editedOutput).report;
      break;
    case 21: ctx.reviewReport = editedOutput; break;
    case 22: ctx.revision = editedOutput; break;
  }
}
