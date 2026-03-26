import { z } from "zod";
import { COOKIE_NAME } from "@shared/const";
import { getSessionCookieOptions } from "./_core/cookies";
import { systemRouter } from "./_core/systemRouter";
import { publicProcedure, protectedProcedure, router } from "./_core/trpc";
import { nanoid } from "nanoid";
import * as db from "./db";
import { unifiedSearch } from "./literature";
import { executePipeline, approveStage, rejectStage, isAwaitingApproval, type EventEmitter } from "./pipeline-engine";
import { PIPELINE_STAGES, DEFAULT_RUN_CONFIG, CONFERENCE_TEMPLATES, type PipelineEvent, type RunConfig } from "../shared/pipeline";

// ─── In-memory event store for SSE/polling ───
const runEventBuffers = new Map<string, PipelineEvent[]>();
const runEventListeners = new Map<string, Set<(event: PipelineEvent) => void>>();

function createEmitter(runId: string): EventEmitter {
  return (event: PipelineEvent) => {
    if (!runEventBuffers.has(runId)) runEventBuffers.set(runId, []);
    const buf = runEventBuffers.get(runId)!;
    buf.push(event);
    if (buf.length > 500) buf.splice(0, buf.length - 500);

    const listeners = runEventListeners.get(runId);
    if (listeners) {
      listeners.forEach(fn => {
        try { fn(event); } catch {}
      });
    }
  };
}

export function subscribeToRun(runId: string, listener: (event: PipelineEvent) => void): () => void {
  if (!runEventListeners.has(runId)) runEventListeners.set(runId, new Set());
  runEventListeners.get(runId)!.add(listener);
  return () => { runEventListeners.get(runId)?.delete(listener); };
}

export function getRunEvents(runId: string, since = 0): PipelineEvent[] {
  const buf = runEventBuffers.get(runId) || [];
  return buf.filter(e => e.timestamp > since);
}

// ─── Routers ───
const pipelineRouter = router({
  start: publicProcedure
    .input(z.object({
      topic: z.string().min(3, "Topic must be at least 3 characters"),
      autoApprove: z.boolean().default(true),
      datasetFileIds: z.array(z.number()).default([]),
      config: z.object({
        targetConference: z.string().default("NeurIPS"),
        experimentMode: z.enum(["simulated", "sandbox"]).default("simulated"),
        maxRetries: z.number().min(0).max(5).default(2),
        timeoutMinutes: z.number().min(10).max(300).default(120),
        qualityThreshold: z.number().min(0).max(1).default(0.7),
        dataSources: z.object({
          arxiv: z.boolean().default(true),
          semanticScholar: z.boolean().default(true),
          springer: z.boolean().default(true),
          pubmed: z.boolean().default(true),
          crossref: z.boolean().default(true),
        }).default({ arxiv: true, semanticScholar: true, springer: true, pubmed: true, crossref: true }),
      }).default({ targetConference: "NeurIPS", experimentMode: "simulated" as const, maxRetries: 2, timeoutMinutes: 120, qualityThreshold: 0.7, dataSources: { arxiv: true, semanticScholar: true, springer: true, pubmed: true, crossref: true } }),
    }))
    .mutation(async ({ input, ctx }) => {
      const runId = `rc-${Date.now()}-${nanoid(8)}`;
      const config: RunConfig = {
        ...DEFAULT_RUN_CONFIG,
        autoApprove: input.autoApprove,
        ...input.config,
        datasetFileIds: input.datasetFileIds,
      };

      await db.createPipelineRun({
        runId,
        userId: ctx.user?.id || null,
        topic: input.topic,
        status: "pending",
        autoApprove: input.autoApprove ? 1 : 0,
        config: config as any,
      });

      // Assign dataset files to this run
      if (input.datasetFileIds.length > 0) {
        await db.assignDatasetFilesToRun(input.datasetFileIds, runId);
      }

      // Start pipeline in background
      const emit = createEmitter(runId);
      executePipeline(runId, input.topic, config, emit).catch(err => {
        console.error(`[Pipeline] Run ${runId} crashed:`, err);
        db.updatePipelineRun(runId, { status: "failed", errorMessage: err?.message || String(err) });
        emit({ type: "run_fail", runId, message: err?.message || "Pipeline crashed", timestamp: Date.now() });
      });

      return { runId, status: "pending" };
    }),

  stop: publicProcedure
    .input(z.object({ runId: z.string() }))
    .mutation(async ({ input }) => {
      await db.updatePipelineRun(input.runId, { status: "stopped" });
      return { success: true };
    }),

  // ─── Manual Approval Endpoints ───
  approve: publicProcedure
    .input(z.object({
      runId: z.string(),
      editedOutput: z.string().optional(),
    }))
    .mutation(async ({ input }) => {
      if (!isAwaitingApproval(input.runId)) {
        return { success: false, message: "This run is not currently awaiting approval" };
      }
      const ok = approveStage(input.runId, input.editedOutput);
      return { success: ok };
    }),

  reject: publicProcedure
    .input(z.object({
      runId: z.string(),
      reason: z.string().optional(),
    }))
    .mutation(async ({ input }) => {
      if (!isAwaitingApproval(input.runId)) {
        return { success: false, message: "This run is not currently awaiting approval" };
      }
      const ok = rejectStage(input.runId, input.reason);
      return { success: ok };
    }),

  approvalStatus: publicProcedure
    .input(z.object({ runId: z.string() }))
    .query(({ input }) => {
      return { awaiting: isAwaitingApproval(input.runId) };
    }),

  get: publicProcedure
    .input(z.object({ runId: z.string() }))
    .query(async ({ input }) => {
      const run = await db.getPipelineRun(input.runId);
      if (!run) return null;
      const stages = await db.getStageLogsForRun(input.runId);
      const artifactsList = await db.getArtifactsForRun(input.runId);
      const datasets = await db.getDatasetFilesForRun(input.runId);
      const experiments = await db.getExperimentResultsForRun(input.runId);
      return { ...run, stages, artifacts: artifactsList, datasets, experiments };
    }),

  list: publicProcedure
    .input(z.object({ limit: z.number().min(1).max(100).default(20) }).default({ limit: 20 }))
    .query(async ({ ctx, input }) => {
      return db.listPipelineRuns(ctx.user?.id || undefined, input.limit);
    }),

  events: publicProcedure
    .input(z.object({ runId: z.string(), since: z.number().default(0) }))
    .query(({ input }) => {
      return getRunEvents(input.runId, input.since);
    }),

  stages: publicProcedure.query(() => PIPELINE_STAGES),

  templates: publicProcedure.query(() => CONFERENCE_TEMPLATES),
});

const literatureRouter = router({
  search: publicProcedure
    .input(z.object({
      query: z.string().min(2),
      maxPerSource: z.number().min(1).max(50).default(10),
      sources: z.object({
        arxiv: z.boolean().default(true),
        semanticScholar: z.boolean().default(true),
        springer: z.boolean().default(true),
        pubmed: z.boolean().default(true),
        crossref: z.boolean().default(true),
      }).default({ arxiv: true, semanticScholar: true, springer: true, pubmed: true, crossref: true }),
    }))
    .query(async ({ input }) => {
      return unifiedSearch(input.query, {
        maxPerSource: input.maxPerSource,
        semanticScholarApiKey: process.env.SEMANTIC_SCHOLAR_API_KEY,
        springerApiKey: process.env.SPRINGER_API_KEY,
        sources: input.sources,
      });
    }),

  forRun: publicProcedure
    .input(z.object({ runId: z.string() }))
    .query(async ({ input }) => {
      return db.getPapersForRun(input.runId);
    }),
});

const settingsRouter = router({
  get: publicProcedure
    .input(z.object({ key: z.string() }))
    .query(async ({ ctx, input }) => {
      return db.getUserSetting(ctx.user?.id || null, input.key);
    }),

  set: publicProcedure
    .input(z.object({ key: z.string(), value: z.string() }))
    .mutation(async ({ ctx, input }) => {
      await db.setUserSetting(ctx.user?.id || null, input.key, input.value);
      return { success: true };
    }),

  getAll: publicProcedure.query(async ({ ctx }) => {
    const keys = ["targetConference", "experimentMode", "maxRetries", "timeoutMinutes", "qualityThreshold"];
    const settings: Record<string, string | null> = {};
    for (const key of keys) {
      settings[key] = await db.getUserSetting(ctx.user?.id || null, key);
    }
    return settings;
  }),
});

const artifactRouter = router({
  forRun: publicProcedure
    .input(z.object({ runId: z.string() }))
    .query(async ({ input }) => {
      return db.getArtifactsForRun(input.runId);
    }),
});

const datasetRouter = router({
  /** List uploaded files for current user (unassigned to any run) */
  myFiles: publicProcedure.query(async ({ ctx }) => {
    if (!ctx.user?.id) return [];
    return db.getUnassignedDatasetFiles(ctx.user.id);
  }),

  /** List all files uploaded by current user */
  allMyFiles: publicProcedure.query(async ({ ctx }) => {
    if (!ctx.user?.id) return [];
    return db.getDatasetFilesByUser(ctx.user.id);
  }),

  /** Get dataset files for a specific run */
  forRun: publicProcedure
    .input(z.object({ runId: z.string() }))
    .query(async ({ input }) => {
      return db.getDatasetFilesForRun(input.runId);
    }),

  /** Get experiment results for a run */
  experimentResults: publicProcedure
    .input(z.object({ runId: z.string() }))
    .query(async ({ input }) => {
      return db.getExperimentResultsForRun(input.runId);
    }),
});

export const appRouter = router({
  system: systemRouter,
  auth: router({
    me: publicProcedure.query(opts => opts.ctx.user),
    logout: publicProcedure.mutation(({ ctx }) => {
      const cookieOptions = getSessionCookieOptions(ctx.req);
      ctx.res.clearCookie(COOKIE_NAME, { ...cookieOptions, maxAge: -1 });
      return { success: true } as const;
    }),
  }),
  pipeline: pipelineRouter,
  literature: literatureRouter,
  settings: settingsRouter,
  artifacts: artifactRouter,
  datasets: datasetRouter,
});

export type AppRouter = typeof appRouter;
