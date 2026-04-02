import { eq, desc, and, sql } from "drizzle-orm";
import { drizzle } from "drizzle-orm/mysql2";
import { InsertUser, users, pipelineRuns, stageLogs, papers, artifacts, userSettings, datasetFiles, experimentResults } from "../drizzle/schema";
import type { InsertPipelineRun, InsertStageLog, InsertPaper, InsertArtifact, InsertDatasetFile, InsertExperimentResult } from "../drizzle/schema";
import { ENV } from './_core/env';

let _db: ReturnType<typeof drizzle> | null = null;

export async function getDb() {
  if (!_db && process.env.DATABASE_URL) {
    try {
      _db = drizzle(process.env.DATABASE_URL);
    } catch (error) {
      console.warn("[Database] Failed to connect:", error);
      _db = null;
    }
  }
  return _db;
}

export async function upsertUser(user: InsertUser): Promise<void> {
  if (!user.openId) throw new Error("User openId is required for upsert");
  const db = await getDb();
  if (!db) { console.warn("[Database] Cannot upsert user: database not available"); return; }
  try {
    const values: InsertUser = { openId: user.openId };
    const updateSet: Record<string, unknown> = {};
    const textFields = ["name", "email", "loginMethod"] as const;
    type TextField = (typeof textFields)[number];
    const assignNullable = (field: TextField) => {
      const value = user[field];
      if (value === undefined) return;
      const normalized = value ?? null;
      values[field] = normalized;
      updateSet[field] = normalized;
    };
    textFields.forEach(assignNullable);
    if (user.lastSignedIn !== undefined) { values.lastSignedIn = user.lastSignedIn; updateSet.lastSignedIn = user.lastSignedIn; }
    if (user.role !== undefined) { values.role = user.role; updateSet.role = user.role; }
    else if (user.openId === ENV.ownerOpenId) { values.role = 'admin'; updateSet.role = 'admin'; }
    if (!values.lastSignedIn) values.lastSignedIn = new Date();
    if (Object.keys(updateSet).length === 0) updateSet.lastSignedIn = new Date();
    await db.insert(users).values(values).onDuplicateKeyUpdate({ set: updateSet });
  } catch (error) { console.error("[Database] Failed to upsert user:", error); throw error; }
}

export async function getUserByOpenId(openId: string) {
  const db = await getDb();
  if (!db) return undefined;
  const result = await db.select().from(users).where(eq(users.openId, openId)).limit(1);
  return result.length > 0 ? result[0] : undefined;
}

// ─── Pipeline Runs ───
export async function createPipelineRun(run: InsertPipelineRun) {
  const db = await getDb();
  if (!db) throw new Error("Database not available");
  await db.insert(pipelineRuns).values(run);
  return run;
}

export async function getPipelineRun(runId: string) {
  const db = await getDb();
  if (!db) return null;
  const result = await db.select().from(pipelineRuns).where(eq(pipelineRuns.runId, runId)).limit(1);
  return result[0] || null;
}

export async function listPipelineRuns(userId?: number, limit = 20) {
  const db = await getDb();
  if (!db) return [];
  if (userId) {
    return db.select().from(pipelineRuns).where(eq(pipelineRuns.userId, userId)).orderBy(desc(pipelineRuns.createdAt)).limit(limit);
  }
  return db.select().from(pipelineRuns).orderBy(desc(pipelineRuns.createdAt)).limit(limit);
}

export async function updatePipelineRun(runId: string, updates: Partial<InsertPipelineRun>) {
  const db = await getDb();
  if (!db) return;
  await db.update(pipelineRuns).set(updates).where(eq(pipelineRuns.runId, runId));
}

// ─── Stage Logs ───
export async function createStageLog(log: InsertStageLog) {
  const db = await getDb();
  if (!db) throw new Error("Database not available");
  await db.insert(stageLogs).values(log);
}

export async function getStageLogsForRun(runId: string) {
  const db = await getDb();
  if (!db) return [];
  return db.select().from(stageLogs).where(eq(stageLogs.runId, runId)).orderBy(stageLogs.stageNumber);
}

export async function updateStageLog(runId: string, stageNumber: number, updates: Partial<InsertStageLog>) {
  const db = await getDb();
  if (!db) return;
  await db.update(stageLogs).set(updates).where(
    and(eq(stageLogs.runId, runId), eq(stageLogs.stageNumber, stageNumber))
  );
}

export async function updateStageLogWhileRunning(runId: string, stageNumber: number, updates: Partial<InsertStageLog>) {
  const db = await getDb();
  if (!db) return;
  await db.update(stageLogs).set(updates).where(
    and(
      eq(stageLogs.runId, runId),
      eq(stageLogs.stageNumber, stageNumber),
      eq(stageLogs.status, "running"),
    )
  );
}

// ─── Papers ───
export async function insertPapers(papersData: InsertPaper[]) {
  const db = await getDb();
  if (!db) throw new Error("Database not available");
  if (papersData.length === 0) return;
  await db.insert(papers).values(papersData);
}

export async function getPapersForRun(runId: string) {
  const db = await getDb();
  if (!db) return [];
  return db.select().from(papers).where(eq(papers.runId, runId));
}

// ─── Artifacts ───
export async function insertArtifact(artifact: InsertArtifact) {
  const db = await getDb();
  if (!db) throw new Error("Database not available");
  await db.insert(artifacts).values(artifact);
}

export async function getArtifactsForRun(runId: string) {
  const db = await getDb();
  if (!db) return [];
  return db.select().from(artifacts).where(eq(artifacts.runId, runId));
}

// ─── User Settings ───
export async function getUserSetting(userId: number | null, key: string) {
  const db = await getDb();
  if (!db) return null;
  const condition = userId
    ? and(eq(userSettings.userId, userId), eq(userSettings.settingKey, key))
    : eq(userSettings.settingKey, key);
  const result = await db.select().from(userSettings).where(condition).limit(1);
  return result[0]?.settingValue || null;
}

export async function setUserSetting(userId: number | null, key: string, value: string) {
  const db = await getDb();
  if (!db) return;
  const existing = await getUserSetting(userId, key);
  if (existing !== null) {
    const condition = userId
      ? and(eq(userSettings.userId, userId), eq(userSettings.settingKey, key))
      : eq(userSettings.settingKey, key);
    await db.update(userSettings).set({ settingValue: value }).where(condition);
  } else {
    await db.insert(userSettings).values({ userId, settingKey: key, settingValue: value });
  }
}

// ─── Dataset Files ───
export async function insertDatasetFile(file: InsertDatasetFile) {
  const db = await getDb();
  if (!db) throw new Error("Database not available");
  const [inserted] = await db.insert(datasetFiles).values(file).$returningId();
  return { ...file, id: inserted.id };
}

export async function getDatasetFilesForRun(runId: string) {
  const db = await getDb();
  if (!db) return [];
  return db.select().from(datasetFiles).where(eq(datasetFiles.runId, runId));
}

export async function getDatasetFilesByUser(userId: number) {
  const db = await getDb();
  if (!db) return [];
  return db.select().from(datasetFiles).where(eq(datasetFiles.userId, userId)).orderBy(desc(datasetFiles.createdAt));
}

export async function getUnassignedDatasetFiles(userId: number) {
  const db = await getDb();
  if (!db) return [];
  return db.select().from(datasetFiles).where(
    and(eq(datasetFiles.userId, userId), sql`${datasetFiles.runId} IS NULL`)
  ).orderBy(desc(datasetFiles.createdAt));
}

export async function assignDatasetFilesToRun(fileIds: number[], runId: string) {
  const db = await getDb();
  if (!db) return;
  for (const id of fileIds) {
    await db.update(datasetFiles).set({ runId }).where(eq(datasetFiles.id, id));
  }
}

// ─── Experiment Results ───
export async function insertExperimentResult(result: InsertExperimentResult) {
  const db = await getDb();
  if (!db) throw new Error("Database not available");
  const [inserted] = await db.insert(experimentResults).values(result).$returningId();
  return inserted;
}

export async function getExperimentResultsForRun(runId: string) {
  const db = await getDb();
  if (!db) return [];
  return db.select().from(experimentResults).where(eq(experimentResults.runId, runId)).orderBy(experimentResults.stageNumber);
}

export async function updateExperimentResult(id: number, updates: Partial<InsertExperimentResult>) {
  const db = await getDb();
  if (!db) return;
  await db.update(experimentResults).set(updates).where(eq(experimentResults.id, id));
}
