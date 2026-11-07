/**
 * Queued resumes. Completing a workflow task (or a server event) used to run
 * the rest of the workflow inside the caller's request; now the caller
 * enqueues a `task_resume` job on workflow_queue and returns at once with the
 * run id, and the queue worker resumes the run (kicked right away).
 *
 * enqueue_workflow is service-role only (20261118000000); as defence in depth a
 * resume job still carries an HMAC (keyed with serverSigningKey(), which only the
 * server holds) over its tenant, workflow, run, event and payload. The worker
 * runs only jobs whose signature verifies, and only while the run is still
 * waiting on that event of that workflow and tenant.
 */
import { createHmac, timingSafeEqual } from "node:crypto"
import type { SupabaseClient } from "@supabase/supabase-js"
import { createServiceClient } from "@/lib/cron-auth"
import { serverSigningKey } from "@/lib/server-signing-key"
import { kickQueueProcessor } from "@/lib/process-queue"
import { RunNotResumableError } from "@/lib/workflow-run-errors"

export const RESUME_TRIGGER_TYPE = "task_resume"

/** Shape stored under trigger_data._task_resume. */
export interface QueuedResume {
  run_id: string
  event_id: string
  /** JSON of the variables to merge (a string, so the signature is over exact bytes). */
  payload: string
  sig: string
}

interface ResumeTarget {
  tenantId: string
  workflowId: string
  runId: string
  eventId: string
  payload: string
}

function signingKey(): string {
  const key = serverSigningKey()
  if (!key) throw new Error("Set ENCRYPTION_KEY (or SUPABASE_SERVICE_ROLE_KEY) to queue workflow resumes")
  return key
}

export function signResume(target: ResumeTarget): string {
  return createHmac("sha256", signingKey())
    .update(["task_resume", target.tenantId, target.workflowId, target.runId, target.eventId, target.payload].join("\n"))
    .digest("hex")
}

function verifyResume(target: ResumeTarget, sig: unknown): boolean {
  if (typeof sig !== "string" || !/^[0-9a-f]{64}$/.test(sig)) return false
  const expected = Buffer.from(signResume(target), "hex")
  return timingSafeEqual(expected, Buffer.from(sig, "hex"))
}

export interface EnqueuedResume {
  run_id: string
  workflow_id: string
  job_id: string
  resumed: "queued"
}

/**
 * Queue the resume of the run waiting on `eventId` (scoped to `tenantId`) with
 * `variables`, and kick the processor. Throws RunNotResumableError when no
 * run of the tenant is waiting on that event.
 */
export async function enqueueResume(params: {
  tenantId: string
  eventId: string
  variables: Record<string, unknown>
  /** Service-role client; created when omitted. */
  supabase?: SupabaseClient
}): Promise<EnqueuedResume> {
  const supabase = params.supabase ?? createServiceClient()
  const { data: run } = await supabase
    .from("workflow_runs")
    .select("id, workflow_id, tenant_id, event_id")
    .eq("event_id", params.eventId)
    .eq("tenant_id", params.tenantId)
    .in("status", ["waiting_for_event", "paused"])
    .limit(1)
    .maybeSingle()
  if (!run) throw new RunNotResumableError()

  const target: ResumeTarget = {
    tenantId: run.tenant_id,
    workflowId: run.workflow_id,
    runId: run.id,
    eventId: String(run.event_id),
    payload: JSON.stringify(params.variables ?? {}),
  }
  const resume: QueuedResume = {
    run_id: target.runId,
    event_id: target.eventId,
    payload: target.payload,
    sig: signResume(target),
  }
  const { data: jobId, error } = await supabase.rpc("enqueue_workflow", {
    p_workflow_id: target.workflowId,
    p_tenant_id: target.tenantId,
    p_task_id: null,
    p_trigger_type: RESUME_TRIGGER_TYPE,
    p_trigger_data: { _task_resume: resume },
    p_priority: 0,
    p_delay_seconds: 0,
  })
  if (error || !jobId) throw new Error(`Failed to queue the workflow resume: ${error?.message ?? "no job id"}`)

  kickQueueProcessor()
  return { run_id: target.runId, workflow_id: target.workflowId, job_id: String(jobId), resumed: "queued" }
}

/** Queue worker side of a `task_resume` job. */
export async function runQueuedResume(
  resume: unknown,
  job: { workflowId: string; tenantId: string },
  options: { supabase: SupabaseClient },
): Promise<{ status: string; node_results: Record<string, unknown>; run_id?: string; reason?: string; error?: string }> {
  const r = (resume ?? {}) as Partial<QueuedResume>
  if (typeof r.run_id !== "string" || typeof r.event_id !== "string" || typeof r.payload !== "string") {
    return { status: "skipped", node_results: {}, reason: "Malformed resume job" }
  }
  const target: ResumeTarget = {
    tenantId: job.tenantId,
    workflowId: job.workflowId,
    runId: r.run_id,
    eventId: r.event_id,
    payload: r.payload,
  }
  if (!verifyResume(target, r.sig)) {
    return { status: "skipped", node_results: {}, run_id: r.run_id, reason: "Resume job signature is invalid" }
  }

  // The run must still belong to the job's workflow and tenant; resumeWorkflow
  // then claims it only while it is waiting on this event.
  const { data: run } = await options.supabase
    .from("workflow_runs")
    .select("id")
    .eq("id", target.runId)
    .eq("workflow_id", target.workflowId)
    .eq("tenant_id", target.tenantId)
    .maybeSingle()
  if (!run) return { status: "skipped", node_results: {}, run_id: target.runId, reason: "Run not found" }

  let variables: Record<string, unknown>
  try {
    const parsed = JSON.parse(target.payload)
    variables = parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {}
  } catch {
    variables = {}
  }

  // Loaded here so request paths that only enqueue don't load the engine.
  const { resumeWorkflow } = await import("@/lib/workflow-engine")
  try {
    const result = await resumeWorkflow(target.runId, variables, undefined, {
      supabase: options.supabase,
      eventId: target.eventId,
    })
    return { ...result, run_id: target.runId }
  } catch (error) {
    // Already resumed (double submit) or cancelled meanwhile: nothing to do.
    if (error instanceof RunNotResumableError) {
      return { status: "skipped", node_results: {}, run_id: target.runId, reason: error.message }
    }
    throw error
  }
}
