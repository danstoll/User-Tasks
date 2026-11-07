/**
 * Who started a workflow run: the `workflow_started_by` / `workflow_started_via`
 * run variables (and `workflow_owner_id`, the workflow's creator).
 *
 *  - Engine-owned. A caller's start variables, a resumed task's values and a
 *    data-table get never set them; executeWorkflow takes the initiator from
 *    its trusted `options.initiator` (the start path decides it).
 *  - `workflow_started_by` is the initiator's user id, falling back to the
 *    workflow's owner when the start has no user (schedules, webhooks, events
 *    without a person), so a step assigned to it still lands with someone.
 *  - Queued starts: enqueue_workflow is service-role only (20261118000000);
 *    as defence in depth a queued job carries its initiator under
 *    `trigger_data._started_by` with an HMAC (keyed with the service-role
 *    key, which only the server holds) over the tenant, workflow, user, via,
 *    time and the rest of the trigger data. The worker trusts only a
 *    signature that verifies (and is under 7 days old); anything else runs
 *    as a start with no user. Task triggers (queued by a database trigger)
 *    take the actor from the task row itself.
 */
import { createHmac, timingSafeEqual } from "node:crypto"
import type { SupabaseClient } from "@supabase/supabase-js"
import { serverSigningKey } from "@/lib/server-signing-key"

/** How a run was started (`{{workflow_started_via}}`). */
export type StartedVia =
  | "ui" // Run dialog / designer Run
  | "test" // designer Test run
  | "api" // REST /api/v1 (API key or OAuth token)
  | "cli" // the usertasks CLI (REST with its user agent)
  | "mcp" // MCP workflows_start
  | "form" // a form button's Start workflow, or the Form submitted trigger
  | "agent" // an agent's start_workflow_run tool
  | "task" // a task trigger (created, completed, ...)
  | "data_table" // a data-table row event
  | "subworkflow" // Call sub-workflow
  | "schedule"
  | "webhook"
  | "event" // workflow error / SLA / retention triggers
  | "system"

export const STARTED_VIA_VALUES: readonly StartedVia[] = [
  "ui", "test", "api", "cli", "mcp", "form", "agent", "task", "data_table", "subworkflow", "schedule", "webhook", "event", "system",
]

export interface RunInitiator {
  /** The person who started the run; null when no person did (the owner is used). */
  userId: string | null
  via: StartedVia
}

/** Run variables only the engine sets. */
export const INITIATOR_VARIABLES = ["workflow_started_by", "workflow_started_via", "workflow_owner_id"] as const

export const SYSTEM_INITIATOR: RunInitiator = { userId: null, via: "system" }

/** Key of the signed initiator in a queued job's trigger_data. */
export const QUEUED_INITIATOR_KEY = "_started_by"

const MAX_SIGNATURE_AGE_MS = 7 * 24 * 60 * 60 * 1000

function isStartedVia(value: unknown): value is StartedVia {
  return typeof value === "string" && (STARTED_VIA_VALUES as readonly string[]).includes(value)
}

function cleanUserId(value: unknown): string | null {
  const text = typeof value === "string" ? value.trim() : ""
  return text ? text : null
}

/** `variables` without the engine-owned initiator variables (a new object). */
export function stripInitiatorVariables<T extends Record<string, unknown>>(variables: T | null | undefined): Record<string, unknown> {
  const out: Record<string, unknown> = { ...(variables ?? {}) }
  for (const key of INITIATOR_VARIABLES) delete out[key]
  return out
}

/**
 * The run variables of an initiator: started_by (the owner when no user),
 * started_via and owner_id.
 */
export function initiatorVariables(initiator: RunInitiator | null | undefined, ownerId: string | null | undefined): Record<(typeof INITIATOR_VARIABLES)[number], string> {
  const owner = cleanUserId(ownerId) ?? ""
  return {
    workflow_started_by: cleanUserId(initiator?.userId) ?? owner,
    workflow_started_via: isStartedVia(initiator?.via) ? initiator!.via : "system",
    workflow_owner_id: owner,
  }
}

/**
 * The initiator a running run hands to what it starts (sub-workflows,
 * data-table events it writes): its own started_by.
 */
export function initiatorFromRunVariables(variables: Record<string, unknown>, via: StartedVia): RunInitiator {
  return { userId: cleanUserId(variables.workflow_started_by), via }
}

/* ── Service contexts ──────────────────────────────────────────────── */

interface InitiatorContext {
  actorId: string | null
  credentialOwnerId?: string | null
  source: "api" | "mcp" | "ui" | "job"
}

/**
 * The initiator of a start through a service context: the signed-in user
 * (UI), the key's user — a tenant key's creator — (API / MCP / background
 * jobs). `via` overrides the channel (form buttons, the CLI).
 */
export function initiatorFromContext(ctx: InitiatorContext, via?: StartedVia): RunInitiator {
  const channel: StartedVia = via ?? (ctx.source === "ui" ? "ui" : ctx.source === "mcp" ? "mcp" : "api")
  return { userId: cleanUserId(ctx.actorId) ?? cleanUserId(ctx.credentialOwnerId), via: channel }
}

/** A REST request from the usertasks CLI (its User-Agent; a label, not a credential). */
export function isCliUserAgent(userAgent: string | null | undefined): boolean {
  return typeof userAgent === "string" && /^usertasks-cli\//i.test(userAgent.trim())
}

/* ── Queued starts ─────────────────────────────────────────────────── */

export interface QueuedInitiator {
  user_id: string | null
  via: StartedVia
  /** Signing time (ms since epoch). */
  at: number
  sig: string
}

/** JSON with object keys sorted at every level, so jsonb round trips sign the same bytes. */
export function canonicalJson(value: unknown): string {
  if (value !== null && typeof value === "object" && typeof (value as { toJSON?: unknown }).toJSON === "function") {
    return canonicalJson((value as { toJSON: () => unknown }).toJSON())
  }
  if (value === null || typeof value !== "object") return JSON.stringify(value) ?? "null"
  if (Array.isArray(value)) return `[${value.map((item) => (item === undefined ? "null" : canonicalJson(item))).join(",")}]`
  const entries = Object.keys(value as Record<string, unknown>)
    .filter((key) => (value as Record<string, unknown>)[key] !== undefined)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalJson((value as Record<string, unknown>)[key])}`)
  return `{${entries.join(",")}}`
}

/** The trigger data the signature covers: everything but the signature and the version pin. */
function signedPayload(triggerData: Record<string, unknown>): string {
  const rest: Record<string, unknown> = { ...triggerData }
  delete rest[QUEUED_INITIATOR_KEY]
  delete rest._version_id
  return canonicalJson(rest)
}

function sign(key: string, target: { tenantId: string; workflowId: string; userId: string | null; via: StartedVia; at: number; payload: string }): string {
  return createHmac("sha256", key)
    .update(["workflow_initiator", target.tenantId, target.workflowId, target.userId ?? "", target.via, String(target.at), target.payload].join("\n"))
    .digest("hex")
}

/**
 * `triggerData` with the initiator signed in under `_started_by`. Without a
 * signing key (never on a configured server) the data goes out unsigned and
 * the run starts with no user.
 */
export function withQueuedInitiator(
  triggerData: Record<string, unknown>,
  target: { tenantId: string; workflowId: string },
  initiator: RunInitiator,
  now: number = Date.now(),
): Record<string, unknown> {
  const out: Record<string, unknown> = { ...triggerData }
  delete out[QUEUED_INITIATOR_KEY]
  const key = serverSigningKey()
  if (!key) return out
  const userId = cleanUserId(initiator.userId)
  const via = isStartedVia(initiator.via) ? initiator.via : "system"
  const queued: QueuedInitiator = {
    user_id: userId,
    via,
    at: now,
    sig: sign(key, { ...target, userId, via, at: now, payload: signedPayload(out) }),
  }
  out[QUEUED_INITIATOR_KEY] = queued
  return out
}

/**
 * The verified initiator of a queued job (null when absent, forged, for
 * another tenant/workflow/payload, or expired). `triggerData` is the job's
 * trigger data as stored (with `_started_by`).
 */
export function readQueuedInitiator(
  triggerData: Record<string, unknown> | null | undefined,
  target: { tenantId: string; workflowId: string },
  now: number = Date.now(),
): RunInitiator | null {
  const raw = triggerData?.[QUEUED_INITIATOR_KEY] as Partial<QueuedInitiator> | undefined
  if (!raw || typeof raw !== "object") return null
  if (typeof raw.sig !== "string" || !/^[0-9a-f]{64}$/.test(raw.sig)) return null
  if (!isStartedVia(raw.via) || typeof raw.at !== "number" || !Number.isFinite(raw.at)) return null
  if (now - raw.at > MAX_SIGNATURE_AGE_MS || raw.at - now > 5 * 60 * 1000) return null
  const key = serverSigningKey()
  if (!key) return null
  const userId = cleanUserId(raw.user_id)
  const expected = Buffer.from(sign(key, { ...target, userId, via: raw.via, at: raw.at, payload: signedPayload(triggerData!) }), "hex")
  const given = Buffer.from(raw.sig, "hex")
  if (expected.length !== given.length || !timingSafeEqual(expected, given)) return null
  return { userId, via: raw.via }
}

/* ── Unsigned queued jobs ──────────────────────────────────────────── */

export const TASK_TRIGGER_TYPES = ["task_created", "task_received", "task_submitted", "task_updated", "task_completed", "task_rejected"] as const

/** The channel of a queued job with no (valid) signed initiator, by its trigger type. */
export function viaForTriggerType(triggerType: string | null | undefined): StartedVia {
  const type = String(triggerType ?? "")
  if ((TASK_TRIGGER_TYPES as readonly string[]).includes(type)) return "task"
  if (type === "schedule") return "schedule"
  if (type === "webhook_received" || type === "middleware_received") return "webhook"
  if (type === "data_table_event") return "data_table"
  if (type === "form_submitted") return "form"
  if (type === "api" || type === "manual") return "api"
  if (type === "workflow_error" || type === "workflow_sla" || type.startsWith("retention_policy")) return "event"
  return "system"
}

/**
 * The person behind a task event, from the task row (written by the server,
 * so trusted): created / received → the task's assigner; accepted (task_
 * submitted) / rejected → the assignee who answered; completed / updated →
 * whoever last actioned it, else the assignee, else the assigner.
 */
export function taskEventActor(
  triggerType: string,
  task: { assigner_id?: string | null; assignee_id?: string | null; actioned_by?: string | null } | null | undefined,
): string | null {
  if (!task) return null
  const assigner = cleanUserId(task.assigner_id)
  const assignee = cleanUserId(task.assignee_id)
  const actioned = cleanUserId(task.actioned_by)
  switch (triggerType) {
    case "task_created":
    case "task_received":
      return assigner
    case "task_submitted":
    case "task_rejected":
      return assignee ?? actioned
    case "task_completed":
    case "task_updated":
    default:
      return actioned ?? assignee ?? assigner
  }
}

/**
 * The initiator of a queued job: its verified signature, else (task
 * triggers) the task's actor, else none — the run then uses the owner.
 */
export async function resolveQueuedInitiator(
  supabase: SupabaseClient,
  job: { tenant_id: string; workflow_id: string; task_id?: string | null; trigger_type?: string | null },
  triggerData: Record<string, unknown> | null | undefined,
): Promise<RunInitiator> {
  const signed = readQueuedInitiator(triggerData, { tenantId: job.tenant_id, workflowId: job.workflow_id })
  if (signed) return signed
  const triggerType = String(job.trigger_type ?? "")
  const via = viaForTriggerType(triggerType)
  const taskId = cleanUserId(job.task_id) ?? cleanUserId(triggerData?.task_id)
  if (via === "task" && taskId) {
    try {
      const { data } = await supabase
        .from("tasks")
        .select("assigner_id, assignee_id, actioned_by")
        .eq("id", taskId)
        .eq("tenant_id", job.tenant_id)
        .maybeSingle()
      return { userId: taskEventActor(triggerType, data as never), via }
    } catch {
      return { userId: null, via }
    }
  }
  return { userId: null, via }
}
