/**
 * Branch ports of workflow nodes — the labelled outputs a condition, switch,
 * loop, parallel or multi-outcome user task shows under its card. Shared by
 * the canvas node (chip rendering), the edge router (port anchors) and the
 * auto-layout (branch column order) so all three agree on the same order.
 */

import type { WorkflowEdge, WorkflowNode } from './workflow-schema'

function labelOf(item: unknown, withValue: boolean): string {
  const o = (item ?? {}) as { label?: unknown; value?: unknown }
  const v = o.label ?? (withValue ? o.value : undefined)
  return v != null ? String(v) : String(item)
}

/** Ports of a condition configured with rule groups (or one legacy rule) instead of branches. */
export const TRUE_FALSE_LABELS = ['True', 'False'] as const

/** A condition without branches whose rule groups / legacy rule decide True or False. */
export function isTrueFalseCondition(config: Record<string, unknown> | null | undefined): boolean {
  const c = config ?? {}
  if (Array.isArray(c.branches) && c.branches.length > 0) return false
  return (Array.isArray(c.groups) && c.groups.length > 0) || (typeof c.field === 'string' && c.field.trim() !== '')
}

/** Labels of a node's branch ports, in display order (left → right). Empty for single-output nodes. */
export function getBranchLabels(node: Pick<WorkflowNode, 'type' | 'config'>): string[] {
  const config = node.config ?? {}
  switch (node.type) {
    case 'condition': {
      const branches = (config.branches ?? []) as { label?: string }[]
      // Rule groups (or the legacy single rule) without branches: a True / False condition.
      if (branches.length === 0) return isTrueFalseCondition(config) ? [...TRUE_FALSE_LABELS] : []
      const labels = branches.map((b) => b?.label || 'If')
      if (config.has_else !== false) labels.push('Else')
      return labels
    }
    case 'switch_case': {
      const raw = (config.cases ?? []) as unknown[]
      const cases = raw
        .map((c) => (typeof c === 'string' ? c : labelOf(c, true)))
        .filter(Boolean) as string[]
      if (config.has_default !== false) cases.push('Default')
      return cases
    }
    case 'loop':
    case 'loop_n_times':
      return ['Done', 'Each Item']
    case 'parallel': {
      const raw = (config.branches ?? []) as unknown[]
      const labels = raw.map((b) => (typeof b === 'string' ? b : labelOf(b, false))).filter(Boolean) as string[]
      // 'Done' runs once after every branch finishes (branches do not join).
      return [...(labels.length > 0 ? labels : ['Branch 1', 'Branch 2']), 'Done']
    }
    case 'user_task': {
      const outcomes = (config.outcomes ?? []) as unknown[]
      return outcomes.filter((o) => typeof o === 'string' && o) as string[]
    }
    default:
      return []
  }
}

/** True when the node renders labelled branch ports under its card. */
export function hasBranchPorts(node: Pick<WorkflowNode, 'type' | 'config'>): boolean {
  return getBranchLabels(node).length > 0
}

const norm = (s: string | undefined | null) => (s ?? '').toLowerCase().trim()

/** Labels that mark an error / failure path; those edges leave from the right side into the error lane. */
const ERROR_LABELS = new Set(['error', 'on error', 'onerror', 'failed', 'failure', 'on failure', 'catch', 'exception'])

export function isErrorEdge(edge: Pick<WorkflowEdge, 'label'>): boolean {
  return ERROR_LABELS.has(norm(edge.label))
}

/**
 * Index of the branch port an edge leaves from, or -1 when the edge does not
 * belong to a labelled port. Legacy condition labels "true"/"false" map to
 * the first branch / the else port.
 */
export function branchIndexOf(labels: string[], edge: Pick<WorkflowEdge, 'label'>, nodeType?: string): number {
  if (labels.length === 0) return -1
  const label = norm(edge.label)
  const idx = labels.findIndex((l) => norm(l) === label)
  if (idx >= 0) return idx
  if (nodeType === 'condition') {
    if (label === 'true' || label === 'yes') return 0
    if (label === 'false' || label === 'no') return labels.length - 1
  }
  if ((nodeType === 'loop' || nodeType === 'loop_n_times') && !label) return 1 // unlabelled loop edge = body
  return -1
}

const PORT_ORDER: Record<string, number> = { left: 0, bottom: 1, right: 2 }

/**
 * Outgoing edges of a node in a stable, readable order: by branch port
 * (config order) first, then by side port (left, bottom, right), error paths
 * last, then by creation order.
 */
export function orderOutgoingEdges(node: Pick<WorkflowNode, 'type' | 'config'>, outEdges: WorkflowEdge[]): WorkflowEdge[] {
  const labels = getBranchLabels(node)
  return outEdges
    .map((edge, i) => {
      const b = branchIndexOf(labels, edge, node.type)
      const port = PORT_ORDER[edge.sourcePort ?? 'bottom'] ?? 1
      const key = (isErrorEdge(edge) ? 1_000_000 : 0) + (b >= 0 ? b : 1000) * 10 + port
      return { edge, key, i }
    })
    .sort((a, b) => a.key - b.key || a.i - b.i)
    .map((x) => x.edge)
}
