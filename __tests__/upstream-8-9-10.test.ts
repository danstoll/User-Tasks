/**
 * homepresso/User-Tasks #8 (a signing key without Supabase), #9 (parallel
 * branches and the Done port) and #10 (the example workflows run as written).
 */
import { readFileSync, readdirSync } from 'node:fs'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const createClientMock = vi.hoisted(() => vi.fn())
vi.mock('@supabase/supabase-js', () => ({ createClient: createClientMock }))
vi.mock('@/lib/audit-log', () => ({ writeAudit: vi.fn(async () => {}) }))

import { serverSigningKey } from '@/lib/server-signing-key'
import { signResume } from '@/lib/workflow-resume-queue'
import { executeWorkflow } from '@/lib/workflow-engine'
import { getBranchLabels } from '@/lib/workflow-branches'
import { handleFormatDate, handleJsonBuildArray, handleModifyString } from '@/lib/workflow-node-handlers'
import { createFakeSupabase } from './helpers/fake-supabase'

/* eslint-disable @typescript-eslint/no-explicit-any -- node configs / fake rows */

describe('#8 serverSigningKey', () => {
  afterEach(() => vi.unstubAllEnvs())

  const target = { tenantId: 't', workflowId: 'w', runId: 'r', eventId: 'e', payload: '{}' }

  it('uses SUPABASE_SERVICE_ROLE_KEY when it is set', () => {
    vi.stubEnv('SUPABASE_SERVICE_ROLE_KEY', 'service-key')
    vi.stubEnv('ENCRYPTION_KEY', 'enc-key')
    expect(serverSigningKey()).toBe('service-key')
  })

  it('without Supabase, derives a key from ENCRYPTION_KEY that is not the key itself', () => {
    vi.stubEnv('SUPABASE_SERVICE_ROLE_KEY', '')
    vi.stubEnv('ENCRYPTION_KEY', 'enc-key')
    const key = serverSigningKey()
    expect(key).toMatch(/^[0-9a-f]{64}$/)
    expect(key).not.toBe('enc-key')
    expect(signResume(target)).toMatch(/^[0-9a-f]{64}$/)
  })

  it('never uses CRON_SECRET (schedulers hold it), and is undefined without a key', () => {
    vi.stubEnv('SUPABASE_SERVICE_ROLE_KEY', '')
    vi.stubEnv('ENCRYPTION_KEY', '')
    vi.stubEnv('CRON_SECRET', 'cron')
    expect(serverSigningKey()).toBeUndefined()
    expect(() => signResume(target)).toThrow(/ENCRYPTION_KEY/)
  })
})

const TENANT = '0fb00000-0000-4000-8000-00000000000a'
const WF = '0fb00000-0000-4000-8000-0000000000a1'
const OWNER = '0fb00000-0000-4000-8000-0000000000b1'

function run(def: { trigger_type?: string; nodes: any[]; edges: any[] }, variables: Record<string, any> = {}) {
  const fake = createFakeSupabase({
    workflows: [{
      id: WF, tenant_id: TENANT, name: 'T', is_active: true, trigger_type: def.trigger_type ?? 'manual', run_count: 0,
      published_version_id: null, user_variables: [], created_by: OWNER, nodes: def.nodes, edges: def.edges,
    }],
    workflow_runs: [], workflow_queue: [], tasks: [],
  })
  fake.rpcs.record_workflow_run = () => ({})
  createClientMock.mockImplementation(() => fake.client)
  return executeWorkflow(WF, undefined, false, variables, undefined, undefined, undefined, { supabase: fake.client })
}

describe('engine', () => {
  beforeEach(() => {
    vi.stubEnv('SUPABASE_SERVICE_ROLE_KEY', 'test-service-role-key')
    vi.stubEnv('NEXT_PUBLIC_SUPABASE_URL', 'http://127.0.0.1:54321')
    vi.stubGlobal('fetch', vi.fn(async () => new Response('{}')))
  })
  afterEach(() => { vi.unstubAllGlobals(); vi.unstubAllEnvs() })

  const parallelFlow = (branches: unknown[]) => ({
    nodes: [
      { id: 'start', type: 'manual', label: 'Start', config: {} },
      { id: 'p', type: 'parallel', label: 'P', config: { mode: 'parallel', branches } },
      { id: 'a', type: 'set_variable', label: 'A', config: { name: 'a', expression: '1' } },
      { id: 'b', type: 'set_variable', label: 'B', config: { name: 'b', expression: '2' } },
      { id: 'after', type: 'log', label: 'After', config: { message: 'a={{a}} b={{b}}' } },
    ],
    edges: [
      { id: 'e1', source: 'start', target: 'p' },
      { id: 'e2', source: 'p', target: 'a', label: 'A' },
      { id: 'e3', source: 'p', target: 'b', label: 'B' },
      { id: 'e4', source: 'p', target: 'after', label: 'Done' },
    ],
  })

  it('#10 the manual trigger is a trigger, not an unknown node', async () => {
    const r = await run(parallelFlow([{ label: 'A', order: 0 }, { label: 'B', order: 1 }]))
    expect(r.node_results.start?.output).toMatchObject({ triggered: true })
  })

  it('#9 a Done edge runs once, after every branch, and sees both results', async () => {
    const r = await run(parallelFlow([{ label: 'A', order: 0 }, { label: 'B', order: 1 }]))
    expect(r.error).toBeUndefined()
    expect(r.node_results.after?.output?.logged).toBe('a=1 b=2')
  })

  it('#9 plain-string branches run (the editor accepts them)', async () => {
    const r = await run(parallelFlow(['A', 'B']))
    expect(r.error).toBeUndefined()
    expect(r.node_results.p?.output?.branches).toEqual(['A', 'B'])
    expect(r.node_results.after?.output?.logged).toBe('a=1 b=2')
  })

  it('#9 the editor shows a Done port after the branches', () => {
    expect(getBranchLabels({ type: 'parallel', config: { branches: [{ label: 'X' }] } } as any)).toEqual(['X', 'Done'])
  })

  it('#9 a branch named Done gets no second Done port', () => {
    expect(getBranchLabels({ type: 'parallel', config: { branches: ['Review', 'Done'] } } as any)).toEqual(['Review', 'Done'])
  })

  it('#9 the Done step runs again with every result, when a branch reached it first', async () => {
    const flow = parallelFlow([{ label: 'A', order: 0 }, { label: 'B', order: 1 }])
    flow.edges.push({ id: 'e5', source: 'a', target: 'after' })
    const r = await run(flow)
    expect(r.error).toBeUndefined()
    expect(r.node_results.after?.output?.logged).toBe('a=1 b=2')
  })

  it('#10 handler review fixes: repeated tokens, inherited keys, objects without value', () => {
    const vars: Record<string, any> = {}
    const ctx: any = { variables: vars }
    const d = handleFormatDate({ date: '2026-11-07T10:00:00', format: 'DD/MM/YYYY - DD' }, ctx)
    expect(d.result).toBe('07/11/2026 - 07')
    expect(handleModifyString({ input: 'ab', operation: 'constructor' }, ctx).operation).toBe('constructor')
    expect(handleModifyString({ input: 'ab', operation: 'upper' }, ctx).result).toBe('AB')
    expect(handleJsonBuildArray({ items: [{ value: 'a' }, { label: 'x' }, 'b'] }, ctx).count).toBe(2)
    expect(vars.json_array).toEqual(['a', 'b'])
  })

  // Files that need data tables, a decision table or a sub-workflow are left out.
  const EXAMPLES = readdirSync(join(__dirname, '..', 'test-workflows'))
    .filter((f) => /^(0\d|1[0-4])-.*\.json$/.test(f))

  it.each(EXAMPLES)('#10 %s runs with no unknown node and no error', async (file) => {
    const def = JSON.parse(readFileSync(join(__dirname, '..', 'test-workflows', file), 'utf8'))
    const r = await run({ trigger_type: 'webhook_received', nodes: def.nodes, edges: def.edges }, { _raw: 'test' })
    expect(r.error, file).toBeUndefined()
    for (const [id, result] of Object.entries<any>(r.node_results)) {
      expect(String(result?.output?.reason ?? ''), `${file} ${id}`).not.toMatch(/Unknown node type/)
      expect(result?.status, `${file} ${id}`).not.toBe('error')
    }
  })

  it('#10 examples 02, 03, 06 and 09 give the documented results', async () => {
    const load = (f: string) => JSON.parse(readFileSync(join(__dirname, '..', 'test-workflows', f), 'utf8'))
    const r3 = await run({ ...load('03-loops-and-collections.json'), trigger_type: 'webhook_received' })
    expect(r3.node_results.n2?.output?.count).toBe(5)
    const r6 = await run({ ...load('06-date-math-formatting.json'), trigger_type: 'webhook_received' })
    const today = Object.values<any>(r6.node_results).find((n) => n?.output?.variable === 'today')
    expect(today?.output?.result).toMatch(/^\d{4}-\d{2}-\d{2}$/)
    const r2 = await run({ ...load('02-string-operations.json'), trigger_type: 'webhook_received' }, { full_name: 'ada lovelace' })
    const upper = Object.values<any>(r2.node_results).find((n) => n?.output?.operation === 'uppercase')
    expect(upper).toBeDefined()
    const r9 = await run({ ...load('09-loop-n-times-parallel.json'), trigger_type: 'webhook_received' })
    const delay = Object.values<any>(r9.node_results).find((n) => n?.output?.waited_ms !== undefined)
    expect(delay?.output?.waited_ms).toBeGreaterThanOrEqual(900)
  }, 20_000)
})
