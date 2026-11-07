import { readFileSync } from 'fs'
import path from 'path'
import { describe, expect, it } from 'vitest'
import {
  handleCollectionCount, handleCollectionFilter, handleCollectionGet, handleDateMath, handleFormatDate, handleJsonAddToArray,
  handleJsonBuildArray, handleJsonBuildObject, handleJsonGetValue, handleMath, handleModifyString, handleRegex, handleSwitchCase,
  handleTextTemplate,
} from '@/lib/workflow-node-handlers'

const read = (f: string) => readFileSync(path.join(process.cwd(), f), 'utf8')
const panel = read('components/automations/node-config-panel.tsx')
const engine = read('lib/workflow-engine.ts')
const handlers = read('lib/workflow-node-handlers.ts')

/**
 * Settings fields converted from plain text inputs to the expression input
 * (VariableInput: {{ }} pills + the Insert data picker). Each one is
 * resolved with resolveExpression at runtime — `resolved` is the source
 * snippet that proves it (engine or handler).
 */
const CONVERTED: Array<{ node: string; key: string; bind: RegExp; resolved: string | RegExp }> = [
  { node: 'update_task', key: 'value', bind: /updateConfig\("value", value\)/, resolved: 'resolveExpression(config.value' },
  { node: 'set_title', key: 'title', bind: /updateConfig\("title", value\)/, resolved: 'resolveExpression(config.title' },
  { node: 'set_sla', key: 'warning_amount', bind: /updateConfig\("warning_amount", value\)/, resolved: /slaAmount\(\s*config\.warning_amount/ },
  { node: 'set_sla', key: 'amount', bind: /updateConfig\("amount", value\)/, resolved: 'slaAmount(config.amount' },
  { node: 'set_sla', key: 'sla_label', bind: /updateConfig\("sla_label", value\)/, resolved: 'resolveExpression(config.sla_label' },
  { node: 'json_build_object', key: 'fields[].value', bind: /updated\[i\] = \{ \.\.\.updated\[i\], value: value \}\s*updateConfig\(\"fields\"/, resolved: 'resolveExpression(pair.value' },
  { node: 'json_build_array', key: 'items[].value', bind: /updated\[i\] = \{ value: value \}/, resolved: 'resolveExpression(raw, ctx)' },
  { node: 'json_add_to_array', key: 'fields[].value', bind: /updated\[i\] = \{ \.\.\.updated\[i\], value: value \}\s*updateConfig\(\"fields\"/, resolved: 'resolveExpression(f.value' },
  { node: 'json_get_value', key: 'source', bind: /updateConfig\("source", value\)/, resolved: 'resolveExpression(config.source' },
  { node: 'read_csv / read_excel / file_to_base64 / read_file', key: 'file_url', bind: /updateConfig\("file_url", value\)/, resolved: 'resolveExpression(config.file_url' },
  { node: 'read_csv / read_excel', key: 'data_var', bind: /updateConfig\("data_var", value\)/, resolved: 'resolveExpression(config.data_var' },
  { node: 'read_csv / read_excel / base64_to_file / download_file', key: 'file_name', bind: /updateConfig\("file_name", value\)/, resolved: 'resolveExpression(config.file_name' },
  { node: 'read_excel', key: 'sheet_name', bind: /updateConfig\("sheet_name", value\)/, resolved: 'resolveExpression(config.sheet_name' },
  { node: 'http_request / download_file', key: 'url', bind: /updateConfig\("url", value\)/, resolved: 'resolveExpression(config.url' },
  { node: 'http_request', key: 'auth_token', bind: /updateConfig\("auth_token", value\)/, resolved: 'resolveExpression(config.auth_token' },
  { node: 'http_request', key: 'auth_key_value', bind: /updateConfig\("auth_key_value", value\)/, resolved: 'resolveExpression(config.auth_key_value' },
  { node: 'http_request', key: 'auth_username', bind: /updateConfig\("auth_username", value\)/, resolved: 'resolveExpression(config.auth_username' },
  { node: 'http_request', key: 'headers[].value', bind: /updated\[i\] = \{ \.\.\.updated\[i\], value: value \}\s*updateConfig\(\"headers\"/, resolved: 'resolveExpression(h.value' },
  { node: 'http_request', key: 'params[].value', bind: /updated\[i\] = \{ \.\.\.updated\[i\], value: value \}\s*updateConfig\(\"params\"/, resolved: 'resolveExpression(p.value' },
  { node: 'http_request / send_email', key: 'body', bind: /updateConfig\("body", value\)/, resolved: 'resolveExpression(config.body' },
  { node: 'create_subtask', key: 'assignee', bind: /updateConfig\("assignee", value\)/, resolved: 'resolveExpression(config.assignee' },
  { node: 'auto_action', key: 'target_task_id', bind: /updateConfig\("target_task_id", value\)/, resolved: 'resolveExpression(config.target_task_id' },
  { node: 'auto_action', key: 'outcome', bind: /updateConfig\("outcome", value\)/, resolved: 'resolveExpression(config.outcome' },
  { node: 'auto_action', key: 'outcome_notes', bind: /updateConfig\("outcome_notes", value\)/, resolved: 'resolveExpression(config.outcome_notes' },
  { node: 'auto_action', key: 'assignee_id', bind: /updateConfig\("assignee_id", value\)/, resolved: 'resolveExpression(config.assignee_id' },
  { node: 'switch_case', key: 'field', bind: /updateConfig\("field", value\)/, resolved: 'resolveExpression(config.field' },
  { node: 'loop_n_times', key: 'count', bind: /updateConfig\("count", value\)/, resolved: 'resolveExpression(config.count' },
  { node: 'pause_until / format_date / date_math', key: 'date', bind: /updateConfig\("date", value\)/, resolved: 'resolveExpression(config.date' },
  { node: 'end_workflow', key: 'reason', bind: /updateConfig\("reason", value\)/, resolved: 'resolveExpression(config.reason' },
  { node: 'end_workflow (HTTP response)', key: 'response_status_code', bind: /updateConfig\("response_status_code", value\)/, resolved: 'resolveExpression(config.response_status_code' },
  { node: 'math', key: 'expression', bind: /updateConfig\("expression", value\)/, resolved: 'resolveExpression(config.expression' },
  { node: 'modify_string / regex', key: 'input', bind: /updateConfig\("input", value\)/, resolved: 'resolveExpression(config.input' },
  { node: 'modify_string', key: 'replace_with', bind: /updateConfig\("replace_with", value\)/, resolved: 'resolveExpression(config.replace_with' },
  { node: 'text_template', key: 'template', bind: /updateConfig\("template", value\)/, resolved: /const template = String\(config\.template[^\n]*\n\s*const result = resolveExpression\(template/ },
  { node: 'date_math', key: 'date2', bind: /updateConfig\("date2", value\)/, resolved: 'resolveExpression(config.date2' },
  { node: 'date_math', key: 'amount', bind: /updateConfig\("amount", value\)/, resolved: 'resolveExpression(config.amount' },
  { node: 'collection_*', key: 'collection', bind: /updateConfig\("collection", value\)/, resolved: 'resolveExpression(config.collection' },
  { node: 'collection_get', key: 'index', bind: /updateConfig\("index", value\)/, resolved: 'resolveExpression(config.index' },
  { node: 'collection_get', key: 'key', bind: /updateConfig\("key", value\)/, resolved: 'resolveExpression(config.key' },
  { node: 'collection_filter', key: 'value', bind: /updateConfig\("value", value\)/, resolved: 'resolveExpression(config.value' },
  { node: 'send_email', key: 'to', bind: /updateConfig\("to", value\)/, resolved: /resolveExpression\(\s*config\.to\b|config\.to[\s\S]{0,80}resolveExpression/ },
  { node: 'send_email', key: 'subject', bind: /updateConfig\("subject", value\)/, resolved: /resolveExpression\(\s*emailTemplate\?\.subject \?\? config\.subject/ },
  { node: 'send_email', key: 'from', bind: /updateConfig\("from", value\)/, resolved: 'resolveExpression(config.from' },
  { node: 'send_email', key: 'reply_to', bind: /updateConfig\("reply_to", value\)/, resolved: 'resolveExpression(config.reply_to' },
  { node: 'server_event', key: 'event_name', bind: /updateConfig\("event_name", value\)/, resolved: 'resolveExpression(config.event_name' },
  { node: 'server_event', key: 'custom_payload', bind: /updateConfig\('custom_payload', value\)/, resolved: 'config.custom_payload' },
  { node: 'base64_to_file', key: 'base64_data', bind: /updateConfig\("base64_data", value\)/, resolved: 'resolveExpression(config.base64_data' },
  { node: 'user_task', key: 'task_description', bind: /onUpdate\('task_description', value\)/, resolved: /resolveTaskDescriptionTemplate\(\s*config\.task_description/ },
  { node: 'update_data_table', key: 'data', bind: /onUpdate\("data", value\)/, resolved: 'resolveExpression(config.data' },
  { node: 'run_agent', key: 'user_message', bind: /onUpdate\("user_message", value\)/, resolved: 'resolveExpression(config.user_message' },
]

describe('converted settings fields use the expression input and resolve at runtime', () => {
  it.each(CONVERTED.map((c) => [`${c.node}.${c.key}`, c] as const))('%s', (_name, c) => {
    // Bound through VariableInput (its onChange hands the value, not an event).
    expect(panel).toMatch(c.bind)
    // Resolved with resolveExpression (or the task description template) at runtime.
    const runtime = engine + handlers
    if (typeof c.resolved === 'string') expect(runtime).toContain(c.resolved)
    else expect(runtime).toMatch(c.resolved)
  })

  it('no plain input is left bound to these keys with e.target.value', () => {
    for (const key of ['title', 'sla_label', 'source', 'file_url', 'data_var', 'file_name', 'sheet_name', 'url', 'auth_token', 'assignee', 'target_task_id',
      'outcome', 'outcome_notes', 'assignee_id', 'count', 'date', 'date2', 'reason', 'response_status_code', 'expression', 'input', 'template',
      'collection', 'index', 'to', 'subject', 'from', 'reply_to', 'event_name', 'base64_data', 'user_message']) {
      expect(panel, key).not.toMatch(new RegExp(`(updateConfig|onUpdate)\\(["']${key}["'], e\\.target\\.value\\)`))
    }
    expect(panel).not.toMatch(/onUpdate\('task_description', e\.target\.value\)/)
    // modify_string's replace_with resolves; regex's is used verbatim (capture groups like $1), so stays plain.
    expect(panel.match(/updateConfig\("replace_with", value\)/g)?.length).toBe(1)
    expect(handlers).toContain('String(config.replace_with ?? "")')
  })

  it('fields that are not resolved at runtime stay plain (names, keys, patterns)', () => {
    // Variable names, JSON paths, header names and regex patterns are used verbatim.
    for (const key of ['output_var', 'source_var', 'array_var', 'path', 'pattern', 'item_var', 'index_var']) {
      expect(panel, key).toMatch(new RegExp(`updateConfig\\("${key}", e\\.target\\.value\\)`))
    }
    expect(handlers).toContain("const pattern = config.pattern ?? \"\"")
    // The basic-auth password stays a masked input.
    expect(panel).toMatch(/updateConfig\("auth_password", e\.target\.value\)[^\n]*\n[^\n]*type="password"/)
    expect(handlers).toContain("const path = config.path ?? ''")
  })
})

describe('the converted handler fields resolve {{ }} expressions', () => {
  const ctx = () => ({ variables: { n: 6, name: 'Ada Lovelace', when: '2026-03-01T00:00:00.000Z', days: 2, items: [{ id: 'a', v: 1 }, { id: 'b', v: 2 }], idx: 1, key: 'b', obj: { a: { b: 7 } }, field: 'high' } as Record<string, unknown>, task: {} }) as never
  const rule = (f: string, op: string, v: string) => (op === 'equals' ? f === v : false)

  it('math, modify_string, text_template, regex', () => {
    expect(handleMath({ expression: '{{n}} * 2' }, ctx()).result).toBe(12)
    expect(handleModifyString({ input: '{{name}}', operation: 'uppercase' }, ctx()).result).toBe('ADA LOVELACE')
    expect(handleModifyString({ input: 'Hi X', operation: 'replace', search: 'X', replace_with: '{{name}}' }, ctx()).result).toBe('Hi Ada Lovelace')
    expect(handleTextTemplate({ template: 'Hello {{name}}' }, ctx()).result).toBe('Hello Ada Lovelace')
    expect(handleRegex({ input: '{{name}}', pattern: 'Ada', operation: 'test' }, ctx()).result).toBe(true)
  })

  it('dates', () => {
    expect(handleFormatDate({ date: '{{when}}', format: 'YYYY-MM-DD' }, ctx()).result).toContain('2026')
    const r = handleDateMath({ date: '{{when}}', operation: 'add', amount: '{{days}}', unit: 'days' }, ctx())
    expect(String(r.result)).toContain('2026-03-03')
  })

  it('collections and JSON', () => {
    expect(handleCollectionCount({ collection: '{{items}}' }, ctx()).count).toBe(2)
    expect(handleCollectionGet({ collection: '{{items}}', index: '{{idx}}' }, ctx()).result).toEqual({ id: 'b', v: 2 })
    expect(handleCollectionGet({ collection: '{{items}}', by: 'key', key_field: 'id', key: '{{key}}' }, ctx()).result).toEqual({ id: 'b', v: 2 })
    expect(handleCollectionFilter({ collection: '{{items}}', field: 'id', operator: 'equals', value: '{{key}}' }, ctx(), rule).count).toBe(1)
    const c1 = ctx(); handleJsonBuildObject({ fields: [{ key: 'who', value: '{{name}}' }] }, c1)
    expect((c1 as unknown as { variables: Record<string, unknown> }).variables.json_object).toEqual({ who: 'Ada Lovelace' })
    const c2 = ctx(); handleJsonBuildArray({ items: [{ value: '{{n}}' }] }, c2)
    expect((c2 as unknown as { variables: Record<string, unknown> }).variables.json_array).toEqual([6])
    const c3 = ctx(); handleJsonAddToArray({ array_var: 'out', fields: [{ key: 'who', value: '{{name}}' }] }, c3)
    expect((c3 as unknown as { variables: Record<string, unknown> }).variables.out).toEqual([{ who: 'Ada Lovelace' }])
    const c4 = ctx(); handleJsonGetValue({ source: '{{obj}}', path: 'a.b' }, c4)
    expect((c4 as unknown as { variables: Record<string, unknown> }).variables.json_value).toBe(7)
    expect(handleSwitchCase({ field: '{{field}}', cases: ['low', 'high'] }, ctx()).matched_case).toBe('high')
  })
})
