import type { NodeType } from '@/lib/workflow-schema'

/**
 * Hand-written notes for the generated node reference (/docs/workflows/nodes).
 * Labels, categories and descriptions come from NODE_CATALOG, required
 * settings from validateNode() and the canvas summary from
 * getNodeConfigSummary(), so only the parts the code cannot tell us live
 * here. Typed as Record<NodeType, …> and covered by __tests__/docs-reference.test.ts,
 * so a new node type fails the build until it is documented.
 */
export interface NodeDoc {
  /** Main settings, in prose (config keys in backticks). */
  settings: string
  /** Variables the node sets for later nodes. */
  outputs?: string
  /** Representative `config` object, shown as JSON with its canvas summary. */
  example?: Record<string, unknown>
}

const TASK_TRIGGER = 'Optional filters narrow which tasks start a run. The task is available as `{{task.*}}` (title, status, priority, assignee) and its fields as `{{task_data.*}}`. An optional **Task form** (`fields`, designed in the form designer or linked from the Forms library) declares the fields the task carries, so they appear in the data picker as `{{task.data.<key>}}`.'

export const NODE_DOCS: Record<NodeType, NodeDoc> = {
  // ─── Triggers ───
  task_created: { settings: TASK_TRIGGER, outputs: 'Trigger variables for the task: `task_id`, `task.*`, `task_data.*`.' },
  task_received: { settings: `Fires when a task lands in a queue or inbox. ${TASK_TRIGGER}`, outputs: 'Task trigger variables.' },
  task_submitted: { settings: `Fires when a queued task is accepted. ${TASK_TRIGGER}`, outputs: 'Task trigger variables.' },
  task_updated: { settings: `Fires on task edits (status, priority, assignee, fields). ${TASK_TRIGGER}`, outputs: 'Task trigger variables plus the changed values.' },
  task_completed: { settings: `Fires when a task is completed. ${TASK_TRIGGER}`, outputs: 'Task trigger variables, `outcome` and `outcome_notes`.' },
  task_rejected: { settings: `Fires when a task is rejected from the queue. ${TASK_TRIGGER}`, outputs: 'Task trigger variables.' },
  workflow_error: {
    settings: '`error_scope` chooses any workflow or specific `workflow_ids`. Use it to build one central error handler that notifies, logs or retries.',
    outputs: '`error_workflow_id`, `error_workflow_name`, `error_run_id`, `error_message`, `error_node`, `error_timestamp`.',
    example: { error_scope: 'specific', workflow_ids: ['<workflow-id>'] },
  },
  workflow_sla: {
    settings: '`workflow_scope` / `workflow_ids` choose the workflows; `event_types` picks warning, breach or both.',
    outputs: 'The run, workflow, SLA label, due time and SLA status.',
  },
  retention_policy_review: {
    settings: '`policy_scope` / `policy_ids` choose which retention policies start a review run.',
    outputs: 'Policy id and name, the asset under review and its due date.',
  },
  retention_policy_executed: {
    settings: '`policy_scope` / `policy_ids` choose the policies; `action_types` filters purge, archive or other actions.',
    outputs: 'Policy, action type and affected asset.',
  },
  data_table_event: {
    settings: '`table_id` picks the data table (any table when empty) and `event_types` the row events (`created`, `updated`, `deleted`; all when empty).',
    outputs: '`data_event_type`, `data_table_id`, `data_table_name`, `row_id`, `row` (the row values) and one variable per column.',
    example: { table_id: '<table-id>', event_types: ['created', 'updated'] },
  },
  form_submitted: {
    settings: '`form_id` picks a reusable form (from the Forms library); `button_ids` optionally limits it to some buttons or outcomes (any when empty). Fires for every submission — the form\'s own page (`/f/<id>`), `POST /api/v1/forms/{id}/submit`, MCP `forms_submit`, and a User Task whose form is linked to it. A cached copy of the form\'s fields (`fields`) types the payload in the data picker — see /docs/tasks/forms.',
    outputs: '`form.<key>` for every field (typed; `form.<key>_item` for lookups), `form_id`, `form_name`, `form_version`, `form_submission_id`, `form_button`, `form_outcome`, `form_source` (standalone, api, task), `submitted_by`, `submitted_by_name`, `submitted_by_email`, `submitted_at` and `task_id` for task forms.',
    example: { form_id: '<form-id>', form_name: 'Customer complaint', button_ids: ['submit'] },
  },
  middleware_received: {
    settings: 'Receives an inbound webhook and holds the HTTP response open until the run finishes, so the workflow can validate, enrich or route a request synchronously. Keep these runs short.',
    outputs: 'The request body (`_raw` and mapped fields); the final variables become the response.',
  },
  schedule: {
    settings: '`schedule_mode` is `every_minutes` (`interval_minutes`), `hourly` (`minute_of_hour`), `daily` / `weekly` / `monthly` (`time_of_day`, `day_of_week`, `day_of_month`) or `once` (`run_at`), with an IANA `timezone`. Each occurrence fires exactly once; a missed slot catches up only the most recent occurrence.',
    outputs: '`scheduled_at` and the workflow\'s variables.',
    example: { schedule_mode: 'daily', time_of_day: '06:30', timezone: 'Europe/London' },
  },
  webhook_received: {
    settings: 'Create an inbound webhook for the workflow (Settings → Webhooks or `usertasks webhooks create <workflow-id>`); callers POST JSON to `/api/webhooks/inbound?hook=<slug>` with the webhook\'s secret as a Bearer token. A payload mapping can rename fields into variables.',
    outputs: '`_raw` (the full body) and one variable per mapped field.',
  },

  // ─── Logic & flow ───
  condition: {
    settings: '`branches` is an ordered list of if / else-if branches, each with rules (`field`, `operator`, `value`) joined by AND/OR; `has_else` adds an Else path. Connect one edge per branch label. Operators: `equals`, `not_equals`, `contains`, `not_contains`, `starts_with`, `ends_with`, `in` (comma-separated list), `matches` (regular expression), `gt`, `gte`, `lt`, `lte` (also `greater_than`, `>=`, …), `is_empty`, `is_not_empty`, `is_true`, `is_false`. `is_true` / `is_false` need no value: `true`, `"true"`, `1`, `"1"`, `"yes"` and `"on"` are true; `false`, `"false"`, `0`, `"0"`, `""`, `"no"`, `"off"` and a missing value are false.',
    example: { branches: [{ label: 'Large', logic: 'and', rules: [{ field: '{{amount}}', operator: 'greater_than', value: '10000' }] }], has_else: true },
  },
  switch_case: {
    settings: '`field` is the value to test, `cases` the values (one outgoing edge each), `has_default` adds a fallback path.',
    example: { field: '{{region}}', cases: [{ value: 'EU' }, { value: 'US' }], has_default: true },
  },
  loop: {
    settings: '`items` is the collection to iterate (the field suggests every list available at that point, including a form\'s Attachments field — `{{task_data.<key>}}` / `{{form.<key>}}`); `item_var` (default `item`) and `index_var` (default `index`) name the loop variables. Nodes connected to the loop body run once per item, and the data picker shows the item\'s fields there as Current item (for files: `{{item.name}}`, `{{item.size}}`, `{{item.mime_type}}`, `{{item.url}}`).',
    outputs: '`{{item}}` and `{{index}}` inside the body.',
    example: { items: '{{vendors}}', item_var: 'vendor' },
  },
  loop_n_times: {
    settings: '`count` repetitions with `index_var` (default `i`).',
    outputs: '`{{i}}` inside the body.',
    example: { count: 3 },
  },
  delay: {
    settings: '`duration` and `unit` (`seconds`, `minutes`, `hours`, `days`). Waits of up to 30 seconds happen inline; longer waits pause the run (status `waiting_for_event`) and a queued `delay_resume` job continues it.',
    outputs: '`delay_ms`, `resume_at`.',
    example: { duration: 2, unit: 'hours' },
  },
  pause_until: {
    settings: '`date` is an ISO date/time or expression. The run pauses and resumes from the queue at that time.',
    example: { date: '{{=ADD_DAYS(NOW(), 1)}}' },
  },
  end_workflow: { settings: 'Stops the run immediately with status completed. Optional `reason` is written to the run log.' },
  goto_activity: {
    settings: '`target_node_id` is the activity to jump to; `input_overrides` can reset variables first. Useful for rework loops (send back to the requester, then re-approve).',
    example: { target_node_id: '<node-id>', target_node_label: 'Manager approval' },
  },
  parallel: {
    settings: '`branches` (two or more) and `mode`: `parallel` runs every branch at once, `sequential` one after another. Branches do not join: when two branches reach the same step, it runs once, for the first branch. To continue after every branch has finished, connect the `Done` port (an edge labelled `Done`).',
    example: { mode: 'parallel', branches: [{ label: 'Notify finance' }, { label: 'Update ERP' }] },
  },

  // ─── Operations ───
  set_title: { settings: '`title` sets the run\'s display title (shown in run lists, reports and the task tracker). Expressions allowed.', example: { title: 'Invoice {{invoice_number}} — {{vendor.name}}' } },
  set_sla: {
    settings: '`mode` `relative` (`amount` + `unit`) or `exact` (`due_at`). `amount` and `warning_amount` accept a number or an expression (`{{sla_days}}`, `{{=ADD(base_days, 2)}}`), resolved when the node runs; a non-numeric or non-positive amount fails the node with a message naming the value; optional warning threshold (`warning_amount`/`warning_unit` or `warning_at`) and `sla_label`. SLA warnings and breaches fire `workflow_sla` triggers and appear in reports and the event stream.',
    example: { sla_label: 'Invoice approval', mode: 'relative', amount: 3, unit: 'days', warning_amount: 1, warning_unit: 'days' },
  },
  set_variable: { settings: '`name` and `expression`. The value is resolved with the full expression syntax, so it can be a literal, `{{variable}}` or `{{=FUNCTION()}}`.', example: { name: 'needs_cfo', expression: '{{=IF(amount > 50000, true, false)}}' } },
  math: { settings: '`expression` is evaluated as arithmetic over variables (`+ - * / %`, parentheses); `output_var` (default `result`).', outputs: 'The number in `output_var`.', example: { expression: '{{subtotal}} * 1.2', output_var: 'total' } },
  modify_string: {
    settings: '`input` and `operation`: `uppercase`, `lowercase`, `trim`, `trim_start`, `trim_end`, `replace` (`search`, `replace_with`, `replace_all`), `substring` (`start`, `end`), `pad_start` / `pad_end` (`length`, `pad_char`), `reverse` or `slug`.',
    outputs: '`output_var`.',
    example: { input: '{{vendor.name}}', operation: 'slug', output_var: 'vendor_slug' },
  },
  text_template: { settings: '`template` with `{{variables}}` and inline functions; `output_var` (default `text`).', example: { template: 'Invoice {{invoice_number}} for {{=FORMAT_NUMBER(amount, 2)}} {{currency}}', output_var: 'summary' } },
  format_date: { settings: '`date` (default now) and `format`: `iso`, `date`, `time`, `datetime`, `short`, `relative` or `custom` with `custom_format` (`YYYY-MM-DD HH:mm:ss` tokens); `output_var` (default `formatted_date`).', example: { date: '{{due_date}}', format: 'custom', custom_format: 'DD/MM/YYYY' } },
  date_math: { settings: '`operation` `add` / `subtract` (`amount`, `unit` minutes … years) or `interval` between `date` and `date2` (returns ms, seconds, minutes, hours, days); `output_var`.', example: { date: '{{received_at}}', operation: 'add', amount: 30, unit: 'days', output_var: 'pay_by' } },
  regex: { settings: '`input`, `pattern`, `flags` (default `g`) and `operation`: `match`, `test`, `replace` (`replace_with`) or `split`. Patterns are checked for catastrophic backtracking before they run.', example: { input: '{{reference}}', pattern: 'INV-(\\d+)', operation: 'match', output_var: 'invoice_match' } },
  json_build_object: { settings: '`fields` (key/value pairs, values may be expressions); `source_var` merges into an existing object; `output_var` (default `json_object`).', example: { fields: [{ key: 'vendor', value: '{{vendor.id}}' }, { key: 'amount', value: '{{amount}}' }], output_var: 'payment' } },
  json_build_array: { settings: '`items` builds a new array, or `source_var` pushes into an existing one; `output_var`.', example: { items: ['{{a}}', '{{b}}'], output_var: 'pair' } },
  json_add_to_array: { settings: '`array_var` is the array variable to append to; `fields` builds the object to push (or `value`).', example: { array_var: 'lines', fields: [{ key: 'sku', value: '{{item.sku}}' }] } },
  json_get_value: { settings: '`source` object or array and a dotted `path` (`items.0.price`); `output_var` (default `json_value`).', example: { source: '{{response}}', path: 'data.vendor.risk', output_var: 'risk' } },
  check_permission: { settings: '`user_id` and `permission` (a key from the permissions table). Branch on the result.', outputs: '`has_permission`, `permissions`.', example: { user_id: '{{task.assignee_id}}', permission: 'tasks.complete' } },

  // ─── Collections ───
  collection_count: { settings: '`collection`; `output_var` (default `count`).', example: { collection: '{{lines}}', output_var: 'line_count' } },
  collection_get: { settings: '`collection` and `by`: `index` (`index`), `last`, or `key` (`key_field` + `key`); `output_var`.', example: { collection: '{{vendors}}', by: 'key', key_field: 'id', key: '{{vendor_id}}', output_var: 'vendor' } },
  collection_filter: { settings: '`collection`, item `field`, `operator` and `value`; `output_var`.', example: { collection: '{{lines}}', field: 'amount', operator: 'greater_than', value: '0', output_var: 'billable' } },
  collection_sort: { settings: '`collection`, `field` and `direction` (`asc`/`desc`); `output_var`.', example: { collection: '{{lines}}', field: 'amount', direction: 'desc' } },
  collection_unique: { settings: '`collection` and optional `field` to deduplicate on; `output_var`.', example: { collection: '{{emails}}' } },
  collection_join: { settings: '`collection`, `separator` and optional `field` to join; `output_var`.', example: { collection: '{{lines}}', field: 'sku', separator: ', ' } },

  // ─── Actions ───
  update_task: { settings: '`field` (one of the workflow-updatable task fields such as `status`, `priority`, `due_date`, `assignee_id`, `title`) and `value`.', example: { field: 'priority', value: 'high' } },
  update_data_table: {
    settings: '`table_id` and `operation`: `insert`, `get`, `update`, `delete`, `list` (with `filters`, `sort_column`, `limit`, `offset`) or `set_retention`. Rows are matched by `row_id` or `match_column` + `match_value`; `column_values` / `field_mappings` supply values. Output variable names are configurable (`output_row_var`, `output_rows_var`, `output_count_var`, `output_row_id_var`). `get` writes the row to `output_row_var` (`{{row.status}}`) and each column as `{{<prefix>_<column>}}` (prefix: the output prefix or the node label in snake case). Columns are also copied to top-level variables for older workflows, except where the name is a workflow input or a run variable (`task_id`, `workflow_id`, `outcome`, `item`, …) — those are left unchanged and a warning is logged. Prefer `{{row.<column>}}`.',
    example: { table_id: '<vendors-table-id>', table_name: 'Vendors', operation: 'get', match_column: 'vendor_id', match_value: '{{vendor_id}}' },
  },
  retention_policies: { settings: '`operation`: `list_policies`, `create_policy` (`name`, `policy_mode`, `duration_value`/`duration_unit` or `exact_date`, `applies_to`), `delete_policy`, `list_assets` or `assign_policy` (`target_type` data_table / data_row / role, `target_id`).', outputs: '`policies`, `policy`, `assets`, counts.' },
  call_reference: {
    settings: '`reference_id` picks an integration reference (a saved REST/OData/database operation). `input_mappings` fill its inputs from variables; `output_mappings` copy response fields into variables and `custom_outputs` (`[{ name, path }]`, added with + in the response model tree) read any path of the response. `_ref_response_schema` is a snapshot of the reference\'s response model for the data picker. A file input (the `content` of a storage connector upload — S3, Azure Blob, SharePoint, OneDrive, Box, Google Drive) takes a file object such as `{{item}}` from a Loop over an Attachments field: its bytes and type are sent (multipart for Box and Google Drive), read for the workflow\'s workspace only. Calls are logged with redacted headers and appear in the event stream as `integration.call`. An HTTP error status (4xx/5xx) does not fail the step: the response and status are stored so a Condition can branch on them (for example `{{status}}` `gte` `400`). Only a transport failure (DNS, refused connection, timeout) fails the step, which then follows the node\'s *On error* setting.',
    outputs: '`response`, `status` (the HTTP status code; also `<prefix>_response` / `<prefix>_status`), mapped outputs, custom outputs, and file fields for binary responses. Pick fields with the data picker — see Reading values from an API response.',
    example: { reference_id: '<reference-id>', reference_name: 'Get vendor', reference_method: 'GET', input_mappings: { vendor_id: '{{vendor_id}}' } },
  },
  http_request: {
    settings: '`method`, `url`, `headers`, `params`, `body`, `content_type` (`json`, `form`, `text`, `none`, `multipart` — `multipart` parts `[{ key, value, type: "text" | "file" }]`, a file part sends a file object or each file of a list such as `{{item}}` / `{{task_data.<attachments>}}` — or `binary`, the body is one file\'s bytes) and `auth_type` (`bearer`, `basic`, `api_key`). `_response_schema` (captured from a pasted sample under Response model) makes the response pickable in later steps. Prefer an integration reference for anything reused: references keep credentials encrypted and calls governed. Like Call API / Reference, an HTTP error status (4xx/5xx) is stored rather than failing the step — branch on `{{<prefix>_status}}`; only a transport failure or timeout fails it.',
    outputs: '`response` (or `output_var`) and `<prefix>_status` (the HTTP status code), plus `file` / `file_url` for binary responses.',
    example: { method: 'POST', url: 'https://hooks.example.com/notify', body: '{"invoice":"{{invoice_number}}"}' },
  },
  custom_activity: {
    settings: '`custom_activity_id` picks an active custom workflow step; `input_mappings` fill its typed inputs. Code runs only when the server sets `ENABLE_CUSTOM_ACTIVITY_CODE=true`.',
    outputs: 'The step\'s declared outputs, prefixed with the node\'s output prefix.',
  },
  create_subtask: { settings: '`title`, optional `assignee` and `parent_task_id` (defaults to the workflow\'s task).', example: { title: 'Collect W-9 from {{vendor.name}}', assignee: '{{requester_id}}' } },
  auto_action: { settings: 'Set the task\'s `status`, `outcome` (+ `outcome_notes`), `acceptance_status`, `priority` or `assignee_id` without a person acting; `target_task_id` defaults to the current task.', example: { status: 'completed', outcome: 'Auto-approved' } },
  log: { settings: '`message` (expressions allowed) is written to the run log and the event stream as `workflow.step.log`.', example: { message: 'Vendor {{vendor.id}} risk {{risk}}' } },
  send_email: {
    settings: '`to` (comma-separated, expressions allowed), `subject` and `body`, or an `email_template_id`; optional `from`, `reply_to` and `attachments` — a file or a list of files: a form\'s Attachments field (`{{task_data.<key>}}` attaches every file), `{{item}}` inside a Loop over them (one), or a generated document; several expressions separated by commas are all attached (up to 20 files, 25 MB). Sent through the configured email provider (Resend by default).',
    outputs: '`sent`, `email_id`, `to` and, with attachments, their names and sizes (never contents).',
    example: { to: '{{requester_email}}', email_template_id: '<template-id>', attachments: '{{task_data.receipts}}' },
  },
  call_sub_workflow: {
    settings: '`sub_workflow_id` and `input_mappings` (variables passed to the child). By default the parent waits for the child to finish (`executionMode: "sync"`); set the node to async to fire and forget.',
    outputs: '`status`, `results` (the child\'s variables) and `error`.',
    example: { sub_workflow_id: '<workflow-id>', sub_workflow_name: 'Vendor onboarding', input_mappings: { vendor_id: '{{vendor_id}}' } },
  },
  decision_table: {
    settings: '`decision_table_id` and `input_mappings` (one per table input). Rules are evaluated in order; the first enabled match wins.',
    outputs: '`matched`, `rule_id`, `rule_label` and `outputs` (each output column), also prefixed with the node\'s output prefix.',
    example: { decision_table_id: '<table-id>', decision_table_name: 'Invoice approval policy', input_mappings: { amount: '{{amount}}', risk: '{{risk}}' } },
  },
  user_task: {
    settings: '`task_title`, `task_description`, `outcomes` (each outcome is an outgoing edge label), a form: typed `fields` (text, long text, number, currency, email, phone, URL, date, date-time, checkbox, single and multi-select, person, autocomplete / lookup from a data table or API reference, attachments, heading, divider) with validation and `visible_when`, an optional grid `form_layout` and `form_theme`, or a linked reusable form (`form_id`; drafts follow its latest version, publishing pins `form_version`) — see /docs/tasks/forms; assignment (`assignment_type` individual or team, `assignee_id` or `assignee_variable`, `team_id`), `completion_rule` for multi-assignee tasks (first_response, all_complete, consensus, unanimous, custom), due date, `reminders`, escalation and redirect options, and an optional `notification_template_id`.',
    outputs: 'While waiting: `user_task_id`, `user_task_title`, `user_task_description` (plain text) and `user_task_description_html`. On resume: `outcome`, `outcome_notes`, `actioned_by` / `actioned_by_name` (who submitted it), `actioned_on_behalf_of` / `actioned_on_behalf_of_name` and `on_behalf_note` (set when a manager acted for the assignee — see /docs/tasks/manager-view), `response_count` and `task_data.<field>` for every field (typed: numbers, booleans, ISO dates, multi-select arrays, attachments as a list of file objects `{ id, name, size, mime_type, url, storage_ref, uploaded_by, uploaded_at, checksum_sha256 }`), plus `task_data.<field>_item` (the selected item) for each lookup field and `task_data.<field>_count` for each attachments field.',
    example: { task_title: 'Approve invoice {{invoice_number}}', outcomes: ['Approved', 'Rejected'], assignment_type: 'team', team_id: '<team-id>', fields: [{ key: 'cost_centre', label: 'Cost centre', type: 'choice', options: ['OPS', 'FIN'] }] },
  },
  placeholder: { settings: 'A no-op stand-in while you sketch a flow; `note` explains what goes here.', example: { note: 'Post to ERP once the API is ready' } },
  server_event: {
    settings: '`event_name` plus an optional outbound action: call an integration reference (`reference_id`) or write to a data table (`table_id`), with the payload built from variables.',
    outputs: '`event_id`, `event_name`.',
    example: { event_name: 'invoice.approved' },
  },
  run_agent: {
    settings: '`agent_id`, the prompt in `user_message`, optional `files` (a file or a list, e.g. `{{form.receipts}}` or `{{item}}`: up to 10; images go to vision models as images, PDFs as documents where the API takes them (OpenAI), other documents — and PDFs elsewhere — as extracted text (PDF, Word, Excel, PowerPoint, text); read for the run\'s workspace only; the agent also gets the `read_attached_file` tool), `max_iterations`, `timeout_ms` and `output_variable`. The agent uses only the tools and references it was granted.',
    outputs: 'The agent\'s structured output in `output_variable`; with files, `files` lists each file\'s name, size and how it was sent (never contents).',
    example: { agent_id: '<agent-id>', agent_name: 'Invoice triage', user_message: 'Classify invoice {{invoice_number}}', files: '{{form.invoice_scan}}', output_variable: 'triage' },
  },
  add_user_to_role: { settings: '`user_id` and `role_id` or `role_name`. Audited as a permission change.', outputs: '`success`, `role_id`, `role_name`, `audited`.' },
  remove_user_from_role: { settings: '`user_id` and `role_id` or `role_name`. Audited as a permission change.', outputs: '`success`, `role_id`, `role_name`, `audited`.' },

  // ─── Files and documents ───
  file_to_base64: { settings: '`file_url` (task file, storage path or URL); `output_var`.', example: { file_url: '{{task.files.0.url}}', output_var: 'pdf_base64' } },
  base64_to_file: { settings: '`base64_data`, `file_name`, `content_type`; saves to storage; `output_var` receives the file URL.', example: { base64_data: '{{pdf_base64}}', file_name: 'invoice.pdf' } },
  download_file: { settings: '`url` and `file_name`; downloads into storage; `output_var`.', example: { url: '{{response.document_url}}', file_name: 'statement.pdf' } },
  read_file: { settings: '`file_url` and `encoding`; reads text content into `output_var`.', example: { file_url: '{{file_url}}' } },
  read_csv: { settings: '`mode` `read` (`file_url`, `delimiter`, `has_headers`) into `output_var`, or `write` (`data_var`, `file_name`) to produce a CSV file.', outputs: 'Rows, or `file` / `file_url` when writing.', example: { mode: 'read', file_url: '{{file_url}}', has_headers: true, output_var: 'rows' } },
  read_excel: { settings: '`mode` `read` (`file_url`, `sheet_name`, `has_headers`) or `write` (`data_var`, `file_name`).', outputs: 'Rows, or `file` / `file_url` when writing.', example: { mode: 'write', data_var: 'rows', file_name: 'payments.xlsx' } },
  doc_word: { settings: 'Template from a URL, a file object (an uploaded attachment such as `{{item}}` or the first file of `{{task_data.<key>}}`, or a generated file) or a data-table file (`template_url` or `source_mode: "table"`), `mode` such as `find_replace` with `replacements`, tables, lists or images; `output_var` receives the generated file. In `replacements` the `find` text is literal — `{{VENDOR}}` matches that placeholder as written — and only `replace` is resolved as an expression; an empty `find` is skipped with a warning, and a replacement that would grow a document part past 100 MB fails the node.', example: { template_url: '{{template_url}}', mode: 'find_replace', replacements: [{ find: '{{VENDOR}}', replace: '{{vendor.name}}' }], output_var: 'contract' } },
  doc_excel: { settings: 'Template source as for Word; update cells, find/replace or insert rows; `output_var`.', example: { template_url: '{{template_url}}', mode: 'find_replace', replacements: [{ find: '{{TOTAL}}', replace: '{{total}}' }] } },
  doc_powerpoint: { settings: 'Template source as for Word; find/replace, tables, lists and images in slides; `output_var`.', example: { template_url: '{{template_url}}', mode: 'find_replace', replacements: [{ find: '{{QUARTER}}', replace: 'Q3' }] } },

  // ─── Document actions (pages: /docs/workflows/documents) ───
  doc_read: {
    settings: '`source` — a file object (form attachment, a file from another step), a list (the first file), a `storage://` reference, a URL or text. Options: `pages` (PDF pages / slides, e.g. `2-5`), `sheet`, `range` (`B2:F40`), `header_row` (`auto`, `first`, `none` or a row number), `max_rows`, `max_chars`, `include_tables`, `include_images`, `tracked_changes` (`accept` or `show`), `ocr` (`off`, `auto` for pages without a text layer, `force`) and `ocr_languages`. See /docs/workflows/documents/read-document.',
    outputs: '`<output_var>` (default `document`): `text`, `markdown`, `html` (sanitised), `metadata` (title, author, created, modified, pages / slides / sheets, words, characters, reading time), `structure` (headings, links, tables, sheets, slides, pages, comments, tracked changes, front matter, outline, form fields, data) and `rows`; plus `<output_var>_text`, `<output_var>_rows` and `<output_var>_count`.',
    example: { source: '{{form.invoice}}', ocr: 'auto', output_var: 'invoice' },
  },
  doc_compare: {
    settings: '`original` and `revised` — files, text, lists of rows or JSON. `mode`: `auto`, `lines`, `words`, `characters`, `sentences`, `sheet` (rows matched on `key_columns`; `compare_columns`, `ignore_columns`, `numeric_tolerance`) or `json` (`array_key`). `ignore_whitespace`, `ignore_case`, `ignore_punctuation`; `redline` (`none`, `docx`, `pdf`, `both`; `redline_style` `formatting` or `tracked`) and `report` (`none`, `html`, `pdf`). See /docs/workflows/documents/compare-documents.',
    outputs: '`<output_var>` (default `comparison`): `identical`, `similarity` (0–100), `stats`, `changes`, `unified` (text diffs), `html` (report) and the files `redline_docx`, `redline_pdf`, `report`; plus `<output_var>_identical`, `_similarity`, `_html` and one variable per file.',
    example: { original: '{{contract_v1}}', revised: '{{contract_v2}}', mode: 'words', redline: 'docx', report: 'html', output_var: 'contract_diff' },
  },
  doc_convert: {
    settings: '`source` and `target` (`pdf`, `docx`, `html`, `markdown`, `text`, `csv`, `xlsx`, `json`); `engine` `auto` (the external converter for Office → PDF when DOCUMENT_CONVERTER_URL is set), `builtin` or `external`; `sheet`, `delimiter`, `page_size`, `title`, `output_name`. See /docs/workflows/documents/convert-document.',
    outputs: '`<output_var>` (default `converted_file`): a file object (`name`, `url`, `size`, `mime_type`, `storage_ref`, `checksum_sha256`); also `file` and `file_url`.',
    example: { source: '{{doc_file}}', target: 'pdf', output_name: 'Contract {{contract_number}}' },
  },
  doc_pdf: {
    settings: '`operation`: `merge` (`sources`: a list of PDFs), `split` (`ranges` such as `1-3; 4-6`, or `every_n`), `extract_pages` (`pages`), `page_count`, `watermark` (`text`, `position`, `font_size`, `opacity`, `angle`, `color`, `pages`), `rotate` (`degrees`, `pages`), `fill_form` (`fields` name/value pairs or `field_values`, `flatten`, `strict`) or `read_form`. See /docs/workflows/documents/pdf-tools.',
    outputs: '`<output_var>`: the new PDF file object (merge, extract, watermark, rotate, fill), a list of files (`split`), the page count, or `{ fields, values }` (`read_form`).',
    example: { operation: 'watermark', source: '{{converted_file}}', text: 'APPROVED {{=TODAY()}}', position: 'top-right', output_var: 'stamped' },
  },
  doc_extract: {
    settings: '`source`; `extract_tables` and `table_index` (a number or `all`); `patterns` (`name`, `pattern`, optional `group` and `all`); `finders` (`emails`, `urls`, `phones`, `dates`, `amounts`, `percentages`; `date_order` `DMY` or `MDY`); `to_table` with `table_id` and an optional `column_mapping` (`column` ← `field`) to insert the rows into a data table. See /docs/workflows/documents/extract.',
    outputs: '`<output_var>` (default `extracted`): `tables`, `rows`, `values` (one per pattern, also `<output_var>_<name>`), `found` and `table_rows_inserted`; plus `<output_var>_rows` and `<output_var>_count`.',
    example: { source: '{{form.invoice}}', patterns: [{ name: 'invoice_number', pattern: 'Invoice\\s*#?\\s*(\\w+)' }], finders: ['dates', 'amounts'] },
  },
  doc_redact: {
    settings: '`source`; `presets` (`email`, `phone`, `credit_card`, `iban`, `ssn`, `ip_address`, `url`, `date`), `terms`, `patterns`; `replacement` `block` (█), `label` (`[EMAIL]`) or `text` (`replacement_text`); `output` `same` (Word, PowerPoint and Excel redacted in place; text formats kept), `pdf`, `html` or `text`; `clear_author`. See /docs/workflows/documents/redact.',
    outputs: '`<output_var>` (default `redacted`): `file`, `text`, `counts` per kind and `total`; plus `<output_var>_file`, `_text` and `_count`.',
    example: { source: '{{form.statement}}', presets: ['email', 'phone', 'credit_card', 'iban'], replacement: 'label' },
  },
  doc_inspect: {
    settings: '`source`; `operation` `all`, `detect` (type from the content, not the name), `checksum` (`algorithms`: sha256, sha1, md5, sha512), `stats` or `search` (`query`, `regex`, `case_sensitive`, `whole_word`, `context_chars`). See /docs/workflows/documents/inspect-document.',
    outputs: '`<output_var>` (default `inspection`): `type` (format, MIME type, extension mismatch), `checksums`, `stats`, `metadata` and `search` (`count`, `matches` with line, page and context); plus `<output_var>_matches` and `_match_count`.',
    example: { source: '{{form.upload}}', operation: 'search', query: 'termination', whole_word: true },
  },

  // ─── Canvas tools ───
  section_group: { settings: 'A labelled, collapsible frame (`width`, `height`, `color`) for grouping activities on the canvas. It does not run.' },
  live_reference: {
    settings: '`reference_id`, `binding_name` and `input_mappings`. Instead of running once, the reference is called whenever an expression reads `{{<binding_name>.*}}`, so values are always fresh.',
    example: { reference_id: '<reference-id>', reference_name: 'Get exchange rate', binding_name: 'fx' },
  },
}
