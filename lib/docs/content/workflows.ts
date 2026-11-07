import type { DocPageDef } from '../types'

export const WORKFLOW_PAGES: DocPageDef[] = [
  {
    slug: 'workflows',
    title: 'Workflows',
    description: 'Durable, event-driven workflows that coordinate systems, data, people and agents — designed visually or authored as JSON.',
    body: `
A workflow is a directed graph: one **trigger** node, then nodes connected by edges. The engine executes the published version node by node, carrying **variables** from step to step, recording every step, and pausing safely whenever it has to wait.

::image designer | The workflow designer | The designer: palette on the left, canvas in the middle, the selected node's settings on the right.

## What a workflow can do

- **Start** from a task event, a data-table row change, a webhook, a schedule, an SLA or error in another workflow, a retention review, or a direct API / MCP / CLI call. See [Triggers](/docs/workflows/triggers).
- **Decide** with conditions, value switches and [decision tables](/docs/decision-tables).
- **Repeat** with for-each and N-times loops, and run **parallel** branches.
- **Call** integration references, HTTP endpoints, [custom workflow steps](/docs/custom-steps), [AI agents](/docs/agents) and other workflows.
- **Change data** in [data tables](/docs/data-tables) and tasks; transform text, dates, JSON, collections, CSV/Excel files and Office documents.
- **Involve people** with [User Tasks](/docs/tasks) that wait for an outcome, and send email from [templates](/docs/email-templates).
- **Wait** for minutes, days or a date without holding a server process (see [Delays and pauses](/docs/workflows/timing)).

All 72 node types are listed in the [node reference](/docs/workflows/nodes).

## Lifecycle

1. **Draft** — edit on the canvas or send a definition with \`PATCH /api/v1/workflows/{id}\` (\`usertasks workflows update\`). Validation runs as you edit.
2. **Test** — the designer's Test run executes the draft with sample variables and shows each node's input, output and log.
3. **Publish** — freezes the draft as a new version. Runs always use the published version. See [Versions and runs](/docs/workflows/versions-and-runs).
4. **Activate** — an active workflow listens to its trigger; deactivate it to stop new runs without deleting anything.

## Authoring as JSON

The designer and the API use the same definition: \`nodes\`, \`edges\`, \`user_variables\` and optional \`swimlanes\`.

\`\`\`json title="A minimal workflow definition"
{
  "name": "Notify on large invoices",
  "trigger_type": "webhook_received",
  "nodes": [
    { "id": "t1", "type": "webhook_received", "label": "Invoice received", "position": { "x": 400, "y": 40 }, "config": {} },
    { "id": "n1", "type": "condition", "label": "Over 10k?", "position": { "x": 400, "y": 200 },
      "config": { "branches": [{ "label": "Large", "logic": "AND", "rules": [{ "field": "{{amount}}", "operator": "greater_than", "value": "10000" }] }], "has_else": true } },
    { "id": "n2", "type": "send_email", "label": "Tell finance", "position": { "x": 250, "y": 360 },
      "config": { "to": "finance@example.com", "subject": "Large invoice {{invoice_number}}", "body": "Amount: {{amount}}" } }
  ],
  "edges": [
    { "id": "e1", "source": "t1", "target": "n1" },
    { "id": "e2", "source": "n1", "target": "n2", "label": "Large" }
  ]
}
\`\`\`

\`\`\`bash
usertasks workflows create -f notify.json
usertasks workflows publish <id>
\`\`\`

AI assistants can author workflows the same way through MCP (\`workflows_catalog\`, \`workflows_create\`, \`workflows_update\`); see [MCP](/docs/mcp).
`,
  },
  {
    slug: 'workflows/designer',
    title: 'The designer',
    description: 'Build workflows on the visual canvas: palette, settings panel, sections, test runs and validation.',
    body: `
Open **Automations**, then a workflow (or **New workflow**). The designer has three areas:

- **Palette** — every node type, grouped as triggers, logic, actions, operations and collections, plus your active [custom workflow steps](/docs/custom-steps). Drag a node onto the canvas, or drop it on an edge to insert it.
- **Canvas** — nodes and edges. Each node shows a one-line summary of its settings (for example \`GET Get vendor\` or \`→ Invoice approval policy\`), so a published flow reads like a specification. Group related nodes in a **Section**.
- **Settings panel** — opens when you select a node. Fields accept [expressions](/docs/workflows/expressions); the variable picker lists the trigger's variables and the outputs of earlier nodes.

::image designer | Designer with the User Task settings panel open | Configuring a User Task: title, outcomes (each becomes an outgoing edge), typed fields and assignment.

## Connecting nodes

Drag from a node's port to another node. Nodes with several exits label their edges:

| Node | Edge labels |
|---|---|
| Branch by Condition | the branch labels, plus **Else** |
| Branch by Value | the case values, plus **Default** |
| User Task | the outcomes, e.g. **Approved** / **Rejected** |
| Loop | the loop body, then the path after the loop |
| Parallel | one edge per branch, plus an optional **Done** edge |

## Validation

The designer validates every node as you work (the same \`validateNode\` the API uses) and shows problems on the node and in the publish dialog — for example "No reference selected" or "Input \\"amount\\" required". The node reference lists each node's validation rules.

## Test runs

**Test run** executes the current draft with variables you enter. You see each step's status, duration, input, output and log lines, and the path taken. Test runs are marked as tests and do not count in reports.

## Variables panel

Declare **user variables** with defaults; they are available to every node and can be overridden by the trigger or the caller (\`variables\` on \`POST /workflows/start\`).

## Keyboard and layout tips

- Select several nodes to move them together; sections move their contents.
- Use **Placeholder** nodes to sketch a flow before every integration exists.
- **Go To Activity** keeps rework loops readable instead of drawing long edges back up the canvas.
`,
  },
  {
    slug: 'workflows/triggers',
    title: 'Triggers',
    description: 'How runs start: form, task, data, webhook, middleware, schedule, SLA, error and retention triggers, plus API, MCP and CLI starts.',
    body: `
Every workflow has exactly one trigger node. Its type is also stored as the workflow's \`trigger_type\`.

| Trigger | Starts a run when… | Typical use |
|---|---|---|
| Task Created / Received / Submitted / Updated / Completed / Rejected | a task changes state (optionally filtered) | Route, enrich or follow up on work |
| Data Table Event | a row is created, updated or deleted | React to data changes |
| Form Submitted | someone submits a reusable form — its page (\`/f/<id>\`), the API, MCP or a User Task linked to it | Requests, intake, sign-ups — see [Forms](/docs/tasks/forms#the-form-submitted-trigger) |
| Webhook | an external system POSTs to the workflow's inbound webhook | Integrations, forms, other apps |
| Middleware | a webhook arrives and the caller waits for the workflow's answer | Synchronous validation or enrichment |
| Schedule | a schedule occurrence is due | Nightly syncs, reminders, reports |
| Workflow Error | another workflow fails | Central error handling and alerting |
| Workflow SLA | a run nears or breaches its SLA | Escalation |
| Policy Review / Policy Executed | a retention policy review is due or an action ran | Records management |

Any workflow can also be started directly — whatever its trigger — with \`POST /api/v1/workflows/start\`, the MCP tool \`workflows_start\` or \`usertasks workflows start\`:

\`\`\`bash
usertasks workflows start --set workflow_id=<id> --set variables:='{"invoice_number":"INV-1042","amount":12400}'
# add --set async:=true to queue the run and return immediately with a run id
\`\`\`

## Webhooks

Create an inbound webhook for the workflow (Settings → Webhooks, \`usertasks webhooks create <workflow-id>\`, or MCP). Callers POST JSON with the webhook's secret:

\`\`\`bash
curl -X POST "https://tasks.example.com/api/webhooks/inbound?hook=<slug>" \\
  -H "Authorization: Bearer <webhook-secret>" -H "Content-Type: application/json" \\
  -d '{"invoice_number":"INV-1042","amount":12400,"vendor_id":"V-311"}'
\`\`\`

The body is available as \`{{_raw}}\`, and a payload mapping can turn fields into named variables. Secrets are stored only as bcrypt hashes; rotate one with \`usertasks webhooks rotate <webhook-id>\`.

## Schedules

A schedule trigger runs \`every_minutes\`, \`hourly\`, \`daily\`, \`weekly\`, \`monthly\` or \`once\`, in an IANA time zone. pg_cron checks schedules every minute and fires each occurrence exactly once; if the scheduler was down, only the most recent missed occurrence runs.

\`\`\`json
{ "schedule_mode": "weekly", "day_of_week": 1, "time_of_day": "07:00", "timezone": "Pacific/Auckland" }
\`\`\`

## Variables a trigger provides

Every run starts with \`workflow_id\`, \`workflow_name\`, \`workflow_started_at\`, \`workflow_started_by\`, \`workflow_started_via\`, \`workflow_owner_id\`, \`tenant_id\`, \`task_id\` and \`trigger_type\`, plus the user variables' defaults and whatever the trigger or caller passes. The per-trigger variables are listed in the [node reference](/docs/workflows/nodes#category-trigger).

### Who started the run

\`{{workflow_started_by}}\` is the user id of the person who started the run, so a User Task with the assignee variable \`{{workflow_started_by}}\` goes back to them. \`{{workflow_started_via}}\` says how it started, and \`{{workflow_owner_id}}\` is always the workflow's creator.

| Started by | \`workflow_started_by\` | \`workflow_started_via\` |
|---|---|---|
| A form button's **Start workflow**, or the **Form submitted** trigger | The person who submitted the form | \`form\` |
| The **Run** dialog / designer | The signed-in user | \`ui\` (\`test\` for a Test run) |
| REST \`POST /api/v1/workflows/start\` | The API key's or token's user (a workspace key: the person who created it) | \`api\` |
| The CLI (\`usertasks workflows run\`) | The signed-in CLI user | \`cli\` |
| MCP \`workflows_start\` | The MCP connection's user | \`mcp\` |
| An agent's start-workflow tool | The agent's acting user | \`agent\` |
| A task trigger | Created / received: the task's assigner. Accepted / rejected: the assignee. Completed / updated: whoever actioned it last (else the assignee) | \`task\` |
| A data-table row event | The person who wrote the row (a workflow's write: that run's \`workflow_started_by\`) | \`data_table\` |
| **Call Sub Workflow** | The parent run's \`workflow_started_by\` | \`subworkflow\` |
| A schedule, webhook, workflow error / SLA or retention trigger | Nobody, so the workflow's owner | \`schedule\`, \`webhook\`, \`event\` |

These three variables are owned by the engine: a caller's start variables, a task's submitted values and a data-table get never change them. Steps that act with the workflow's own authority (an AI agent's acting user, the governance and retention actions' actor) use the owner, \`workflow_owner_id\`.
`,
  },
  {
    slug: 'workflows/nodes',
    title: 'Node reference',
    description: 'Every workflow node type with its settings, validation rules, output variables and an example configuration — generated from the engine.',
    tocDepth: 2,
    body: `
This reference is generated from the engine's node catalog (\`lib/workflow-schema.ts\`), its validation rules and its canvas summaries, so it always lists exactly the nodes the designer offers. "Validation" shows the messages you see when a required setting is missing. Every node's results are also available as \`{{<prefix>_<name>}}\`, where the prefix is the node's label in snake_case (or its configured output prefix), so two nodes of the same type don't overwrite each other.

Every node also has two engine-level options:

- \`onError\`: \`stop\` (default) fails the run; \`continue\` logs the error, sets \`last_error\`, \`last_error_node\` and \`has_error\`, and carries on. See [Error handling](/docs/workflows/error-handling).
- \`executionMode\`: \`sync\` (default) or \`async\` for nodes that can be fired and forgotten, such as Call Sub Workflow.

::nodes
`,
  },
  {
    slug: 'workflows/expressions',
    title: 'Variables and expressions',
    description: 'Move data between nodes with {{variables}}, dotted paths, task data and inline {{=FUNCTION()}} expressions.',
    body: `
Almost every setting accepts expressions. They are resolved when the node runs.

## Variables

| Syntax | Resolves to |
|---|---|
| \`{{amount}}\` | A variable (trigger data, user variable or a node output) |
| \`{{vendor.name}}\` | A property of an object variable |
| \`{{lines.0.sku}}\` or \`{{lines[0].sku}}\` | An array element by index (0 = first) |
| \`{{task.title}}\`, \`{{task.status}}\` | The run's task |
| \`{{task_data.cost_centre}}\` | A User Task field value after the task is completed (also \`{{task.data.cost_centre}}\`) |
| \`{{outcome}}\`, \`{{outcome_notes}}\` | The outcome chosen on the last completed User Task |
| \`{{actioned_by}}\`, \`{{actioned_on_behalf_of}}\`, \`{{on_behalf_note}}\` | Who submitted the last User Task, and — when a manager acted for the assignee — the assignee and the manager's reason (empty otherwise) |
| \`{{_raw}}\` | The full webhook payload |
| \`{{last_error}}\` | The last error message when a node continues on error |

When a setting is **only** an expression (\`{{lines}}\`), the raw value is passed through, so numbers stay numbers and arrays stay arrays. When an expression is part of a longer string (\`Invoice {{number}}\`), values are converted to text.

## Inline functions

\`{{=FUNCTION(arguments)}}\` evaluates one of the [built-in functions](/docs/workflows/functions). Arguments can be literals, variables (\`{{amount}}\` or just \`amount\`) and other functions:

\`\`\`text
{{=UPPER(LEFT(vendor.name, 3))}}-{{=FORMAT_DATE(NOW(), "YYYYMMDD")}}
{{=IF(amount > 10000, "CFO", "Manager")}}
{{=ADD_DAYS(received_at, 30)}}
\`\`\`

A whole-string function returns its typed value and is evaluated exactly once, so \`{{=GUID()}}\` or \`{{=NOW()}}\` give one value per node.

## Operators and truthiness

Inside \`{{= … }}\` — as a whole expression or as a function argument — you can compare and calculate:

\`\`\`text
{{= price * qty }}                       → 39.98
{{= (subtotal + shipping) * 1.15 }}
{{=IF(status == "approved", "Go", "Stop")}}
{{=ROUND(total / count, 2)}}
Order {{= line_count + 1 }} of {{max}}
\`\`\`

| Operator | Meaning |
|---|---|
| \`==\` \`!=\` | Equal / not equal. Numbers (or numeric text such as \`"10"\`) compare as numbers, so \`"10" == 10.0\`; anything else compares as exact, case-sensitive text. A null or missing value equals \`""\`. |
| \`>\` \`<\` \`>=\` \`<=\` | Numeric order when both sides are numeric, otherwise text order (ISO dates such as \`2026-01-05\` order correctly). Always false when either side is null, missing or \`""\`. |
| \`+\` | Adds numbers; when either side is not numeric it joins them as text (\`"Hello " + name\`). |
| \`-\` \`*\` \`/\` \`%\` | Arithmetic on numbers. A non-numeric operand or division by zero gives null (empty). |
| \`( )\` | Grouping. \`*\` \`/\` \`%\` bind tighter than \`+\` \`-\`, which bind tighter than comparisons. |

Operands are literals (\`"text"\`, \`12.5\`, \`true\`, \`false\`, \`null\`), variables (\`{{amount}}\` or \`amount\`), functions and parenthesised expressions. If an argument names something that is not a variable, it is read as plain text, as before — quote text to be safe. Unquoted dates and ids with two or more separators (\`2026-01-05\`, \`021-555-1234\`) are text, not subtraction.

**Truthiness.** \`IF\`, \`AND\`, \`OR\` and \`NOT\` treat \`false\`, \`0\`, \`""\`, \`"false"\`, \`"0"\`, \`"no"\`, \`"off"\`, \`"null"\`, null, missing values and empty lists as false, and everything else as true — the same rules as the Condition node's \`is_true\` / \`is_false\`. So a form field or API value of \`"false"\` takes the else branch.

## Node outputs

Nodes write their results into variables. Most let you name the output (\`output_var\`); integration calls, decision tables and agents also write prefixed variables (\`{{get_vendor_response}}\`, \`{{approval_policy_outputs}}\`) so several calls in one run don't collide. The designer's data picker lists everything available at the selected node — see [Reading values from an API response](/docs/workflows/api-responses).

## User variables

Declare variables with defaults in the designer's variables panel or in the definition's \`user_variables\`. They are the workflow's [inputs](/docs/workflows/versions-and-runs#running-workflows): callers override them with \`variables\` when starting a run, and a variable can be marked required:

\`\`\`json
{ "workflow_id": "…", "variables": { "currency": "EUR", "amount": 1200 } }
\`\`\`

## Live references

A **Live Reference** node binds an integration reference to a name; reading \`{{fx.rate}}\` calls the reference at that moment, so long-running workflows always see fresh values.
`,
  },
  {
    slug: 'workflows/api-responses',
    title: 'Reading values from an API response',
    description: 'Pick a single value, find an item, list a field or loop through a collection from a reference or HTTP response — with the data picker, no hand-written JSON paths.',
    body: `
An API call returns one JSON document; a workflow usually needs one value from it (the first customer's name), one item (the customer whose id is 42), one field of every item (all emails) or a loop over the items. The designer's **data picker** does all four from the response **model**, so you click fields instead of writing paths.

## Where the model comes from

Every [reference](/docs/integrations/references#response-model) carries a response model — the nested fields, lists and their item fields, types, descriptions and example values:

| Source | How |
|---|---|
| OpenAPI / Swagger import | The 2xx response schema, with every \`$ref\` resolved |
| **Test** in the reference builder | Inferred from the real response (samples are merged, so optional fields show up); **Capture from test response** merges another sample on demand |
| Database references | The table's columns, as a list of rows |
| Connectors | The fields the connector catalogue declares |
| Older references | Derived from their declared outputs until the next test |

An **HTTP Request** node gets one by pasting a sample response under **Response model**. Data-table queries (rows), agents' output schemas and custom steps' declared outputs are modelled automatically.

## The data picker

Every expression field in the node settings has an **Insert data** button (\`[ ]\`), and the toolbar's **Variables → Data** tab shows the same tree. It lists only what exists when the selected step runs:

- **Steps** before this one (following the connections), each with its outputs — for a reference: \`response\` and its levels, e.g. \`Get Customers › response › data[] › address › city\`.
- **Current item** inside a loop body (and **Outer loop item** for nested loops), with the item's fields.
- Task fields, workflow variables and the trigger row.

Search by name, move with the arrow keys (→ opens a level, ← closes it) and press **Enter** to insert. A plain value inserts its path, e.g. \`{{get_customers_response.total}}\`.

The same tree docks beside the settings panel as the **Data** panel (**Data** in the panel header, or **Alt+D**), and any row can be dragged onto a setting — form fields come first. See [Use the values downstream](/docs/tasks/forms#use-the-values-downstream-the-data-panel-and-drag-and-drop).

## Single value or collection

A list (shown as \`data[]\`), or any field inside one, asks how you want it:

| Choice | Inserts | Result for the sample below |
|---|---|---|
| First item | \`{{get_customers_response.data.0.email}}\` | \`ada@example.com\` |
| Item at index… (1 = first) | \`{{get_customers_response.data.1.email}}\` | \`grace@example.com\` |
| Find item where… | \`{{=FIND_WHERE(get_customers_response.data, "id", 2, "email")}}\` | \`grace@example.com\` |
| All values of a field as a list | \`{{=PLUCK(get_customers_response.data, "email")}}\` | \`["ada@example.com", "grace@example.com"]\` |
| Count of items | \`{{=COUNT(get_customers_response.data)}}\` | \`2\` |
| Loop through each item | \`{{item.email}}\`, and wraps the step in a Loop | one run of the step per customer |

\`\`\`json title="Sample response (Get Customers)"
{ "total": 2, "data": [
  { "id": 1, "name": "Ada", "email": "ada@example.com", "address": { "city": "Wellington" } },
  { "id": 2, "name": "Grace", "email": "grace@example.com", "address": { "city": "Auckland" } }
] }
\`\`\`

Paths index lists with a number segment (\`data.0.email\`); \`data[0].email\` works too. Nested fields use dots: \`{{=PLUCK(get_customers_response.data, "address.city")}}\`. Find's value can be a variable: \`{{=FIND_WHERE(get_customers_response.data, "id", {{task_data.customer_id}}, "name")}}\`.

## Loops

**Loop through each item** inserts a **Loop For Each** before the step: the step becomes the loop body (**Each Item**), the steps after it continue from **Done**, and \`{{item.<field>}}\` is inserted. Inside the body the picker shows **Current item** with the item's fields.

On a Loop node, **Items to loop through** suggests every list available at that point — lists inside responses, data-table rows, set-variable and split results, workflow variables holding lists, form Attachments fields, agent and custom-step list outputs, and the current item's lists for a nested loop — grouped by step, with an example item. Type to filter, ↑↓ and **Enter** to choose. Free text still works; a value that is known not to be a list shows a warning.

## Loops over files

A form's **Attachments** field is a list of files (\`{{task_data.<key>}}\` after a User Task, \`{{form.<key>}}\` in a Form submitted workflow), so a Loop handles each file: choose it under **Items to loop through**, or drag it from the Data sidebar onto that box. Inside the loop the picker's **Current item** has the file's fields — \`{{item.name}}\`, \`{{item.size}}\`, \`{{item.mime_type}}\`, \`{{item.url}}\` — and steps that take files accept \`{{item}}\` itself:

- **Send Email → Attachments:** \`{{item}}\` attaches one file; \`{{task_data.<key>}}\` (outside a loop) attaches them all; several expressions separated by commas attach everything they name (up to 20 files, 25 MB).
- **HTTP Request:** body **Multipart form (files)** with a **File(s)** part (\`{{item}}\`, or the whole list — one part per file, with its name and type), or **Binary (one file)** to send the file as the body.
- **Call Reference** file inputs, such as the content of a storage connector's upload (S3, Azure Blob, SharePoint, OneDrive, Box, Google Drive): \`{{item}}\` sends the file's bytes and type (multipart for Box and Google Drive).
- **Word / Excel / PowerPoint** steps: \`{{item}}\` (or a list — its first file) as the template.
- **Read File / Read CSV / Read Excel / Download File / File to Base64:** \`{{item}}\` as the file.
- **Run Agent → Files:** the files go to the model (images to vision models, PDFs as documents on OpenAI, other documents as extracted text), and the agent's \`read_attached_file\` tool reads them by id.

Files are read for the workflow's own workspace only, and run details show their names and sizes, never their contents. \`{{task_data.<key>_count}}\` is the number of files (a Condition can skip the loop when it is 0).

\`\`\`json title="Log each file, then email them all"
[
  { "id": "each", "type": "loop", "config": { "items": "{{task_data.receipts}}" } },
  { "id": "log", "type": "log", "config": { "message": "Receipt {{item.name}} ({{item.size}} bytes)" } },
  { "id": "mail", "type": "send_email", "config": { "to": "{{task.assignee_email}}", "subject": "Receipts", "body": "{{=FILE_NAMES(task_data.receipts)}}", "attachments": "{{task_data.receipts}}" } }
]
\`\`\`

## Named outputs are optional

You can still map reference outputs to variables (**Outputs → Variables**). In a Call Reference node the **Response model** tree also has **+** on each field to add a named output (\`custom_outputs\`: name + path relative to the response). Existing output mappings keep working unchanged.
`,
  },
  {
    slug: 'workflows/functions',
    title: 'Expression functions',
    description: 'All inline expression functions — text, dates, math, logic, conversion, lists, regex, data and files — with signatures and examples.',
    tocDepth: 2,
    body: `
Use these inside \`{{= … }}\` in any setting. Optional arguments are in brackets. The list is generated from the expression engine (\`lib/expression-functions.ts\`).

::functions
`,
  },
  {
    slug: 'workflows/timing',
    title: 'Delays, pauses and SLAs',
    description: 'Wait for a duration or a date without holding a process, set SLAs on runs, and react when they are at risk.',
    body: `
## Pause for Duration

A **Pause for Duration** (\`delay\`) node waits for \`duration\` \`unit\` (seconds, minutes, hours or days).

- Waits of up to **30 seconds** happen inline.
- Longer waits **pause the run**: its status becomes \`waiting_for_event\`, nothing is held in memory, and a \`delay_resume\` job on the queue continues the run at \`resume_at\`. The node's output records \`delay_ms\` and \`resume_at\`.

## Pause Until Date

**Pause Until Date** (\`pause_until\`) works the same way with an absolute date or expression, for example \`{{=ADD_DAYS(invoice_due, -3)}}\`.

> [!NOTE]
> Timed resumes are picked up by the queue processor, which pg_cron calls every minute. On a self-hosted install without the Vault secrets, paused runs wait until the processor is called. See [Scheduled jobs](/docs/self-hosting#3-scheduled-jobs-pg_cron--vault).

## Waiting for an external event

A run can also wait for something outside UserTasks. Resume it with the event id it is waiting on:

\`\`\`bash
usertasks events complete <event-id> --set return_variables:='{"payment_status":"settled"}'
\`\`\`

## SLAs

**Set SLA** gives the run a deadline (relative, like 3 days, or an exact time) and an optional warning threshold. The SLA sweep (every 5 minutes) emits \`workflow.run.sla_warning\` and \`workflow.run.sla_breached\` events, fires **Workflow SLA** triggers, and SLA state shows in run lists and reports.

Task SLAs (due dates, reminders, escalations) are separate and covered in [Assignment and SLAs](/docs/tasks/assignment-and-slas).

## Schedules

Schedule triggers start new runs on a timetable; see [Triggers](/docs/workflows/triggers#schedules).
`,
  },
  {
    slug: 'workflows/sub-workflows',
    title: 'Sub-workflows, loops and parallel paths',
    description: 'Reuse workflows as steps, iterate over collections and run branches in parallel.',
    body: `
## Call Sub Workflow

**Call Sub Workflow** runs another published workflow as a step. Map the parent's variables to the child's with \`input_mappings\`.

- **Sync** (default): the parent waits; the child's final variables come back as \`results\`, with \`status\` and \`error\`.
- **Async**: set the node's execution mode to async to start the child and continue immediately.

Use sub-workflows for shared steps such as "create vendor in ERP" or "notify approvers", so a fix lands in every workflow that uses it.

## Loops

- **Loop For Each** iterates \`items\` with \`{{item}}\` and \`{{index}}\` (rename them with \`item_var\` / \`index_var\`).
- **Loop N Times** repeats \`count\` times with \`{{i}}\`.

Nodes connected to the loop's body edge run for each iteration; the other edge continues after the loop. Decision tables, references and data-table columns used inside a loop are read once per run, so loops of hundreds of iterations stay fast.

## Parallel / Sequential

**Parallel / Sequential** runs two or more branches. In \`parallel\` mode they start together; in \`sequential\` mode one after another. Branches do not join: when two branches reach the same step, that step runs once, for the first branch that reaches it. To continue after every branch has finished, connect the **Done** port. The step on the Done edge runs once and sees the variables that every branch set. If a branch pauses (a User Task, a wait event or a long delay), the Done step does not run after the resume, and the run log shows a warning. Do not use a branch name **Done**: it turns the Done port off.

## Go To Activity

**Go To Activity** jumps to another node, optionally resetting variables first — ideal for "send back for rework, then approve again" loops.
`,
  },
  {
    slug: 'workflows/error-handling',
    title: 'Error handling',
    description: 'Decide per node whether a failure stops the run, inspect errors, retry runs and build central error workflows.',
    body: `
## Per node: stop or continue

Each node has an \`onError\` setting:

- \`stop\` (default) — the run fails, the error is recorded on the node and the run, and **Workflow Error** triggers fire.
- \`continue\` — the error is logged as a warning and the run follows the node's outgoing edges. These variables are set for downstream nodes:

| Variable | Value |
|---|---|
| \`has_error\` | \`true\` |
| \`last_error\` | The error message |
| \`last_error_node\` / \`last_error_node_id\` / \`last_error_node_type\` | Which node failed |

Branch on \`{{has_error}}\` to compensate, notify or create a task for a person.

## Central error workflows

A workflow with a **Workflow Error** trigger runs whenever another workflow (or only the ones you choose) fails, with \`error_workflow_id\`, \`error_workflow_name\`, \`error_run_id\`, \`error_message\`, \`error_node\` and \`error_timestamp\`. Use one to page on-call, open an incident or log to a data table.

## Retrying

- Failed **runs**: \`usertasks runs action <run-id> --set action=resume\` (or pause / cancel) — or from the run detail page.
- Failed **queue jobs**: \`usertasks queue action <job-id> --set action=retry\`.

Queue jobs are retried automatically with backoff before they are marked dead; \`workflow.queue.failed\` and \`workflow.queue.dead\` events tell you when that happens.

::image run-detail | A run's detail page with the path taken and each step's status | Run detail: the path taken, each step's status and duration, and the failing step's error.

## Finding failures

- **Automations → Runs** filters by status; each run shows its path, step logs and the error.
- \`usertasks logs tail --type workflow.run.failed\` follows failures live.
- Ship \`severity=error\` events to your log platform and alert on them; see [Observability](/docs/observability).
`,
  },
  {
    slug: 'workflows/versions-and-runs',
    title: 'Versions, runs and the queue',
    description: 'Drafts, published versions, running a workflow by hand with its inputs, run statuses, migrating live instances, the durable queue and run history.',
    body: `
## Drafts and versions

Editing a workflow changes its **draft**. Publishing freezes the draft as a numbered **version** and makes it the one new runs use; runs already in progress finish on the version they started with, unless you [migrate them](#migrating-live-instances).

\`\`\`bash
usertasks workflows definition <id> -o json > wf.json     # the draft, ready to edit
usertasks workflows update <id> -f wf.json                # save the draft (changed: false if identical)
usertasks workflows publish <id> --set notes="Add CFO step"
usertasks workflows versions <id>                         # history with notes
usertasks workflows version <id> <version-id> -o json     # one version's definition
\`\`\`

::image cli-edit-workflow | Terminal: editing and publishing a workflow with the CLI | The edit round trip from a terminal: export the draft, change it, save and publish.

## Running workflows

Any active, published workflow can be started by hand, whatever its trigger. Its **inputs** are the workflow variables of the published version (declare them in the designer's **Workflow Variables** panel): each has a type, an optional default and description, and can be marked **Required input**. A text variable can be refined to a date, date and time, email, URL or a choice list.

| Variable | Input field |
|---|---|
| String | Text (or date, date & time, email, URL, choice list when set) |
| Number | Number |
| Boolean | Checkbox (true / false) |
| Object | JSON |

**In the app:** choose **Run** on the workflow's row in **Automations → Workflows**, or in the designer header. The dialog shows a form for the inputs, with defaults filled in, descriptions as help text and required inputs marked. **Run in background** (the default) queues the run with an optional priority (0-100); **Wait for result** runs it now and shows the final status and step outputs. Either way you get a link to the run. The button is disabled until the workflow is published and active, and only shown to people with the **Activate workflows** permission (\`workflows.activate\`).

::image run-workflow | Run workflow dialog with a required text input, a number with a default, a checkbox and the background / wait options | Running a workflow from the list: its inputs become a form.

From a terminal, \`usertasks workflows run\` reads the inputs, converts each \`--input key=value\` to the input's type and asks for missing required inputs. Without a terminal (scripts, CI) it lists the missing inputs and exits with code 2 instead.

\`\`\`bash
usertasks workflows inputs <id>                                       # the inputs: key, type, required, default
usertasks workflows run <id> --input customer="Acme Ltd" --input amount=1200
usertasks workflows run <id> -f inputs.json --wait                    # run now, print status and step outputs
usertasks workflows run <id> --input customer=Acme --priority 80      # queued ahead of other jobs
\`\`\`

Over REST, \`GET /api/v1/workflows/{id}/inputs\` returns the fields (\`key\`, \`label\`, \`type\`, \`required\`, \`default\`, \`description\`, \`options\`) and \`POST /api/v1/workflows/start\` starts the run:

\`\`\`bash
curl -X POST "$USERTASKS_URL/api/v1/workflows/start" \\
  -H "Authorization: Bearer $USERTASKS_API_KEY" -H "Content-Type: application/json" \\
  -d '{"workflow_id":"<id>","variables":{"customer":"Acme Ltd","amount":"1200"},"async":true,"priority":50}'
\`\`\`

Values are checked against the inputs and converted to their type (\`"1200"\` becomes \`1200\` for a number input). A missing required input or a value of the wrong type is a \`400\` with one entry per field in \`details\`, and nothing starts. Variables that are not declared inputs are passed through unchanged and listed in \`warnings\`; a workflow without inputs accepts any variables. MCP has \`workflows_inputs_get\` and \`workflows_start\`.

## Run statuses

| Status | Meaning |
|---|---|
| \`running\` | Executing now |
| \`waiting_for_event\` | Paused: waiting on a User Task, a timed delay or an external event |
| \`paused\` | Paused by an operator |
| \`completed\` | Finished (including End Workflow) |
| \`failed\` | A node failed with \`onError: stop\` |
| \`cancelled\` | Cancelled by an operator or the API |
| \`migrated\` | Closed by a live instance migration; a new run continues it on another version |

## Migrating live instances

A **live instance** is a run that is waiting (on a User Task, a timed delay or an external event) or paused. When you publish a new version, live instances stay on the version they started with. Migrating moves them to another version:

1. You pick the **target version** and, for each node the instances are at, the **node in that version where they continue**. A node with the same id is pre-selected; you can choose any step.
2. The old instance is **closed out**: its status becomes \`migrated\`, its open task is cancelled (the task history records it) and any queued resume or timed wake-up for it is dropped.
3. A **new run** starts on the target version at the chosen node. It carries the old run's variables (new variables of the target version get their defaults), its task and its trace id, and links back to the old run. A User Task at the target creates a fresh task; any other step runs straight away.

Both runs are audited and emit a \`workflow.run.migrated\` event with the ids of the other run. The run pages show **Migrated to →** and **Migrated from ←** banners.

**In the app:** the workflow designer shows a badge such as *2 live instances on older versions*. It opens **Live instances**: each instance's current node, version, waiting time and the assignee of its open task, with counts per version (also shown in **Version history**). Select instances and choose **Migrate to version…**.

::image live-instances | Live instances panel listing waiting runs with their version, current node, waiting time and assignee | Live instances of a workflow, with counts per version.
 The wizard asks for the target version and the target node for each current node, with a preview of the target graph, then shows a dry-run review before anything changes. Migrating more than ten instances asks you to type the number to confirm.

::image migrate-instances | Migration wizard mapping the current node to a node of the target version, with a graph preview | Choose where each group of instances continues; the node with the same id is pre-selected.

::image migrate-instances-review | Migration review table listing each instance, its target node, what is closed and warnings | The dry-run review: nothing changes until you confirm.
 A single run can be moved with **Move to version…** on its run page or in the workflow queue.

Every instance is planned and migrated on its own. An instance whose target node does not exist is **blocked** and left alone, and one that moved on in the meantime (its task was completed, or someone resumed it) is reported as a **conflict** and left alone. The review warns about variables the target path uses that the instance does not have, and about resumes that were already queued.

\`\`\`bash
usertasks workflows instances <id>                                  # live instances, current node, version
usertasks workflows migrate-instances <id> --to <version-id> --all --dry-run
usertasks workflows migrate-instances <id> --to <version-id> --from <old-version-id> \\
  --map review=screen                                                # instances at "review" continue at "screen"
usertasks workflows migrate-instances <id> --to <version-id> --run <run-id> --target approve
\`\`\`

::image cli-migrate-instances | Terminal: listing live instances and dry-running a migration with the CLI | Listing live instances and planning a migration from a terminal.

The CLI always plans first, prints the plan and asks before migrating (\`--yes\` skips the prompt). The target node for each instance is \`--map\` for its current node, else \`--target\`, else the node with the same id. Over REST it is \`POST /api/v1/workflows/{id}/instances/migrate\` with \`to_version_id\`, the instances (\`run_ids\`, \`from_version_id\` or \`all_active\`), optional \`node_map\` and \`default_target_node_id\`, and \`dry_run\`. MCP has \`workflows_instances_list\` and \`workflows_instances_migrate\`. One request handles up to 500 instances. Migrating needs the **Migrate live runs** permission (\`workflows.manage_runs\`; Owner, Admin and Manager have it by default, and OAuth apps get it with \`workflows:write\`). Listing needs **View runs**.

## The queue

Asynchronous starts, triggered runs, resumes after tasks and timed waits go through a durable queue in Postgres. The processor claims jobs in batches and runs up to \`WORKFLOW_CONCURRENCY\` (default 5) at once per invocation, re-kicking itself while work remains. Enqueuing code kicks it immediately; pg_cron calls it every minute as a safety net.

\`\`\`bash
usertasks queue list --status failed
usertasks queue action <job-id> --set action=retry
\`\`\`

## Run history

Each run records its trigger, variables, every step (status, duration, input and output, capped in size), logs and the execution path. Open it in **Automations → Runs**, fetch it with \`usertasks runs get <id>\`, or follow its events with \`usertasks logs tail --run <id>\`. Finished queue rows are kept for \`QUEUE_RETENTION_DAYS\` (default 7); run history follows your [retention policies](/docs/governance/audit-and-retention).
`,
  },
]
