# Test Workflows

Import these into the workflow builder via the **Import** button on the toolbar.

Each workflow is triggered by **Webhook** so you can test via Save & Test without any external dependencies.

## Workflows

| # | File | Tests | Key Features |
|---|------|-------|-------------|
| 01 | `01-basic-logic.json` | Variables, math, conditions | Set variable, math expression, if/else branching, logging |
| 02 | `02-string-operations.json` | String manipulation | Uppercase, lowercase, text template, regex extract, parallel paths |
| 03 | `03-loops-and-collections.json` | Loops & collections | Loop each, collection count, filter, sort, join |
| 04 | `04-switch-case-branching.json` | Switch/case | Multi-branch routing, 5 cases + default, merge back |
| 05 | `05-json-operations.json` | JSON building | Build object, build array, get value, add to array, join |
| 06 | `06-date-math-formatting.json` | Dates | Format date, add days, subtract days, parallel date ops |
| 07 | `07-user-task-approval.json` | Human-in-the-loop | User task with outcomes, approval/rejection branching, update task |
| 08 | `08-sections-and-error-handling.json` | Sections + errors | Collapsible sections, onError=continue, error checking, dual sections |
| 09 | `09-loop-n-times-parallel.json` | Loop N + parallel | Loop 5 times with math, parallel 3-branch, delay node |
| 10 | `10-expressions-functions.json` | Expression functions | TRIM, LEN, LEFT, REPLACE, CONCAT, IF, ROUND, ADD_DAYS, NOW |
| 11 | `11-full-data-pipeline.json` | Data pipeline | JSON array build, nested loops, filter/sort/get, stats object, accumulators |
| 12 | `12-order-processing.json` | Order flow | Switch routing, regex, string ops, per-item math, formatted summary |
| 13 | `13-collections-strings-errors.json` | Collections + errors | All 6 collection ops, end_workflow, switch, regex, string ops |
| 14 | `14-multi-path-conditions.json` | Branching matrix | Stacked multi-branch conditions, date math, regex, deep merge paths |
| 15 | `15-kitchen-sink-stress.json` | Kitchen sink | 57 nodes: loops in loops, switch + condition (`match_mode: first`), accumulators, bare-identifier expressions, full-report ordering |
| 16 | `16-ultimate-stress.json` | **Ultimate stress** | 68 nodes, 30 node types — everything in 15 **plus** `decision_table`, all 5 `update_data_table` ops, parallel (sequential mode), user task, sub-workflow, http error drill, delay, end_workflow |

## Workflow 16 setup (one-time)

Workflow 16 exercises the two node types that need real database entities: **Decision Table** and **Update Data Table**. Create these before running it:

**1. Data table `Stress Orders`** (Data → New Table) with columns:
`order_no, customer, tier, region, item_count, order_total, discount_pct, ship_method, ship_cost, grand_total, status, notes`

**2. Data table `Stress Order Lines`** with columns:
`order_no, sku, name, qty, unit_price, line_total, category`

**3. Decision table `Order Pricing Rules`** (Business Rules) —
inputs: `order_total` (number), `tier` (text), `item_count` (number);
outputs: `discount_pct` (number), `ship_method` (text), `approval_required` (boolean), `priority` (text);
rules (top-down, first match wins):

| Rule | Conditions | discount_pct | ship_method | approval_required | priority |
|------|-----------|--------------|-------------|-------------------|----------|
| Big order — needs approval | order_total > 2000 | 15 | express | true | urgent |
| Gold high value | tier = gold AND order_total > 500 | 12 | express | false | high |
| Silver high value | tier = silver AND order_total > 500 | 8 | standard | false | normal |
| Bulk basket | item_count >= 5 | 5 | standard | false | normal |
| Default | (no conditions) | 0 | economy | false | low |

**4. After importing the JSON**, open these nodes and pick your entities:
- `Stress Order Lines` table → **Insert Order Line Row**, **List Saved Lines**
- `Stress Orders` table → **Insert Order Header**, **Get Order Row**, **Price Order**, **Insert Temp Row**, **Delete Temp Row**, **Complete Order**
- `Order Pricing Rules` decision table → **Evaluate Pricing Rules**

With the default inputs (tier `silver`, 5 line items ≈ $1,638) the run should match rule **Silver high value** (8% / standard), verify the saved line count, survive the deliberate HTTP failure, insert + delete a temp row, and finish with a fully-populated report and a `COMPLETE` order row.

Optional drills via workflow variables: set `approval_mode` to `manual` **and** raise the order over $2,000 (edit a qty) to pause on the **Approve Order** user task (set `approver_user_id` first); set `enable_subworkflow` to `true` (and select workflow 15 in **Call Kitchen Sink**) to run 15 as a sub-workflow; set `enable_error_drill` to `false` to skip the failing HTTP call.

## How to Test

1. Open any automation in the workflow builder
2. Click **Import** in the toolbar
3. Select one of the JSON files
4. Click **Save & Test**
5. For workflow 07 (approval), you'll need to action the user task in the test runner
6. For workflow 16, do the one-time setup above first

## What to Verify

- **Canvas rendering**: nodes positioned correctly, edges connecting properly
- **Sections** (08): collapse/expand, edges reroute correctly, nodes hide/show
- **Execution**: all nodes execute in correct order
- **Branching**: condition/switch follow the right path
- **Loops**: iterate correct number of times, variables update per iteration
- **Expressions**: functions resolve to expected values
- **Error handling** (08): onError=continue doesn't stop the workflow
- **User tasks** (07): workflow pauses, outcomes work, post-approval branching
- **Parallel** (09): all branches execute, then the `Done` edge runs once
- **Decision table** (16): inputs resolve from variables, matched rule + outputs land in the variable picker and drive branching
- **Data tables** (16): insert per loop iteration, get by row id, list with filter, update by matching a column value, delete; partial updates keep the other columns intact
