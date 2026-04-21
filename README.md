## 📦 YAML #1 — metric_1_1_unscored.yaml
```
version: 1.0.0

metric:
  id: metric.unscored_unrated_customer_count
  label: "1.1"
  name: "Total number of unscored or unrated customers in the unit"

sets:
  - id: set.population
    description: "Customers in RESL snapshot for review date and lifecycle 115"

  - id: set.rated
    description: "Customers present in scored customer dataset"

rules:
  - id: rule.population_filter
    logic: "cbs_effectv_dt = '2025-10-31' AND lifecy_cd = 115"

queries:
  - id: query.metric_1_1
    path: "sql/1_1_unscored.sql"

relationships:

  - type: derived_from
    from: metric.unscored_unrated_customer_count
    to: set.population

  - type: excludes
    from: metric.unscored_unrated_customer_count
    to: set.rated

  - type: sourced_from
    from: set.population
    to: dataset.ra_fy_2025.resl_full_gen_5

  - type: filtered_by
    from: set.population
    to: rule.population_filter

  - type: sourced_from
    from: set.rated
    to: dataset.rafy2025_centralized.scored_cust_cde_1_1_fy25

  - type: implemented_by
    from: metric.unscored_unrated_customer_count
    to: query.metric_1_1

  - type: scoped_to
    from: metric.unscored_unrated_customer_count
    to: assessable_unit.301270

  - type: owned_by
    from: metric.unscored_unrated_customer_count
    to: team.fcrm
```

## 📦 YAML #2 — metric_SD1.yaml
```
version: 1.0.0

metric:
  id: metric.sd1_customer_count
  label: "SD1"
  name: "Total number of customers in scope (lifecycle 115)"

sets:
  - id: set.sd1_population
    description: "Customers within date range and lifecycle 115"

rules:
  - id: rule.sd1_filter
    logic: "cbs_effectv_dt BETWEEN '2024-11-01' AND '2025-10-31' AND lifecy_cd = 115"

queries:
  - id: query.sd1
    path: "sql/sd1.sql"

relationships:

  - type: derived_from
    from: metric.sd1_customer_count
    to: set.sd1_population

  - type: sourced_from
    from: set.sd1_population
    to: dataset.ra_fy_2025.resl_full_gen_5

  - type: filtered_by
    from: set.sd1_population
    to: rule.sd1_filter

  - type: implemented_by
    from: metric.sd1_customer_count
    to: query.sd1

  - type: scoped_to
    from: metric.sd1_customer_count
    to: assessable_unit.301270

  - type: owned_by
    from: metric.sd1_customer_count
    to: team.fcrm
```

## YAML #3 — metric_SD3_split.yaml
```
version: 1.0.0

metric:
  id: metric.sd3_customer_split
  label: "SD3"
  name: "Customer count split by personal and non-personal (non-CA)"

sets:
  - id: set.resl_base
    description: "Base RESL population for snapshot date and lifecycle"

  - id: set.personal
    description: "Personal customers outside Canada"

  - id: set.nonpersonal
    description: "Non-personal customers resolved via incorporation/legal country"

rules:
  - id: rule.sd3_base_filter
    logic: "cbs_effectv_dt = '2025-10-31' AND lifecy_cd = 115"

queries:
  - id: query.sd3
    path: "sql/sd3.sql"

relationships:

  - type: sourced_from
    from: set.resl_base
    to: dataset.ra_fy_2025.resl_full_gen_5

  - type: filtered_by
    from: set.resl_base
    to: rule.sd3_base_filter

  - type: derived_from
    from: set.personal
    to: set.resl_base

  - type: derived_from
    from: set.nonpersonal
    to: set.resl_base

  - type: joined_with
    from: set.personal
    to: dataset.ra_adido_2025.country_ref_list_ca2025

  - type: joined_with
    from: set.nonpersonal
    to: dataset.ra_fy_2025.cif_non_personal_FY25_cpb_au

  - type: joined_with
    from: set.nonpersonal
    to: dataset.ra_fy_2025.cif_compl_npers_FY25_cpb_au

  - type: derived_from
    from: metric.sd3_customer_split
    to: set.personal

  - type: derived_from
    from: metric.sd3_customer_split
    to: set.nonpersonal

  - type: implemented_by
    from: metric.sd3_customer_split
    to: query.sd3

  - type: scoped_to
    from: metric.sd3_customer_split
    to: assessable_unit.301270

  - type: owned_by
    from: metric.sd3_customer_split
    to: team.fcrm
```

# 📦 YAML #4 — metric_CDE1_6.yaml
```
version: 1.0.0

metric:
  id: metric.cde1_6_high_risk_country
  label: "CDE1_6"
  name: "Customers associated with very high risk countries"

sets:
  - id: set.cde_population
    description: "Customers in RESL filtered for active lifecycle"

rules:
  - id: rule.cde_filter
    logic: "cbs_effectv_dt = '2025-10-31' AND lifecy_cd IN (114,116,117) AND cbs_country_mn <> 'CA'"

queries:
  - id: query.cde1_6
    path: "sql/cde1_6.sql"

relationships:

  - type: derived_from
    from: metric.cde1_6_high_risk_country
    to: set.cde_population

  - type: sourced_from
    from: set.cde_population
    to: dataset.ra_fy_2025.resl_full_gen_5

  - type: filtered_by
    from: set.cde_population
    to: rule.cde_filter

  - type: joined_with
    from: set.cde_population
    to: dataset.ra_fy25_view.sanctions_country_risk_rating_fy2025

  - type: classified_by
    from: metric.cde1_6_high_risk_country
    to: dataset.ra_fy25_view.sanctions_country_risk_rating_fy2025

  - type: implemented_by
    from: metric.cde1_6_high_risk_country
    to: query.cde1_6

  - type: scoped_to
    from: metric.cde1_6_high_risk_country
    to: assessable_unit.301270

  - type: owned_by
    from: metric.cde1_6_high_risk_country
    to: team.fcrm
```

## 📦 YAML #5 — metric_SD2_PEP.yaml
```
version: 1.0.0

metric:
  id: metric.sd2_pep_customers
  label: "SD2"
  name: "Customers identified as PEP"

sets:
  - id: set.resl_customers
    description: "Customers from RESL dataset"

  - id: set.pep_list
    description: "Customers identified in PEP list"

queries:
  - id: query.sd2_pep
    path: "sql/sd2_pep.sql"

relationships:

  - type: sourced_from
    from: set.resl_customers
    to: dataset.ra_fy_2025.resl_full_gen_5

  - type: sourced_from
    from: set.pep_list
    to: dataset.ra_adido_2025.pep_list_2025_exploded

  - type: joined_with
    from: set.pep_list
    to: set.resl_customers

  - type: derived_from
    from: metric.sd2_pep_customers
    to: set.pep_list

  - type: implemented_by
    from: metric.sd2_pep_customers
    to: query.sd2_pep

  - type: scoped_to
    from: metric.sd2_pep_customers
    to: assessable_unit.301270

  - type: owned_by
    from: metric.sd2_pep_customers
    to: team.fcrm
```
---

## The Scenario
You want to build a web app that pulls trending GitHub repos, displays them with stats, and lets you filter by language and category.

---

## Step 1: Install & Start
```bash
npx get-shit-done-cc --claude --global
claude  # open Claude Code
/gsd:new-project
```

GSD immediately starts **interviewing you**:

> *"I detected no existing code. Let's define your project. What are you building?"*

You say: *"A GitHub trending dashboard with filters for language, category, and time range. Show repo stats like stars, forks, and contributor count."*

GSD follows up with targeted questions per category:

> - **APIs/Data:** "Use GitHub's official API or scrape github.com/trending?"
> - **UI:** "Card layout or table view? Dark mode? Mobile responsive?"
> - **Stack:** "Any preference — React, Vue, plain HTML?"
> - **Auth:** "Do users need to save favorites or is it read-only?"

You answer each one. The output is a `CONTEXT.md` file — a locked spec that every downstream agent reads.

---

## Step 2: Research Phase
GSD spawns **4 parallel researcher agents**, each with a fresh 200k context window:

| Agent | What it investigates |
|---|---|
| Stack Researcher | Best libraries for GitHub API calls, rate limiting, caching |
| Features Researcher | How to handle unauthenticated vs authenticated API limits |
| Architecture Researcher | Component structure, state management options |
| Pitfalls Researcher | Known issues with GitHub API pagination, CORS, etc. |

Each writes results to `.planning/research/`. Then a **Synthesizer agent** reads all four and produces a `SUMMARY.md`. You never touched a token for any of this — it ran in the background.

---

## Step 3: Planning Phase
A **Planner agent** reads `CONTEXT.md` + `SUMMARY.md` and breaks the project into atomic phases:

```
PHASE 1 — Data Layer
  Task 1.1: GitHub API client with rate limit handling
  Task 1.2: Caching layer (localStorage, 5min TTL)

PHASE 2 — UI Components  
  Task 2.1: RepoCard component
  Task 2.2: FilterBar (language, time range)
  Task 2.3: Results grid with pagination

PHASE 3 — Integration & Polish
  Task 3.1: Wire filters to API calls
  Task 3.2: Loading states + error handling
  Task 3.3: Dark mode toggle
```

Each task is sized to fit in ~50% of a fresh context window. You review the plan and confirm.

---

## Step 4: Execution (The Magic Part)
GSD runs **Wave 1** — all independent tasks in parallel:

```
WAVE 1 (parallel):
  ├── Task 1.1: GitHub API client       ← fresh agent, 200k context
  ├── Task 1.2: Caching layer           ← fresh agent, 200k context
  └── Task 2.1: RepoCard component      ← fresh agent, 200k context

WAVE 2 (waits for Wave 1, then parallel):
  ├── Task 2.2: FilterBar               ← reads Wave 1 outputs
  └── Task 2.3: Results grid

WAVE 3:
  └── Task 3.x: Integration & polish    ← reads everything
```

Every task gets its own **git commit** when done:
```
✅ feat: GitHub API client with rate limiting
✅ feat: localStorage caching layer  
✅ feat: RepoCard component
✅ feat: FilterBar with language/time filters
...
```

---

## Step 5: Verification
A **Verifier agent** runs last — it doesn't ask "what did we build?" It asks **"what must be TRUE for this to work?"**

> - Does filtering by language actually change the API query?
> - Does the cache invalidate after 5 minutes?
> - Does the UI handle a 0-result response gracefully?
> - Does it work on mobile viewport?

If anything fails, it flags it with a specific task reference — not a vague "something's broken."

---

## What You End Up With
- A working app built to your exact spec
- Clean git history — every task is its own revertable commit
- `.planning/` folder documenting every decision made
- Zero context rot — task 12 was built with the same quality as task 1

---

## vs. Raw Claude Code (no GSD)

| | Raw Claude Code | With GSD |
|---|---|---|
| Task 1–5 | Great output | Great output |
| Task 10+ | Starts cutting corners | Same quality |
| Forgotten requirements | Common | Rare — locked in CONTEXT.md |
| Git history | Manual | Automatic per task |
| Parallelism | None | Wave-based |

The whole thing — from `/gsd:new-project` to working app — might take **45–60 minutes of wall time**, most of which is GSD working while you grab a coffee. That's the pitch.





```
Structural

derived_from
rollup_of
variant_of
equivalent_to
supersedes

Lineage

sourced_from
joined_with
filtered_by
excludes
classified_by
references

Implementation

implemented_by
uses_rule
uses_key

Business context

scoped_to
defined_as
owned_by
stewarded_by
```

The goal should be:

* easy for humans to understand
* expressive enough for lineage
* small enough to govern
* stable across metrics, sets, datasets, rules, and teams

## Recommended relationship taxonomy v1

## 1. Structural relationships

These describe how a metric or object is structurally composed.

### `derived_from`

Use when one object is logically produced from another.

Examples:

* metric derived from set
* metric derived from metric
* set derived from set

Example:

* `metric.1_1_unscored_unrated_customer_count` **derived_from** `set.population`

### `rollup_of`

Use when one metric is an aggregate of lower-level metrics.

Examples:

* total metric rollup of segment metrics
* business-unit metric rollup of child-unit metrics

### `variant_of`

Use when two metrics are the same general concept but differ by scope, segment, or treatment.

Examples:

* personal customer count is a variant of total customer count
* FY25 variant of a standard metric pattern

### `equivalent_to`

Use when two objects mean the same thing for business purposes.

Examples:

* two business terms that are synonyms
* replacement object treated as same semantic meaning

### `supersedes`

Use when one object replaces another over time.

Examples:

* new metric definition supersedes prior definition
* new rule supersedes previous rule

---

## 2. Lineage relationships

These describe where data comes from and how it moves.

### `sourced_from`

Use when an object directly uses a dataset, table, or source system as input.

Examples:

* set sourced from RESL table
* metric sourced from reference table

### `joined_with`

Use when a set or derivation depends on joining another set or dataset.

Examples:

* population joined with country reference
* resl joined with pep list

### `filtered_by`

Use when a set or metric is constrained by a rule.

Examples:

* population filtered by review date
* customer set filtered by lifecycle code

### `excludes`

Use when a final set or metric is formed by removing another set.

Examples:

* unscored customers excludes rated customers
* eligible population excludes closed accounts

This is especially important for your `1.1` case.

### `classified_by`

Use when a result depends on a classification reference or category mapping.

Examples:

* customer classified by risk rating
* country classified by sanctions risk

### `references`

Use when an object uses another object for lookup, supporting logic, or interpretation, but not as a direct lineage source.

Examples:

* rule references country code list
* metric references legal entity type table

---

## 3. Implementation relationships

These describe how the logic is technically realized.

### `implemented_by`

Use when a metric or set is implemented by SQL, notebook, pipeline, or job.

Examples:

* metric implemented by query
* set implemented by SQL view

### `uses_rule`

Use when an implementation depends on a reusable business or technical rule.

Examples:

* metric uses customer activity rule
* population uses personal/non-personal derivation rule

### `uses_key`

Use when an object depends on a specific business key or technical join key.

Examples:

* rated set uses `cust_cust_no`
* sanctions match uses normalized customer number

This one is optional for v1, but useful if you want medium lineage.

---

## 4. Business context relationships

These place the object in the governance and business model.

### `scoped_to`

Use when an object belongs to an assessable unit, segment, or scope boundary.

Examples:

* metric scoped to AU 301270
* data element scoped to customer risk section

### `defined_as`

Use when a metric or set is linked to a business term.

Examples:

* metric defined as “customer”
* set defined as “active customer population”

### `owned_by`

Use when a team is accountable for the object.

Examples:

* metric owned by FCRM
* rule owned by risk methodology team

### `stewarded_by`

Use when a team or person maintains the object operationally, even if they are not the business owner.

Examples:

* dataset stewarded by CAEDW
* rule stewarded by enterprise reporting

This can be optional if ownership is enough for now.

---

# Recommended top-level taxonomy structure

I would organize the taxonomy like this:

```yaml
version: 1.0.0
metadata:
  name: Relationship Taxonomy
  owner: FCRM Spec Library

relationship_categories:
  - structural
  - lineage
  - implementation
  - business_context

relationship_types:
  - type: derived_from
    category: structural
    description: Source object is logically derived using the target object.
    directional: true

  - type: rollup_of
    category: structural
    description: Source object is an aggregate of the target object.
    directional: true

  - type: variant_of
    category: structural
    description: Source object is a scoped or specialized form of the target object.
    directional: true

  - type: equivalent_to
    category: structural
    description: Source and target have the same business meaning.
    directional: false

  - type: supersedes
    category: structural
    description: Source replaces the target for ongoing use.
    directional: true

  - type: sourced_from
    category: lineage
    description: Source object uses target dataset or source as direct input.
    directional: true

  - type: joined_with
    category: lineage
    description: Source object is built using a join with the target object.
    directional: true

  - type: filtered_by
    category: lineage
    description: Source object is constrained by the target rule.
    directional: true

  - type: excludes
    category: lineage
    description: Source object is formed by excluding the target set or population.
    directional: true

  - type: classified_by
    category: lineage
    description: Source object uses the target classification or rating reference.
    directional: true

  - type: references
    category: lineage
    description: Source object refers to the target for lookup or supporting context.
    directional: true

  - type: implemented_by
    category: implementation
    description: Source object is implemented by the target query, pipeline, or job.
    directional: true

  - type: uses_rule
    category: implementation
    description: Source object depends on the target reusable rule.
    directional: true

  - type: uses_key
    category: implementation
    description: Source object depends on the target join or business key.
    directional: true

  - type: scoped_to
    category: business_context
    description: Source object belongs to the target assessable unit or scope.
    directional: true

  - type: defined_as
    category: business_context
    description: Source object is defined using the target business term.
    directional: true

  - type: owned_by
    category: business_context
    description: Source object is accountable to the target team.
    directional: true

  - type: stewarded_by
    category: business_context
    description: Source object is maintained by the target steward or team.
    directional: true
```

# My recommendation for v1 scope

For your first version, I would actually use only these 9 in practice:

* `derived_from`
* `sourced_from`
* `filtered_by`
* `joined_with`
* `excludes`
* `implemented_by`
* `scoped_to`
* `defined_as`
* `owned_by`

That is enough to cover most of the SQL you showed without overcomplicating things.

# How this applies to your examples

## 1.1 Unscored customers

You would likely use:

* `derived_from`
* `excludes`
* `sourced_from`
* `filtered_by`
* `implemented_by`
* `scoped_to`
* `owned_by`

## SD1

You would likely use:

* `derived_from`
* `sourced_from`
* `filtered_by`
* `implemented_by`
* `scoped_to`

## SD3

You would likely use:

* `derived_from`
* `joined_with`
* `classified_by`
* `sourced_from`
* `implemented_by`
* maybe `variant_of` if you split personal and non-personal into separate metrics

# Important design guidance

A few rules I would strongly suggest:

### 1. Relationship names should be verbs or verb phrases

Good:

* `derived_from`
* `sourced_from`
* `owned_by`

Avoid vague names like:

* `related_to`
* `linked_to`

Those become meaningless quickly.

### 2. Prefer directed relationships

Most of yours should be directional.

Good:

* metric → sourced_from → dataset
* set → filtered_by → rule

Only a few should be non-directional, like `equivalent_to`.

### 3. Keep relationship meaning singular

Each relationship type should mean one thing only.

For example:

* `sourced_from` should not also mean “joined with and filtered by”
* `derived_from` should not mean “same as”

### 4. Keep the taxonomy smaller than you think

A small clean taxonomy is much better than 40 subtly overlapping relationship types.

# My suggested final taxonomy summary

If I reduce it to the simplest useful form:

**Structural**

* `derived_from`
* `rollup_of`
* `variant_of`
* `equivalent_to`
* `supersedes`

**Lineage**

* `sourced_from`
* `joined_with`
* `filtered_by`
* `excludes`
* `classified_by`
* `references`

**Implementation**

* `implemented_by`
* `uses_rule`
* `uses_key`

**Business context**

* `scoped_to`
* `defined_as`
* `owned_by`
* `stewarded_by`

