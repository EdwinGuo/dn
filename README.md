ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILa4sqwPAfEqjocTfcjOL5cAAVsYDPyzY2idSenRg8P0 edwinguo@edwinguos-MacBook-Pro-2.local
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
