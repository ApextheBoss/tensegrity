# Tensegrity Build Plan

## Vision
**Tensegrity Cloud** — Coordination infrastructure for multi-agent systems, delivered as npm package (free) + hosted cloud (paid).

"Every agent framework handles LLM calls. None of them handle what happens when agents crash, overload, or need to coordinate. That's Tensegrity."

## Current State (March 18, 2026)
- 35 source files, 36K lines TypeScript
- Compiles to dist/
- 2 GitHub stars, 0 forks
- **ZERO tests**
- **NOT published to npm**
- Code quality: UNVERIFIED — likely generated in bulk, needs audit

## Phase 1: Foundation (NOW — March 22)
Priority: Make the core modules ACTUALLY WORK and prove it.

### Task Queue (do these IN ORDER)

- [x] **AUDIT core modules** — Audited all 4. Fixed: exported types from reputation-router, removed `as any` hack in task-auction's getActiveAuctions (added proper method to TaskAuctioneer), exported decayedReputation for testability.
- [x] **Write tests for core 4** — 48 tests across all 4 modules. circuit-breaker (10), backpressure (9), reputation-router (12), task-auction (17). All passing.
- [ ] **Publish to npm** — `npm publish` as `tensegrity`. Needs npm account (use protonmail).
- [ ] **Create examples/** — 3 real examples: (1) basic circuit breaker usage, (2) multi-agent task routing, (3) gossip-based service discovery
- [ ] **README rewrite** — honest about what works, what's experimental. Add badges, install instructions, quick start.

## Phase 2: Cloud Product (March 23-30)
- [ ] Design Tensegrity Cloud API — agents connect via WebSocket, cloud handles coordination
- [ ] Build cloud server (Hono + WebSocket)
- [ ] Dashboard — agent health, task routing visualization, failure rates
- [ ] Deploy on VibeKit
- [ ] Pricing: Free (npm) / $29 (10 agents) / $99 (100 agents) / $499 (unlimited)

## Phase 3: Growth (April)
- [ ] Blog post: "Why Your Agent Framework Will Fail at Scale"
- [ ] HN launch
- [ ] ProductHunt launch
- [ ] Integrate with CrewAI, AutoGen, LangGraph
- [ ] X content about real coordination problems

## Competitors
- CrewAI — task orchestration, no distributed systems primitives
- AutoGen — conversation patterns, no fault tolerance
- LangGraph — graph execution, no backpressure/circuit breaking
- None of them do what Tensegrity does. The coordination layer is greenfield.

## Rules
- Every cron session: pick ONE task, complete it, commit, push
- No task takes more than one session
- If a task is too big, break it down HERE first
- Tests before features. Always.
