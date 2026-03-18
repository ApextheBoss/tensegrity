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

## Quality Audit Findings (March 18, 2026 — automated cron)

**Status:** 48/48 tests passing ✅ | TypeScript compiles clean ✅ | No runtime errors

### Bugs Found

1. **CircuitBreaker: success resets sliding window (design issue)**
   - `onSuccess()` in closed state sets `failures = 0` and clears `failureTimestamps[]`
   - This means failures must be *consecutive* to trip the breaker — a single success resets everything
   - A bursty pattern like `fail, fail, success, fail, fail, success, fail` never trips threshold=3
   - The sliding `monitorWindowMs` is effectively useless since successes wipe it
   - **Recommendation:** Only prune failures by time window, not on success. Success should not clear the window.

2. **CircuitBreakerRegistry.get() silently ignores config on existing breakers**
   - If you call `registry.get('agent-a', { failureThreshold: 5 })` and then later `registry.get('agent-a', { failureThreshold: 1 })`, the second config is silently ignored
   - **Recommendation:** Either warn/throw when config differs, or document that config is first-call-only

3. **BackpressureController: drop-oldest still counts dropped message in inRate**
   - When `drop-oldest` fires, the *old* message was already recorded as `recordIn()`, and the *new* message also gets `recordIn()`. The dropped old message inflates the historical in-rate
   - Minor issue, but `inRate` in metrics will be slightly inaccurate under sustained drop-oldest pressure

### Missing Test Coverage (31 of 35 modules untested)
- Only 4 modules have tests: circuit-breaker, backpressure, reputation-router, task-auction
- High-risk untested: distributed-lock-manager, gossip-protocol-engine, crdt-registry, lease-consensus
- All untested modules compile and export correctly — but no behavioral verification

### Architecture Observations
- 36K lines of TypeScript across 35 modules — very large surface area for 0 users
- Many modules duplicate utilities (fnv1a hash, EWMATracker, WelfordStats) — could extract to shared utils
- No timer/interval cleanup in modules that don't have explicit destroy() methods
- index.ts exports are comprehensive and match source files

### Next Actions
- [x] Fix CircuitBreaker sliding window bug (stop clearing on success) — success no longer clears failureTimestamps in closed state; failures age out via monitorWindowMs
- [x] Add tests for distributed-lock-manager (36 tests covering all subsystems)
- [x] Add tests for gossip-protocol-engine (65 tests covering all 7 subsystems)
- [x] Extract shared utilities (fnv1a, EWMA, Welford) into common module — created src/shared-utils.ts, removed duplicates from 25 files, 12 new tests
- [x] Add dispose/destroy methods to all modules with timers — added destroy() to FederationRouter, RequestCoalescer, ResourcePoolManager; 7 new tests

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
