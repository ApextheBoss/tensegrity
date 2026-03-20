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
- [x] **Create examples/** — 3 real examples: (1) basic circuit breaker usage, (2) multi-agent task routing, (3) gossip-based service discovery
- [x] **README rewrite** — honest about what works, what's experimental. Add badges, install instructions, quick start. Split modules into tested ✅ vs experimental. 719 tests, 20 suites.

## Quality Audit Findings (March 19, 2026 — automated cron)

**Status:** 323/323 tests passing ✅ | TypeScript compiles clean (0 errors) ✅ | No runtime errors

### Bugs Fixed (March 19 — AM audit)

5. **CausalEventLog: double-tick on local emit (FIXED)**
   - `emit()` ticked the clock, then `deliver()` called `merge()` which ticked again
   - Result: local event clocks skipped values (1,3,5...) instead of (1,2,3...)
   - Broke causal ordering for any consumer receiving events — out-of-order events couldn't be delivered because sequence gaps made `canDeliver` fail
   - **Fix:** `deliver()` now only merges clock for remote events; local events already have the correct clock from `emit()`

### Bugs Fixed (March 19 — earlier)

4. **TypeScript compilation errors in test files (FIXED)**
   - `destroy-methods.test.ts` used stale interface shapes for `FederatedRequest`, `FederationConfig`, and `FederationPeer` — fields renamed/added in source but tests never updated
   - `distributed-lock-manager.test.ts` used `Partial<typeof PRESETS['fast-locks']>` which produced literal types from `as const`, making overrides with different values fail type checks — changed to `Partial<LockManagerConfig>`
   - Tests still passed at runtime (Vitest doesn't type-check) but `tsc --noEmit` had 12 errors

### Bugs Found & Fixed (March 21, 2026 — cron audit)

11. **BTreeIndex: remove() only decrements size when entire key is emptied (FIXED)**
    - `remove(key, docId)` filtered out the docId from the values array, but only decremented `this.size` when `values[i].length === 0` (entire key removed)
    - `insert()` increments `size` on every call, so size tracks total entries, not keys
    - Result: after inserting 3 entries under the same key and removing 1, `getSize()` returned 3 instead of 2
    - **Fix:** Compute `removed = before - after` from the filter and decrement size by that amount

### Remaining Issues

### Bugs Found & Fixed (March 20, 2026 — PM audit)

10. **ServiceDiscoveryMesh: tick() previousHealth captured new state instead of old (FIXED)**
    - `tick()` saved `oldScore` but then overwrote `inst.health` via `probe()`, then built `previousHealth` by spreading `inst.health` (already new) and only overriding `score`
    - Result: `previousHealth` in watch notifications had new `alive`, `ready`, `latencyEwma`, `consecutiveFailures` fields — only `score` was actually old
    - **Fix:** Capture full `previousHealth = { ...inst.health }` before calling `probe()`

### Bugs Found & Fixed (March 20, 2026 — AM audit)

7. **Vitest picking up compiled dist/__tests__/ files (FIXED)**
   - No vitest.config.ts existed, so vitest's default include glob matched `dist/__tests__/*.test.js`
   - These CommonJS files can't import vitest, causing 16 test file failures
   - **Fix:** Added `vitest.config.ts` with explicit `include: ['src/__tests__/**/*.test.ts']` and `exclude: ['dist/**']`

8. **ResourcePoolManager: burst tracking never consumed (FIXED)**
   - `getBurstAllowance()` checked `tracking.used` but nothing ever incremented it
   - Result: agents could burst indefinitely within a window — burst allowance was effectively infinite
   - **Fix:** `checkQuota()` now increments `tracking.used` when allocation exceeds base `maxAllocation`

9. **ResourcePoolManager: unused variable in computeFairShare (FIXED)**
   - `currentUsage` was computed but never used in the fair-share calculation
   - Removed dead code

6. **ObservableStateMachine: tick() only notifies observers for fatal invariant violations**
   - `InvariantChecker.check()` returns `false` only when severity is `'fatal'`
   - `tick()` only calls `observers.notify()` when `check()` returns `false`
   - Result: `'error'` and `'warning'` invariant violations are recorded in `getInvariantViolations()` but observers never receive notification
   - **Recommendation:** Notify observers for ALL violations, not just fatal ones. Use the `check()` return value only for deciding whether to halt the machine.

### Bugs Found (earlier)

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

11. **QueryPlanner doesn't handle 'covering' index type (BUG)**
    - `plan()` switch cases for query types only check `idx.type === 'hash' | 'btree' | 'inverted'`
    - Indexes with `type: 'covering'` never match any case, so queries always fall back to fullscan
    - Covering indexes store data correctly (including payload from coveringFields) but are never selected by the planner
    - **Recommendation:** Treat 'covering' the same as 'hash' in the planner's type checks

5. **Duplicate utility classes in 3 files** — `EWMATracker` and `WelfordStats` are still duplicated in `lease-consensus.ts`, `eventually-consistent-index.ts`, and `transactional-outbox.ts` instead of importing from `shared-utils.ts`

### Missing Test Coverage (14 of 35 modules untested)
- 22 modules have tests: circuit-breaker, backpressure, reputation-router, task-auction, distributed-lock-manager, gossip-protocol-engine, shared-utils, destroy-methods, causal-broadcast, crdt-registry, lease-consensus, vector-clock-causality, work-queue-exactly-once, transactional-outbox, observable-state-machine, adaptive-work-stealing, resource-pool-manager, adaptive-routing-mesh, service-discovery-mesh, backoff-coordinator, adaptive-throttle-governor, eventually-consistent-index
- ~~High-priority untested: chaos-testing-harness~~ ✅ 57 tests added
- `state-machine.ts` is a duplicate of `observable-state-machine.ts` (pre-fix version) — skip
- ~~`eventually-consistent-index.ts`~~ ✅ 64 tests added
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
- [x] Fix TypeScript compilation errors in test files (12 errors from stale types + literal type conflicts)
- [x] Remove remaining duplicate EWMATracker/WelfordStats from lease-consensus, eventually-consistent-index, transactional-outbox
- [x] Add tests for causal-broadcast (41 tests), crdt-registry (31 tests), lease-consensus (43 tests)
- [x] Fix CausalEventLog double-tick bug — local emit was double-ticking clock via merge
- [x] Add tests for vector-clock-causality (38 tests covering clocks, DVVs, matrix clocks, barriers, event log, stability, conflict strategies)
- [x] Add tests for work-queue-exactly-once (37 tests covering enqueue/dedup, claim/priority, complete/fence, fail/retry, lease renewal/expiry, compaction, DLQ, poison pill, partitioning, exactly-once guarantees)
- [x] Add tests for transactional-outbox (44 tests covering all 9 subsystems: OutboxStore, IdempotencyRegistry, OrderingGuaranteeManager, DeadLetterHandler, CDC, CompactionManager, PartitionRouter, RelayDispatcher, TransactionalOutboxEngine integration)
- [x] Add tests for observable-state-machine (78 tests covering StateRegistry, TransitionEngine, ObserverManager, TimeoutManager, InvariantChecker, TransitionLog, SnapshotManager, DeadlockDetector, ParallelRegionCoordinator, core machine lifecycle, guards/actions, hierarchical states with history, priority resolution, wildcard transitions, all 3 preset machines)
- [x] Fix: observable-state-machine tick() only notifies observers for fatal invariant violations — changed check() to return {anyViolation, fatal}, tick() now notifies for all severities
- [x] Add tests for distributed-barrier-synchronizer (62 tests covering all 8 subsystems + found/fixed falsy-zero openedAt bug in tick())
- [x] Add tests for adaptive-work-stealing (62 tests covering all 9 subsystems: WorkDeque, TopologyCostModel, AffinityTracker, VictimSelector, LoadImbalanceDetector, TaskFragmentationAnalyzer, StealPolicyController, TaskSplitter, AdaptiveWorkStealingPool + presets)
- [x] Add vitest.config.ts to exclude dist/ from test discovery (was causing 16 false test failures)
- [x] Add tests for resource-pool-manager (44 tests covering allocation, release, renewal, preemption, quotas, burst tracking, expiry reclamation, fair-share, health monitoring, scaling triggers, priority wait queue, destroy, templates)
- [x] Fix ResourcePoolManager burst tracking bug — burst allowance was never consumed, allowing infinite burst within a window
- [x] Fix ResourcePoolManager unused variable in computeFairShare
- [x] Add tests for adaptive-routing-mesh (60 tests covering all 9 subsystems: TopologyTracker, LatencyPredictor, PathScorer, RouteCache, MultiPathRouter, CongestionDetector, FailureCorrelator, TrafficShaper, AdaptiveRoutingEngine + presets)
- [x] Fix adaptive-routing-mesh Dijkstra bug — compared weighted distance against maxHops instead of hop count, breaking all multi-hop routing
- [x] Add tests for service-discovery-mesh (41 tests covering LocalRegistry, HealthChecker, LocalityScorer, WatchManager, GossipDisseminator, SplitBrainDetector, full mesh lifecycle, gossip round-trip, query filtering by attributes/version/load)
- [x] Fix ServiceDiscoveryMesh tick() previousHealth bug — was capturing new health state instead of old
- [x] Add tests for backoff-coordinator (30 tests covering BackoffCalculator strategies, BackoffCoordinator lifecycle, correlated failure detection, escalation/blackout, dependency inheritance, SlotManager, CorrelationDetector, presets)
- [x] Add tests for chaos-testing-harness (57 tests covering MetricCollector, HypothesisEvaluator, BlastRadiusController, KillSwitchMonitor, TargetResolver, FaultInjector, PreflightChecker, ExperimentEngine full lifecycle, GameDayCoordinator, pre-built scenarios)
- [x] Add tests for eventually-consistent-index (64 tests covering InvertedIndex, BTreeIndex, HashIndex, IndexVersionVector, ConflictResolver, QueryPlanner, StaleReadDetector, IndexCompactor, ConvergenceChecker, full orchestrator)
- [x] Add tests for eventually-consistent-index (64 tests covering InvertedIndex BM25/search/remove/prefix, BTreeIndex insert/search/range/remove/splits, HashIndex insert/lookup/unique/remove, IndexVersionVector increment/merge/dominates/divergence, ConflictResolver lww/highest_version/merge_union/priority, QueryPlanner hash/btree/fullscan/covering, StaleReadDetector stale/fresh/rate, full orchestrator upsert/delete/range/fulltext/remote-updates/convergence/tick/rebuild/sparse/presets)
- [x] Fix BTreeIndex.remove() size tracking bug — size was only decremented when entire key was emptied, not when individual entries were removed. After inserting N entries under the same key and removing one, getSize() was still N instead of N-1.

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

## Competitive Landscape (Updated March 20, 2026)

### Direct npm Competitors (multi-agent coordination space)
- **swarm-mail** (joelhooks) — 3.3K monthly downloads, 2 dependents. Event sourcing primitives for multi-agent coordination. Local-first, no external servers. Overlap: coordination primitives. Difference: event-sourcing focused, not distributed systems.
- **swarm-queue** (joelhooks) — 3.3K monthly downloads. Distributed job queue using BullMQ + Redis. Requires Redis (external dependency). Tensegrity is zero-dep.
- **@grackle-ai/web** — 4.3K weekly downloads, actively maintained (updated today). React dashboard for multi-agent coordination platform. Full platform play with UI. Potential competitor if they expand beyond dashboard.
- **opencastle** — 6.6K monthly downloads. Multi-agent orchestration for AI coding assistants. BUSL-1.1 license (not open source). Different niche (coding agents).
- **myagents** — 2.3K monthly. BMAD multi-agent orchestration. Description-to-code pipeline. Different layer.
- **opensquad** — 1.8K monthly. Claude Code multi-agent orchestration.

### Adjacent (resilience libraries, NOT agent-specific)
- **cockatiel** — 4.4M monthly downloads, 153 dependents. Circuit breaker, retry, backoff, timeout, bulkhead. The established resilience library for Node.js (Polly-inspired). Tensegrity's circuit breaker competes here but cockatiel doesn't do agent coordination, gossip, consensus, etc.
- **@fastify/circuit-breaker** — 33K monthly. Fastify-specific.

### Framework-level (Python-dominant, not npm)
- **CrewAI** — Task orchestration, no distributed systems primitives
- **AutoGen** — Conversation patterns, no fault tolerance
- **LangGraph** — Graph execution, no backpressure/circuit breaking
- **Temporal.io** — Workflow orchestration (not agent-specific but solves some overlapping problems at scale). Cloud product, VC-funded.

### Key Insight
**Nobody owns the "distributed systems primitives for agents" niche in the npm ecosystem.** The closest competitors are either:
1. Event-sourcing focused (swarm-mail) with Redis deps
2. General resilience (cockatiel) without agent awareness
3. Full platforms (grackle) that bundle UI + coordination
4. Python-only (CrewAI, AutoGen, LangGraph)

Tensegrity's unique position: **zero-dependency, TypeScript-native, 35 composable modules** covering the full stack from circuit breakers to BFT consensus. No one else has this breadth in JS/TS.

### Pricing & Positioning Strategy

**npm package (Free, MIT):** The wedge. Get adoption through the open-source library. Target developers already using CrewAI/AutoGen/LangGraph who need coordination primitives in their JS/TS stack.

**Tensegrity Cloud (Paid):** The business.
- **Solo** — Free: npm only, self-hosted, community support
- **Team** — $49/mo: Up to 25 agents, managed WebSocket coordination, dashboard, 7-day metric retention
- **Pro** — $149/mo: Up to 250 agents, 90-day retention, priority support, SLA
- **Enterprise** — $499/mo: Unlimited agents, SSO, audit logs, dedicated support, custom SLA

*Revised from original $29/$99/$499 — the $29 tier was too cheap for B2B SaaS. $49 entry captures indie/startup teams; $149 is the sweet spot for growing companies.*

**Cloud differentiators vs self-hosting:**
- Zero-config WebSocket mesh (no infra to manage)
- Real-time dashboard (agent health, task routing, failure cascades)
- Hosted gossip protocol (agents just connect, cloud handles discovery)
- Alerting & anomaly detection on coordination metrics
- Cross-region agent coordination

### Go-to-Market Priority
1. **Publish npm + examples + README** (Phase 1 — in progress)
2. **Blog: "Why Your Agent Framework Will Fail at Scale"** — position as thought leader
3. **HN launch** — target distributed systems crowd, they'll appreciate the depth
4. **Integration guides** for CrewAI, AutoGen, LangGraph — meet users where they are
5. **Cloud beta** — invite early npm users, iterate on what they actually need

## Rules
- Every cron session: pick ONE task, complete it, commit, push
- No task takes more than one session
- If a task is too big, break it down HERE first
- Tests before features. Always.
