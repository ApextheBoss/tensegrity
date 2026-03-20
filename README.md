# tensegrity

[![npm](https://img.shields.io/npm/v/tensegrity)](https://www.npmjs.com/package/tensegrity)
[![tests](https://img.shields.io/badge/tests-719%20passing-brightgreen)](https://github.com/nicobailon/tensegrity)
[![zero dependencies](https://img.shields.io/badge/dependencies-0-brightgreen)](https://www.npmjs.com/package/tensegrity)
[![TypeScript](https://img.shields.io/badge/TypeScript-strict-blue)](https://www.typescriptlang.org/)
[![license](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

**Distributed systems primitives for multi-agent coordination. Zero dependencies. Pure TypeScript.**

```
npm install tensegrity
```

## Why

Every agent framework handles the LLM calls. None of them handle what happens next.

- Agent goes down → three dependents fail → cascade kills the system in 8 seconds
- Fast producer, slow consumer → queue fills → memory spikes → crash
- Two agents need shared state → you Google "distributed consensus" → 47 PhD papers

These aren't hypothetical. These are Tuesday. And every team building multi-agent systems solves them from scratch, badly, in application code that should be infrastructure.

Tensegrity is that infrastructure. 34 composable modules covering everything from circuit breakers to BFT consensus, all in one zero-dependency TypeScript package.

## Quick Start

### Circuit breaker between agents

```typescript
import { CircuitBreaker } from 'tensegrity';

const breaker = new CircuitBreaker('agent-0x1234', {
  failureThreshold: 3,
  resetTimeoutMs: 30000,
  monitorWindowMs: 60000
});

try {
  const result = await breaker.execute(() => agent.doSomething());
} catch (err) {
  // After 3 failures within the window, breaker opens and fast-fails for 30s
}
```

### Backpressure control

```typescript
import { BackpressureController } from 'tensegrity';

const bp = new BackpressureController({
  maxQueueDepth: 1000,
  highWaterMark: 0.8,
  strategy: 'throttle'
});

const accepted = await bp.enqueue({
  id: 'msg-1', payload: data, priority: 5,
  enqueuedAt: Date.now(), sender: 'agent-A'
});
```

### Task auction

```typescript
import { TaskAuctioneer } from 'tensegrity';

const auctioneer = new TaskAuctioneer(config);

auctioneer.submitBid({
  agentId: 'agent-A', price: 0.02,
  estimatedLatencyMs: 200, capabilities: ['gpt-4']
});
auctioneer.submitBid({
  agentId: 'agent-B', price: 0.01,
  estimatedLatencyMs: 50, capabilities: ['gpt-4', 'vision']
});

const winner = auctioneer.resolve();
```

### Gossip-based service discovery

```typescript
import { createGossipEngine, createMesh } from 'tensegrity';

const gossip = createGossipEngine('agent-001', 'medium-network');
gossip.membership.addMember({
  id: 'agent-002', address: 'ws://...', metadata: {}, generation: 1, heartbeat: 0
});

const mesh = createMesh('node-1', 'medium-network');
mesh.register({
  instanceId: 'compute-001', serviceType: 'agent.compute',
  serviceName: 'GPT Worker', agentAddress: '0x...',
  endpoint: 'ws://...', version: '1.2.0',
  locality: { region: 'us-east', zone: 'a' }
});

const best = mesh.resolveOne('agent.compute', { region: 'us-east' });
```

More examples in [`examples/`](./examples/).

## Modules

### Core Primitives (tested ✅, start here)

| Module | What It Does |
|--------|-------------|
| **CircuitBreaker** | 3-state circuit breaker with sliding window failure tracking |
| **BackpressureController** | 4 strategies: drop-newest, drop-oldest, reject, throttle. Priority queuing. |
| **TaskAuctioneer** | Sealed-bid and open ascending auctions for task allocation |
| **ReputationWeightedRouter** | Routes work based on track record with exponential decay |

### Distributed Systems (tested ✅)

| Module | What It Does |
|--------|-------------|
| **GossipEngine** | SWIM failure detection + Plumtree gossip + Merkle anti-entropy |
| **VectorClockCausality** | Happened-before tracking, dotted version vectors, matrix clocks, causal barriers |
| **DistributedLockManager** | Bakery, Maekawa quorum, Redlock, intention locks, deadlock detection |
| **CausalBroadcast** | Reliable causal delivery with partition-aware broadcasting |
| **LeaseConsensus** | Lease-based consensus with conflict resolution |
| **CRDTRegistry** | Conflict-free replicated data types for eventual consistency |

### Resource Management (tested ✅)

| Module | What It Does |
|--------|-------------|
| **ResourcePoolManager** | Shared pools with fair allocation, preemption, burst tracking, auto-scaling |
| **AdaptiveWorkStealing** | Work-stealing pool with topology awareness and task splitting |
| **AdaptiveRoutingMesh** | Multi-path routing with congestion detection and failure correlation |
| **BackoffCoordinator** | Coordinated retry with jitter, correlation detection, backoff inheritance |

### Reliability (tested ✅)

| Module | What It Does |
|--------|-------------|
| **DistributedBarrierSynchronizer** | Phased workflows with tree aggregation and straggler detection |
| **ObservableStateMachine** | State machines with observers, invariants, deadlock detection, parallel regions |
| **TransactionalOutbox** | Exactly-once event publishing with dead-letter queues and CDC |
| **ExactlyOnceQueue** | Bloom filter dedup, lease-based visibility, fencing tokens, poison pill detection |
| **ServiceDiscoveryMesh** | Decentralized service registry, gossip-based, health-aware, locality-scored |

### Experimental (compiles, no tests yet)

These modules are functional but haven't been through the test gauntlet. Use with caution and please report issues.

| Module | What It Does |
|--------|-------------|
| **HierarchicalConsensus** | BFT consensus with intra-shard and cross-shard coordination |
| **ConsensusViewSynchronizer** | BFT view sync with adaptive pacemaker and leader reputation |
| **AdaptiveThrottleGovernor** | TCP-style rate control (AIMD + Vegas + CoDel + PI controller) |
| **ResourceContentionArbiter** | Game-theoretic allocation: Vickrey auctions, Nash bargaining |
| **ChaosTestingHarness** | Fault injection and GameDay orchestration |
| **NetworkPartitioner** | Phi-accrual failure detection, split-brain resolution |
| **FederationRouter** | Cross-network coordination with per-network rate budgets |
| **ContractUpgradeProxy** | Hot-swap agent protocols with migration pipelines |
| **TokenEconomyEngine** | Token micro-economy with minting, staking, AMM |
| **AgentCapabilityMarketplace** | Service marketplace with escrow and dispute arbitration |
| **SchedulerAffinityGraph** | Task-to-agent affinity learning |
| **CapabilityHealthMonitor** | Health tracking with predictive failure analysis |
| **AutonomousTaskDecomposer** | Automatic task decomposition |
| **EventuallyConsistentIndex** | Secondary indexes with convergence checking |

## Design Principles

**Zero dependencies.** The entire package is pure TypeScript. No native modules, no C++ bindings, no supply chain risk.

**Presets over config.** Every module ships with presets (conservative, balanced, aggressive). Start with a preset, customize when you need to.

**Independently importable.** Use the circuit breaker without pulling in the gossip engine. Tree-shaking friendly.

**Not a framework.** Tensegrity doesn't manage prompts, chains, tool calls, or LLM providers. Use it alongside CrewAI, AutoGen, LangGraph, or your own framework. It handles the coordination problems they ignore.

## Status

**v0.1.0** — 719 tests across 20 test suites. Core modules (circuit breakers, backpressure, gossip, distributed locks, CRDTs, work stealing, routing, state machines) are tested and working. Experimental modules compile and export correctly but need test coverage. APIs may change before v1.0.

## License

MIT
