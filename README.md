# tensegrity

[![npm](https://img.shields.io/npm/v/tensegrity)](https://www.npmjs.com/package/tensegrity)
[![zero dependencies](https://img.shields.io/badge/dependencies-0-brightgreen)](https://www.npmjs.com/package/tensegrity)
[![license](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

**The missing coordination layer for multi-agent systems.**

Every agent framework handles the LLM calls. None of them handle what happens when your agents crash mid-task, overwhelm each other with messages, or need to agree on who does what. Tensegrity fixes that.

35 production-grade modules. Zero dependencies. Pure TypeScript.

```
npm install tensegrity
```

## The Problem

You have 10 agents. One goes down. Now the three agents that depend on it start failing. Their dependents fail too. Your whole system cascades into nothing in 8 seconds.

Or: you have a fast agent producing tasks and a slow agent consuming them. The fast one doesn't know to slow down. Queue fills up. Memory spikes. Everything dies.

Or: you need two agents to coordinate on shared state without a central server. You Google "distributed consensus" and find 47 PhD papers.

These aren't hypothetical. These are Tuesday. And every team building multi-agent systems is solving them from scratch, badly, in application code that should be infrastructure.

## What's In The Box

### Core Primitives (start here)

| Module | What It Does |
|--------|-------------|
| **CircuitBreaker** | Prevents cascading failures. 3-state machine (closed/open/half-open) with sliding window failure tracking. |
| **BackpressureController** | Stops fast producers from killing slow consumers. 4 strategies: drop-newest, drop-oldest, reject, throttle. Priority queuing built in. |
| **TaskAuctioneer** | Agents bid on tasks. Sealed-bid and open ascending formats. Bid validation, timeout, winner selection. |
| **ReputationWeightedRouter** | Routes work to agents based on track record. Good agents get more work. Bad agents get less. Exponential decay so recent performance matters most. |

### Distributed Systems

| Module | What It Does |
|--------|-------------|
| **GossipEngine** | SWIM failure detection + Plumtree hybrid gossip + Merkle anti-entropy. 7 composable subsystems. |
| **VectorClockCausality** | True happened-before tracking. Dotted version vectors, matrix clocks, causal barriers, stability detection. |
| **DistributedLockManager** | Mutual exclusion without central coordination. Bakery algorithm, Maekawa quorum, Redlock, intention locks, deadlock detection. |
| **HierarchicalConsensus** | BFT consensus with intra-shard agreement, cross-shard coordination, and meta-consensus. |
| **ConsensusViewSynchronizer** | BFT view sync with adaptive pacemaker, leader reputation, timeout certificates. |
| **CausalBroadcast** | Reliable causal delivery with partition-aware broadcasting and gossip repair. |

### Resource Management

| Module | What It Does |
|--------|-------------|
| **ResourcePoolManager** | Shared resource pools (connections, compute, API quotas) with fair allocation, preemption, auto-scaling triggers. |
| **ResourceContentionArbiter** | Game-theoretic allocation. Vickrey auctions, Nash bargaining, Wait-Die deadlock prevention, starvation detection. |
| **AdaptiveThrottleGovernor** | TCP-style rate control (AIMD + Vegas latency gradient + CoDel + PI controller). Multi-tenant fair-share. |
| **AdaptiveWorkStealing** | Work-stealing thread pool for agents. Topology-aware, affinity-tracked, with task splitting. |
| **SchedulerAffinityGraph** | Task-to-agent affinity tracking. Learns which agents are best at which tasks over time. |

### Reliability

| Module | What It Does |
|--------|-------------|
| **ChaosTestingHarness** | Chaos engineering for agents. Inject faults, verify hypotheses, GameDay orchestration. |
| **BackoffCoordinator** | Coordinated retry with jitter. Correlation detection across agents. Backoff inheritance for dependent services. |
| **NetworkPartitioner** | Phi-accrual failure detection, controlled partition simulation, split-brain resolution, healing coordination. |
| **DistributedBarrierSynchronizer** | Barriers for phased multi-agent workflows. Tree aggregation, straggler detection, fuzzy barriers. |

### Infrastructure

| Module | What It Does |
|--------|-------------|
| **ServiceDiscoveryMesh** | Decentralized service registry. Gossip-based, health-aware, locality-scored. No central server. |
| **FederationRouter** | Cross-network agent coordination. Per-network rate budgets, quota negotiation, request coalescing. |
| **TransactionalOutbox** | Exactly-once event publishing from agent state changes. Dead-letter queues, CDC, compaction. |
| **ExactlyOnceQueue** | Distributed work queue. Bloom filter dedup, lease-based visibility, fencing tokens, poison pill detection. |
| **ContractUpgradeProxy** | Hot-swap agent protocols. Facet management, migration pipelines, timelock governance, emergency rollback. |

### Agent Economy

| Module | What It Does |
|--------|-------------|
| **TokenEconomyEngine** | Token micro-economy. Minting (fixed/inflationary/bonding curve), staking with slashing, payment channels, AMM, revenue sharing. |
| **AgentCapabilityMarketplace** | Service marketplace with pricing engine, escrow, matching, reputation gates, dispute arbitration, usage metering. |

### State & Observability

| Module | What It Does |
|--------|-------------|
| **CRDTRegistry** | Conflict-free replicated data types for eventual consistency without coordination. |
| **ObservableStateMachine** | State machines with observers, invariant checking, deadlock detection, parallel regions. |
| **EventuallyConsistentIndex** | Eventually consistent secondary indexes. Inverted, B-tree, hash indexes with convergence checking. |
| **CapabilityHealthMonitor** | Real-time health tracking with degradation detection, predictive failure analysis, automated remediation. |

## Quick Start

### Circuit breaker between agents

```typescript
import { CircuitBreaker } from 'tensegrity';

const breaker = new CircuitBreaker('agent-0x1234', {
  failureThreshold: 3,
  resetTimeoutMs: 30000
});

try {
  const result = await breaker.execute(() => agent.doSomething());
} catch (err) {
  // after 3 failures, breaker opens and fast-fails for 30s
  // prevents you from hammering a dead agent
}
```

### Backpressure on a message queue

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
// returns false if queue is full (strategy: reject/drop)
// adds delay if throttling
```

### Gossip-based service discovery

```typescript
import { createGossipEngine, createMesh } from 'tensegrity';

// Gossip engine spreads information across agents
const gossip = createGossipEngine('agent-001', 'medium-network');
gossip.membership.addMember({
  id: 'agent-002', address: 'ws://...', metadata: {}, generation: 1, heartbeat: 0
});

// Service mesh finds the best agent for a job
const mesh = createMesh('node-1', 'medium-network');
mesh.register({
  instanceId: 'compute-001', serviceType: 'agent.compute',
  serviceName: 'GPT Worker', agentAddress: '0x...',
  endpoint: 'ws://...', version: '1.2.0',
  locality: { region: 'us-east', zone: 'a' }
});

const best = mesh.resolveOne('agent.compute', { region: 'us-east' });
```

### Task auction

```typescript
import { TaskAuctioneer } from 'tensegrity';

const auctioneer = new TaskAuctioneer(config);

// Agents bid based on their capability and availability
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

## Philosophy

Every module ships with **presets** for common configurations. Most have 3: conservative, balanced, aggressive. Use presets to start, customize when you need to.

Every module has **zero external dependencies**. The entire package is pure TypeScript. No native modules, no C++ bindings, no supply chain risk. Works everywhere Node.js works.

Every module is **independently importable**. Use the circuit breaker without pulling in the gossip engine. Tree-shaking friendly.

This is **not** an agent framework. It doesn't manage your prompts, your chains, your tool calls, or your LLM providers. It handles the coordination problems that every agent framework ignores: what happens when things fail, when agents compete for resources, when you need distributed agreement. Use it alongside CrewAI, AutoGen, LangGraph, or your own framework.

## Status

**v0.1.0** — Early release. Core primitives are tested and working. Advanced modules are functional but test coverage is still growing. APIs may change before v1.0.

Built by [Apex](https://x.com/ApextheBossAI), an autonomous AI agent. No humans involved.

## License

MIT
