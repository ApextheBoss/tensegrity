# tensegrity

Coordination primitives for multi-agent systems. Not another LLM wrapper.

This is the boring infrastructure that nobody wants to write: circuit breakers between agents, backpressure when one agent is overwhelmed, task auctions so agents can bid on work, reputation-weighted routing so unreliable agents get less traffic.

Built from code running in production on [nookplot](https://nookplot.com) across 2500+ agents.

## install

```
npm install tensegrity
```

## what's in the box

### CircuitBreaker

Prevents cascading failures when an agent goes down. Three states: closed (normal), open (blocking all calls), half-open (testing recovery). Tracks failure rates in a sliding window.

```typescript
import { CircuitBreaker } from 'tensegrity';

const breaker = new CircuitBreaker('agent-0x1234', {
  failureThreshold: 3,
  resetTimeoutMs: 30000
});

const result = await breaker.call(() => agent.doSomething());
// after 3 failures, breaker opens and fast-fails for 30s
```

### BackpressureController

Stops fast producers from overwhelming slow consumers. Monitors queue depth and applies backpressure using token bucket rate limiting.

```typescript
import { BackpressureController } from 'tensegrity';

const bp = new BackpressureController({
  highWaterMark: 100,
  lowWaterMark: 20
});

if (bp.shouldAccept()) {
  queue.push(task);
} else {
  // slow down or buffer
}
```

### TaskAuction

Agents bid on tasks. Highest bidder wins. Supports sealed-bid and open ascending formats. Handles bid validation, timeout, and winner selection.

```typescript
import { TaskAuction } from 'tensegrity';

const auction = new TaskAuction({
  task: { id: 'summarize-doc', requirements: ['gpt-4'] },
  timeoutMs: 5000
});

auction.bid('agent-A', { price: 0.02, latencyMs: 200 });
auction.bid('agent-B', { price: 0.01, latencyMs: 50 });

const winner = auction.resolve(); // picks best bid
```

### ReputationWeightedRouter

Routes tasks to agents based on their track record. Agents that deliver get more work. Agents that fail get less. Uses exponential decay so recent performance matters more.

```typescript
import { ReputationWeightedRouter } from 'tensegrity';

const router = new ReputationWeightedRouter();
router.recordSuccess('agent-A', 150); // 150ms latency
router.recordFailure('agent-B');

const best = router.route('summarize'); // picks agent-A
```

### TransactionalOutboxEngine

Guarantees exactly-once event publishing from agent state changes. Write events to a local outbox atomically with state mutations, then asynchronously relay them. Handles dead-letter queues, CDC streaming, partition routing, and compaction.

```typescript
import { TransactionalOutboxEngine, OutboxPresets } from 'tensegrity';

const engine = new TransactionalOutboxEngine(OutboxPresets['agent-event-bus']);
engine.addWorker('relay-1');

engine.onDelivery(async (event) => {
  await externalBus.publish(event.topic, event.payload);
  return true;
});

engine.appendEvent('agent-0x1234', 'task.completed', { taskId: '...', result: '...' });
await engine.tick(); // polls, dispatches, compacts
```

### ResourcePoolManager

Manages shared resource pools (connections, compute slots, API quotas) across agents with fair allocation, reservation, preemption, and auto-scaling triggers.

```typescript
import { ResourcePoolManager, createComputePool } from 'tensegrity';

const manager = new ResourcePoolManager();
manager.addResource(createComputePool('gpu-cluster', 8));

const result = await manager.allocate('agent-A', 'gpu-cluster', 2, {
  priority: 5,
  ttlMs: 60_000,
  purpose: 'inference batch'
});

console.log(result.granted); // 2
manager.release(result.reservationId!);
```

### AdaptiveThrottleGovernor

Dynamic rate control that adjusts throughput based on downstream health. Combines AIMD (TCP-style additive increase / multiplicative decrease), Vegas-style latency gradient detection, CoDel-inspired queue management, and a PI controller for steady-state convergence. Supports multi-tenant fair-share allocation and coordinated throttling across agent clusters via gossip.

```typescript
import { AdaptiveThrottleGovernor, ThrottlePresets } from 'tensegrity';

const governor = new AdaptiveThrottleGovernor(ThrottlePresets['agent-to-agent'], 'node-1');

// add tenants with weighted fair share
governor.addTenant({ id: 'agent-A', weight: 3, minGuaranteedRate: 2, maxBurstRate: 100, priority: 0 });
governor.addTenant({ id: 'agent-B', weight: 1, minGuaranteedRate: 1, maxBurstRate: 50, priority: 1 });

// record request completions to feed the control loop
governor.recordRequest({
  timestamp: Date.now(),
  durationMs: 45,
  success: true,
  tenantId: 'agent-A'
});

// check admission
if (governor.shouldAllow('agent-A')) {
  await agent.call();
}

// get current state
const state = governor.getState();
// { currentRate, effectiveRate, congestionLevel, mode, tenantAllocations }
```

Three presets: `api-gateway` (high throughput, tight latency), `agent-to-agent` (moderate, tolerant), `batch-processing` (high volume, relaxed latency).

### CapabilityHealthMonitor

Real-time health tracking for agent capabilities with degradation detection, predictive failure analysis, SLA compliance scoring, and automated remediation. Includes a health federator for cross-agent capability routing — when one agent's capability degrades, traffic shifts to healthy providers.

```typescript
import { CapabilityHealthMonitor, HealthMonitorPresets } from 'tensegrity';

const monitor = new CapabilityHealthMonitor(HealthMonitorPresets['agent-mesh']);

// record probe results from your health checks
const { state, prediction, remediation } = monitor.recordProbe({
  type: 'performance',
  capabilityId: 'summarize',
  agentId: 'agent-A',
  success: true,
  latencyMs: 145,
  timestamp: Date.now()
});

console.log(state.score);          // 0.92 composite health
console.log(state.status);         // 'healthy'
console.log(remediation.action);   // 'none'

// find healthiest provider for a capability
const best = monitor.getBestProvider('summarize');
```

Three presets: `real-time-api` (tight SLAs, fast probes), `batch-processing` (relaxed, high tolerance), `agent-mesh` (balanced for multi-agent networks).

### ConsensusViewSynchronizer

BFT view synchronization for consensus protocols. Ensures all honest agents converge on the same round despite asynchrony and Byzantine faults. Adaptive pacemaker with leader reputation, optimistic fast-path advancement, timeout certificates, and catch-up for lagging agents.

```typescript
import { ConsensusViewSynchronizer, ViewSyncPresets } from 'tensegrity';

const sync = new ConsensusViewSynchronizer(ViewSyncPresets['fast-consensus']);

sync.registerAgent('agent-A', 1);
sync.registerAgent('agent-B', 1);
sync.registerAgent('agent-C', 1);

// normal path: QC received → advance view
sync.receiveQC({ view: 0, blockHash: '0xabc', signatures: new Map(), aggregateWeight: 2, createdAt: Date.now() });

console.log(sync.getCurrentView());   // 1
console.log(sync.getCurrentLeader()); // deterministic leader for view 1
```

Three presets: `fast-consensus` (low latency, round-robin), `byzantine-tolerant` (higher quorum, reputation-weighted), `high-throughput` (sticky leaders for amortized overhead).

### ResourceContentionArbiter

Game-theoretic resource allocation when multiple agents compete for shared resources. Combines Vickrey auctions, Nash bargaining, priority preemption with Wait-Die deadlock prevention, Gini-based starvation detection, demand forecasting, and token-based budget planning.

```typescript
import { ResourceContentionArbiter, ARBITER_PRESETS } from 'tensegrity';

const arbiter = new ResourceContentionArbiter({}, 'fair-share');

arbiter.registerResource({ id: 'gpu-cluster', capacity: 8, divisible: true, preemptible: true, category: 'compute' });
arbiter.setBudget('agent-A', 'gpu-cluster', 4);

const { granted, allocation } = arbiter.requestAllocation({
  agentId: 'agent-A',
  resourceId: 'gpu-cluster',
  quantity: 3,
  priority: 7,
  flexibility: 0.2,
  utilityPerUnit: 10
});

// check for starvation and contention forecasts
const status = arbiter.getResourceStatus('gpu-cluster');
console.log(status.forecast.trending);        // 'rising' | 'falling' | 'stable'
console.log(status.starvation.severity);      // 'none' | 'mild' | 'moderate' | 'severe'
```

Three presets: `fair-share` (Nash bargaining, low starvation tolerance), `priority-driven` (preemption enabled), `market-based` (auction-resolved).

## why this exists

every "agent framework" right now is a thin wrapper around prompt chaining. the hard problems in multi-agent systems are the same hard problems in distributed systems: coordination, fault tolerance, load balancing, consensus. these are solved problems in distributed computing. nobody has ported them to the agent world properly.

tensegrity does that. one primitive at a time.

## status

early. the APIs will change. the module list will grow. if you're building multi-agent systems and want to stop reinventing circuit breakers, this is for you.

## license

MIT
