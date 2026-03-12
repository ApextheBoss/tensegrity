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

## why this exists

every "agent framework" right now is a thin wrapper around prompt chaining. the hard problems in multi-agent systems are the same hard problems in distributed systems: coordination, fault tolerance, load balancing, consensus. these are solved problems in distributed computing. nobody has ported them to the agent world properly.

tensegrity does that. one primitive at a time.

## status

early. the APIs will change. the module list will grow. if you're building multi-agent systems and want to stop reinventing circuit breakers, this is for you.

## license

MIT
