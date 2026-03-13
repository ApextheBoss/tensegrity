# Task Auction System

Advanced combinatorial task-to-agent assignment via sealed-bid auctions.

## Overview

The Task Auction module implements a full auction-based scheduling system for distributed agent networks. It replaces naive round-robin or random assignment with economically-sound mechanisms that account for agent capabilities, capacity, and strategic behavior.

## Key Components

### `TaskAuctioneer`
Orchestrates the full auction lifecycle: task announcement → bid collection → winner determination → payment calculation → result notification.

### `AgentTaskAuctionScheduler`
High-level scheduler that manages multiple concurrent auctions, tracks agent capacity, and handles auction result caching.

### `WinnerDetermination`
Solves the combinatorial allocation problem — finding the optimal assignment of tasks to agents given bundle bids and capacity constraints.

### `VCGPaymentEngine`
Implements Vickrey-Clarke-Groves payment calculation, ensuring truthful bidding is the dominant strategy for agents.

### `BidScreener`
Anti-collusion and bid validation. Screens for suspicious patterns, enforces reserve prices, and validates bid feasibility.

### `CapacityValidator`
Ensures agents don't over-commit by tracking concurrent task limits and resource availability.

## Features

- **Sealed-bid auctions** — agents can't see others' bids
- **VCG pricing** — truthful bidding is optimal (no gaming)
- **Bundle bidding** — agents can bid on task combinations
- **Anti-collusion detection** — screens for bid-rigging patterns
- **Capacity-aware** — respects agent resource limits
- **Result caching** — avoids re-running identical auctions
- **Revenue tracking** — monitors auction economics

## Usage

```typescript
import { AgentTaskAuctionScheduler } from 'tensegrity';

const scheduler = new AgentTaskAuctionScheduler({
  maxConcurrentAuctions: 10,
  bidTimeoutMs: 5000,
  reservePriceMultiplier: 0.8,
});

// Register agents with capabilities
scheduler.registerAgent('agent-1', {
  capabilities: ['code-review', 'testing'],
  maxConcurrent: 3,
});

// Submit task for auction
const result = await scheduler.scheduleTask({
  type: 'code-review',
  complexity: 'high',
  deadline: Date.now() + 3600000,
});

console.log(result.winner);    // 'agent-1'
console.log(result.payment);   // VCG price paid
```

## When to Use

- Multi-agent systems where tasks need intelligent routing
- Scenarios where agents have heterogeneous capabilities
- When you need fair, game-theoretically sound allocation
- Workloads with varying complexity and agent capacity constraints

## Related Modules

- [Scheduler Affinity Graph](./scheduler-affinity-graph.md) — learn agent-task affinity over time
- [Resource Pool Manager](./resource-pool-manager.md) — manage shared resource pools
- [Reputation Router](./reputation-router.md) — route based on agent reputation scores
