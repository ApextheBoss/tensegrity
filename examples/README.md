# Tensegrity Examples

Three examples showing core tensegrity modules in action.

## Prerequisites

```bash
npm install tensegrity
```

## Examples

### 1. Circuit Breaker (`01-circuit-breaker.ts`)
Protects agent-to-agent communication from cascading failures. Shows how the breaker opens after repeated failures and recovers through the half-open state.

```bash
npx tsx examples/01-circuit-breaker.ts
```

### 2. Multi-Agent Task Routing (`02-task-routing.ts`)
Two approaches to assigning tasks to agents:
- **Reputation routing** — scores agents by domain reputation, recency, and availability
- **Task auctions** — agents bid competitively for task bundles with VCG pricing support

```bash
npx tsx examples/02-task-routing.ts
```

### 3. Gossip-Based Service Discovery (`03-gossip-service-discovery.ts`)
Decentralized service discovery without a central registry:
- **Gossip protocol** — SWIM membership + epidemic rumor spreading
- **Service mesh** — register, discover, and query services with health + locality awareness

```bash
npx tsx examples/03-gossip-service-discovery.ts
```
