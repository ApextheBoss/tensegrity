import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  FederationRouter,
  RequestCoalescer,
  type FederationConfig,
} from '../rate-aware-federation';
import {
  ResourcePoolManager,
  createComputePool,
} from '../resource-pool-manager';

// ─── RequestCoalescer.destroy ────────────────────────────────────────────────

describe('RequestCoalescer.destroy', () => {
  it('clears pending timers', () => {
    const flushed: string[] = [];
    const coalescer = new RequestCoalescer(
      5000, // long window so timer won't fire during test
      10,
      (key) => flushed.push(key),
    );

    coalescer.add({
      id: '1',
      sourceNetwork: 'net-a',
      targetNetwork: 'net-b',
      type: 'query',
      payload: {},
      priority: 'normal',
      timestampMs: Date.now(),
      coalescingKey: 'group1',
    });

    coalescer.destroy();

    // After destroy, pending should be cleared
    // Calling flushAll should produce nothing
    coalescer.flushAll();
    expect(flushed).toHaveLength(0);
  });

  it('can be called multiple times safely', () => {
    const coalescer = new RequestCoalescer(5000, 10, () => {});
    coalescer.destroy();
    coalescer.destroy();
    // No throw
  });
});

// ─── FederationRouter.destroy ────────────────────────────────────────────────

describe('FederationRouter.destroy', () => {
  const baseConfig: FederationConfig = {
    localNetworkId: 'test-net',
    defaultBudgetCapacity: 100,
    windowDurationMs: 60_000,
    maxPeerBudgetFraction: 0.5,
    maxNegotiationRounds: 3,
    negotiationTtlMs: 30_000,
    coalescingWindowMs: 5000,
    maxCoalesceSize: 10,
    rateLimitCircuitThreshold: 5,
    circuitFailThreshold: 5,
    circuitOpenDurationMs: 10_000,
    forecastWindowSize: 20,
    budgetWarningThreshold: 0.2,
    shedLowAt: 0.1,
    shedNormalAt: 0.05,
    shedHighAt: 0.02,
    maxRetries: 3,
    baseRetryMs: 100,
  };

  it('cleans up without errors', () => {
    const router = new FederationRouter(baseConfig);
    router.addPeer({
      networkId: 'peer-1',
      endpoint: 'http://peer-1',
      trustScore: 0.8,
      latencyMs: 50,
      online: true,
    });
    router.destroy();
    // Should not throw
  });

  it('can be called multiple times safely', () => {
    const router = new FederationRouter(baseConfig);
    router.destroy();
    router.destroy();
  });
});

// ─── ResourcePoolManager.destroy ─────────────────────────────────────────────

describe('ResourcePoolManager.destroy', () => {
  it('clears all pools and pending wait queues', async () => {
    const manager = new ResourcePoolManager();
    const pool = createComputePool('gpu-pool', 4);
    manager.addResource(pool);

    // Allocate some resources
    await manager.allocate('agent-1', 'gpu-pool', 2);

    manager.destroy();

    // After destroy, pools are cleared so allocate returns 0-grant
    const result = await manager.allocate('agent-1', 'gpu-pool', 1);
    expect(result.granted).toBe(0);
  });

  it('clears timeout handles in wait queue entries', async () => {
    const manager = new ResourcePoolManager();
    const pool = createComputePool('small-pool', 1);
    manager.addResource(pool);

    // Fill the pool
    await manager.allocate('agent-1', 'small-pool', 1);

    // Destroy should clear the wait queue and timers
    manager.destroy();
    // No lingering timers - pool is gone
    const result = await manager.allocate('agent-2', 'small-pool', 1);
    expect(result.granted).toBe(0);
  });

  it('can be called multiple times safely', () => {
    const manager = new ResourcePoolManager();
    manager.destroy();
    manager.destroy();
  });
});
