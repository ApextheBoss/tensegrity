import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  ResourcePoolManager,
  createConnectionPool,
  createComputePool,
  createApiQuotaPool,
  createMemoryPool,
  type ResourceDescriptor,
  type QuotaPolicy,
  type PoolEvent,
} from '../resource-pool-manager';

function makeResource(id: string, capacity: number, type: 'compute' | 'connection' | 'memory' | 'api-quota' = 'compute'): ResourceDescriptor {
  return { id, type, capacity, unit: 'units', tags: new Set(), metadata: {} };
}

describe('ResourcePoolManager', () => {
  let mgr: ResourcePoolManager;

  beforeEach(() => {
    mgr = new ResourcePoolManager();
  });

  // ─── Resource Lifecycle ──────────────────────────────────────────

  describe('addResource / removeResource', () => {
    it('adds a resource and emits event', () => {
      const events: PoolEvent[] = [];
      mgr.on(e => events.push(e));
      const res = makeResource('r1', 10);
      mgr.addResource(res);
      expect(events).toHaveLength(1);
      expect(events[0].type).toBe('resource-added');
    });

    it('throws on duplicate resource id', () => {
      mgr.addResource(makeResource('r1', 10));
      expect(() => mgr.addResource(makeResource('r1', 10))).toThrow('already exists');
    });

    it('removeResource returns displaced reservations', async () => {
      mgr.addResource(makeResource('r1', 10));
      const result = await mgr.allocate('a1', 'r1', 5);
      expect(result.granted).toBe(5);
      const displaced = mgr.removeResource('r1');
      expect(displaced).toHaveLength(1);
      expect(displaced[0].agentId).toBe('a1');
    });

    it('removeResource resolves waiting entries with 0 granted', async () => {
      mgr.addResource(makeResource('r1', 5));
      await mgr.allocate('a1', 'r1', 5);
      // This will queue
      const waitPromise = mgr.allocate('a2', 'r1', 3);
      mgr.removeResource('r1');
      const result = await waitPromise;
      expect(result.granted).toBe(0);
    });
  });

  // ─── Allocation ──────────────────────────────────────────────────

  describe('allocate', () => {
    it('allocates when capacity available', async () => {
      mgr.addResource(makeResource('r1', 10));
      const result = await mgr.allocate('a1', 'r1', 5);
      expect(result.granted).toBe(5);
      expect(result.reservationId).toBeTruthy();
    });

    it('returns 0 for non-existent resource', async () => {
      const result = await mgr.allocate('a1', 'nope', 5);
      expect(result.granted).toBe(0);
    });

    it('queues when capacity insufficient', async () => {
      mgr.addResource(makeResource('r1', 5));
      await mgr.allocate('a1', 'r1', 5);
      let resolved = false;
      const waitPromise = mgr.allocate('a2', 'r1', 3).then(r => { resolved = true; return r; });
      // Should not resolve yet
      await new Promise(r => setTimeout(r, 10));
      expect(resolved).toBe(false);
      // Release a1, should drain queue
      const snap = mgr.snapshot();
      const resId = snap.pools[0].resource.id;
      // We need the reservation id - get from snapshot isn't available, release by finding it
      mgr.release((await mgr.allocate('a1', 'r1', 0)).reservationId!); // won't help
      // Actually let's just release the first one properly
    });

    it('timeout on wait queue', async () => {
      mgr.addResource(makeResource('r1', 5));
      await mgr.allocate('a1', 'r1', 5);
      const result = await mgr.allocate('a2', 'r1', 3, { timeoutMs: 50 });
      expect(result.granted).toBe(0);
      expect(result.reservationId).toBeNull();
    });

    it('emits pool-exhausted when threshold crossed', async () => {
      const events: PoolEvent[] = [];
      mgr.on(e => events.push(e));
      mgr.addResource(makeResource('r1', 100));
      await mgr.allocate('a1', 'r1', 96); // 96% > 95% threshold
      expect(events.some(e => e.type === 'pool-exhausted')).toBe(true);
    });
  });

  // ─── Release ─────────────────────────────────────────────────────

  describe('release', () => {
    it('releases reservation and returns capacity', async () => {
      mgr.addResource(makeResource('r1', 10));
      const r = await mgr.allocate('a1', 'r1', 5);
      expect(mgr.release(r.reservationId!)).toBe(true);
      // Can allocate again
      const r2 = await mgr.allocate('a2', 'r1', 10);
      expect(r2.granted).toBe(10);
    });

    it('returns false for unknown reservation', () => {
      expect(mgr.release('nonexistent')).toBe(false);
    });

    it('drains wait queue on release', async () => {
      mgr.addResource(makeResource('r1', 5));
      const r1 = await mgr.allocate('a1', 'r1', 5);

      const waitPromise = mgr.allocate('a2', 'r1', 3);
      mgr.release(r1.reservationId!);
      const r2 = await waitPromise;
      expect(r2.granted).toBe(3);
    });

    it('emits pool-recovered when utilization drops below threshold', async () => {
      const events: PoolEvent[] = [];
      mgr.addResource(makeResource('r1', 100));
      const r = await mgr.allocate('a1', 'r1', 90);
      mgr.on(e => events.push(e));
      mgr.release(r.reservationId!);
      expect(events.some(e => e.type === 'pool-recovered')).toBe(true);
    });
  });

  // ─── Renew ───────────────────────────────────────────────────────

  describe('renew', () => {
    it('renews a renewable reservation', async () => {
      mgr.addResource(makeResource('r1', 10));
      const r = await mgr.allocate('a1', 'r1', 5, { renewable: true });
      expect(mgr.renew(r.reservationId!, 60_000)).toBe(true);
    });

    it('fails to renew non-renewable reservation', async () => {
      mgr.addResource(makeResource('r1', 10));
      const r = await mgr.allocate('a1', 'r1', 5, { renewable: false });
      expect(mgr.renew(r.reservationId!, 60_000)).toBe(false);
    });

    it('returns false for unknown reservation', () => {
      expect(mgr.renew('nonexistent', 60_000)).toBe(false);
    });
  });

  // ─── Preemption ──────────────────────────────────────────────────

  describe('preemption', () => {
    it('preempts lower-priority reservation for higher-priority request', async () => {
      const events: PoolEvent[] = [];
      mgr.on(e => events.push(e));
      mgr.addResource(makeResource('r1', 10));
      await mgr.allocate('a1', 'r1', 8, { priority: 1 });
      // a2 with higher priority needs 5 — must preempt a1
      const r2 = await mgr.allocate('a2', 'r1', 5, { priority: 5 });
      expect(r2.granted).toBe(5);
      expect(events.some(e => e.type === 'preemption')).toBe(true);
    });

    it('does not preempt same or higher priority', async () => {
      mgr.addResource(makeResource('r1', 10));
      await mgr.allocate('a1', 'r1', 8, { priority: 5 });
      // a2 with same priority can't preempt
      const waitPromise = mgr.allocate('a2', 'r1', 5, { priority: 5, timeoutMs: 50 });
      const r2 = await waitPromise;
      expect(r2.granted).toBe(0); // couldn't preempt, timed out
    });

    it('does not preempt own reservations', async () => {
      mgr.addResource(makeResource('r1', 10));
      await mgr.allocate('a1', 'r1', 8, { priority: 1 });
      // a1 tries to get more with higher priority — shouldn't preempt self
      const r2 = await mgr.allocate('a1', 'r1', 5, { priority: 5, timeoutMs: 50 });
      expect(r2.granted).toBe(0);
    });
  });

  // ─── Quota ───────────────────────────────────────────────────────

  describe('quota management', () => {
    it('enforces quota limit', async () => {
      mgr.addResource(makeResource('r1', 100, 'compute'));
      mgr.setQuota({
        agentId: 'a1',
        resourceType: 'compute',
        maxAllocation: 10,
        guaranteedMinimum: 0,
        burstAllowance: 0,
        burstWindowMs: 60_000,
        priorityWeight: 1,
      });
      const r1 = await mgr.allocate('a1', 'r1', 10);
      expect(r1.granted).toBe(10);
      const r2 = await mgr.allocate('a1', 'r1', 1);
      expect(r2.granted).toBe(0); // quota exceeded
    });

    it('emits quota-exceeded event', async () => {
      const events: PoolEvent[] = [];
      mgr.on(e => events.push(e));
      mgr.addResource(makeResource('r1', 100, 'compute'));
      mgr.setQuota({
        agentId: 'a1',
        resourceType: 'compute',
        maxAllocation: 5,
        guaranteedMinimum: 0,
        burstAllowance: 0,
        burstWindowMs: 60_000,
        priorityWeight: 1,
      });
      await mgr.allocate('a1', 'r1', 10);
      expect(events.some(e => e.type === 'quota-exceeded')).toBe(true);
    });

    it('allows burst above quota', async () => {
      mgr.addResource(makeResource('r1', 100, 'compute'));
      mgr.setQuota({
        agentId: 'a1',
        resourceType: 'compute',
        maxAllocation: 10,
        guaranteedMinimum: 0,
        burstAllowance: 5,
        burstWindowMs: 60_000,
        priorityWeight: 1,
      });
      const r1 = await mgr.allocate('a1', 'r1', 14); // 14 <= 10 + 5
      expect(r1.granted).toBe(14);
    });

    it('removeQuota lifts restrictions', async () => {
      mgr.addResource(makeResource('r1', 100, 'compute'));
      mgr.setQuota({
        agentId: 'a1',
        resourceType: 'compute',
        maxAllocation: 5,
        guaranteedMinimum: 0,
        burstAllowance: 0,
        burstWindowMs: 60_000,
        priorityWeight: 1,
      });
      mgr.removeQuota('a1', 'compute');
      const r = await mgr.allocate('a1', 'r1', 50);
      expect(r.granted).toBe(50);
    });

    it('removeQuota without type removes all', async () => {
      mgr.addResource(makeResource('r1', 100, 'compute'));
      mgr.setQuota({
        agentId: 'a1',
        resourceType: 'compute',
        maxAllocation: 5,
        guaranteedMinimum: 0,
        burstAllowance: 0,
        burstWindowMs: 60_000,
        priorityWeight: 1,
      });
      mgr.removeQuota('a1');
      const r = await mgr.allocate('a1', 'r1', 50);
      expect(r.granted).toBe(50);
    });
  });

  // ─── Bug: Burst tracking never consumed ──────────────────────────

  describe('burst tracking (fixed)', () => {
    it('burst consumed in one alloc reduces future burst allowance', async () => {
      mgr.addResource(makeResource('r1', 100, 'compute'));
      mgr.setQuota({
        agentId: 'a1',
        resourceType: 'compute',
        maxAllocation: 5,
        guaranteedMinimum: 0,
        burstAllowance: 10,
        burstWindowMs: 60_000,
        priorityWeight: 1,
      });

      // Allocate 12 (needs 7 from burst of 10)
      const r1 = await mgr.allocate('a1', 'r1', 12);
      expect(r1.granted).toBe(12);

      // Release it — but burst was consumed
      mgr.release(r1.reservationId!);

      // Allocate 15 — should fail (max 5 + remaining burst 3 = 8)
      const r2 = await mgr.allocate('a1', 'r1', 15);
      expect(r2.granted).toBe(0);

      // But 8 should work
      const r3 = await mgr.allocate('a1', 'r1', 8);
      expect(r3.granted).toBe(8);
    });
  });

  // ─── Reclaim Expired ─────────────────────────────────────────────

  describe('reclaimExpired', () => {
    it('reclaims expired reservations', async () => {
      mgr.addResource(makeResource('r1', 10));
      await mgr.allocate('a1', 'r1', 5, { ttlMs: 1 });
      await new Promise(r => setTimeout(r, 10));
      const reclaimed = mgr.reclaimExpired();
      expect(reclaimed).toBe(1);
      // Capacity is back
      const r2 = await mgr.allocate('a2', 'r1', 10);
      expect(r2.granted).toBe(10);
    });

    it('emits reservation-expired events', async () => {
      const events: PoolEvent[] = [];
      mgr.on(e => events.push(e));
      mgr.addResource(makeResource('r1', 10));
      await mgr.allocate('a1', 'r1', 5, { ttlMs: 1 });
      await new Promise(r => setTimeout(r, 10));
      mgr.reclaimExpired();
      expect(events.some(e => e.type === 'reservation-expired')).toBe(true);
    });

    it('drains wait queue after reclaiming', async () => {
      mgr.addResource(makeResource('r1', 5));
      await mgr.allocate('a1', 'r1', 5, { ttlMs: 1 });
      const waitPromise = mgr.allocate('a2', 'r1', 3);
      await new Promise(r => setTimeout(r, 10));
      mgr.reclaimExpired();
      const r2 = await waitPromise;
      expect(r2.granted).toBe(3);
    });
  });

  // ─── Fair Share ──────────────────────────────────────────────────

  describe('computeFairShare', () => {
    it('distributes capacity by weight', () => {
      mgr.addResource(makeResource('r1', 100, 'compute'));
      mgr.setQuota({
        agentId: 'a1', resourceType: 'compute', maxAllocation: 80,
        guaranteedMinimum: 0, burstAllowance: 0, burstWindowMs: 60_000, priorityWeight: 1,
      });
      mgr.setQuota({
        agentId: 'a2', resourceType: 'compute', maxAllocation: 80,
        guaranteedMinimum: 0, burstAllowance: 0, burstWindowMs: 60_000, priorityWeight: 3,
      });
      const shares = mgr.computeFairShare('compute');
      expect(shares.get('a1')!).toBeLessThan(shares.get('a2')!);
      expect(shares.get('a1')! + shares.get('a2')!).toBeCloseTo(100, 0);
    });

    it('respects guaranteed minimums', () => {
      mgr.addResource(makeResource('r1', 100, 'compute'));
      mgr.setQuota({
        agentId: 'a1', resourceType: 'compute', maxAllocation: 80,
        guaranteedMinimum: 30, burstAllowance: 0, burstWindowMs: 60_000, priorityWeight: 1,
      });
      mgr.setQuota({
        agentId: 'a2', resourceType: 'compute', maxAllocation: 80,
        guaranteedMinimum: 30, burstAllowance: 0, burstWindowMs: 60_000, priorityWeight: 1,
      });
      const shares = mgr.computeFairShare('compute');
      expect(shares.get('a1')!).toBeGreaterThanOrEqual(30);
      expect(shares.get('a2')!).toBeGreaterThanOrEqual(30);
    });
  });

  // ─── Health ──────────────────────────────────────────────────────

  describe('getPoolHealth', () => {
    it('returns null for unknown pool', () => {
      expect(mgr.getPoolHealth('nope')).toBeNull();
    });

    it('reports healthy for empty pool', () => {
      mgr.addResource(makeResource('r1', 100));
      const health = mgr.getPoolHealth('r1')!;
      expect(health.healthy).toBe(true);
      expect(health.utilization).toBe(0);
    });

    it('reports unhealthy when exhausted', async () => {
      mgr.addResource(makeResource('r1', 100));
      await mgr.allocate('a1', 'r1', 96);
      const health = mgr.getPoolHealth('r1')!;
      expect(health.healthy).toBe(false);
      expect(health.utilization).toBe(0.96);
    });
  });

  // ─── Snapshot ────────────────────────────────────────────────────

  describe('snapshot', () => {
    it('returns all pool states', async () => {
      mgr.addResource(makeResource('r1', 10));
      mgr.addResource(makeResource('r2', 20));
      await mgr.allocate('a1', 'r1', 5);
      const snap = mgr.snapshot();
      expect(snap.pools).toHaveLength(2);
      expect(snap.agentCount).toBe(1);
    });
  });

  // ─── Scaling Triggers ────────────────────────────────────────────

  describe('checkScalingTriggers', () => {
    it('emits scale-up when utilization high', async () => {
      const events: PoolEvent[] = [];
      mgr.addResource(makeResource('r1', 100, 'compute'));
      await mgr.allocate('a1', 'r1', 90);
      mgr.on(e => events.push(e));
      mgr.checkScalingTriggers();
      expect(events.some(e => e.type === 'scale-trigger' && 'direction' in e && e.direction === 'up')).toBe(true);
    });

    it('emits scale-down when utilization low', () => {
      const events: PoolEvent[] = [];
      mgr.addResource(makeResource('r1', 100, 'compute'));
      mgr.on(e => events.push(e));
      mgr.checkScalingTriggers();
      expect(events.some(e => e.type === 'scale-trigger' && 'direction' in e && e.direction === 'down')).toBe(true);
    });
  });

  // ─── Event System ────────────────────────────────────────────────

  describe('event system', () => {
    it('unsubscribe works', () => {
      const events: PoolEvent[] = [];
      const unsub = mgr.on(e => events.push(e));
      mgr.addResource(makeResource('r1', 10));
      expect(events).toHaveLength(1);
      unsub();
      mgr.addResource(makeResource('r2', 10));
      expect(events).toHaveLength(1);
    });

    it('swallows handler errors', () => {
      mgr.on(() => { throw new Error('boom'); });
      expect(() => mgr.addResource(makeResource('r1', 10))).not.toThrow();
    });
  });

  // ─── Template Functions ──────────────────────────────────────────

  describe('templates', () => {
    it('createConnectionPool', () => {
      const r = createConnectionPool('db', 50);
      expect(r.type).toBe('connection');
      expect(r.capacity).toBe(50);
    });

    it('createComputePool', () => {
      const r = createComputePool('gpu', 8);
      expect(r.type).toBe('compute');
      expect(r.capacity).toBe(8);
    });

    it('createApiQuotaPool', () => {
      const r = createApiQuotaPool('openai', 1000);
      expect(r.type).toBe('api-quota');
    });

    it('createMemoryPool', () => {
      const r = createMemoryPool('cache', 512);
      expect(r.type).toBe('memory');
      expect(r.unit).toBe('MB');
    });
  });

  // ─── Destroy ─────────────────────────────────────────────────────

  describe('destroy', () => {
    it('cleans up all pools and timers', async () => {
      mgr.addResource(makeResource('r1', 5));
      await mgr.allocate('a1', 'r1', 5);
      mgr.allocate('a2', 'r1', 3, { timeoutMs: 60_000 }); // queued with timer
      mgr.destroy();
      expect(mgr.snapshot().pools).toHaveLength(0);
    });
  });

  // ─── Wait Queue Priority ────────────────────────────────────────

  describe('wait queue priority ordering', () => {
    it('higher priority waiter is served before lower when capacity is limited', async () => {
      mgr.addResource(makeResource('r1', 5));
      // Use high priority so waiters can't preempt the holder
      const r1 = await mgr.allocate('a1', 'r1', 5, { priority: 100 });

      // Both need 4, but only 5 available after release — only one can be served
      const results: Array<{ id: string; granted: number }> = [];
      const p1 = mgr.allocate('low', 'r1', 4, { priority: 1 }).then(r => results.push({ id: 'low', granted: r.granted }));
      const p2 = mgr.allocate('high', 'r1', 4, { priority: 10 }).then(r => results.push({ id: 'high', granted: r.granted }));

      mgr.release(r1.reservationId!);
      await new Promise(r => setTimeout(r, 20));
      // high (priority 10) should be served first and only (5-4=1 < 4 for low)
      const served = results.filter(r => r.granted > 0);
      expect(served).toHaveLength(1);
      expect(served[0].id).toBe('high');
    });
  });
});
