import { describe, it, expect, beforeEach } from 'vitest';
import {
  DistributedLockManager,
  BakeryOrderer,
  MaekawaQuorum,
  FencingTokenGenerator,
  WaitForGraph,
  PhantomDetector,
  WoundWaitManager,
  RedlockRegionManager,
  isCompatible,
  PRESETS,
  type LockRequest,
  type LockGrant,
  type LockManagerConfig,
  type DLMEvent,
} from '../distributed-lock-manager';

function makeRequest(overrides: Partial<LockRequest> = {}): LockRequest {
  return {
    id: `req-${Math.random().toString(36).slice(2, 8)}`,
    agentId: 'agent-1',
    resourceId: 'resource-A',
    mode: 'exclusive',
    priority: 1,
    timestamp: Date.now(),
    timeout: 5000,
    ...overrides,
  };
}

function makeDLM(overrides: Partial<LockManagerConfig> = {}): DistributedLockManager {
  return new DistributedLockManager({ ...PRESETS['fast-locks'], ...overrides });
}

// ─── Lock Compatibility ─────────────────────────────────────────

describe('isCompatible', () => {
  it('shared + shared = compatible', () => {
    expect(isCompatible('shared', 'shared')).toBe(true);
  });
  it('exclusive + anything = incompatible', () => {
    expect(isCompatible('exclusive', 'shared')).toBe(false);
    expect(isCompatible('exclusive', 'exclusive')).toBe(false);
    expect(isCompatible('shared', 'exclusive')).toBe(false);
  });
  it('intention locks are compatible with each other', () => {
    expect(isCompatible('intention-shared', 'intention-exclusive')).toBe(true);
    expect(isCompatible('intention-exclusive', 'intention-shared')).toBe(true);
  });
});

// ─── FencingTokenGenerator ──────────────────────────────────────

describe('FencingTokenGenerator', () => {
  it('generates monotonically increasing tokens per resource', () => {
    const gen = new FencingTokenGenerator();
    expect(gen.next('r1')).toBe(1);
    expect(gen.next('r1')).toBe(2);
    expect(gen.next('r2')).toBe(1); // independent per resource
  });

  it('validates current token', () => {
    const gen = new FencingTokenGenerator();
    gen.next('r1');
    gen.next('r1');
    expect(gen.validate('r1', 2)).toBe(true);
    expect(gen.validate('r1', 1)).toBe(false); // stale
  });
});

// ─── BakeryOrderer ──────────────────────────────────────────────

describe('BakeryOrderer', () => {
  it('assigns increasing tickets', () => {
    const bakery = new BakeryOrderer();
    const t1 = bakery.takeTicket('a');
    const t2 = bakery.takeTicket('b');
    expect(t2).toBeGreaterThan(t1);
  });

  it('orders agents by ticket', () => {
    const bakery = new BakeryOrderer();
    bakery.takeTicket('a');
    bakery.takeTicket('b');
    expect(bakery.compare('a', 'b')).toBeLessThan(0);
  });

  it('releases tickets', () => {
    const bakery = new BakeryOrderer();
    bakery.takeTicket('a');
    bakery.release('a');
    // After release, next ticket should be 1 again (max is 0)
    expect(bakery.takeTicket('b')).toBe(1);
  });
});

// ─── WaitForGraph ───────────────────────────────────────────────

describe('WaitForGraph', () => {
  it('detects simple cycle', () => {
    const g = new WaitForGraph();
    g.addEdge('a', 'b', 'r1');
    g.addEdge('b', 'a', 'r2');
    const cycle = g.detectCycle();
    expect(cycle).not.toBeNull();
    expect(cycle!.length).toBeGreaterThanOrEqual(2);
  });

  it('returns null when no cycle', () => {
    const g = new WaitForGraph();
    g.addEdge('a', 'b', 'r1');
    g.addEdge('b', 'c', 'r2');
    expect(g.detectCycle()).toBeNull();
  });

  it('removes edges', () => {
    const g = new WaitForGraph();
    g.addEdge('a', 'b', 'r1');
    g.addEdge('b', 'a', 'r2');
    g.removeEdgesFor('a');
    expect(g.detectCycle()).toBeNull();
  });

  it('selects youngest victim', () => {
    const g = new WaitForGraph();
    const timestamps = new Map([['a', 100], ['b', 200], ['c', 300]]);
    const victim = g.selectVictim(['a', 'b', 'c'], timestamps, 'youngest');
    expect(victim).toBe('c');
  });
});

// ─── MaekawaQuorum ──────────────────────────────────────────────

describe('MaekawaQuorum', () => {
  it('returns quorum of √N size', () => {
    const agents = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'];
    const mq = new MaekawaQuorum(agents);
    const q = mq.getQuorum('a');
    expect(q.length).toBe(3); // √9 = 3
  });

  it('checks quorum with majority', () => {
    const agents = ['a', 'b', 'c', 'd'];
    const mq = new MaekawaQuorum(agents);
    const quorum = mq.getQuorum('a');
    const votes = new Set(quorum);
    expect(mq.hasQuorum(votes, 'a')).toBe(true);
  });
});

// ─── PhantomDetector ────────────────────────────────────────────

describe('PhantomDetector', () => {
  it('detects agents without heartbeat', () => {
    const pd = new PhantomDetector(50);
    const agents = new Set(['a', 'b']);
    // 'a' never heartbeated
    pd.heartbeat('b');
    const phantoms = pd.detectPhantoms(agents);
    expect(phantoms).toContain('a');
    expect(phantoms).not.toContain('b');
  });

  it('detects stale heartbeats', async () => {
    const pd = new PhantomDetector(50);
    pd.heartbeat('a');
    await new Promise(r => setTimeout(r, 80));
    const phantoms = pd.detectPhantoms(new Set(['a']));
    expect(phantoms).toContain('a');
  });
});

// ─── WoundWaitManager ───────────────────────────────────────────

describe('WoundWaitManager', () => {
  it('older agent wounds younger', () => {
    const ww = new WoundWaitManager();
    ww.registerAgent('old', 100);
    ww.registerAgent('young', 200);
    expect(ww.resolve('old', 'young')).toBe('wound');
  });

  it('younger agent waits for older', () => {
    const ww = new WoundWaitManager();
    ww.registerAgent('old', 100);
    ww.registerAgent('young', 200);
    expect(ww.resolve('young', 'old')).toBe('wait');
  });
});

// ─── RedlockRegionManager ───────────────────────────────────────

describe('RedlockRegionManager', () => {
  it('achieves quorum with majority', () => {
    const rm = new RedlockRegionManager(3);
    rm.voteFromRegion('r1', 0);
    expect(rm.hasQuorum('r1')).toBe(false);
    rm.voteFromRegion('r1', 1);
    expect(rm.hasQuorum('r1')).toBe(true); // 2/3 = majority
  });
});

// ─── DistributedLockManager ─────────────────────────────────────

describe('DistributedLockManager', () => {
  let dlm: DistributedLockManager;
  let events: DLMEvent[];

  beforeEach(() => {
    dlm = makeDLM();
    events = [];
    dlm.onEvent(e => events.push(e));
  });

  it('grants an exclusive lock', () => {
    const grant = dlm.acquire(makeRequest());
    expect(grant).not.toBeNull();
    expect(grant!.fencingToken).toBe(1);
    expect(events.some(e => e.type === 'lock-granted')).toBe(true);
  });

  it('denies conflicting exclusive locks', () => {
    dlm.acquire(makeRequest({ agentId: 'a' }));
    const second = dlm.acquire(makeRequest({ agentId: 'b' }));
    expect(second).toBeNull();
  });

  it('allows multiple shared locks', () => {
    const g1 = dlm.acquire(makeRequest({ agentId: 'a', mode: 'shared' }));
    const g2 = dlm.acquire(makeRequest({ agentId: 'b', mode: 'shared' }));
    expect(g1).not.toBeNull();
    expect(g2).not.toBeNull();
  });

  it('releases locks and processes wait queue', () => {
    dlm.acquire(makeRequest({ agentId: 'a' }));
    dlm.acquire(makeRequest({ agentId: 'b' })); // queued
    
    dlm.release('a', 'resource-A');
    // b should now be granted from the queue
    const grants = dlm.getGrantsForResource('resource-A');
    expect(grants.some(g => g.agentId === 'b')).toBe(true);
  });

  it('enforces max locks per agent', () => {
    const dlmStrict = makeDLM({ maxLocksPerAgent: 1 });
    dlmStrict.acquire(makeRequest({ resourceId: 'r1' }));
    const second = dlmStrict.acquire(makeRequest({ resourceId: 'r2' }));
    expect(second).toBeNull();
  });

  it('fencing tokens increase monotonically', () => {
    const g1 = dlm.acquire(makeRequest({ agentId: 'a' }))!;
    dlm.release('a', 'resource-A');
    const g2 = dlm.acquire(makeRequest({ agentId: 'b' }))!;
    expect(g2.fencingToken).toBeGreaterThan(g1.fencingToken);
  });

  it('validates fencing tokens', () => {
    const g = dlm.acquire(makeRequest())!;
    expect(dlm.validateFencingToken('resource-A', g.fencingToken)).toBe(true);
    // Stale token
    dlm.release('agent-1', 'resource-A');
    dlm.acquire(makeRequest({ agentId: 'agent-2' }));
    expect(dlm.validateFencingToken('resource-A', g.fencingToken)).toBe(false);
  });

  it('renews locks within renewal window', async () => {
    const dlmShort = makeDLM({ defaultTtlMs: 200, renewalWindowMs: 150 });
    dlmShort.acquire(makeRequest());
    // Wait until within renewal window
    await new Promise(r => setTimeout(r, 80));
    expect(dlmShort.renew('agent-1', 'resource-A')).toBe(true);
  });

  it('rejects renewal outside window', () => {
    const dlmLong = makeDLM({ defaultTtlMs: 30000, renewalWindowMs: 1000 });
    dlmLong.acquire(makeRequest());
    // Still way before expiry
    expect(dlmLong.renew('agent-1', 'resource-A')).toBe(false);
  });

  it('expires locks', async () => {
    const dlmShort = makeDLM({ defaultTtlMs: 50 });
    dlmShort.acquire(makeRequest());
    await new Promise(r => setTimeout(r, 80));
    const expired = dlmShort.checkExpiredLocks();
    expect(expired).toBe(1);
    expect(dlmShort.getGrantsForResource('resource-A').length).toBe(0);
  });

  it('detects and resolves deadlocks', () => {
    // Create a deadlock: a holds r1, wants r2; b holds r2, wants r1
    dlm.acquire(makeRequest({ agentId: 'a', resourceId: 'r1' }));
    dlm.acquire(makeRequest({ agentId: 'b', resourceId: 'r2' }));
    dlm.acquire(makeRequest({ agentId: 'a', resourceId: 'r2' })); // queued, waits on b
    dlm.acquire(makeRequest({ agentId: 'b', resourceId: 'r1' })); // queued, waits on a → cycle!

    const resolved = dlm.detectAndResolveDeadlocks();
    expect(resolved).toBeGreaterThanOrEqual(1);
    expect(events.some(e => e.type === 'deadlock-detected')).toBe(true);
  });

  it('wound-wait: older agent wounds younger holder', () => {
    const dlmWW = makeDLM({ woundWaitEnabled: true });
    const evts: DLMEvent[] = [];
    dlmWW.onEvent(e => evts.push(e));

    // 'old' registers with earlier timestamp
    dlmWW.acquire(makeRequest({ agentId: 'young', resourceId: 'r1', timestamp: 200 }));
    // 'old' tries to acquire same resource — should wound 'young'
    const grant = dlmWW.acquire(makeRequest({ agentId: 'old', resourceId: 'r1', timestamp: 100 }));
    expect(grant).not.toBeNull();
    expect(evts.some(e => e.type === 'wound-triggered')).toBe(true);
  });

  it('phantom detection cleans stale locks', async () => {
    const dlmPhantom = makeDLM({ phantomDetectionMs: 50 });
    dlmPhantom.acquire(makeRequest({ agentId: 'ghost' }));
    await new Promise(r => setTimeout(r, 80));
    const cleaned = dlmPhantom.detectAndCleanPhantoms();
    expect(cleaned).toBe(1);
    expect(dlmPhantom.getGrantsForResource('resource-A').length).toBe(0);
  });

  it('hierarchical locking acquires intention + target', () => {
    const grants = dlm.acquireHierarchical('agent-1', ['db', 'table', 'row'], 'exclusive');
    expect(grants.length).toBe(3); // IX on db, IX on db/table, X on db/table/row
    expect(grants[0].mode).toBe('intention-exclusive');
    expect(grants[2].mode).toBe('exclusive');
  });

  it('redlock acquires after quorum', () => {
    const dlmR = makeDLM({ regionCount: 3 });
    const req = makeRequest();
    expect(dlmR.acquireWithRedlock(req, 0)).toBeNull();
    const grant = dlmR.acquireWithRedlock(req, 1); // 2/3 = quorum
    expect(grant).not.toBeNull();
  });

  it('stats track grants and denials', () => {
    dlm.acquire(makeRequest({ agentId: 'a' }));
    dlm.acquire(makeRequest({ agentId: 'b' })); // denied
    const stats = dlm.getStats();
    expect(stats.totalGrants).toBe(1);
    expect(stats.totalDenials).toBe(1);
    expect(stats.contentionRatio).toBe(0.5);
  });

  it('tick runs all maintenance', async () => {
    const dlmTick = makeDLM({ defaultTtlMs: 50, phantomDetectionMs: 50 });
    dlmTick.acquire(makeRequest());
    await new Promise(r => setTimeout(r, 80));
    dlmTick.tick(); // should expire + clean phantoms
    expect(dlmTick.getGrantsForResource('resource-A').length).toBe(0);
  });
});
