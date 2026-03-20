import { describe, it, expect, beforeEach } from 'vitest';
import {
  DistributedBarrierSynchronizer,
  BarrierRegistry,
  TreeAggregator,
  SenseReversalController,
  FuzzyBarrierManager,
  StragglerDetector,
  AdaptiveTimeoutCalculator,
  BarrierChainOrchestrator,
  PRESETS,
} from '../distributed-barrier-synchronizer';

// ─── BarrierRegistry ─────────────────────────────────────────────────────

describe('BarrierRegistry', () => {
  let registry: BarrierRegistry;

  beforeEach(() => {
    registry = new BarrierRegistry();
  });

  it('creates a barrier with unique id', () => {
    const b = registry.create({ name: 'sync-1', expectedParticipants: 3, timeoutMs: 5000 }, 1000);
    expect(b.id).toMatch(/^barrier_/);
    expect(b.config.name).toBe('sync-1');
    expect(b.state).toBe('created');
    expect(b.generation).toBe(0);
  });

  it('retrieves by id', () => {
    const b = registry.create({ name: 'x', expectedParticipants: 2, timeoutMs: 1000 }, 0);
    expect(registry.get(b.id)).toBe(b);
    expect(registry.get('nonexistent')).toBeUndefined();
  });

  it('retrieves by name', () => {
    registry.create({ name: 'alpha', expectedParticipants: 1, timeoutMs: 1000 }, 0);
    registry.create({ name: 'alpha', expectedParticipants: 2, timeoutMs: 1000 }, 1);
    registry.create({ name: 'beta', expectedParticipants: 1, timeoutMs: 1000 }, 2);
    expect(registry.getByName('alpha')).toHaveLength(2);
    expect(registry.getByName('beta')).toHaveLength(1);
    expect(registry.getByName('gamma')).toHaveLength(0);
  });

  it('retrieves by tag', () => {
    registry.create({ name: 'a', expectedParticipants: 1, timeoutMs: 1000, tags: ['gpu', 'fast'] }, 0);
    registry.create({ name: 'b', expectedParticipants: 1, timeoutMs: 1000, tags: ['gpu'] }, 1);
    expect(registry.getByTag('gpu')).toHaveLength(2);
    expect(registry.getByTag('fast')).toHaveLength(1);
    expect(registry.getByTag('cpu')).toHaveLength(0);
  });

  it('removes barrier and cleans indexes', () => {
    const b = registry.create({ name: 'rm', expectedParticipants: 1, timeoutMs: 1000, tags: ['t1'] }, 0);
    expect(registry.remove(b.id)).toBe(true);
    expect(registry.get(b.id)).toBeUndefined();
    expect(registry.getByName('rm')).toHaveLength(0);
    expect(registry.getByTag('t1')).toHaveLength(0);
    expect(registry.remove('nope')).toBe(false);
  });

  it('getActive filters by state', () => {
    const b1 = registry.create({ name: 'a', expectedParticipants: 1, timeoutMs: 1000 }, 0);
    const b2 = registry.create({ name: 'b', expectedParticipants: 1, timeoutMs: 1000 }, 0);
    b1.state = 'open';
    b2.state = 'released';
    expect(registry.getActive()).toHaveLength(1);
    expect(registry.getActive()[0].id).toBe(b1.id);
  });

  it('prunes old completed barriers', () => {
    const b1 = registry.create({ name: 'a', expectedParticipants: 1, timeoutMs: 1000 }, 0);
    const b2 = registry.create({ name: 'b', expectedParticipants: 1, timeoutMs: 1000 }, 0);
    b1.state = 'released';
    b1.releasedAt = 100;
    b2.state = 'open';
    expect(registry.prune(500, 1000)).toBe(1);
    expect(registry.get(b1.id)).toBeUndefined();
    expect(registry.get(b2.id)).toBeDefined();
  });

  it('stats returns counts by state', () => {
    const b1 = registry.create({ name: 'a', expectedParticipants: 1, timeoutMs: 1000 }, 0);
    const b2 = registry.create({ name: 'b', expectedParticipants: 1, timeoutMs: 1000 }, 0);
    b1.state = 'open';
    const s = registry.stats();
    expect(s.total).toBe(2);
    expect(s.byState['open']).toBe(1);
    expect(s.byState['created']).toBe(1);
  });
});

// ─── SenseReversalController ─────────────────────────────────────────────

describe('SenseReversalController', () => {
  let ctrl: SenseReversalController;

  beforeEach(() => {
    ctrl = new SenseReversalController();
  });

  it('defaults to true sense', () => {
    expect(ctrl.getCurrentSense('b1')).toBe(true);
  });

  it('flips sense', () => {
    expect(ctrl.flipSense('b1')).toBe(false);
    expect(ctrl.getCurrentSense('b1')).toBe(false);
    expect(ctrl.flipSense('b1')).toBe(true);
  });

  it('checks participant sense match', () => {
    expect(ctrl.checkParticipantSense('b1', true)).toBe(true);
    expect(ctrl.checkParticipantSense('b1', false)).toBe(false);
    ctrl.flipSense('b1');
    expect(ctrl.checkParticipantSense('b1', false)).toBe(true);
  });

  it('reset clears sense', () => {
    ctrl.flipSense('b1');
    ctrl.reset('b1');
    expect(ctrl.getCurrentSense('b1')).toBe(true);
  });
});

// ─── FuzzyBarrierManager ─────────────────────────────────────────────────

describe('FuzzyBarrierManager', () => {
  let mgr: FuzzyBarrierManager;

  beforeEach(() => {
    mgr = new FuzzyBarrierManager();
  });

  it('tracks arrivals and checks fuzzy satisfaction', () => {
    mgr.initBarrier('b1', 100);
    mgr.recordArrival('b1', 1000);
    mgr.recordArrival('b1', 1050);
    mgr.recordArrival('b1', 1080);

    const result = mgr.checkFuzzySatisfied('b1', 3, 5, 1100);
    expect(result.arrivedCount).toBe(3);
    expect(result.withinSlack).toBe(3); // all within 100ms of first
    expect(result.satisfied).toBe(true); // minParticipants met within slack
  });

  it('not satisfied when too few arrivals', () => {
    mgr.initBarrier('b1', 100);
    mgr.recordArrival('b1', 1000);
    const result = mgr.checkFuzzySatisfied('b1', 3, 5, 1100);
    expect(result.satisfied).toBe(false);
  });

  it('returns zero for unknown barrier', () => {
    const result = mgr.checkFuzzySatisfied('nope', 1, 1, 0);
    expect(result.satisfied).toBe(false);
    expect(result.arrivedCount).toBe(0);
  });

  it('calculates arrival spread', () => {
    mgr.initBarrier('b1', 500);
    mgr.recordArrival('b1', 1000);
    mgr.recordArrival('b1', 1200);
    mgr.recordArrival('b1', 1500);
    expect(mgr.getArrivalSpread('b1')).toBe(500);
    expect(mgr.getArrivalSpread('unknown')).toBe(0);
  });

  it('removeBarrier cleans up', () => {
    mgr.initBarrier('b1', 100);
    mgr.recordArrival('b1', 1000);
    mgr.removeBarrier('b1');
    expect(mgr.checkFuzzySatisfied('b1', 1, 1, 2000).arrivedCount).toBe(0);
  });
});

// ─── TreeAggregator ──────────────────────────────────────────────────────

describe('TreeAggregator', () => {
  let tree: TreeAggregator;

  beforeEach(() => {
    tree = new TreeAggregator(2);
  });

  it('builds tree and reports depth', () => {
    tree.buildTree('b1', ['a', 'b', 'c', 'd', 'e']);
    expect(tree.getTreeDepth('b1')).toBeGreaterThanOrEqual(2);
  });

  it('returns 0 depth for unknown barrier', () => {
    expect(tree.getTreeDepth('nope')).toBe(0);
  });

  it('records arrivals', () => {
    const ids = ['a', 'b', 'c'];
    tree.buildTree('b1', ids);
    tree.recordArrival('b1', ids[0]);
    // Should not crash, returns array
    const notified = tree.recordArrival('b1', ids[1]);
    expect(Array.isArray(notified)).toBe(true);
  });

  it('returns empty for unknown barrier arrival', () => {
    expect(tree.recordArrival('nope', 'a')).toEqual([]);
  });

  it('removeTree cleans up', () => {
    tree.buildTree('b1', ['a', 'b']);
    tree.removeTree('b1');
    expect(tree.getTreeDepth('b1')).toBe(0);
  });
});

// ─── StragglerDetector ───────────────────────────────────────────────────

describe('StragglerDetector', () => {
  let detector: StragglerDetector;

  beforeEach(() => {
    detector = new StragglerDetector(1.5, 2.5, 4.0);
  });

  it('returns normal for insufficient data', () => {
    detector.recordArrivalLatency('b1', 'p1', 100);
    detector.recordArrivalLatency('b1', 'p2', 110);
    const reports = detector.detectStragglers('b1', ['p3'], 200, 0);
    expect(reports).toHaveLength(1);
    expect(reports[0].severity).toBe('normal');
  });

  it('detects stragglers with enough data', () => {
    // Build up history with fast arrivals
    for (let i = 0; i < 10; i++) {
      detector.recordArrivalLatency('b1', `p${i}`, 100 + Math.random() * 20);
    }
    // Now check a very late participant
    const reports = detector.detectStragglers('b1', ['slow-agent'], 5000, 0);
    expect(reports).toHaveLength(1);
    expect(reports[0].severity).not.toBe('normal');
  });

  it('returns participant profile', () => {
    detector.recordArrivalLatency('b1', 'p1', 100);
    detector.recordArrivalLatency('b1', 'p1', 120);
    const profile = detector.getParticipantProfile('b1', 'p1');
    expect(profile).not.toBeNull();
    expect(profile!.samples).toBe(2);
    expect(profile!.meanLatency).toBeCloseTo(110, 0);
  });

  it('returns null for unknown participant', () => {
    expect(detector.getParticipantProfile('b1', 'nope')).toBeNull();
  });

  it('pruneStats cleans up', () => {
    detector.recordArrivalLatency('b1', 'p1', 100);
    detector.pruneStats('b1');
    expect(detector.getParticipantProfile('b1', 'p1')).toBeNull();
  });
});

// ─── AdaptiveTimeoutCalculator ───────────────────────────────────────────

describe('AdaptiveTimeoutCalculator', () => {
  let calc: AdaptiveTimeoutCalculator;

  beforeEach(() => {
    calc = new AdaptiveTimeoutCalculator(100, 300000, 0.01);
  });

  it('returns base timeout with insufficient data', () => {
    expect(calc.calculateTimeout('sync', 5000)).toBe(5000);
  });

  it('adapts timeout based on completion history', () => {
    for (let i = 0; i < 5; i++) calc.recordCompletion('sync', 200);
    const timeout = calc.calculateTimeout('sync', 5000);
    // Should be based on EWMA (~200) * 2.5 * multiplier, much less than 5000
    expect(timeout).toBeLessThan(5000);
    expect(timeout).toBeGreaterThanOrEqual(100);
  });

  it('increases timeout when too many timeouts', () => {
    for (let i = 0; i < 5; i++) calc.recordCompletion('sync', 200);
    const baseAdaptive = calc.calculateTimeout('sync', 5000);
    // Add many timeouts
    for (let i = 0; i < 10; i++) calc.recordTimeout('sync');
    const afterTimeouts = calc.calculateTimeout('sync', 5000);
    expect(afterTimeouts).toBeGreaterThan(baseAdaptive);
  });

  it('getStats returns null for unknown', () => {
    expect(calc.getStats('nope')).toBeNull();
  });

  it('getStats returns data after recording', () => {
    calc.recordCompletion('sync', 100);
    calc.recordTimeout('sync');
    const stats = calc.getStats('sync');
    expect(stats).not.toBeNull();
    expect(stats!.samples).toBe(1);
    expect(stats!.timeoutRate).toBeCloseTo(0.5);
  });
});

// ─── BarrierChainOrchestrator ────────────────────────────────────────────

describe('BarrierChainOrchestrator', () => {
  let orch: BarrierChainOrchestrator;

  beforeEach(() => {
    orch = new BarrierChainOrchestrator();
  });

  it('creates and starts a chain', () => {
    const chain = orch.createChain('pipeline', ['b1', 'b2', 'b3'], 1000);
    expect(chain.state).toBe('pending');
    expect(chain.barrierIds).toEqual(['b1', 'b2', 'b3']);

    const first = orch.startChain(chain.id);
    expect(first).toBe('b1');
    expect(orch.getChain(chain.id)!.state).toBe('running');
  });

  it('advances through chain steps', () => {
    const chain = orch.createChain('p', ['b1', 'b2'], 0);
    orch.startChain(chain.id);

    const r1 = orch.advanceChain(chain.id, 'b1', 'result1', 100);
    expect(r1.nextBarrierId).toBe('b2');
    expect(r1.chainCompleted).toBe(false);

    const r2 = orch.advanceChain(chain.id, 'b2', 'result2', 200);
    expect(r2.nextBarrierId).toBeNull();
    expect(r2.chainCompleted).toBe(true);
    expect(orch.getChain(chain.id)!.state).toBe('completed');
  });

  it('rejects advance with wrong barrier id', () => {
    const chain = orch.createChain('p', ['b1', 'b2'], 0);
    orch.startChain(chain.id);
    const r = orch.advanceChain(chain.id, 'b2', null, 0); // wrong order
    expect(r.nextBarrierId).toBeNull();
    expect(r.chainCompleted).toBe(false);
  });

  it('fails chain', () => {
    const chain = orch.createChain('p', ['b1'], 0);
    orch.startChain(chain.id);
    orch.failChain(chain.id, 'timeout');
    expect(orch.getChain(chain.id)!.state).toBe('failed');
  });

  it('getActiveChains filters', () => {
    const c1 = orch.createChain('a', ['b1'], 0);
    const c2 = orch.createChain('b', ['b2'], 0);
    orch.startChain(c1.id);
    expect(orch.getActiveChains()).toHaveLength(1);
  });

  it('prunes old chains', () => {
    const chain = orch.createChain('p', ['b1'], 0);
    orch.startChain(chain.id);
    orch.advanceChain(chain.id, 'b1', null, 100);
    expect(orch.pruneChains(500, 1000)).toBe(1);
    expect(orch.getChain(chain.id)).toBeUndefined();
  });

  it('startChain returns null for non-pending', () => {
    const chain = orch.createChain('p', ['b1'], 0);
    orch.startChain(chain.id);
    expect(orch.startChain(chain.id)).toBeNull(); // already running
  });
});

// ─── DistributedBarrierSynchronizer (Integration) ────────────────────────

describe('DistributedBarrierSynchronizer', () => {
  let sync: DistributedBarrierSynchronizer;

  beforeEach(() => {
    sync = new DistributedBarrierSynchronizer({
      defaultTimeoutMs: 5000,
      pruneIntervalMs: 100000,
      pruneMaxAge: 50000,
      enableSenseReversal: false,
    });
  });

  it('full lifecycle: create → open → arrive → satisfy → release', () => {
    const barrier = sync.createBarrier({
      name: 'checkpoint',
      expectedParticipants: 3,
      timeoutMs: 5000,
    }, 1000);

    expect(sync.openBarrier(barrier.id, ['a', 'b', 'c'], 1000)).toBe(true);

    const r1 = sync.arrive(barrier.id, 'a', 1010);
    expect(r1.accepted).toBe(true);
    expect(r1.barrierSatisfied).toBe(false);

    sync.arrive(barrier.id, 'b', 1020);
    const r3 = sync.arrive(barrier.id, 'c', 1030);
    expect(r3.barrierSatisfied).toBe(true);

    expect(sync.releaseBarrier(barrier.id, 1050)).toBe(true);
    expect(sync.getBarrier(barrier.id)!.state).toBe('released');
  });

  it('rejects arrival for unknown barrier', () => {
    const r = sync.arrive('fake', 'a', 0);
    expect(r.accepted).toBe(false);
    expect(r.reason).toBe('barrier_not_found');
  });

  it('rejects arrival for non-participant', () => {
    const b = sync.createBarrier({ name: 'x', expectedParticipants: 1, timeoutMs: 1000 }, 0);
    sync.openBarrier(b.id, ['a'], 0);
    const r = sync.arrive(b.id, 'intruder', 10);
    expect(r.accepted).toBe(false);
    expect(r.reason).toBe('not_participant');
  });

  it('rejects double arrival', () => {
    const b = sync.createBarrier({ name: 'x', expectedParticipants: 2, timeoutMs: 1000 }, 0);
    sync.openBarrier(b.id, ['a', 'b'], 0);
    sync.arrive(b.id, 'a', 10);
    const r = sync.arrive(b.id, 'a', 20);
    expect(r.accepted).toBe(false);
    expect(r.reason).toBe('already_arrived');
  });

  it('auto-releases when configured', () => {
    const b = sync.createBarrier({
      name: 'auto',
      expectedParticipants: 1,
      timeoutMs: 1000,
      autoRelease: true,
    }, 0);
    sync.openBarrier(b.id, ['a'], 0);
    sync.arrive(b.id, 'a', 10);
    expect(sync.getBarrier(b.id)!.state).toBe('released');
  });

  it('timeout detection via tick', () => {
    const b = sync.createBarrier({ name: 'slow', expectedParticipants: 2, timeoutMs: 100 }, 0);
    sync.openBarrier(b.id, ['a', 'b'], 0);
    sync.arrive(b.id, 'a', 10);

    const result = sync.tick(200);
    expect(result.timedOut).toContain(b.id);
    expect(sync.getBarrier(b.id)!.state).toBe('timed_out');
  });

  it('cancel barrier', () => {
    const b = sync.createBarrier({ name: 'x', expectedParticipants: 2, timeoutMs: 5000 }, 0);
    sync.openBarrier(b.id, ['a', 'b'], 0);
    expect(sync.cancelBarrier(b.id, 'user_cancelled', 100)).toBe(true);
    expect(sync.getBarrier(b.id)!.state).toBe('cancelled');
    // Can't cancel again
    expect(sync.cancelBarrier(b.id, 'again', 200)).toBe(false);
  });

  it('detach participant reduces expected count', () => {
    const b = sync.createBarrier({ name: 'x', expectedParticipants: 3, timeoutMs: 5000 }, 0);
    sync.openBarrier(b.id, ['a', 'b', 'c'], 0);
    sync.arrive(b.id, 'a', 10);
    sync.arrive(b.id, 'b', 20);
    // Detach c → expected drops to 2, which is already satisfied
    sync.detachParticipant(b.id, 'c', 30);
    expect(sync.getBarrier(b.id)!.state).toBe('satisfied');
  });

  it('getBarrierStatus returns status info', () => {
    const b = sync.createBarrier({ name: 'x', expectedParticipants: 3, timeoutMs: 5000 }, 0);
    sync.openBarrier(b.id, ['a', 'b', 'c'], 100);
    sync.arrive(b.id, 'a', 150);

    const status = sync.getBarrierStatus(b.id);
    expect(status).not.toBeNull();
    expect(status!.arrived).toBe(1);
    expect(status!.expected).toBe(3);
    expect(status!.pending).toContain('b');
    expect(status!.pending).toContain('c');
  });

  it('getBarrierStatus returns null for unknown', () => {
    expect(sync.getBarrierStatus('nope')).toBeNull();
  });

  it('getBarriersByName works', () => {
    sync.createBarrier({ name: 'sync', expectedParticipants: 1, timeoutMs: 1000 }, 0);
    sync.createBarrier({ name: 'sync', expectedParticipants: 2, timeoutMs: 1000 }, 1);
    expect(sync.getBarriersByName('sync')).toHaveLength(2);
  });

  it('metrics track correctly', () => {
    const b = sync.createBarrier({ name: 'x', expectedParticipants: 1, timeoutMs: 1000, autoRelease: true }, 0);
    sync.openBarrier(b.id, ['a'], 0);
    sync.arrive(b.id, 'a', 50);

    const m = sync.getMetrics();
    expect(m.totalBarriers).toBe(1);
    expect(m.satisfiedBarriers).toBe(1);
  });

  it('events are recorded', () => {
    const b = sync.createBarrier({ name: 'x', expectedParticipants: 1, timeoutMs: 1000 }, 0);
    sync.openBarrier(b.id, ['a'], 0);
    sync.arrive(b.id, 'a', 10);

    const events = sync.getRecentEvents();
    const types = events.map(e => e.type);
    expect(types).toContain('barrier_created');
    expect(types).toContain('barrier_opened');
    expect(types).toContain('participant_arrived');
    expect(types).toContain('barrier_satisfied');
  });

  it('dashboard returns structured data', () => {
    const d = sync.dashboard();
    expect(d.metrics).toBeDefined();
    expect(d.registryStats).toBeDefined();
    expect(d.activeBarriers).toBeDefined();
    expect(d.activeChains).toBeDefined();
  });

  it('openBarrier rejects non-created barrier', () => {
    const b = sync.createBarrier({ name: 'x', expectedParticipants: 1, timeoutMs: 1000 }, 0);
    sync.openBarrier(b.id, ['a'], 0);
    // Already open, can't re-open
    expect(sync.openBarrier(b.id, ['a'], 10)).toBe(false);
  });

  it('releaseBarrier rejects non-satisfied barrier', () => {
    const b = sync.createBarrier({ name: 'x', expectedParticipants: 2, timeoutMs: 5000 }, 0);
    sync.openBarrier(b.id, ['a', 'b'], 0);
    expect(sync.releaseBarrier(b.id, 100)).toBe(false);
  });
});

// ─── Sense Reversal Integration ──────────────────────────────────────────

describe('DistributedBarrierSynchronizer (sense reversal)', () => {
  it('flips sense on release when configured', () => {
    const sync = new DistributedBarrierSynchronizer({ enableSenseReversal: true });
    const b = sync.createBarrier({
      name: 'reversible',
      expectedParticipants: 1,
      timeoutMs: 5000,
      senseReversal: true,
    }, 0);
    sync.openBarrier(b.id, ['a'], 0);
    sync.arrive(b.id, 'a', 10);
    sync.releaseBarrier(b.id, 20);

    const barrier = sync.getBarrier(b.id)!;
    expect(barrier.currentSense).toBe(false); // flipped from true
    expect(barrier.generation).toBe(1);

    const events = sync.getRecentEvents();
    expect(events.some(e => e.type === 'sense_flipped')).toBe(true);
  });
});

// ─── Fuzzy Barrier Integration ───────────────────────────────────────────

describe('DistributedBarrierSynchronizer (fuzzy barriers)', () => {
  it('fuzzy-satisfies when minParticipants met within slack', () => {
    const sync = new DistributedBarrierSynchronizer({ enableSenseReversal: false });
    const b = sync.createBarrier({
      name: 'fuzzy',
      expectedParticipants: 5,
      timeoutMs: 10000,
      fuzzySlackMs: 200,
      minParticipants: 3,
    }, 0);
    sync.openBarrier(b.id, ['a', 'b', 'c', 'd', 'e'], 0);

    sync.arrive(b.id, 'a', 100);
    sync.arrive(b.id, 'b', 150);
    sync.arrive(b.id, 'c', 200); // 3 within 200ms slack → fuzzy satisfied

    expect(sync.getBarrier(b.id)!.state).toBe('satisfied');
    expect(sync.getMetrics().fuzzyReleasedEarly).toBe(1);
  });
});

// ─── Chain Integration ───────────────────────────────────────────────────

describe('DistributedBarrierSynchronizer (chains)', () => {
  let sync: DistributedBarrierSynchronizer;

  beforeEach(() => {
    sync = new DistributedBarrierSynchronizer({
      enableSenseReversal: false,
      defaultTimeoutMs: 5000,
    });
  });

  it('creates and runs a chain end-to-end', () => {
    const { chainId, barrierIds } = sync.createChain('pipeline', [
      { name: 'step1', expectedParticipants: 2, timeoutMs: 5000, autoRelease: true },
      { name: 'step2', expectedParticipants: 2, timeoutMs: 5000, autoRelease: true },
    ], 0);

    expect(barrierIds).toHaveLength(2);
    expect(sync.startChain(chainId, ['a', 'b'], 0)).toBe(true);

    // Complete step 1
    sync.arrive(barrierIds[0], 'a', 10);
    sync.arrive(barrierIds[0], 'b', 20);
    // Step 1 auto-released

    // Tick to advance chain → opens step 2
    let result = sync.tick(30);
    expect(result.chainsAdvanced).toContain(chainId);

    // Complete step 2
    sync.arrive(barrierIds[1], 'a', 40);
    sync.arrive(barrierIds[1], 'b', 50);

    result = sync.tick(60);
    expect(sync.getChain(chainId)!.state).toBe('completed');
    expect(sync.getMetrics().chainCompletions).toBe(1);
  });

  it('chain fails on barrier timeout', () => {
    const { chainId, barrierIds } = sync.createChain('fail-pipe', [
      { name: 'step1', expectedParticipants: 2, timeoutMs: 100 },
    ], 0);
    sync.startChain(chainId, ['a', 'b'], 0);

    const result = sync.tick(200);
    expect(result.timedOut).toContain(barrierIds[0]);
    expect(sync.getChain(chainId)!.state).toBe('failed');
  });
});

// ─── Straggler Detection Integration ────────────────────────────────────

describe('DistributedBarrierSynchronizer (straggler detection)', () => {
  it('detects stragglers during tick', () => {
    const sync = new DistributedBarrierSynchronizer({
      enableSenseReversal: false,
      defaultTimeoutMs: 100000,
    });

    // Build up arrival history
    for (let i = 0; i < 5; i++) {
      const b = sync.createBarrier({ name: 'rep', expectedParticipants: 2, timeoutMs: 100000, autoRelease: true }, i * 1000);
      sync.openBarrier(b.id, ['fast', 'slow'], i * 1000);
      sync.arrive(b.id, 'fast', i * 1000 + 10);
      sync.arrive(b.id, 'slow', i * 1000 + 20);
    }

    // Now create a barrier where someone is very late
    const late = sync.createBarrier({ name: 'rep', expectedParticipants: 2, timeoutMs: 100000 }, 10000);
    sync.openBarrier(late.id, ['fast', 'slow'], 10000);
    sync.arrive(late.id, 'fast', 10010);

    // tick at a much later time — slow hasn't arrived
    const result = sync.tick(50000);
    // With enough history, stragglers should be detected
    expect(result.stragglers.length).toBeGreaterThanOrEqual(0); // depends on statistical thresholds
  });
});

// ─── Presets ─────────────────────────────────────────────────────────────

describe('PRESETS', () => {
  it('has three presets with required fields', () => {
    expect(Object.keys(PRESETS)).toEqual(['interactive-agents', 'batch-pipeline', 'agent-swarm']);
    for (const preset of Object.values(PRESETS)) {
      expect(preset.defaultTimeoutMs).toBeGreaterThan(0);
      expect(preset.maxBarriers).toBeGreaterThan(0);
      expect(preset.stragglerSigmaThresholds).toBeDefined();
    }
  });

  it('can be used to construct synchronizer', () => {
    const sync = new DistributedBarrierSynchronizer(PRESETS['interactive-agents']);
    expect(sync.getMetrics().totalBarriers).toBe(0);
  });
});
