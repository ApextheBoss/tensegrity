import { describe, it, expect } from 'vitest';
import {
  AdaptiveWorkStealingPool,
  WorkDeque,
  TopologyCostModel,
  AffinityTracker,
  VictimSelector,
  LoadImbalanceDetector,
  TaskFragmentationAnalyzer,
  StealPolicyController,
  TaskSplitter,
  PRESETS,
  Task,
  PoolConfig,
  StealPolicy,
} from '../adaptive-work-stealing';

function makeTask(overrides: Partial<Task> = {}): Task {
  return {
    id: `task-${Math.random().toString(36).slice(2, 8)}`,
    type: 'compute',
    priority: 1,
    estimatedCostMs: 100,
    data: {},
    createdAt: 1000,
    splittable: false,
    ...overrides,
  };
}

function makePool(config?: Partial<PoolConfig>): AdaptiveWorkStealingPool {
  return new AdaptiveWorkStealingPool({ ...PRESETS['interactive'], ...config });
}

// ─── WorkDeque ───────────────────────────────────────────────────────────────

describe('WorkDeque', () => {
  it('pushBottom/popBottom is LIFO', () => {
    const dq = new WorkDeque();
    const t1 = makeTask({ id: 'a' });
    const t2 = makeTask({ id: 'b' });
    dq.pushBottom(t1);
    dq.pushBottom(t2);
    expect(dq.popBottom()?.id).toBe('b');
    expect(dq.popBottom()?.id).toBe('a');
  });

  it('stealTop takes from front (FIFO)', () => {
    const dq = new WorkDeque();
    for (let i = 0; i < 6; i++) dq.pushBottom(makeTask({ id: `t${i}` }));
    const stolen = dq.stealTop(2);
    expect(stolen.map(t => t.id)).toEqual(['t0', 't1']);
    // steals min(count, floor(size/2))
    expect(dq.size()).toBe(4);
  });

  it('stealTop steals at most half', () => {
    const dq = new WorkDeque();
    dq.pushBottom(makeTask({ id: 'only' }));
    const stolen = dq.stealTop(5);
    expect(stolen).toHaveLength(0); // floor(1/2) = 0
  });

  it('popBottom returns null when empty', () => {
    expect(new WorkDeque().popBottom()).toBeNull();
  });

  it('peek returns last element without removing', () => {
    const dq = new WorkDeque();
    dq.pushBottom(makeTask({ id: 'x' }));
    expect(dq.peek()?.id).toBe('x');
    expect(dq.size()).toBe(1);
  });

  it('inspect returns copy of items', () => {
    const dq = new WorkDeque();
    dq.pushBottom(makeTask({ id: 'a' }));
    const items = dq.inspect();
    expect(items).toHaveLength(1);
    expect(items[0].id).toBe('a');
  });
});

// ─── TopologyCostModel ───────────────────────────────────────────────────────

describe('TopologyCostModel', () => {
  const topo = new TopologyCostModel();

  function agentNode(id: string, zone: string, rack: string): any {
    return { id, zone, rack };
  }

  it('same agent = distance 0, cost 1', () => {
    const a = agentNode('a', 'z1', 'r1');
    expect(topo.distance(a, a)).toBe(0);
    expect(topo.cost(a, a)).toBe(1.0);
  });

  it('same rack = distance 1', () => {
    const a = agentNode('a', 'z1', 'r1');
    const b = agentNode('b', 'z1', 'r1');
    expect(topo.distance(a, b)).toBe(1);
    expect(topo.cost(a, b)).toBe(1.5);
  });

  it('same zone different rack = distance 2', () => {
    const a = agentNode('a', 'z1', 'r1');
    const b = agentNode('b', 'z1', 'r2');
    expect(topo.distance(a, b)).toBe(2);
    expect(topo.cost(a, b)).toBe(3.0);
  });

  it('cross zone = distance 3', () => {
    const a = agentNode('a', 'z1', 'r1');
    const b = agentNode('b', 'z2', 'r1');
    expect(topo.distance(a, b)).toBe(3);
    expect(topo.cost(a, b)).toBe(10.0);
  });

  it('effectiveValue discounts by topology cost', () => {
    const a = agentNode('a', 'z1', 'r1');
    const b = agentNode('b', 'z2', 'r1');
    const task = makeTask({ estimatedCostMs: 100, priority: 2 });
    const val = topo.effectiveValue(task, a, b);
    expect(val).toBe(200 / 10.0); // 20
  });
});

// ─── AffinityTracker ─────────────────────────────────────────────────────────

describe('AffinityTracker', () => {
  it('returns 0 for unknown task type', () => {
    const at = new AffinityTracker(60000);
    expect(at.getScore('unknown', 'agent1', 1000)).toBe(0);
  });

  it('builds affinity from completions', () => {
    const at = new AffinityTracker(60000);
    at.recordCompletion('compute', 'agent1', 50, 1000);
    at.recordCompletion('compute', 'agent1', 60, 1100);
    const score = at.getScore('compute', 'agent1', 1200);
    expect(score).toBeGreaterThan(0);
  });

  it('affinity decays over time', () => {
    const at = new AffinityTracker(1000); // 1s half-life
    at.recordCompletion('compute', 'agent1', 50, 1000);
    const fresh = at.getScore('compute', 'agent1', 1000);
    const decayed = at.getScore('compute', 'agent1', 3000);
    expect(decayed).toBeLessThan(fresh);
  });

  it('bestAgent picks highest affinity', () => {
    const at = new AffinityTracker(60000);
    at.recordCompletion('compute', 'a', 50, 1000);
    at.recordCompletion('compute', 'a', 50, 1100);
    at.recordCompletion('compute', 'b', 50, 1200);
    expect(at.bestAgent('compute', ['a', 'b'], 1300)).toBe('a');
  });

  it('prune removes stale entries', () => {
    const at = new AffinityTracker(1000);
    at.recordCompletion('compute', 'a', 50, 1000);
    const pruned = at.prune(100000, 5000);
    expect(pruned).toBe(1);
    expect(at.getScore('compute', 'a', 100000)).toBe(0);
  });
});

// ─── LoadImbalanceDetector ───────────────────────────────────────────────────

describe('LoadImbalanceDetector', () => {
  const detector = new LoadImbalanceDetector();

  function agentWithLoad(id: string, queueSize: number, maxConc: number = 10): any {
    return {
      id,
      deque: { size: () => queueSize },
      maxConcurrency: maxConc,
    };
  }

  it('gini = 0 for equal loads', () => {
    const agents = [agentWithLoad('a', 5), agentWithLoad('b', 5)];
    expect(detector.computeGini(agents)).toBe(0);
  });

  it('gini > 0 for unequal loads', () => {
    const agents = [agentWithLoad('a', 0), agentWithLoad('b', 10)];
    expect(detector.computeGini(agents)).toBeGreaterThan(0);
  });

  it('gini = 0 for single agent', () => {
    expect(detector.computeGini([agentWithLoad('a', 5)])).toBe(0);
  });

  it('gini = 0 for all-zero loads', () => {
    const agents = [agentWithLoad('a', 0), agentWithLoad('b', 0)];
    expect(detector.computeGini(agents)).toBe(0);
  });

  it('detect identifies overloaded/underloaded agents', () => {
    const agents = [
      agentWithLoad('a', 0, 10),
      agentWithLoad('b', 20, 5),
    ];
    const result = detector.detect(agents, 0.1, 1000);
    expect(result.imbalanced).toBe(true);
    expect(result.overloaded).toContain('b');
    expect(result.underloaded).toContain('a');
  });

  it('trend returns stable with insufficient data', () => {
    const d = new LoadImbalanceDetector();
    expect(d.trend()).toBe('stable');
  });
});

// ─── StealPolicyController ───────────────────────────────────────────────────

describe('StealPolicyController', () => {
  const basePolicy: StealPolicy = {
    minImbalanceRatio: 0.2,
    maxStealBatchSize: 3,
    stealCooldownMs: 200,
    backoffBaseMs: 500,
    backoffMaxMs: 10000,
    localityWeight: 0.3,
    affinityWeight: 0.3,
    loadWeight: 0.4,
  };

  it('reduces aggression on low success rate', () => {
    const ctrl = new StealPolicyController(basePolicy);
    for (let i = 0; i < 20; i++) ctrl.recordAttempt(false, 1000 + i);
    const adapted = ctrl.getAdaptedPolicy(1020);
    expect(adapted.stealCooldownMs).toBeGreaterThan(basePolicy.stealCooldownMs);
    expect(adapted.maxStealBatchSize).toBeLessThanOrEqual(basePolicy.maxStealBatchSize);
  });

  it('increases aggression on high success rate', () => {
    const ctrl = new StealPolicyController(basePolicy);
    for (let i = 0; i < 20; i++) ctrl.recordAttempt(true, 1000 + i);
    const adapted = ctrl.getAdaptedPolicy(1020);
    expect(adapted.stealCooldownMs).toBeLessThan(basePolicy.stealCooldownMs);
  });

  it('getStats reports attempts and rate', () => {
    const ctrl = new StealPolicyController(basePolicy);
    ctrl.recordAttempt(true, 1000);
    ctrl.recordAttempt(false, 1001);
    const stats = ctrl.getStats();
    expect(stats.attempts).toBe(2);
    expect(stats.successRate).toBe(0.5);
  });
});

// ─── TaskSplitter ────────────────────────────────────────────────────────────

describe('TaskSplitter', () => {
  const splitter = new TaskSplitter();

  it('splits a splittable task', () => {
    const task = makeTask({ id: 'big', splittable: true, estimatedCostMs: 400 });
    const parts = splitter.split(task, 4, 1000);
    expect(parts).toHaveLength(4);
    expect(parts![0].parentId).toBe('big');
    expect(parts![0].splittable).toBe(false);
    expect(parts![0].estimatedCostMs).toBe(100);
  });

  it('returns null for non-splittable', () => {
    const task = makeTask({ splittable: false });
    expect(splitter.split(task, 2, 1000)).toBeNull();
  });

  it('returns null for invalid parts count', () => {
    const task = makeTask({ splittable: true });
    expect(splitter.split(task, 1, 1000)).toBeNull();
    expect(splitter.split(task, 9, 1000)).toBeNull();
  });

  it('optimalParts returns 1 when no idle agents', () => {
    const task = makeTask({ estimatedCostMs: 500 });
    expect(splitter.optimalParts(task, 0)).toBe(1);
    expect(splitter.optimalParts(task, 1)).toBe(1);
  });

  it('optimalParts caps at 4', () => {
    const task = makeTask({ estimatedCostMs: 10000 });
    expect(splitter.optimalParts(task, 10)).toBe(4);
  });
});

// ─── TaskFragmentationAnalyzer ───────────────────────────────────────────────

describe('TaskFragmentationAnalyzer', () => {
  it('tracks fragmentation ratio', () => {
    const fa = new TaskFragmentationAnalyzer();
    fa.recordSplit('parent1', ['c1', 'c2', 'c3']);
    expect(fa.fragmentationRatio(10)).toBe(0.3);
  });

  it('recordCompletion reduces fragments', () => {
    const fa = new TaskFragmentationAnalyzer();
    fa.recordSplit('parent1', ['c1', 'c2']);
    fa.recordCompletion('c1');
    expect(fa.fragmentationRatio(10)).toBe(0.1);
  });

  it('cleans up parent when all children complete', () => {
    const fa = new TaskFragmentationAnalyzer();
    fa.recordSplit('parent1', ['c1', 'c2']);
    fa.recordCompletion('c1');
    fa.recordCompletion('c2');
    expect(fa.fragmentationRatio(10)).toBe(0);
  });

  it('returns 0 fragmentation for 0 total tasks', () => {
    const fa = new TaskFragmentationAnalyzer();
    expect(fa.fragmentationRatio(0)).toBe(0);
  });
});

// ─── VictimSelector ──────────────────────────────────────────────────────────

describe('VictimSelector', () => {
  it('selects victim with highest composite score', () => {
    const topo = new TopologyCostModel();
    const affinity = new AffinityTracker(60000);
    const selector = new VictimSelector(topo, affinity);

    const makeAgent = (id: string, zone: string, rack: string, queueSize: number): any => {
      const dq = new WorkDeque();
      for (let i = 0; i < queueSize; i++) dq.pushBottom(makeTask({ type: 'compute' }));
      return { id, zone, rack, maxConcurrency: 10, deque: dq, capabilities: new Set(['compute']) };
    };

    const thief = makeAgent('thief', 'z1', 'r1', 0);
    const nearby = makeAgent('nearby', 'z1', 'r1', 5);
    const far = makeAgent('far', 'z2', 'r2', 5);

    const policy: StealPolicy = {
      minImbalanceRatio: 0.2, maxStealBatchSize: 2, stealCooldownMs: 100,
      backoffBaseMs: 100, backoffMaxMs: 5000,
      localityWeight: 0.8, affinityWeight: 0.1, loadWeight: 0.1,
    };

    const victim = selector.selectVictim(thief, [thief, nearby, far], policy, 3, 1000);
    expect(victim?.id).toBe('nearby'); // locality-weighted
  });

  it('returns null when no candidates have tasks', () => {
    const topo = new TopologyCostModel();
    const affinity = new AffinityTracker(60000);
    const selector = new VictimSelector(topo, affinity);

    const thief: any = { id: 'thief', zone: 'z1', rack: 'r1', maxConcurrency: 10, deque: new WorkDeque() };
    const other: any = { id: 'other', zone: 'z1', rack: 'r1', maxConcurrency: 10, deque: new WorkDeque() };

    const policy: StealPolicy = {
      minImbalanceRatio: 0.2, maxStealBatchSize: 2, stealCooldownMs: 100,
      backoffBaseMs: 100, backoffMaxMs: 5000,
      localityWeight: 0.3, affinityWeight: 0.3, loadWeight: 0.4,
    };

    expect(selector.selectVictim(thief, [thief, other], policy, 3, 1000)).toBeNull();
  });

  it('respects max distance', () => {
    const topo = new TopologyCostModel();
    const affinity = new AffinityTracker(60000);
    const selector = new VictimSelector(topo, affinity);

    const thief: any = { id: 'thief', zone: 'z1', rack: 'r1', maxConcurrency: 10, deque: new WorkDeque() };
    const far: any = { id: 'far', zone: 'z2', rack: 'r2', maxConcurrency: 10, deque: new WorkDeque() };
    far.deque.pushBottom(makeTask());

    const policy: StealPolicy = {
      minImbalanceRatio: 0.2, maxStealBatchSize: 2, stealCooldownMs: 100,
      backoffBaseMs: 100, backoffMaxMs: 5000,
      localityWeight: 0.3, affinityWeight: 0.3, loadWeight: 0.4,
    };

    expect(selector.selectVictim(thief, [thief, far], policy, 1, 1000)).toBeNull(); // distance 3 > maxDistance 1
  });
});

// ─── AdaptiveWorkStealingPool ────────────────────────────────────────────────

describe('AdaptiveWorkStealingPool', () => {
  it('submits tasks and tracks stats', () => {
    const pool = makePool();
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    pool.submit(makeTask({ type: 'compute' }), 1000);
    const stats = pool.getStats();
    expect(stats.totalEnqueued).toBe(1);
    expect(stats.currentQueueDepth).toBe(1);
  });

  it('throws when submitting to empty pool', () => {
    const pool = makePool();
    expect(() => pool.submit(makeTask(), 1000)).toThrow('No agents in pool');
  });

  it('routes to affinity hint agent', () => {
    const pool = makePool();
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    pool.addAgent('a2', 'z1', 'r1', ['compute'], 10);
    const agentId = pool.submit(makeTask({ affinityHint: 'a2' }), 1000);
    expect(agentId).toBe('a2');
  });

  it('steals work from overloaded to idle agent', () => {
    const pool = makePool();
    pool.addAgent('busy', 'z1', 'r1', ['compute'], 10);
    pool.addAgent('idle', 'z1', 'r1', ['compute'], 10);

    // Load up busy agent
    for (let i = 0; i < 10; i++) {
      pool.submit(makeTask({ affinityHint: 'busy' }), 1000);
    }

    const result = pool.attemptSteal('idle', 2000);
    expect(result.success).toBe(true);
    expect(result.tasks.length).toBeGreaterThan(0);
    expect(result.victim).toBe('busy');
  });

  it('steal fails when no work available', () => {
    const pool = makePool();
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    pool.addAgent('a2', 'z1', 'r1', ['compute'], 10);
    const result = pool.attemptSteal('a1', 1000);
    expect(result.success).toBe(false);
  });

  it('backoff prevents rapid steal attempts', () => {
    const pool = makePool();
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    pool.addAgent('a2', 'z1', 'r1', ['compute'], 10);

    // First fail triggers backoff
    pool.attemptSteal('a1', 1000);
    // Immediate retry should be blocked by backoff
    const result = pool.attemptSteal('a1', 1001);
    expect(result.success).toBe(false);
  });

  it('completeTask updates stats and affinity', () => {
    const pool = makePool();
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    const task = makeTask({ id: 'done', type: 'compute' });
    pool.submit(task, 1000);
    pool.completeTask('a1', 'done', 50, 1100);
    const stats = pool.getStats();
    expect(stats.totalCompleted).toBe(1);
    expect(stats.perAgent[0].completed).toBe(1);
    expect(stats.perAgent[0].avgProcessingMs).toBe(50);
  });

  it('removeAgent returns orphaned tasks', () => {
    const pool = makePool();
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    pool.submit(makeTask({ affinityHint: 'a1' }), 1000);
    pool.submit(makeTask({ affinityHint: 'a1' }), 1000);
    const orphans = pool.removeAgent('a1');
    expect(orphans).toHaveLength(2);
  });

  it('removeAgent returns empty for unknown agent', () => {
    const pool = makePool();
    expect(pool.removeAgent('nonexistent')).toHaveLength(0);
  });

  it('tick triggers rebalance when imbalanced', () => {
    const pool = makePool({ rebalanceIntervalMs: 0 });
    pool.addAgent('busy', 'z1', 'r1', ['compute'], 5);
    pool.addAgent('idle', 'z1', 'r1', ['compute'], 5);

    for (let i = 0; i < 10; i++) {
      pool.submit(makeTask({ affinityHint: 'busy' }), 1000);
    }

    pool.tick(2000);
    const events = pool.getEvents(100);
    const rebalance = events.find(e => e.type === 'rebalance-triggered');
    expect(rebalance).toBeDefined();
  });

  it('splits large tasks when enabled', () => {
    const pool = makePool({
      enableTaskSplitting: true,
      minTaskCostForSplit: 200,
    });
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    pool.addAgent('a2', 'z1', 'r1', ['compute'], 10); // idle agents needed for split

    // Make a2 truly idle by not giving it tasks — but we need idleCount >= 2
    // Actually both start idle. Submit a splittable task.
    const task = makeTask({ splittable: true, estimatedCostMs: 500 });
    pool.submit(task, 1000);

    const events = pool.getEvents(100);
    const splitEvent = events.find(e => e.type === 'task-split');
    expect(splitEvent).toBeDefined();
  });

  it('getEvents returns recent events', () => {
    const pool = makePool();
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    pool.submit(makeTask(), 1000);
    const events = pool.getEvents(10);
    expect(events.length).toBeGreaterThan(0);
    expect(events[0].type).toBe('task-enqueued');
  });

  it('attemptSteal for unknown thief returns failure', () => {
    const pool = makePool();
    const result = pool.attemptSteal('nonexistent', 1000);
    expect(result.success).toBe(false);
  });
});

// ─── Presets ─────────────────────────────────────────────────────────────────

describe('Presets', () => {
  it('interactive preset has conservative settings', () => {
    expect(PRESETS['interactive'].enableTaskSplitting).toBe(false);
    expect(PRESETS['interactive'].maxStealDistance).toBe(2);
  });

  it('batch-processing preset enables splitting', () => {
    expect(PRESETS['batch-processing'].enableTaskSplitting).toBe(true);
    expect(PRESETS['batch-processing'].stealPolicy.maxStealBatchSize).toBe(5);
  });

  it('heterogeneous-pool preset is affinity-heavy', () => {
    expect(PRESETS['heterogeneous-pool'].stealPolicy.affinityWeight).toBe(0.5);
  });

  it('all presets create valid pools', () => {
    for (const [name, config] of Object.entries(PRESETS)) {
      const pool = new AdaptiveWorkStealingPool(config);
      pool.addAgent('test', 'z1', 'r1', ['*'], 10);
      pool.submit(makeTask(), 1000);
      expect(pool.getStats().totalEnqueued).toBe(1);
    }
  });
});
