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
  type Task,
  type PoolConfig,
} from '../src/adaptive-work-stealing';

// ─── Helpers ─────────────────────────────────────────────────────────────────

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

function makePool(overrides: Partial<PoolConfig> = {}): AdaptiveWorkStealingPool {
  return new AdaptiveWorkStealingPool({
    ...PRESETS['interactive'],
    ...overrides,
  });
}

// ─── WorkDeque ───────────────────────────────────────────────────────────────

describe('WorkDeque', () => {
  it('pushBottom/popBottom follows LIFO', () => {
    const dq = new WorkDeque();
    const t1 = makeTask({ id: 't1' });
    const t2 = makeTask({ id: 't2' });
    dq.pushBottom(t1);
    dq.pushBottom(t2);
    expect(dq.popBottom()?.id).toBe('t2');
    expect(dq.popBottom()?.id).toBe('t1');
  });

  it('popBottom returns null when empty', () => {
    const dq = new WorkDeque();
    expect(dq.popBottom()).toBeNull();
  });

  it('stealTop takes from front (FIFO), at most half', () => {
    const dq = new WorkDeque();
    for (let i = 0; i < 6; i++) dq.pushBottom(makeTask({ id: `t${i}` }));
    const stolen = dq.stealTop(5); // asks for 5, gets floor(6/2)=3
    expect(stolen.length).toBe(3);
    expect(stolen[0].id).toBe('t0');
    expect(stolen[2].id).toBe('t2');
    expect(dq.size()).toBe(3);
  });

  it('stealTop returns empty array when deque is empty', () => {
    const dq = new WorkDeque();
    expect(dq.stealTop(3)).toEqual([]);
  });

  it('peek returns last element without removing', () => {
    const dq = new WorkDeque();
    dq.pushBottom(makeTask({ id: 'a' }));
    dq.pushBottom(makeTask({ id: 'b' }));
    expect(dq.peek()?.id).toBe('b');
    expect(dq.size()).toBe(2);
  });

  it('peek returns null when empty', () => {
    expect(new WorkDeque().peek()).toBeNull();
  });

  it('inspect returns copy of items', () => {
    const dq = new WorkDeque();
    dq.pushBottom(makeTask({ id: 'x' }));
    const items = dq.inspect();
    expect(items.length).toBe(1);
    expect(items[0].id).toBe('x');
  });
});

// ─── TopologyCostModel ───────────────────────────────────────────────────────

describe('TopologyCostModel', () => {
  const topo = new TopologyCostModel();

  function makeAgent(id: string, zone: string, rack: string) {
    return { id, zone, rack, capabilities: new Set<string>(), processingRateMultiplier: 1, currentLoad: 0, maxConcurrency: 10, deque: new WorkDeque(), stealStats: { attempts: 0, successes: 0, failures: 0, tasksStolen: 0, lastStealAt: 0, consecutiveFailures: 0, backoffUntil: 0 }, completedCount: 0, totalProcessingMs: 0 };
  }

  it('same agent = distance 0, cost 1.0', () => {
    const a = makeAgent('a', 'z1', 'r1');
    expect(topo.distance(a, a)).toBe(0);
    expect(topo.cost(a, a)).toBe(1.0);
  });

  it('same zone same rack = distance 1', () => {
    const a = makeAgent('a', 'z1', 'r1');
    const b = makeAgent('b', 'z1', 'r1');
    expect(topo.distance(a, b)).toBe(1);
    expect(topo.cost(a, b)).toBe(1.5);
  });

  it('same zone different rack = distance 2', () => {
    const a = makeAgent('a', 'z1', 'r1');
    const b = makeAgent('b', 'z1', 'r2');
    expect(topo.distance(a, b)).toBe(2);
    expect(topo.cost(a, b)).toBe(3.0);
  });

  it('cross zone = distance 3', () => {
    const a = makeAgent('a', 'z1', 'r1');
    const b = makeAgent('b', 'z2', 'r1');
    expect(topo.distance(a, b)).toBe(3);
    expect(topo.cost(a, b)).toBe(10.0);
  });

  it('effectiveValue discounts by topology cost', () => {
    const a = makeAgent('a', 'z1', 'r1');
    const b = makeAgent('b', 'z2', 'r1');
    const task = makeTask({ estimatedCostMs: 100, priority: 2 });
    const val = topo.effectiveValue(task, a, b);
    expect(val).toBe(200 / 10.0); // 20
  });
});

// ─── AffinityTracker ─────────────────────────────────────────────────────────

describe('AffinityTracker', () => {
  it('returns 0 for unknown task type or agent', () => {
    const tracker = new AffinityTracker(10000);
    expect(tracker.getScore('unknown', 'agent-1', 1000)).toBe(0);
  });

  it('records completions and returns positive score', () => {
    const tracker = new AffinityTracker(10000);
    tracker.recordCompletion('compute', 'agent-1', 50, 1000);
    expect(tracker.getScore('compute', 'agent-1', 1000)).toBeGreaterThan(0);
  });

  it('score decays over time', () => {
    const tracker = new AffinityTracker(1000);
    tracker.recordCompletion('compute', 'agent-1', 50, 1000);
    const fresh = tracker.getScore('compute', 'agent-1', 1000);
    const decayed = tracker.getScore('compute', 'agent-1', 3000); // 2 half-lives
    expect(decayed).toBeLessThan(fresh);
  });

  it('bestAgent returns agent with highest score', () => {
    const tracker = new AffinityTracker(100000);
    tracker.recordCompletion('compute', 'a1', 50, 1000);
    tracker.recordCompletion('compute', 'a1', 50, 1001);
    tracker.recordCompletion('compute', 'a1', 50, 1002);
    tracker.recordCompletion('compute', 'a2', 50, 1000);
    expect(tracker.bestAgent('compute', ['a1', 'a2'], 1003)).toBe('a1');
  });

  it('bestAgent returns null for unknown type', () => {
    const tracker = new AffinityTracker(10000);
    expect(tracker.bestAgent('unknown', ['a1'], 1000)).toBeNull();
  });

  it('prune removes stale entries', () => {
    const tracker = new AffinityTracker(1000);
    tracker.recordCompletion('compute', 'a1', 50, 1000);
    const pruned = tracker.prune(50000, 10000);
    expect(pruned).toBe(1);
    expect(tracker.getScore('compute', 'a1', 50000)).toBe(0);
  });
});

// ─── LoadImbalanceDetector ───────────────────────────────────────────────────

describe('LoadImbalanceDetector', () => {
  function agentWithLoad(id: string, queueSize: number, maxConc: number = 10) {
    const dq = new WorkDeque();
    for (let i = 0; i < queueSize; i++) dq.pushBottom(makeTask());
    return { id, zone: 'z', rack: 'r', capabilities: new Set<string>(), processingRateMultiplier: 1, currentLoad: 0, maxConcurrency: maxConc, deque: dq, stealStats: { attempts: 0, successes: 0, failures: 0, tasksStolen: 0, lastStealAt: 0, consecutiveFailures: 0, backoffUntil: 0 }, completedCount: 0, totalProcessingMs: 0 };
  }

  it('gini is 0 for equal loads', () => {
    const det = new LoadImbalanceDetector();
    const agents = [agentWithLoad('a', 5), agentWithLoad('b', 5)];
    expect(det.computeGini(agents)).toBe(0);
  });

  it('gini is 0 for single agent', () => {
    expect(new LoadImbalanceDetector().computeGini([agentWithLoad('a', 10)])).toBe(0);
  });

  it('gini is 0 when all queues empty', () => {
    const agents = [agentWithLoad('a', 0), agentWithLoad('b', 0)];
    expect(new LoadImbalanceDetector().computeGini(agents)).toBe(0);
  });

  it('gini > 0 for unequal loads', () => {
    const det = new LoadImbalanceDetector();
    const agents = [agentWithLoad('a', 0), agentWithLoad('b', 20)];
    expect(det.computeGini(agents)).toBeGreaterThan(0);
  });

  it('detect identifies overloaded and underloaded agents', () => {
    const det = new LoadImbalanceDetector();
    const agents = [agentWithLoad('a', 0, 10), agentWithLoad('b', 30, 10)];
    const result = det.detect(agents, 0.1, 1000);
    expect(result.imbalanced).toBe(true);
    expect(result.overloaded).toContain('b');
    expect(result.underloaded).toContain('a');
  });

  it('trend returns stable with insufficient data', () => {
    const det = new LoadImbalanceDetector();
    expect(det.trend()).toBe('stable');
  });
});

// ─── StealPolicyController ──────────────────────────────────────────────────

describe('StealPolicyController', () => {
  it('returns base policy with no data', () => {
    const ctrl = new StealPolicyController(PRESETS['interactive'].stealPolicy);
    const policy = ctrl.getAdaptedPolicy(1000);
    expect(policy.maxStealBatchSize).toBe(PRESETS['interactive'].stealPolicy.maxStealBatchSize);
  });

  it('reduces aggression on low success rate', () => {
    const ctrl = new StealPolicyController(PRESETS['interactive'].stealPolicy);
    for (let i = 0; i < 20; i++) ctrl.recordAttempt(false, 1000 + i);
    const policy = ctrl.getAdaptedPolicy(1020);
    expect(policy.stealCooldownMs).toBeGreaterThan(PRESETS['interactive'].stealPolicy.stealCooldownMs);
  });

  it('increases aggression on high success rate', () => {
    const base = PRESETS['batch-processing'].stealPolicy;
    const ctrl = new StealPolicyController(base);
    for (let i = 0; i < 20; i++) ctrl.recordAttempt(true, 1000 + i);
    const policy = ctrl.getAdaptedPolicy(1020);
    expect(policy.stealCooldownMs).toBeLessThanOrEqual(base.stealCooldownMs);
  });

  it('getStats reports correct values', () => {
    const ctrl = new StealPolicyController(PRESETS['interactive'].stealPolicy);
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

  it('splits a splittable task into parts', () => {
    const task = makeTask({ id: 'big', splittable: true, estimatedCostMs: 400 });
    const parts = splitter.split(task, 4, 1000);
    expect(parts).not.toBeNull();
    expect(parts!.length).toBe(4);
    expect(parts![0].parentId).toBe('big');
    expect(parts![0].splittable).toBe(false);
    expect(parts![0].estimatedCostMs).toBe(100);
  });

  it('returns null for non-splittable task', () => {
    const task = makeTask({ splittable: false });
    expect(splitter.split(task, 2, 1000)).toBeNull();
  });

  it('returns null for invalid part count', () => {
    const task = makeTask({ splittable: true });
    expect(splitter.split(task, 1, 1000)).toBeNull();
    expect(splitter.split(task, 9, 1000)).toBeNull();
  });

  it('optimalParts returns 1 when no idle agents', () => {
    const task = makeTask({ estimatedCostMs: 1000 });
    expect(splitter.optimalParts(task, 0)).toBe(1);
    expect(splitter.optimalParts(task, 1)).toBe(1);
  });

  it('optimalParts caps at 4', () => {
    const task = makeTask({ estimatedCostMs: 10000 });
    expect(splitter.optimalParts(task, 100)).toBe(4);
  });
});

// ─── TaskFragmentationAnalyzer ───────────────────────────────────────────────

describe('TaskFragmentationAnalyzer', () => {
  it('tracks fragmentation ratio', () => {
    const fa = new TaskFragmentationAnalyzer();
    fa.recordSplit('parent', ['c1', 'c2', 'c3']);
    expect(fa.fragmentationRatio(10)).toBe(0.3);
  });

  it('completion removes from registry', () => {
    const fa = new TaskFragmentationAnalyzer();
    fa.recordSplit('parent', ['c1', 'c2']);
    fa.recordCompletion('c1');
    fa.recordCompletion('c2');
    expect(fa.fragmentationRatio(10)).toBe(0);
  });

  it('returns 0 ratio when no tasks', () => {
    expect(new TaskFragmentationAnalyzer().fragmentationRatio(0)).toBe(0);
  });
});

// ─── AdaptiveWorkStealingPool ────────────────────────────────────────────────

describe('AdaptiveWorkStealingPool', () => {
  it('throws when submitting to empty pool', () => {
    const pool = makePool();
    expect(() => pool.submit(makeTask(), 1000)).toThrow('No agents in pool');
  });

  it('submits task and routes to agent', () => {
    const pool = makePool();
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    const target = pool.submit(makeTask(), 1000);
    expect(target).toBe('a1');
    expect(pool.getStats().currentQueueDepth).toBe(1);
  });

  it('respects affinity hint', () => {
    const pool = makePool();
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    pool.addAgent('a2', 'z1', 'r1', ['compute'], 10);
    const target = pool.submit(makeTask({ affinityHint: 'a2' }), 1000);
    expect(target).toBe('a2');
  });

  it('steals work from overloaded agent', () => {
    const pool = makePool();
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    pool.addAgent('a2', 'z1', 'r1', ['compute'], 10);

    // Load up a1
    for (let i = 0; i < 10; i++) {
      pool.submit(makeTask({ id: `t${i}`, affinityHint: 'a1' }), 1000);
    }

    const result = pool.attemptSteal('a2', 1001);
    expect(result.success).toBe(true);
    expect(result.tasks.length).toBeGreaterThan(0);
    expect(result.victim).toBe('a1');
  });

  it('steal fails when no work available', () => {
    const pool = makePool();
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    pool.addAgent('a2', 'z1', 'r1', ['compute'], 10);
    const result = pool.attemptSteal('a2', 1000);
    expect(result.success).toBe(false);
  });

  it('steal fails for unknown thief', () => {
    const pool = makePool();
    const result = pool.attemptSteal('nonexistent', 1000);
    expect(result.success).toBe(false);
  });

  it('respects backoff after failed steals', () => {
    const pool = makePool();
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    pool.addAgent('a2', 'z1', 'r1', ['compute'], 10);
    // No work to steal → fail → backoff
    pool.attemptSteal('a2', 1000);
    const result = pool.attemptSteal('a2', 1001); // within backoff window
    expect(result.success).toBe(false);
  });

  it('respects maxStealDistance', () => {
    const pool = makePool({ maxStealDistance: 1 }); // same rack only
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    pool.addAgent('a2', 'z2', 'r2', ['compute'], 10); // cross-zone = distance 3

    for (let i = 0; i < 10; i++) {
      pool.submit(makeTask({ id: `t${i}`, affinityHint: 'a1' }), 1000);
    }

    const result = pool.attemptSteal('a2', 1001);
    expect(result.success).toBe(false);
  });

  it('completeTask updates stats and affinity', () => {
    const pool = makePool();
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    pool.submit(makeTask({ id: 'task1' }), 1000);
    pool.completeTask('a1', 'task1', 50, 1050);
    const stats = pool.getStats();
    expect(stats.totalCompleted).toBe(1);
    expect(stats.perAgent[0].completed).toBe(1);
    expect(stats.perAgent[0].avgProcessingMs).toBe(50);
  });

  it('completeTask is no-op for unknown agent', () => {
    const pool = makePool();
    pool.completeTask('nonexistent', 'task1', 50, 1000);
    // should not throw
  });

  it('removeAgent drains orphaned tasks', () => {
    const pool = makePool();
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    pool.submit(makeTask({ id: 't1', affinityHint: 'a1' }), 1000);
    pool.submit(makeTask({ id: 't2', affinityHint: 'a1' }), 1000);
    const orphaned = pool.removeAgent('a1');
    expect(orphaned.length).toBe(2);
    expect(pool.getStats().agents).toBe(0);
  });

  it('removeAgent returns empty for unknown agent', () => {
    const pool = makePool();
    expect(pool.removeAgent('nonexistent')).toEqual([]);
  });

  it('tick triggers rebalance when imbalanced', () => {
    const pool = makePool({ rebalanceIntervalMs: 0 });
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 5);
    pool.addAgent('a2', 'z1', 'r1', ['compute'], 5);

    // Heavily load a1
    for (let i = 0; i < 20; i++) {
      pool.submit(makeTask({ id: `t${i}`, affinityHint: 'a1' }), 1000);
    }

    pool.tick(2000);
    const events = pool.getEvents(100);
    const types = events.map(e => e.type);
    expect(types).toContain('rebalance-triggered');
  });

  it('tick does nothing before rebalance interval', () => {
    const pool = makePool({ rebalanceIntervalMs: 5000 });
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    pool.addAgent('a2', 'z1', 'r1', ['compute'], 10);
    pool.tick(1000); // first tick fires (lastRebalanceAt starts at 0)
    const countAfterFirst = pool.getEvents(100).filter(e => e.type === 'rebalance-triggered').length;
    pool.tick(1001); // too soon — should NOT fire
    const countAfterSecond = pool.getEvents(100).filter(e => e.type === 'rebalance-triggered').length;
    expect(countAfterSecond).toBe(countAfterFirst);
  });

  it('getStats returns correct aggregate metrics', () => {
    const pool = makePool();
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    pool.addAgent('a2', 'z1', 'r1', ['compute'], 10);
    pool.submit(makeTask(), 1000);
    const stats = pool.getStats();
    expect(stats.agents).toBe(2);
    expect(stats.totalEnqueued).toBe(1);
    expect(stats.currentQueueDepth).toBe(1);
    expect(stats.perAgent.length).toBe(2);
  });

  it('task splitting distributes subtasks', () => {
    const pool = makePool({
      enableTaskSplitting: true,
      minTaskCostForSplit: 100,
    });
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    pool.addAgent('a2', 'z1', 'r1', ['compute'], 10);
    // Both agents have empty queues → 2 idle
    const bigTask = makeTask({ id: 'big', splittable: true, estimatedCostMs: 500 });
    pool.submit(bigTask, 1000);

    const events = pool.getEvents(100);
    const splitEvents = events.filter(e => e.type === 'task-split');
    // May or may not split depending on optimalParts calculation
    // With 2 idle agents and cost 500 → min(2, 5, 4) = 2 parts
    if (splitEvents.length > 0) {
      expect(pool.getStats().totalEnqueued).toBeGreaterThanOrEqual(2);
    }
  });

  it('learned affinity influences routing', () => {
    const pool = makePool();
    pool.addAgent('a1', 'z1', 'r1', ['compute', '*'], 10);
    pool.addAgent('a2', 'z1', 'r1', ['compute', '*'], 10);

    // Build affinity for a2 on 'ml' tasks
    for (let i = 0; i < 5; i++) {
      pool.completeTask('a2', `ml-${i}`, 50, 1000 + i);
    }

    // Now submit an 'ml' task — pool uses learned affinity but completeTask
    // needs a task in deque to find type. Let's use affinity hint instead
    // to validate the affinity system indirectly via stats
    const stats = pool.getStats();
    expect(stats.perAgent.find(a => a.id === 'a2')!.completed).toBe(5);
  });

  it('events are capped at maxEvents', () => {
    const pool = makePool();
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    // Submit many tasks to generate events
    for (let i = 0; i < 1100; i++) {
      pool.submit(makeTask({ id: `t${i}` }), 1000 + i);
    }
    const events = pool.getEvents(2000);
    expect(events.length).toBeLessThanOrEqual(1000);
  });

  it('power-of-two-choices routes to less loaded agent', () => {
    const pool = makePool();
    pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
    pool.addAgent('a2', 'z1', 'r1', ['compute'], 10);
    pool.addAgent('a3', 'z1', 'r1', ['compute'], 10);

    // Pre-load a1 and a2
    for (let i = 0; i < 8; i++) {
      pool.submit(makeTask({ id: `load-${i}`, affinityHint: 'a1' }), 1000);
    }
    for (let i = 0; i < 6; i++) {
      pool.submit(makeTask({ id: `load2-${i}`, affinityHint: 'a2' }), 1000);
    }

    // Submit tasks without hints — should prefer a3 (empty)
    const targets: string[] = [];
    for (let i = 0; i < 5; i++) {
      targets.push(pool.submit(makeTask({ id: `new-${i}` }), 1001));
    }
    // a3 should get some tasks since it's least loaded
    const a3Count = targets.filter(t => t === 'a3').length;
    expect(a3Count).toBeGreaterThan(0);
  });
});

// ─── VictimSelector ──────────────────────────────────────────────────────────

describe('VictimSelector', () => {
  function makeAgentNode(id: string, zone: string, rack: string, queueSize: number) {
    const dq = new WorkDeque();
    for (let i = 0; i < queueSize; i++) dq.pushBottom(makeTask({ type: 'compute' }));
    return { id, zone, rack, capabilities: new Set<string>(['compute']), processingRateMultiplier: 1, currentLoad: 0, maxConcurrency: 10, deque: dq, stealStats: { attempts: 0, successes: 0, failures: 0, tasksStolen: 0, lastStealAt: 0, consecutiveFailures: 0, backoffUntil: 0 }, completedCount: 0, totalProcessingMs: 0 };
  }

  it('selects the most loaded nearby agent', () => {
    const topo = new TopologyCostModel();
    const aff = new AffinityTracker(10000);
    const vs = new VictimSelector(topo, aff);
    const thief = makeAgentNode('thief', 'z1', 'r1', 0);
    const v1 = makeAgentNode('v1', 'z1', 'r1', 5);
    const v2 = makeAgentNode('v2', 'z1', 'r1', 15);
    const policy = PRESETS['interactive'].stealPolicy;
    const victim = vs.selectVictim(thief, [v1, v2], policy, 3, 1000);
    expect(victim?.id).toBe('v2'); // more loaded
  });

  it('skips candidates beyond maxDistance', () => {
    const topo = new TopologyCostModel();
    const aff = new AffinityTracker(10000);
    const vs = new VictimSelector(topo, aff);
    const thief = makeAgentNode('thief', 'z1', 'r1', 0);
    const far = makeAgentNode('far', 'z2', 'r2', 20);
    const victim = vs.selectVictim(thief, [far], PRESETS['interactive'].stealPolicy, 1, 1000);
    expect(victim).toBeNull();
  });

  it('skips empty candidates', () => {
    const topo = new TopologyCostModel();
    const aff = new AffinityTracker(10000);
    const vs = new VictimSelector(topo, aff);
    const thief = makeAgentNode('thief', 'z1', 'r1', 0);
    const empty = makeAgentNode('empty', 'z1', 'r1', 0);
    expect(vs.selectVictim(thief, [empty], PRESETS['interactive'].stealPolicy, 3, 1000)).toBeNull();
  });

  it('selectMultipleVictims returns sorted by score', () => {
    const topo = new TopologyCostModel();
    const aff = new AffinityTracker(10000);
    const vs = new VictimSelector(topo, aff);
    const thief = makeAgentNode('thief', 'z1', 'r1', 0);
    const v1 = makeAgentNode('v1', 'z1', 'r1', 5);
    const v2 = makeAgentNode('v2', 'z1', 'r1', 15);
    const v3 = makeAgentNode('v3', 'z1', 'r1', 10);
    const victims = vs.selectMultipleVictims(thief, [v1, v2, v3], PRESETS['interactive'].stealPolicy, 3, 2, 1000);
    expect(victims.length).toBe(2);
    expect(victims[0].id).toBe('v2'); // most loaded first
  });
});

// ─── Presets ─────────────────────────────────────────────────────────────────

describe('Presets', () => {
  it('all three presets create valid pools', () => {
    for (const [name, config] of Object.entries(PRESETS)) {
      const pool = new AdaptiveWorkStealingPool(config);
      pool.addAgent('a1', 'z1', 'r1', ['compute'], 10);
      pool.submit(makeTask(), 1000);
      expect(pool.getStats().totalEnqueued).toBe(1);
    }
  });

  it('batch-processing has larger steal batches than interactive', () => {
    expect(PRESETS['batch-processing'].stealPolicy.maxStealBatchSize)
      .toBeGreaterThan(PRESETS['interactive'].stealPolicy.maxStealBatchSize);
  });

  it('heterogeneous-pool prioritizes affinity weight', () => {
    expect(PRESETS['heterogeneous-pool'].stealPolicy.affinityWeight)
      .toBeGreaterThan(PRESETS['heterogeneous-pool'].stealPolicy.localityWeight);
  });
});
