import { describe, it, expect, beforeEach } from 'vitest';
import {
  SchedulerAffinityGraph,
  AffinityGraph,
  AgentLoadTracker,
  AntiAffinityManager,
  DeadlineTracker,
  BatchOptimizer,
  PRESETS,
} from '../scheduler-affinity-graph';
import type { AgentProfile, TaskType, Task, SchedulerConfig } from '../scheduler-affinity-graph';

// ─── Helpers ─────────────────────────────────────────────────────────────────

function makeAgent(id: string, caps: string[] = ['compute'], maxConcurrency = 3): AgentProfile {
  return { id, capabilities: new Set(caps), maxConcurrency, currentLoad: 0, costPerUnit: 1, lastSeen: 0 };
}

function makeTaskType(id: string, caps: string[] = ['compute'], opts: Partial<TaskType> = {}): TaskType {
  return { id, requiredCapabilities: caps, priority: 5, idempotent: true, ...opts };
}

function makeTask(id: string, typeId: string, opts: Partial<Task> = {}): Task {
  return { id, typeId, payload: null, submittedAt: 0, priority: 5, attempts: 0, maxAttempts: 3, ...opts };
}

const NOW = 1_000_000;

// ─── AffinityGraph ───────────────────────────────────────────────────────────

describe('AffinityGraph', () => {
  let graph: AffinityGraph;

  beforeEach(() => {
    graph = new AffinityGraph(60_000); // 1 min half-life
  });

  it('returns neutral score for unknown edge', () => {
    expect(graph.getAffinityScore('a1', 'tt1', NOW)).toBe(0.5);
  });

  it('records success and increases affinity', () => {
    graph.recordSuccess('a1', 'tt1', 100, 1, NOW);
    graph.recordSuccess('a1', 'tt1', 120, 1, NOW);
    const score = graph.getAffinityScore('a1', 'tt1', NOW);
    expect(score).toBeGreaterThan(0.5);
  });

  it('records failure and decreases affinity', () => {
    graph.recordFailure('a1', 'tt1', NOW);
    graph.recordFailure('a1', 'tt1', NOW);
    const score = graph.getAffinityScore('a1', 'tt1', NOW);
    expect(score).toBeLessThan(0.5);
  });

  it('decays affinity toward 0.5 over time', () => {
    graph.recordSuccess('a1', 'tt1', 100, 1, NOW);
    graph.recordSuccess('a1', 'tt1', 100, 1, NOW);
    graph.recordSuccess('a1', 'tt1', 100, 1, NOW);
    const fresh = graph.getAffinityScore('a1', 'tt1', NOW);
    const decayed = graph.getAffinityScore('a1', 'tt1', NOW + 600_000); // 10 half-lives
    expect(decayed).toBeCloseTo(0.5, 1);
    expect(fresh).toBeGreaterThan(decayed);
  });

  it('computes average latency and cost', () => {
    graph.recordSuccess('a1', 'tt1', 100, 2, NOW);
    graph.recordSuccess('a1', 'tt1', 200, 4, NOW);
    expect(graph.getAverageLatency('a1', 'tt1')).toBe(150);
    expect(graph.getAverageCost('a1', 'tt1')).toBe(3);
  });

  it('returns null latency/cost for unknown edges', () => {
    expect(graph.getAverageLatency('x', 'y')).toBeNull();
    expect(graph.getAverageCost('x', 'y')).toBeNull();
  });

  it('counts observations', () => {
    expect(graph.getObservationCount('a1', 'tt1')).toBe(0);
    graph.recordSuccess('a1', 'tt1', 100, 1, NOW);
    graph.recordFailure('a1', 'tt1', NOW);
    expect(graph.getObservationCount('a1', 'tt1')).toBe(2);
  });

  it('prunes old edges', () => {
    graph.recordSuccess('a1', 'tt1', 100, 1, NOW);
    expect(graph.edgeCount()).toBe(1);
    // After many half-lives, prune should remove the edge
    const pruned = graph.prune(NOW + 60_000 * 20); // 20 half-lives
    expect(pruned).toBe(1);
    expect(graph.edgeCount()).toBe(0);
  });

  it('does not prune fresh edges', () => {
    graph.recordSuccess('a1', 'tt1', 100, 1, NOW);
    const pruned = graph.prune(NOW);
    expect(pruned).toBe(0);
    expect(graph.edgeCount()).toBe(1);
  });

  it('sampleThompson returns a value between 0 and 1', () => {
    graph.recordSuccess('a1', 'tt1', 100, 1, NOW);
    for (let i = 0; i < 20; i++) {
      const sample = graph.sampleThompson('a1', 'tt1');
      expect(sample).toBeGreaterThanOrEqual(0);
      expect(sample).toBeLessThanOrEqual(1);
    }
  });

  it('sampleThompson with no data returns value in [0,1]', () => {
    const sample = graph.sampleThompson('unknown', 'unknown');
    expect(sample).toBeGreaterThanOrEqual(0);
    expect(sample).toBeLessThanOrEqual(1);
  });

  it('getOrCreateEdge creates edge with uniform prior', () => {
    const edge = graph.getOrCreateEdge('a1', 'tt1', NOW);
    expect(edge.alpha).toBe(1);
    expect(edge.beta).toBe(1);
    expect(edge.successCount).toBe(0);
  });
});

// ─── AgentLoadTracker ────────────────────────────────────────────────────────

describe('AgentLoadTracker', () => {
  let tracker: AgentLoadTracker;

  beforeEach(() => {
    tracker = new AgentLoadTracker();
  });

  it('registers and retrieves agents', () => {
    const agent = makeAgent('a1');
    tracker.registerAgent(agent);
    expect(tracker.getAgent('a1')).toBe(agent);
  });

  it('removes agents', () => {
    tracker.registerAgent(makeAgent('a1'));
    tracker.removeAgent('a1');
    expect(tracker.getAgent('a1')).toBeUndefined();
  });

  it('tracks load through assign/release', () => {
    tracker.registerAgent(makeAgent('a1', ['compute'], 2));
    expect(tracker.getLoadFraction('a1')).toBe(0);

    tracker.assignTask('a1', 't1');
    expect(tracker.getLoadFraction('a1')).toBe(0.5);

    tracker.assignTask('a1', 't2');
    expect(tracker.getLoadFraction('a1')).toBe(1);

    // At capacity — should reject
    expect(tracker.assignTask('a1', 't3')).toBe(false);

    tracker.releaseTask('a1', 't1');
    expect(tracker.getLoadFraction('a1')).toBe(0.5);
  });

  it('returns load 1 for unknown agents', () => {
    expect(tracker.getLoadFraction('unknown')).toBe(1);
  });

  it('returns 0 capacity for unknown agents', () => {
    expect(tracker.getAvailableCapacity('unknown')).toBe(0);
  });

  it('filters eligible agents by capability and capacity', () => {
    tracker.registerAgent(makeAgent('a1', ['compute', 'gpu'], 2));
    tracker.registerAgent(makeAgent('a2', ['compute'], 2));
    tracker.registerAgent(makeAgent('a3', ['compute', 'gpu'], 1));

    // Fill a3 to capacity
    tracker.assignTask('a3', 't0');

    const tt = makeTaskType('tt1', ['compute', 'gpu']);
    const eligible = tracker.getEligibleAgents(tt);
    expect(eligible.map(a => a.id)).toEqual(['a1']); // a2 lacks gpu, a3 full
  });

  it('getAllAgents returns all registered', () => {
    tracker.registerAgent(makeAgent('a1'));
    tracker.registerAgent(makeAgent('a2'));
    expect(tracker.getAllAgents()).toHaveLength(2);
  });

  it('assignTask returns false for unknown agent', () => {
    expect(tracker.assignTask('unknown', 't1')).toBe(false);
  });
});

// ─── AntiAffinityManager ────────────────────────────────────────────────────

describe('AntiAffinityManager', () => {
  let mgr: AntiAffinityManager;

  beforeEach(() => {
    mgr = new AntiAffinityManager(10_000);
  });

  it('returns 0 penalty with no history', () => {
    expect(mgr.getPenalty('tt1', 'a1', NOW)).toBe(0);
  });

  it('increases penalty for concentrated assignments', () => {
    mgr.recordAssignment('tt1', 'a1', NOW);
    mgr.recordAssignment('tt1', 'a1', NOW);
    mgr.recordAssignment('tt1', 'a2', NOW);
    // a1 got 2/3 assignments
    expect(mgr.getPenalty('tt1', 'a1', NOW)).toBeCloseTo(2 / 3);
    expect(mgr.getPenalty('tt1', 'a2', NOW)).toBeCloseTo(1 / 3);
  });

  it('expires old assignments', () => {
    mgr.recordAssignment('tt1', 'a1', NOW);
    // After window expires
    expect(mgr.getPenalty('tt1', 'a1', NOW + 20_000)).toBe(0);
  });

  it('checks explicit exclusions', () => {
    const tt = makeTaskType('tt1', ['compute'], { antiAffinityHints: ['a1'] });
    expect(mgr.isExplicitlyExcluded(tt, 'a1')).toBe(true);
    expect(mgr.isExplicitlyExcluded(tt, 'a2')).toBe(false);
  });

  it('handles task type with no antiAffinityHints', () => {
    const tt = makeTaskType('tt1');
    expect(mgr.isExplicitlyExcluded(tt, 'a1')).toBe(false);
  });
});

// ─── DeadlineTracker ─────────────────────────────────────────────────────────

describe('DeadlineTracker', () => {
  let dt: DeadlineTracker;

  beforeEach(() => {
    dt = new DeadlineTracker();
  });

  it('returns 0.5 urgency for tasks with no deadline', () => {
    const task = makeTask('t1', 'tt1');
    expect(dt.getUrgency(task, NOW, 5000)).toBe(0.5);
  });

  it('returns 1 for past-deadline tasks', () => {
    const task = makeTask('t1', 'tt1', { submittedAt: NOW, deadline: NOW + 100 });
    expect(dt.getUrgency(task, NOW + 200, 5000)).toBe(1);
  });

  it('returns high urgency within buffer zone', () => {
    const task = makeTask('t1', 'tt1', { submittedAt: NOW, deadline: NOW + 10_000 });
    // 2000ms remaining, buffer is 5000 → within buffer
    const urgency = dt.getUrgency(task, NOW + 8_000, 5_000);
    expect(urgency).toBeGreaterThan(0.9);
  });

  it('tracks breached tasks', () => {
    const t1 = makeTask('t1', 'tt1', { deadline: NOW + 100 });
    const t2 = makeTask('t2', 'tt1', { deadline: NOW + 10_000 });
    dt.addTask(t1);
    dt.addTask(t2);
    const breached = dt.getBreachedTasks(NOW + 500);
    expect(breached.map(t => t.id)).toEqual(['t1']);
  });

  it('removes tasks', () => {
    const t1 = makeTask('t1', 'tt1', { deadline: NOW + 100 });
    dt.addTask(t1);
    dt.removeTask('t1');
    expect(dt.getBreachedTasks(NOW + 500)).toHaveLength(0);
  });
});

// ─── BatchOptimizer ──────────────────────────────────────────────────────────

describe('BatchOptimizer', () => {
  let optimizer: BatchOptimizer;

  beforeEach(() => {
    optimizer = new BatchOptimizer();
  });

  it('assigns tasks to highest-scoring agents', () => {
    const tasks = [makeTask('t1', 'tt1', { priority: 5 })];
    const agents = [makeAgent('a1'), makeAgent('a2')];
    const scores = new Map([
      ['t1', new Map([['a1', 0.8], ['a2', 0.9]])],
    ]);
    const caps = new Map([['a1', 2], ['a2', 2]]);

    const results = optimizer.optimize(tasks, agents, scores, caps);
    expect(results).toHaveLength(1);
    expect(results[0].agentId).toBe('a2');
  });

  it('respects capacity limits', () => {
    const tasks = [
      makeTask('t1', 'tt1', { priority: 5 }),
      makeTask('t2', 'tt1', { priority: 5 }),
    ];
    const agents = [makeAgent('a1')];
    const scores = new Map([
      ['t1', new Map([['a1', 0.9]])],
      ['t2', new Map([['a1', 0.8]])],
    ]);
    const caps = new Map([['a1', 1]]);

    const results = optimizer.optimize(tasks, agents, scores, caps);
    expect(results).toHaveLength(1);
  });

  it('prioritizes higher-priority tasks', () => {
    const tasks = [
      makeTask('t-low', 'tt1', { priority: 1 }),
      makeTask('t-high', 'tt1', { priority: 10 }),
    ];
    const agents = [makeAgent('a1')];
    const scores = new Map([
      ['t-low', new Map([['a1', 0.9]])],
      ['t-high', new Map([['a1', 0.9]])],
    ]);
    const caps = new Map([['a1', 1]]);

    const results = optimizer.optimize(tasks, agents, scores, caps);
    expect(results[0].taskId).toBe('t-high');
  });

  it('handles empty inputs', () => {
    expect(optimizer.optimize([], [], new Map(), new Map())).toEqual([]);
  });

  it('skips tasks with no scores', () => {
    const tasks = [makeTask('t1', 'tt1')];
    const results = optimizer.optimize(tasks, [makeAgent('a1')], new Map(), new Map([['a1', 2]]));
    expect(results).toHaveLength(0);
  });
});

// ─── SchedulerAffinityGraph (Orchestrator) ───────────────────────────────────

describe('SchedulerAffinityGraph', () => {
  let scheduler: SchedulerAffinityGraph;
  const config: SchedulerConfig = {
    ...PRESETS['balanced'],
    explorationRate: 0, // deterministic for tests
    batchWindowMs: 0,   // no batching delay
  };

  beforeEach(() => {
    scheduler = new SchedulerAffinityGraph(config);
  });

  function setupBasic() {
    scheduler.registerAgent(makeAgent('a1', ['compute'], 3));
    scheduler.registerAgent(makeAgent('a2', ['compute'], 3));
    scheduler.registerTaskType(makeTaskType('tt1', ['compute'], { maxLatencyMs: 500 }));
  }

  it('assigns tasks immediately', () => {
    setupBasic();
    const task = makeTask('t1', 'tt1');
    const result = scheduler.assignImmediate(task, NOW);
    expect(result).not.toBeNull();
    expect(['a1', 'a2']).toContain(result!.agentId);
  });

  it('returns null for unknown task type', () => {
    setupBasic();
    const task = makeTask('t1', 'unknown-type');
    expect(scheduler.assignImmediate(task, NOW)).toBeNull();
  });

  it('returns null when no eligible agents', () => {
    scheduler.registerAgent(makeAgent('a1', ['gpu'], 3));
    scheduler.registerTaskType(makeTaskType('tt1', ['compute']));
    const task = makeTask('t1', 'tt1');
    expect(scheduler.assignImmediate(task, NOW)).toBeNull();
  });

  it('learns affinity from completions', () => {
    setupBasic();
    // Train a1 as good at tt1
    for (let i = 0; i < 10; i++) {
      scheduler.recordCompletion('a1', `train-${i}`, 'tt1', 50, 1, NOW);
    }
    // Train a2 as bad at tt1
    for (let i = 0; i < 10; i++) {
      scheduler.recordFailure('a2', `train-${i}`, 'tt1', NOW);
    }

    // Now assign — should prefer a1
    const task = makeTask('t1', 'tt1');
    const result = scheduler.assignImmediate(task, NOW);
    expect(result).not.toBeNull();
    expect(result!.agentId).toBe('a1');
  });

  it('respects anti-affinity exclusions', () => {
    scheduler.registerAgent(makeAgent('a1', ['compute'], 3));
    scheduler.registerAgent(makeAgent('a2', ['compute'], 3));
    scheduler.registerTaskType(makeTaskType('tt1', ['compute'], { antiAffinityHints: ['a1'] }));

    const task = makeTask('t1', 'tt1');
    const result = scheduler.assignImmediate(task, NOW);
    expect(result).not.toBeNull();
    expect(result!.agentId).toBe('a2');
  });

  it('processes batch assignments', () => {
    setupBasic();
    for (let i = 0; i < 5; i++) {
      scheduler.submitTask(makeTask(`t${i}`, 'tt1', { priority: i }));
    }
    expect(scheduler.getPendingCount()).toBe(5);

    const results = scheduler.processBatch(NOW);
    expect(results.length).toBeGreaterThan(0);
    expect(results.length).toBeLessThanOrEqual(5);
    expect(scheduler.getPendingCount()).toBeLessThan(5);
  });

  it('tick processes batch and detects deadline breaches', () => {
    // Register agent with no capacity so task stays pending in deadline tracker
    scheduler.registerAgent(makeAgent('a1', ['gpu'], 0)); // wrong capability
    scheduler.registerTaskType(makeTaskType('tt1', ['compute']));
    scheduler.submitTask(makeTask('t1', 'tt1', { deadline: NOW - 100, submittedAt: NOW - 1000 }));

    const { breached } = scheduler.tick(NOW);
    expect(breached.some(t => t.id === 't1')).toBe(true);
  });

  it('tracks graph edges from completions', () => {
    setupBasic();
    expect(scheduler.getGraphEdgeCount()).toBe(0);
    scheduler.recordCompletion('a1', 't1', 'tt1', 100, 1, NOW);
    expect(scheduler.getGraphEdgeCount()).toBe(1);
  });

  it('gets affinity score', () => {
    setupBasic();
    expect(scheduler.getAffinityScore('a1', 'tt1', NOW)).toBe(0.5);
    scheduler.recordCompletion('a1', 't1', 'tt1', 100, 1, NOW);
    expect(scheduler.getAffinityScore('a1', 'tt1', NOW)).toBeGreaterThan(0.5);
  });

  it('emits events', () => {
    setupBasic();
    const task = makeTask('t1', 'tt1');
    scheduler.assignImmediate(task, NOW);
    const events = scheduler.getRecentEvents();
    expect(events.length).toBeGreaterThan(0);
    expect(events.some(e => e.type === 'task-assigned')).toBe(true);
  });

  it('emits completion and affinity-updated events', () => {
    setupBasic();
    scheduler.recordCompletion('a1', 't1', 'tt1', 100, 1, NOW);
    const events = scheduler.getRecentEvents();
    expect(events.some(e => e.type === 'task-completed')).toBe(true);
    expect(events.some(e => e.type === 'affinity-updated')).toBe(true);
  });

  it('emits task-failed events', () => {
    setupBasic();
    scheduler.recordFailure('a1', 't1', 'tt1', NOW);
    const events = scheduler.getRecentEvents();
    expect(events.some(e => e.type === 'task-failed')).toBe(true);
  });

  it('releases capacity on completion', () => {
    setupBasic();
    const task = makeTask('t1', 'tt1');
    const result = scheduler.assignImmediate(task, NOW)!;
    // Agent now has 1 task
    scheduler.recordCompletion(result.agentId, 't1', 'tt1', 100, 1, NOW);
    // Should be able to assign 3 more tasks
    for (let i = 0; i < 3; i++) {
      const r = scheduler.assignImmediate(makeTask(`tx${i}`, 'tt1'), NOW);
      expect(r).not.toBeNull();
    }
  });

  it('requeues unassigned tasks from batch', () => {
    scheduler.registerAgent(makeAgent('a1', ['compute'], 1));
    scheduler.registerTaskType(makeTaskType('tt1', ['compute']));

    // Submit 3 tasks but agent can only handle 1
    for (let i = 0; i < 3; i++) {
      scheduler.submitTask(makeTask(`t${i}`, 'tt1'));
    }
    scheduler.processBatch(NOW);
    expect(scheduler.getPendingCount()).toBe(2); // 2 requeued
  });

  it('respects affinity hints (region bonus)', () => {
    scheduler.registerAgent(makeAgent('a1', ['compute'], 3));
    scheduler.registerAgent(makeAgent('a2', ['compute'], 3));
    scheduler.registerTaskType(makeTaskType('tt1', ['compute'], { affinityHints: ['a2'] }));

    const task = makeTask('t1', 'tt1');
    const result = scheduler.assignImmediate(task, NOW);
    expect(result).not.toBeNull();
    expect(result!.agentId).toBe('a2');
  });
});

// ─── Presets ─────────────────────────────────────────────────────────────────

describe('Presets', () => {
  it('all presets create working schedulers', () => {
    for (const [name, preset] of Object.entries(PRESETS)) {
      const s = new SchedulerAffinityGraph(preset);
      s.registerAgent(makeAgent('a1'));
      s.registerTaskType(makeTaskType('tt1'));
      const result = s.assignImmediate(makeTask('t1', 'tt1'), NOW);
      expect(result, `preset ${name} should assign`).not.toBeNull();
    }
  });

  it('interactive has low batch window', () => {
    expect(PRESETS['interactive'].batchWindowMs).toBeLessThan(100);
  });

  it('batch-processing has high batch size', () => {
    expect(PRESETS['batch-processing'].maxBatchSize).toBeGreaterThan(50);
  });
});
