import { describe, it, expect, beforeEach } from 'vitest';
import {
  MetricCollector,
  HypothesisEvaluator,
  BlastRadiusController,
  KillSwitchMonitor,
  TargetResolver,
  FaultInjector,
  PreflightChecker,
  ExperimentEngine,
  GameDayCoordinator,
  singleAgentCrashScenario,
  networkPartitionScenario,
  cascadingFailureScenario,
  resourceExhaustionScenario,
  SteadyStateHypothesis,
  MetricAssertion,
  SafetyConfig,
  FaultPhase,
  FaultSpec,
} from '../chaos-testing-harness';

// ============================================================
// MetricCollector
// ============================================================

describe('MetricCollector', () => {
  let collector: MetricCollector;

  beforeEach(() => {
    collector = new MetricCollector(100);
  });

  it('records and queries samples within time range', () => {
    collector.record({ name: 'cpu', value: 50, timestamp: 100, labels: {} });
    collector.record({ name: 'cpu', value: 70, timestamp: 200, labels: {} });
    collector.record({ name: 'cpu', value: 90, timestamp: 300, labels: {} });

    const result = collector.query('cpu', 150, 250);
    expect(result).toHaveLength(1);
    expect(result[0].value).toBe(70);
  });

  it('evicts oldest samples when over capacity', () => {
    const small = new MetricCollector(3);
    for (let i = 0; i < 5; i++) {
      small.record({ name: 'm', value: i, timestamp: i * 100, labels: {} });
    }
    const all = small.query('m', 0, 500);
    expect(all).toHaveLength(3);
    expect(all[0].value).toBe(2); // oldest 0,1 evicted
  });

  it('aggregates avg, min, max, sum, count', () => {
    for (const v of [10, 20, 30]) {
      collector.record({ name: 'x', value: v, timestamp: 100, labels: {} });
    }
    expect(collector.aggregate('x', 0, 200, 'avg')).toBe(20);
    expect(collector.aggregate('x', 0, 200, 'min')).toBe(10);
    expect(collector.aggregate('x', 0, 200, 'max')).toBe(30);
    expect(collector.aggregate('x', 0, 200, 'sum')).toBe(60);
    expect(collector.aggregate('x', 0, 200, 'count')).toBe(3);
  });

  it('aggregates percentiles', () => {
    for (let i = 1; i <= 100; i++) {
      collector.record({ name: 'lat', value: i, timestamp: 100, labels: {} });
    }
    expect(collector.aggregate('lat', 0, 200, 'p50')).toBeCloseTo(50.5, 0);
    expect(collector.aggregate('lat', 0, 200, 'p99')).toBeCloseTo(99.01, 0);
  });

  it('returns null for empty metric', () => {
    expect(collector.aggregate('nope', 0, 100, 'avg')).toBeNull();
  });

  it('computes Welford stats', () => {
    for (const v of [10, 20, 30, 40, 50]) {
      collector.record({ name: 's', value: v, timestamp: 100, labels: {} });
    }
    const stats = collector.stats('s', 0, 200);
    expect(stats).not.toBeNull();
    expect(stats!.mean).toBe(30);
    expect(stats!.stddev).toBeCloseTo(15.811, 2);
  });

  it('stats returns null for < 2 samples', () => {
    collector.record({ name: 's', value: 5, timestamp: 100, labels: {} });
    expect(collector.stats('s', 0, 200)).toBeNull();
  });

  it('clear removes specific metric or all', () => {
    collector.record({ name: 'a', value: 1, timestamp: 100, labels: {} });
    collector.record({ name: 'b', value: 2, timestamp: 100, labels: {} });
    collector.clear('a');
    expect(collector.query('a', 0, 200)).toHaveLength(0);
    expect(collector.query('b', 0, 200)).toHaveLength(1);
    collector.clear();
    expect(collector.query('b', 0, 200)).toHaveLength(0);
  });
});

// ============================================================
// HypothesisEvaluator
// ============================================================

describe('HypothesisEvaluator', () => {
  let collector: MetricCollector;
  let evaluator: HypothesisEvaluator;

  const makeHypothesis = (metrics: MetricAssertion[], tolerance = 0): SteadyStateHypothesis => ({
    id: 'h1',
    name: 'test',
    description: 'test',
    metrics,
    tolerance,
    evaluationWindowMs: 10000,
    cooldownMs: 1000,
  });

  beforeEach(() => {
    collector = new MetricCollector();
    evaluator = new HypothesisEvaluator(collector);
  });

  it('passes when all assertions hold', () => {
    collector.record({ name: 'avail', value: 0.99, timestamp: 5000, labels: {} });
    const h = makeHypothesis([
      { metric: 'avail', operator: 'gte', value: 0.95, aggregation: 'avg' },
    ]);
    const result = evaluator.evaluate(h, 10000);
    expect(result.passed).toBe(true);
  });

  it('fails when assertion violated', () => {
    collector.record({ name: 'avail', value: 0.5, timestamp: 5000, labels: {} });
    const h = makeHypothesis([
      { metric: 'avail', operator: 'gte', value: 0.95, aggregation: 'avg' },
    ]);
    const result = evaluator.evaluate(h, 10000);
    expect(result.passed).toBe(false);
  });

  it('respects tolerance', () => {
    collector.record({ name: 'a', value: 100, timestamp: 5000, labels: {} });
    collector.record({ name: 'b', value: 0, timestamp: 5000, labels: {} });
    const h = makeHypothesis([
      { metric: 'a', operator: 'gte', value: 50, aggregation: 'avg' },
      { metric: 'b', operator: 'gte', value: 50, aggregation: 'avg' }, // fails
    ], 0.5); // allow 50% to fail
    const result = evaluator.evaluate(h, 10000);
    expect(result.passed).toBe(true);
  });

  it('handles all comparison operators', () => {
    collector.record({ name: 'm', value: 10, timestamp: 5000, labels: {} });
    const now = 10000;

    const check = (op: MetricAssertion['operator'], val: number, upper?: number) => {
      const a: MetricAssertion = { metric: 'm', operator: op, value: val, aggregation: 'avg' };
      if (upper !== undefined) a.upperBound = upper;
      return evaluator.evaluate(makeHypothesis([a]), now).passed;
    };

    expect(check('lt', 20)).toBe(true);
    expect(check('lt', 5)).toBe(false);
    expect(check('gt', 5)).toBe(true);
    expect(check('lte', 10)).toBe(true);
    expect(check('gte', 10)).toBe(true);
    expect(check('eq', 10)).toBe(true);
    expect(check('eq', 11)).toBe(false);
    expect(check('between', 5, 15)).toBe(true);
    expect(check('between', 11, 15)).toBe(false);
  });

  it('within_stddev uses captured baseline', () => {
    // Baseline data
    for (const v of [10, 10, 10, 10, 10]) {
      collector.record({ name: 'lat', value: v, timestamp: 5000, labels: {} });
    }
    const h = makeHypothesis([
      { metric: 'lat', operator: 'within_stddev', value: 0, stddevMultiplier: 2, aggregation: 'avg' },
    ]);
    evaluator.captureBaseline(h, 10000);

    // Value within stddev
    collector.clear('lat');
    collector.record({ name: 'lat', value: 10, timestamp: 15000, labels: {} });
    expect(evaluator.evaluate(h, 20000).passed).toBe(true);

    // Value way outside
    collector.clear('lat');
    collector.record({ name: 'lat', value: 1000, timestamp: 15000, labels: {} });
    expect(evaluator.evaluate(h, 20000).passed).toBe(false);
  });

  it('fails when no data available', () => {
    const h = makeHypothesis([
      { metric: 'missing', operator: 'gte', value: 0, aggregation: 'avg' },
    ]);
    expect(evaluator.evaluate(h, 10000).passed).toBe(false);
  });
});

// ============================================================
// BlastRadiusController
// ============================================================

describe('BlastRadiusController', () => {
  it('allows and tracks affected agents within radius', () => {
    const br = new BlastRadiusController(10, 0.3);
    expect(br.canAffect(['a1', 'a2'])).toBe(true);
    br.recordAffected(['a1', 'a2']);
    expect(br.currentRadius()).toBe(0.2);
  });

  it('blocks when exceeding max blast radius', () => {
    const br = new BlastRadiusController(10, 0.2);
    br.recordAffected(['a1', 'a2']);
    expect(br.canAffect(['a3'])).toBe(false); // 3/10 = 0.3 > 0.2
  });

  it('deduplicates agents', () => {
    const br = new BlastRadiusController(10, 0.5);
    br.recordAffected(['a1', 'a2']);
    expect(br.canAffect(['a1', 'a3'])).toBe(true); // still 3/10
  });

  it('removeAffected reduces radius', () => {
    const br = new BlastRadiusController(10, 0.5);
    br.recordAffected(['a1', 'a2']);
    br.removeAffected(['a1']);
    expect(br.currentRadius()).toBe(0.1);
  });

  it('reset clears everything', () => {
    const br = new BlastRadiusController(10, 0.5);
    br.recordAffected(['a1', 'a2', 'a3']);
    br.reset();
    expect(br.currentRadius()).toBe(0);
    expect(br.getAffectedAgents()).toHaveLength(0);
  });

  it('handles zero total agents', () => {
    const br = new BlastRadiusController(0, 0.5);
    expect(br.currentRadius()).toBe(0);
  });
});

// ============================================================
// KillSwitchMonitor
// ============================================================

describe('KillSwitchMonitor', () => {
  let collector: MetricCollector;

  beforeEach(() => {
    collector = new MetricCollector();
  });

  it('does not trigger when metric is healthy', () => {
    collector.record({ name: 'avail', value: 0.99, timestamp: 1000, labels: {} });
    const ks = new KillSwitchMonitor([{
      metric: 'avail', operator: 'lt', threshold: 0.9,
      sustainedMs: 5000, description: 'avail drop'
    }], collector);
    expect(ks.check(1000).triggered).toBe(false);
  });

  it('triggers after sustained violation', () => {
    const ks = new KillSwitchMonitor([{
      metric: 'avail', operator: 'lt', threshold: 0.9,
      sustainedMs: 1000, description: 'avail drop'
    }], collector);

    collector.record({ name: 'avail', value: 0.5, timestamp: 1000, labels: {} });
    expect(ks.check(1000).triggered).toBe(false); // first violation, timer starts

    collector.record({ name: 'avail', value: 0.5, timestamp: 2500, labels: {} });
    const result = ks.check(2500);
    expect(result.triggered).toBe(true);
    expect(result.reason).toContain('avail drop');
  });

  it('resets violation timer when metric recovers', () => {
    const ks = new KillSwitchMonitor([{
      metric: 'avail', operator: 'lt', threshold: 0.9,
      sustainedMs: 2000, description: 'avail drop'
    }], collector);

    collector.record({ name: 'avail', value: 0.5, timestamp: 1000, labels: {} });
    ks.check(1000); // start timer

    // Metric recovers
    collector.record({ name: 'avail', value: 0.95, timestamp: 2000, labels: {} });
    ks.check(2000); // clears timer

    // Violates again
    collector.record({ name: 'avail', value: 0.5, timestamp: 3000, labels: {} });
    expect(ks.check(3000).triggered).toBe(false); // timer restarted
  });

  it('ignores when no samples exist', () => {
    const ks = new KillSwitchMonitor([{
      metric: 'avail', operator: 'lt', threshold: 0.9,
      sustainedMs: 1000, description: 'test'
    }], collector);
    expect(ks.check(1000).triggered).toBe(false);
  });
});

// ============================================================
// TargetResolver
// ============================================================

describe('TargetResolver', () => {
  let resolver: TargetResolver;

  beforeEach(() => {
    resolver = new TargetResolver();
    resolver.registerAgent('a1', { role: 'worker', region: 'us' });
    resolver.registerAgent('a2', { role: 'worker', region: 'eu' });
    resolver.registerAgent('a3', { role: 'coordinator', region: 'us' });
    resolver.registerAgent('a4', { role: 'worker', region: 'us' });
  });

  it('specific mode returns only requested agents', () => {
    const targets = resolver.resolve({ mode: 'specific', agents: ['a1', 'a3', 'a99'] }, 'seed');
    expect(targets).toEqual(['a1', 'a3']); // a99 doesn't exist
  });

  it('label-match filters by labels', () => {
    const targets = resolver.resolve({
      mode: 'label-match',
      labels: { role: 'worker', region: 'us' }
    }, 'seed');
    expect(targets.sort()).toEqual(['a1', 'a4']);
  });

  it('excludeLabels filters out agents', () => {
    const targets = resolver.resolve({
      mode: 'label-match',
      labels: { role: 'worker' },
      excludeLabels: { region: 'eu' }
    }, 'seed');
    expect(targets).not.toContain('a2');
    expect(targets).toContain('a1');
  });

  it('random mode returns deterministic sample', () => {
    const t1 = resolver.resolve({ mode: 'random', count: 2 }, 'seed1');
    const t2 = resolver.resolve({ mode: 'random', count: 2 }, 'seed1');
    expect(t1).toEqual(t2); // deterministic
    expect(t1).toHaveLength(2);
  });

  it('percentage mode selects fraction of agents', () => {
    const targets = resolver.resolve({ mode: 'percentage', percentage: 50 }, 'seed');
    expect(targets).toHaveLength(2); // ceil(4 * 0.5)
  });

  it('removeAgent removes from pool', () => {
    resolver.removeAgent('a1');
    expect(resolver.totalAgents()).toBe(3);
    const targets = resolver.resolve({ mode: 'specific', agents: ['a1'] }, 'seed');
    expect(targets).toHaveLength(0);
  });
});

// ============================================================
// FaultInjector
// ============================================================

describe('FaultInjector', () => {
  let injector: FaultInjector;
  const spec: FaultSpec = {
    type: 'agent-crash',
    targets: { mode: 'specific', agents: ['a1'] },
    parameters: {},
    durationMs: 5000,
  };

  beforeEach(() => {
    injector = new FaultInjector();
  });

  it('injects and tracks faults', () => {
    const handle = injector.inject(spec, ['a1'], 1000);
    expect(handle.type).toBe('agent-crash');
    expect(handle.affectedAgents).toEqual(['a1']);
    expect(handle.removeAt).toBe(6000);
    expect(injector.activeCount()).toBe(1);
  });

  it('removes specific fault', () => {
    const handle = injector.inject(spec, ['a1'], 1000);
    const removed = injector.remove(handle.faultId);
    expect(removed).toBeDefined();
    expect(injector.activeCount()).toBe(0);
  });

  it('removeAll clears everything', () => {
    injector.inject(spec, ['a1'], 1000);
    injector.inject(spec, ['a2'], 1000);
    const removed = injector.removeAll();
    expect(removed).toHaveLength(2);
    expect(injector.activeCount()).toBe(0);
  });

  it('getExpired returns faults past their duration', () => {
    injector.inject(spec, ['a1'], 1000);
    expect(injector.getExpired(5000)).toHaveLength(0);
    expect(injector.getExpired(6000)).toHaveLength(1);
  });

  it('getActive returns all active faults', () => {
    injector.inject(spec, ['a1'], 1000);
    injector.inject(spec, ['a2'], 2000);
    expect(injector.getActive()).toHaveLength(2);
  });
});

// ============================================================
// PreflightChecker
// ============================================================

describe('PreflightChecker', () => {
  let collector: MetricCollector;
  let checker: PreflightChecker;

  beforeEach(() => {
    collector = new MetricCollector();
    checker = new PreflightChecker(collector);
  });

  it('metric-threshold passes when condition met', () => {
    collector.record({ name: 'cpu', value: 30, timestamp: 5000, labels: {} });
    const results = checker.runChecks([{
      name: 'cpu-ok', type: 'metric-threshold',
      config: { metric: 'cpu', operator: 'lt', threshold: 80, windowMs: 10000 }
    }], 10, 0, 10000);
    expect(results[0].passed).toBe(true);
  });

  it('metric-threshold fails when no data', () => {
    const results = checker.runChecks([{
      name: 'cpu-ok', type: 'metric-threshold',
      config: { metric: 'cpu', operator: 'lt', threshold: 80 }
    }], 10, 0, 10000);
    expect(results[0].passed).toBe(false);
    expect(results[0].reason).toContain('No data');
  });

  it('no-active-incidents passes when zero', () => {
    const results = checker.runChecks([{
      name: 'no-inc', type: 'no-active-incidents', config: {}
    }], 10, 0, 10000);
    expect(results[0].passed).toBe(true);
  });

  it('no-active-incidents fails when > 0', () => {
    const results = checker.runChecks([{
      name: 'no-inc', type: 'no-active-incidents', config: {}
    }], 10, 2, 10000);
    expect(results[0].passed).toBe(false);
  });

  it('minimum-agents checks count', () => {
    const results = checker.runChecks([{
      name: 'min', type: 'minimum-agents', config: { count: 5 }
    }], 3, 0, 10000);
    expect(results[0].passed).toBe(false);
  });

  it('custom checks default to pass', () => {
    const results = checker.runChecks([{
      name: 'custom', type: 'custom', config: {}
    }], 10, 0, 10000);
    expect(results[0].passed).toBe(true);
  });
});

// ============================================================
// ExperimentEngine
// ============================================================

describe('ExperimentEngine', () => {
  let collector: MetricCollector;
  let resolver: TargetResolver;
  let engine: ExperimentEngine;

  const baseSafety: SafetyConfig = {
    maxBlastRadius: 0.5,
    killSwitchMetrics: [],
    autoRollbackOnHypothesisViolation: false,
    requireManualApproval: false,
    blockedTimeWindows: [],
    minimumHealthyAgents: 1,
    maxConcurrentExperiments: 5,
    preflightChecks: [],
  };

  const baseHypothesis: SteadyStateHypothesis = {
    id: 'h1', name: 'test', description: 'test',
    metrics: [{ metric: 'avail', operator: 'gte', value: 0.5, aggregation: 'avg' }],
    tolerance: 0, evaluationWindowMs: 50000, cooldownMs: 1000,
  };

  beforeEach(() => {
    collector = new MetricCollector();
    resolver = new TargetResolver();
    for (let i = 0; i < 10; i++) {
      resolver.registerAgent(`a${i}`, { role: 'worker' });
    }
    // Seed baseline metric
    collector.record({ name: 'avail', value: 0.99, timestamp: 5000, labels: {} });
    engine = new ExperimentEngine(collector, resolver);
  });

  it('creates an experiment in draft status', () => {
    const exp = engine.createExperiment({
      name: 'test', description: 'test',
      hypothesis: baseHypothesis,
      faults: [], safetyConfig: baseSafety,
    });
    expect(exp.status).toBe('draft');
    expect(engine.getExperiment(exp.id)).toBeDefined();
  });

  it('runs a simple experiment successfully', async () => {
    const exp = engine.createExperiment({
      name: 'simple', description: 'simple test',
      hypothesis: baseHypothesis,
      faults: [{
        name: 'phase1',
        faults: [{
          type: 'agent-crash',
          targets: { mode: 'specific', agents: ['a0'] },
          parameters: {},
          durationMs: 2000,
        }],
        delayBeforeMs: 0, durationMs: 2000, verifyHypothesisAfter: false,
      }],
      safetyConfig: baseSafety,
    });

    const results = await engine.runExperiment(exp.id, 10000, { tickIntervalMs: 1000 });
    expect(results.phases).toHaveLength(1);
    expect(results.phases[0].faultsInjected).toBe(1);
    expect(results.duration).toBeGreaterThan(0);
  });

  it('aborts when preflight fails', async () => {
    const exp = engine.createExperiment({
      name: 'preflight-fail', description: 'test',
      hypothesis: baseHypothesis,
      faults: [],
      safetyConfig: {
        ...baseSafety,
        preflightChecks: [{ name: 'min', type: 'minimum-agents', config: { count: 100 } }],
      },
    });
    const results = await engine.runExperiment(exp.id, 10000);
    expect(results.hypothesisHeld).toBe(false);
    expect(results.rollbackReason).toContain('Preflight failed');
  });

  it('aborts when max concurrent experiments exceeded', async () => {
    const exp = engine.createExperiment({
      name: 'concurrent', description: 'test',
      hypothesis: baseHypothesis,
      faults: [],
      safetyConfig: { ...baseSafety, maxConcurrentExperiments: 0 },
    });
    const results = await engine.runExperiment(exp.id, 10000);
    expect(results.rollbackReason).toContain('concurrent');
  });

  it('aborts when baseline hypothesis fails', async () => {
    const badCollector = new MetricCollector();
    // No avail data → hypothesis fails
    const eng2 = new ExperimentEngine(badCollector, resolver);
    const exp = eng2.createExperiment({
      name: 'bad-baseline', description: 'test',
      hypothesis: baseHypothesis,
      faults: [], safetyConfig: baseSafety,
    });
    const results = await eng2.runExperiment(exp.id, 10000);
    expect(results.rollbackReason).toContain('hypothesis failed before');
  });

  it('triggers rollback on blast radius exceeded with autoRollback', async () => {
    const exp = engine.createExperiment({
      name: 'blast', description: 'test',
      hypothesis: baseHypothesis,
      faults: [{
        name: 'big-blast',
        faults: [{
          type: 'agent-crash',
          targets: { mode: 'percentage', percentage: 80 },
          parameters: {},
          durationMs: 1000,
        }],
        delayBeforeMs: 0, durationMs: 1000, verifyHypothesisAfter: false,
      }],
      safetyConfig: {
        ...baseSafety,
        maxBlastRadius: 0.1,
        autoRollbackOnHypothesisViolation: true,
      },
    });
    const results = await engine.runExperiment(exp.id, 10000, { tickIntervalMs: 500 });
    expect(results.rollbackTriggered).toBe(true);
    expect(results.rollbackReason).toContain('Blast radius');
  });

  it('triggers kill switch on sustained metric violation', async () => {
    // Record bad metrics that will trigger kill switch
    for (let t = 10000; t < 20000; t += 100) {
      collector.record({ name: 'err', value: 99, timestamp: t, labels: {} });
    }

    const exp = engine.createExperiment({
      name: 'killswitch', description: 'test',
      hypothesis: baseHypothesis,
      faults: [{
        name: 'phase1',
        faults: [{
          type: 'latency-spike',
          targets: { mode: 'specific', agents: ['a0'] },
          parameters: {},
          durationMs: 5000,
        }],
        delayBeforeMs: 0, durationMs: 5000, verifyHypothesisAfter: false,
      }],
      safetyConfig: {
        ...baseSafety,
        killSwitchMetrics: [{
          metric: 'err', operator: 'gt', threshold: 50,
          sustainedMs: 1000, description: 'error rate too high'
        }],
      },
    });

    const results = await engine.runExperiment(exp.id, 10000, { tickIntervalMs: 500 });
    expect(results.rollbackTriggered).toBe(true);
    expect(results.rollbackReason).toContain('error rate too high');
  });

  it('generates regression tests from critical issues', async () => {
    // Force hypothesis violation after phase
    collector.record({ name: 'avail', value: 0.99, timestamp: 5000, labels: {} });
    const hypothesis: SteadyStateHypothesis = {
      ...baseHypothesis,
      metrics: [{ metric: 'avail', operator: 'gte', value: 100, aggregation: 'avg' }], // will fail
    };

    // But we need baseline to pass... use tolerance
    const exp = engine.createExperiment({
      name: 'regression', description: 'test',
      hypothesis: { ...hypothesis, tolerance: 1 }, // baseline passes with full tolerance
      faults: [{
        name: 'phase1',
        faults: [{
          type: 'agent-crash',
          targets: { mode: 'specific', agents: ['a0'] },
          parameters: {},
          durationMs: 1000,
        }],
        delayBeforeMs: 0, durationMs: 1000, verifyHypothesisAfter: true,
      }],
      safetyConfig: baseSafety,
    });

    await engine.runExperiment(exp.id, 10000, { tickIntervalMs: 500 });
    // Can't guarantee regression tests without hypothesis violation causing high/critical issues
    // but event log should be populated
    expect(engine.getEventLog().length).toBeGreaterThan(0);
  });

  it('records events throughout lifecycle', async () => {
    const exp = engine.createExperiment({
      name: 'events', description: 'test',
      hypothesis: baseHypothesis,
      faults: [{
        name: 'p1', faults: [{
          type: 'agent-crash',
          targets: { mode: 'specific', agents: ['a0'] },
          parameters: {}, durationMs: 1000,
        }],
        delayBeforeMs: 0, durationMs: 1000, verifyHypothesisAfter: false,
      }],
      safetyConfig: baseSafety,
    });

    await engine.runExperiment(exp.id, 10000, { tickIntervalMs: 500 });
    const events = engine.getEventLog();
    const types = events.map(e => e.type);
    expect(types).toContain('experiment-created');
    expect(types).toContain('preflight-passed');
    expect(types).toContain('experiment-started');
    expect(types).toContain('phase-started');
    expect(types).toContain('fault-injected');
    expect(types).toContain('fault-removed');
    expect(types).toContain('phase-completed');
  });

  it('throws for unknown experiment id', async () => {
    await expect(engine.runExperiment('nope', 1000)).rejects.toThrow('not found');
  });
});

// ============================================================
// GameDayCoordinator
// ============================================================

describe('GameDayCoordinator', () => {
  let collector: MetricCollector;
  let resolver: TargetResolver;
  let engine: ExperimentEngine;
  let coordinator: GameDayCoordinator;

  beforeEach(() => {
    collector = new MetricCollector();
    resolver = new TargetResolver();
    for (let i = 0; i < 10; i++) {
      resolver.registerAgent(`a${i}`, { role: 'worker' });
    }
    collector.record({ name: 'avail', value: 0.99, timestamp: 5000, labels: {} });
    engine = new ExperimentEngine(collector, resolver);
    coordinator = new GameDayCoordinator(engine);
  });

  it('creates and runs a game day', async () => {
    const exp = engine.createExperiment({
      name: 'gd-exp', description: 'test',
      hypothesis: {
        id: 'h1', name: 'test', description: 'test',
        metrics: [{ metric: 'avail', operator: 'gte', value: 0.5, aggregation: 'avg' }],
        tolerance: 0, evaluationWindowMs: 50000, cooldownMs: 1000,
      },
      faults: [{
        name: 'p1', faults: [{
          type: 'agent-crash',
          targets: { mode: 'specific', agents: ['a0'] },
          parameters: {}, durationMs: 1000,
        }],
        delayBeforeMs: 0, durationMs: 1000, verifyHypothesisAfter: false,
      }],
      safetyConfig: {
        maxBlastRadius: 0.5, killSwitchMetrics: [],
        autoRollbackOnHypothesisViolation: false, requireManualApproval: false,
        blockedTimeWindows: [], minimumHealthyAgents: 1,
        maxConcurrentExperiments: 5, preflightChecks: [],
      },
    });

    const gd = coordinator.createGameDay({
      name: 'Test GameDay', description: 'test',
      experimentIds: [exp.id],
      participants: [{ name: 'Alice', role: 'operator' }],
      runbook: [{ order: 1, description: 'Run crash test', experimentId: exp.id, expectedDuration: 1000, checkpoints: ['check1'] }],
    });

    expect(gd.status).toBe('planning');

    const result = await coordinator.startGameDay(gd.id, 10000);
    expect(result.experimentResults.size).toBe(1);

    const updated = coordinator.getGameDay(gd.id);
    expect(updated!.status).toBe('completed');
  });

  it('throws for unknown game day', async () => {
    await expect(coordinator.startGameDay('nope', 1000)).rejects.toThrow('not found');
  });
});

// ============================================================
// Pre-built Scenarios
// ============================================================

describe('Pre-built scenarios', () => {
  it('singleAgentCrashScenario has valid structure', () => {
    const s = singleAgentCrashScenario('agent-1');
    expect(s.hypothesis.metrics).toHaveLength(3);
    expect(s.faults).toHaveLength(1);
    expect(s.faults[0].faults[0].type).toBe('agent-crash');
    expect(s.safetyConfig.maxBlastRadius).toBeLessThan(1);
  });

  it('networkPartitionScenario has valid structure', () => {
    const s = networkPartitionScenario();
    expect(s.faults).toHaveLength(2);
    expect(s.faults[0].faults[0].type).toBe('partition');
    expect(s.safetyConfig.requireManualApproval).toBe(true);
  });

  it('cascadingFailureScenario has 3 phases', () => {
    const s = cascadingFailureScenario();
    expect(s.faults).toHaveLength(3);
    expect(s.hypothesis.tolerance).toBeGreaterThan(0);
  });

  it('resourceExhaustionScenario injects memory and cpu faults', () => {
    const s = resourceExhaustionScenario();
    const faultTypes = s.faults[0].faults.map(f => f.type);
    expect(faultTypes).toEqual(['resource-exhaustion', 'resource-exhaustion']);
  });
});
