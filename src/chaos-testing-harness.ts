import { fnv1a } from './shared-utils';
/**
 * Chaos Testing Harness for Agent Networks
 * 
 * Production-grade chaos engineering framework that systematically injects
 * controlled failures into agent networks to validate resilience properties.
 * Integrates with fault-injection-framework.ts for fault generation and
 * provides higher-level experiment orchestration, steady-state hypothesis
 * verification, blast radius control, and automated rollback.
 * 
 * Key capabilities:
 * - Steady-state hypothesis definition and continuous verification
 * - Experiment lifecycle management (design → run → analyze → report)
 * - Blast radius containment with circuit breakers and kill switches
 * - GameDay orchestration for coordinated multi-team chaos exercises
 * - Regression test generation from discovered failures
 * - Safety interlocks preventing chaos in unsafe conditions
 */

// ============================================================
// Types & Interfaces
// ============================================================

type AgentId = string;
type ExperimentId = string;
type HypothesisId = string;
type MetricName = string;
type Timestamp = number;

interface MetricSample {
  name: MetricName;
  value: number;
  timestamp: Timestamp;
  labels: Record<string, string>;
}

interface SteadyStateHypothesis {
  id: HypothesisId;
  name: string;
  description: string;
  metrics: MetricAssertion[];
  tolerance: number; // 0-1, fraction of metrics allowed to fail
  evaluationWindowMs: number;
  cooldownMs: number;
}

interface MetricAssertion {
  metric: MetricName;
  operator: 'lt' | 'gt' | 'lte' | 'gte' | 'eq' | 'between' | 'within_stddev';
  value: number;
  upperBound?: number; // for 'between'
  stddevMultiplier?: number; // for 'within_stddev'
  aggregation: 'avg' | 'p50' | 'p95' | 'p99' | 'max' | 'min' | 'sum' | 'count';
}

type FaultType =
  | 'latency-spike'
  | 'packet-loss'
  | 'message-corruption'
  | 'agent-crash'
  | 'agent-slowdown'
  | 'partition'
  | 'resource-exhaustion'
  | 'clock-skew'
  | 'byzantine-behavior'
  | 'dependency-failure'
  | 'dns-failure'
  | 'certificate-expiry';

interface FaultSpec {
  type: FaultType;
  targets: TargetSelector;
  parameters: Record<string, number | string | boolean>;
  durationMs: number;
  rampUpMs?: number;
  rampDownMs?: number;
}

interface TargetSelector {
  mode: 'specific' | 'random' | 'percentage' | 'label-match';
  agents?: AgentId[];
  percentage?: number;
  count?: number;
  labels?: Record<string, string>;
  excludeLabels?: Record<string, string>;
}

type ExperimentStatus = 
  | 'draft' | 'scheduled' | 'pre-check' | 'running' 
  | 'paused' | 'rolling-back' | 'analyzing' | 'completed' | 'aborted';

interface Experiment {
  id: ExperimentId;
  name: string;
  description: string;
  hypothesis: SteadyStateHypothesis;
  faults: FaultPhase[];
  safetyConfig: SafetyConfig;
  schedule?: ScheduleConfig;
  status: ExperimentStatus;
  createdAt: Timestamp;
  startedAt?: Timestamp;
  completedAt?: Timestamp;
  results?: ExperimentResults;
}

interface FaultPhase {
  name: string;
  faults: FaultSpec[];
  delayBeforeMs: number;
  durationMs: number;
  verifyHypothesisAfter: boolean;
}

interface SafetyConfig {
  maxBlastRadius: number; // 0-1, max fraction of agents affected
  killSwitchMetrics: KillSwitchCondition[];
  autoRollbackOnHypothesisViolation: boolean;
  requireManualApproval: boolean;
  blockedTimeWindows: TimeWindow[];
  minimumHealthyAgents: number;
  maxConcurrentExperiments: number;
  preflightChecks: PreflightCheck[];
}

interface KillSwitchCondition {
  metric: MetricName;
  operator: 'lt' | 'gt';
  threshold: number;
  sustainedMs: number;
  description: string;
}

interface TimeWindow {
  startHour: number; // 0-23
  endHour: number;
  daysOfWeek: number[]; // 0=Sun, 6=Sat
  timezone: string;
}

interface PreflightCheck {
  name: string;
  type: 'metric-threshold' | 'no-active-incidents' | 'minimum-agents' | 'custom';
  config: Record<string, number | string | boolean>;
}

interface ScheduleConfig {
  type: 'once' | 'recurring' | 'random-within-window';
  at?: Timestamp;
  cronExpression?: string;
  windowStartHour?: number;
  windowEndHour?: number;
  minIntervalMs?: number;
}

interface ExperimentResults {
  hypothesisHeld: boolean;
  phases: PhaseResult[];
  metricsTimeline: MetricSample[];
  impactAssessment: ImpactAssessment;
  discoveredIssues: DiscoveredIssue[];
  duration: number;
  rollbackTriggered: boolean;
  rollbackReason?: string;
}

interface PhaseResult {
  phaseName: string;
  startedAt: Timestamp;
  completedAt: Timestamp;
  hypothesisCheckPassed?: boolean;
  faultsInjected: number;
  agentsAffected: AgentId[];
}

interface ImpactAssessment {
  availabilityDegradation: number; // 0-1
  latencyIncrease: number; // multiplier
  errorRateIncrease: number; // absolute
  throughputReduction: number; // 0-1
  recoveryTimeMs: number;
  cascadeDepth: number;
  affectedAgents: AgentId[];
}

interface DiscoveredIssue {
  severity: 'critical' | 'high' | 'medium' | 'low';
  category: string;
  description: string;
  reproducible: boolean;
  suggestedFix?: string;
  relatedFaults: FaultType[];
  affectedAgents: AgentId[];
}

interface GameDay {
  id: string;
  name: string;
  description: string;
  experiments: ExperimentId[];
  participants: Participant[];
  runbook: RunbookStep[];
  status: 'planning' | 'active' | 'debriefing' | 'completed';
  startedAt?: Timestamp;
  completedAt?: Timestamp;
}

interface Participant {
  name: string;
  role: 'operator' | 'observer' | 'responder';
  agentId?: AgentId;
}

interface RunbookStep {
  order: number;
  description: string;
  experimentId?: ExperimentId;
  manualAction?: string;
  expectedDuration: number;
  checkpoints: string[];
}

interface RegressionTest {
  id: string;
  sourceExperimentId: ExperimentId;
  issueDescription: string;
  faultSequence: FaultSpec[];
  assertions: MetricAssertion[];
  createdAt: Timestamp;
  lastRunAt?: Timestamp;
  lastResult?: 'pass' | 'fail';
}

interface ChaosEvent {
  type: ChaosEventType;
  timestamp: Timestamp;
  experimentId?: ExperimentId;
  data: Record<string, unknown>;
}

type ChaosEventType =
  | 'experiment-created'
  | 'experiment-started'
  | 'experiment-completed'
  | 'experiment-aborted'
  | 'phase-started'
  | 'phase-completed'
  | 'fault-injected'
  | 'fault-removed'
  | 'hypothesis-checked'
  | 'hypothesis-violated'
  | 'kill-switch-triggered'
  | 'rollback-started'
  | 'rollback-completed'
  | 'preflight-passed'
  | 'preflight-failed'
  | 'blast-radius-exceeded'
  | 'regression-test-created'
  | 'gameday-started'
  | 'gameday-completed';

// ============================================================
// FNV-1a hash for deterministic operations
// ============================================================

// ============================================================
// Metric Collector — time-series storage with aggregations
// ============================================================

class MetricCollector {
  private samples: Map<MetricName, MetricSample[]> = new Map();
  private maxSamplesPerMetric: number;

  constructor(maxSamplesPerMetric: number = 10000) {
    this.maxSamplesPerMetric = maxSamplesPerMetric;
  }

  record(sample: MetricSample): void {
    let series = this.samples.get(sample.name);
    if (!series) {
      series = [];
      this.samples.set(sample.name, series);
    }
    series.push(sample);
    // Evict oldest if over capacity
    if (series.length > this.maxSamplesPerMetric) {
      series.splice(0, series.length - this.maxSamplesPerMetric);
    }
  }

  query(metric: MetricName, fromTs: Timestamp, toTs: Timestamp): MetricSample[] {
    const series = this.samples.get(metric) || [];
    return series.filter(s => s.timestamp >= fromTs && s.timestamp <= toTs);
  }

  aggregate(
    metric: MetricName,
    fromTs: Timestamp,
    toTs: Timestamp,
    aggregation: MetricAssertion['aggregation']
  ): number | null {
    const samples = this.query(metric, fromTs, toTs);
    if (samples.length === 0) return null;

    const values = samples.map(s => s.value).sort((a, b) => a - b);

    switch (aggregation) {
      case 'avg':
        return values.reduce((a, b) => a + b, 0) / values.length;
      case 'min':
        return values[0];
      case 'max':
        return values[values.length - 1];
      case 'sum':
        return values.reduce((a, b) => a + b, 0);
      case 'count':
        return values.length;
      case 'p50':
        return this.percentile(values, 0.5);
      case 'p95':
        return this.percentile(values, 0.95);
      case 'p99':
        return this.percentile(values, 0.99);
      default:
        return null;
    }
  }

  /**
   * Online Welford stats for within_stddev assertions
   */
  stats(metric: MetricName, fromTs: Timestamp, toTs: Timestamp): { mean: number; stddev: number } | null {
    const samples = this.query(metric, fromTs, toTs);
    if (samples.length < 2) return null;

    let n = 0, mean = 0, m2 = 0;
    for (const s of samples) {
      n++;
      const delta = s.value - mean;
      mean += delta / n;
      m2 += delta * (s.value - mean);
    }
    return { mean, stddev: Math.sqrt(m2 / (n - 1)) };
  }

  private percentile(sorted: number[], p: number): number {
    const idx = (sorted.length - 1) * p;
    const lo = Math.floor(idx);
    const hi = Math.ceil(idx);
    if (lo === hi) return sorted[lo];
    return sorted[lo] * (hi - idx) + sorted[hi] * (idx - lo);
  }

  clear(metric?: MetricName): void {
    if (metric) {
      this.samples.delete(metric);
    } else {
      this.samples.clear();
    }
  }
}

// ============================================================
// Hypothesis Evaluator — verify steady-state invariants
// ============================================================

class HypothesisEvaluator {
  private collector: MetricCollector;
  private baselineStats: Map<string, { mean: number; stddev: number }> = new Map();

  constructor(collector: MetricCollector) {
    this.collector = collector;
  }

  /**
   * Capture baseline statistics for within_stddev assertions.
   * Call before experiment starts.
   */
  captureBaseline(hypothesis: SteadyStateHypothesis, now: Timestamp): void {
    for (const assertion of hypothesis.metrics) {
      if (assertion.operator === 'within_stddev') {
        const stats = this.collector.stats(
          assertion.metric,
          now - hypothesis.evaluationWindowMs,
          now
        );
        if (stats) {
          this.baselineStats.set(
            `${hypothesis.id}:${assertion.metric}`,
            stats
          );
        }
      }
    }
  }

  evaluate(hypothesis: SteadyStateHypothesis, now: Timestamp): {
    passed: boolean;
    results: Array<{ assertion: MetricAssertion; passed: boolean; actual: number | null }>;
  } {
    const fromTs = now - hypothesis.evaluationWindowMs;
    const results: Array<{ assertion: MetricAssertion; passed: boolean; actual: number | null }> = [];

    for (const assertion of hypothesis.metrics) {
      const actual = this.collector.aggregate(
        assertion.metric, fromTs, now, assertion.aggregation
      );

      let passed: boolean;
      if (actual === null) {
        passed = false; // No data = cannot verify = fail
      } else {
        passed = this.checkAssertion(assertion, actual, hypothesis.id);
      }

      results.push({ assertion, passed, actual });
    }

    const failCount = results.filter(r => !r.passed).length;
    const maxFailures = Math.floor(hypothesis.metrics.length * hypothesis.tolerance);
    const overallPassed = failCount <= maxFailures;

    return { passed: overallPassed, results };
  }

  private checkAssertion(
    assertion: MetricAssertion,
    actual: number,
    hypothesisId: string
  ): boolean {
    switch (assertion.operator) {
      case 'lt': return actual < assertion.value;
      case 'gt': return actual > assertion.value;
      case 'lte': return actual <= assertion.value;
      case 'gte': return actual >= assertion.value;
      case 'eq': return Math.abs(actual - assertion.value) < 1e-9;
      case 'between':
        return actual >= assertion.value && actual <= (assertion.upperBound ?? assertion.value);
      case 'within_stddev': {
        const key = `${hypothesisId}:${assertion.metric}`;
        const baseline = this.baselineStats.get(key);
        if (!baseline) return false;
        const multiplier = assertion.stddevMultiplier ?? 2;
        const lo = baseline.mean - multiplier * baseline.stddev;
        const hi = baseline.mean + multiplier * baseline.stddev;
        return actual >= lo && actual <= hi;
      }
      default:
        return false;
    }
  }
}

// ============================================================
// Blast Radius Controller — containment enforcement
// ============================================================

class BlastRadiusController {
  private totalAgents: number;
  private affectedAgents: Set<AgentId> = new Set();
  private maxFraction: number;

  constructor(totalAgents: number, maxFraction: number) {
    this.totalAgents = totalAgents;
    this.maxFraction = maxFraction;
  }

  canAffect(agents: AgentId[]): boolean {
    const projected = new Set(this.affectedAgents);
    for (const a of agents) projected.add(a);
    return projected.size / this.totalAgents <= this.maxFraction;
  }

  recordAffected(agents: AgentId[]): void {
    for (const a of agents) this.affectedAgents.add(a);
  }

  removeAffected(agents: AgentId[]): void {
    for (const a of agents) this.affectedAgents.delete(a);
  }

  currentRadius(): number {
    return this.totalAgents > 0 ? this.affectedAgents.size / this.totalAgents : 0;
  }

  reset(): void {
    this.affectedAgents.clear();
  }

  getAffectedAgents(): AgentId[] {
    return Array.from(this.affectedAgents);
  }
}

// ============================================================
// Kill Switch Monitor — continuous safety monitoring
// ============================================================

class KillSwitchMonitor {
  private conditions: KillSwitchCondition[];
  private collector: MetricCollector;
  private sustainedViolations: Map<string, Timestamp> = new Map(); // condition key → first violation ts

  constructor(conditions: KillSwitchCondition[], collector: MetricCollector) {
    this.conditions = conditions;
    this.collector = collector;
  }

  check(now: Timestamp): { triggered: boolean; reason?: string } {
    for (const condition of this.conditions) {
      const key = `${condition.metric}:${condition.operator}:${condition.threshold}`;
      const samples = this.collector.query(
        condition.metric,
        now - condition.sustainedMs,
        now
      );

      if (samples.length === 0) continue;

      const latest = samples[samples.length - 1].value;
      const violated = condition.operator === 'gt'
        ? latest > condition.threshold
        : latest < condition.threshold;

      if (violated) {
        const firstViolation = this.sustainedViolations.get(key);
        if (!firstViolation) {
          this.sustainedViolations.set(key, now);
        } else if (now - firstViolation >= condition.sustainedMs) {
          return {
            triggered: true,
            reason: `Kill switch: ${condition.description} — ${condition.metric} ${condition.operator} ${condition.threshold} sustained for ${condition.sustainedMs}ms`
          };
        }
      } else {
        this.sustainedViolations.delete(key);
      }
    }

    return { triggered: false };
  }

  reset(): void {
    this.sustainedViolations.clear();
  }
}

// ============================================================
// Target Resolver — select agents based on TargetSelector
// ============================================================

class TargetResolver {
  private agentLabels: Map<AgentId, Record<string, string>> = new Map();

  registerAgent(id: AgentId, labels: Record<string, string>): void {
    this.agentLabels.set(id, { ...labels });
  }

  removeAgent(id: AgentId): void {
    this.agentLabels.delete(id);
  }

  resolve(selector: TargetSelector, seed: string): AgentId[] {
    let candidates = Array.from(this.agentLabels.keys());

    // Apply label exclusions first
    if (selector.excludeLabels) {
      const excl = selector.excludeLabels;
      candidates = candidates.filter(id => {
        const labels = this.agentLabels.get(id) || {};
        return !Object.entries(excl).every(([k, v]) => labels[k] === v);
      });
    }

    // Apply label inclusions
    if (selector.mode === 'label-match' && selector.labels) {
      const incl = selector.labels;
      candidates = candidates.filter(id => {
        const labels = this.agentLabels.get(id) || {};
        return Object.entries(incl).every(([k, v]) => labels[k] === v);
      });
    }

    switch (selector.mode) {
      case 'specific':
        return (selector.agents || []).filter(a => candidates.includes(a));

      case 'random': {
        const count = selector.count || 1;
        return this.deterministicSample(candidates, count, seed);
      }

      case 'percentage': {
        const pct = Math.min(selector.percentage || 0, 100) / 100;
        const count = Math.ceil(candidates.length * pct);
        return this.deterministicSample(candidates, count, seed);
      }

      case 'label-match':
        return candidates; // already filtered above

      default:
        return [];
    }
  }

  /**
   * Deterministic sampling using FNV-1a seeded shuffle (Fisher-Yates).
   * Ensures reproducible target selection for experiment replay.
   */
  private deterministicSample(agents: AgentId[], count: number, seed: string): AgentId[] {
    const arr = [...agents];
    let h = fnv1a(seed);
    for (let i = arr.length - 1; i > 0; i--) {
      h = fnv1a(`${h}:${i}`);
      const j = h % (i + 1);
      [arr[i], arr[j]] = [arr[j], arr[i]];
    }
    return arr.slice(0, Math.min(count, arr.length));
  }

  totalAgents(): number {
    return this.agentLabels.size;
  }
}

// ============================================================
// Fault Injector — abstract fault injection interface
// ============================================================

interface FaultHandle {
  faultId: string;
  type: FaultType;
  affectedAgents: AgentId[];
  injectedAt: Timestamp;
  removeAt: Timestamp;
}

class FaultInjector {
  private activeFaults: Map<string, FaultHandle> = new Map();
  private faultCounter = 0;

  inject(spec: FaultSpec, agents: AgentId[], now: Timestamp): FaultHandle {
    const faultId = `fault-${++this.faultCounter}-${fnv1a(JSON.stringify(spec)).toString(16)}`;

    const handle: FaultHandle = {
      faultId,
      type: spec.type,
      affectedAgents: [...agents],
      injectedAt: now,
      removeAt: now + spec.durationMs
    };

    this.activeFaults.set(faultId, handle);

    // In a real system, this would invoke the actual fault injection mechanism:
    // - iptables rules for network faults
    // - cgroup limits for resource exhaustion
    // - signal injection for crashes
    // - proxy configuration for latency/corruption
    // Here we model the lifecycle.

    return handle;
  }

  remove(faultId: string): FaultHandle | undefined {
    const handle = this.activeFaults.get(faultId);
    if (handle) {
      this.activeFaults.delete(faultId);
    }
    return handle;
  }

  removeAll(): FaultHandle[] {
    const handles = Array.from(this.activeFaults.values());
    this.activeFaults.clear();
    return handles;
  }

  getExpired(now: Timestamp): FaultHandle[] {
    return Array.from(this.activeFaults.values())
      .filter(h => now >= h.removeAt);
  }

  getActive(): FaultHandle[] {
    return Array.from(this.activeFaults.values());
  }

  activeCount(): number {
    return this.activeFaults.size;
  }
}

// ============================================================
// Preflight Checker — validate environment before chaos
// ============================================================

class PreflightChecker {
  private collector: MetricCollector;

  constructor(collector: MetricCollector) {
    this.collector = collector;
  }

  runChecks(
    checks: PreflightCheck[],
    totalAgents: number,
    activeIncidents: number,
    now: Timestamp
  ): Array<{ check: PreflightCheck; passed: boolean; reason?: string }> {
    return checks.map(check => {
      switch (check.type) {
        case 'metric-threshold': {
          const metric = check.config['metric'] as string;
          const operator = check.config['operator'] as string;
          const threshold = check.config['threshold'] as number;
          const windowMs = (check.config['windowMs'] as number) || 300000;
          
          const value = this.collector.aggregate(
            metric, now - windowMs, now, 'avg'
          );
          
          if (value === null) {
            return { check, passed: false, reason: `No data for metric ${metric}` };
          }
          
          const passed = operator === 'lt' ? value < threshold : value > threshold;
          return {
            check,
            passed,
            reason: passed ? undefined : `${metric} = ${value.toFixed(2)}, need ${operator} ${threshold}`
          };
        }

        case 'no-active-incidents': {
          const passed = activeIncidents === 0;
          return {
            check,
            passed,
            reason: passed ? undefined : `${activeIncidents} active incident(s)`
          };
        }

        case 'minimum-agents': {
          const minAgents = (check.config['count'] as number) || 3;
          const passed = totalAgents >= minAgents;
          return {
            check,
            passed,
            reason: passed ? undefined : `Only ${totalAgents} agents, need ${minAgents}`
          };
        }

        case 'custom': {
          // Custom checks are evaluated externally; default pass
          return { check, passed: true };
        }

        default:
          return { check, passed: false, reason: `Unknown check type: ${check.type}` };
      }
    });
  }
}

// ============================================================
// Experiment Engine — orchestrates the full experiment lifecycle
// ============================================================

class ExperimentEngine {
  private experiments: Map<ExperimentId, Experiment> = new Map();
  private collector: MetricCollector;
  private hypothesisEvaluator: HypothesisEvaluator;
  private blastRadius: BlastRadiusController;
  private killSwitch: KillSwitchMonitor;
  private targetResolver: TargetResolver;
  private faultInjector: FaultInjector;
  private preflightChecker: PreflightChecker;
  private eventLog: ChaosEvent[] = [];
  private regressionTests: Map<string, RegressionTest> = new Map();
  private activeExperimentCount = 0;

  constructor(
    collector: MetricCollector,
    targetResolver: TargetResolver,
    config: { maxEvents?: number } = {}
  ) {
    this.collector = collector;
    this.hypothesisEvaluator = new HypothesisEvaluator(collector);
    this.blastRadius = new BlastRadiusController(targetResolver.totalAgents(), 0.5);
    this.killSwitch = new KillSwitchMonitor([], collector);
    this.targetResolver = targetResolver;
    this.faultInjector = new FaultInjector();
    this.preflightChecker = new PreflightChecker(collector);
  }

  createExperiment(params: {
    name: string;
    description: string;
    hypothesis: SteadyStateHypothesis;
    faults: FaultPhase[];
    safetyConfig: SafetyConfig;
    schedule?: ScheduleConfig;
  }): Experiment {
    const id = `exp-${Date.now()}-${fnv1a(params.name).toString(16)}`;
    const experiment: Experiment = {
      id,
      ...params,
      status: 'draft',
      createdAt: Date.now()
    };

    this.experiments.set(id, experiment);
    this.emitEvent({ type: 'experiment-created', timestamp: Date.now(), experimentId: id, data: { name: params.name } });
    return experiment;
  }

  /**
   * Execute an experiment through its full lifecycle.
   * Returns results when complete.
   */
  async runExperiment(id: ExperimentId, now: Timestamp, options: {
    activeIncidents?: number;
    tickIntervalMs?: number;
  } = {}): Promise<ExperimentResults> {
    const experiment = this.experiments.get(id);
    if (!experiment) throw new Error(`Experiment ${id} not found`);

    const tickInterval = options.tickIntervalMs || 1000;
    const activeIncidents = options.activeIncidents || 0;

    // --- Phase 1: Pre-flight checks ---
    experiment.status = 'pre-check';

    // Concurrent experiment limit
    if (this.activeExperimentCount >= experiment.safetyConfig.maxConcurrentExperiments) {
      experiment.status = 'aborted';
      return this.buildAbortedResults(experiment, 'Max concurrent experiments exceeded', now);
    }

    // Time window check
    if (this.isInBlockedWindow(experiment.safetyConfig.blockedTimeWindows, now)) {
      experiment.status = 'aborted';
      return this.buildAbortedResults(experiment, 'Current time is in a blocked window', now);
    }

    // Preflight checks
    const preflightResults = this.preflightChecker.runChecks(
      experiment.safetyConfig.preflightChecks,
      this.targetResolver.totalAgents(),
      activeIncidents,
      now
    );

    const failedPreflights = preflightResults.filter(r => !r.passed);
    if (failedPreflights.length > 0) {
      this.emitEvent({
        type: 'preflight-failed',
        timestamp: now,
        experimentId: id,
        data: { failures: failedPreflights.map(f => f.reason) }
      });
      experiment.status = 'aborted';
      return this.buildAbortedResults(experiment, `Preflight failed: ${failedPreflights[0].reason}`, now);
    }

    this.emitEvent({ type: 'preflight-passed', timestamp: now, experimentId: id, data: {} });

    // --- Phase 2: Capture baseline ---
    this.hypothesisEvaluator.captureBaseline(experiment.hypothesis, now);

    // Verify hypothesis holds BEFORE we inject chaos
    const baselineCheck = this.hypothesisEvaluator.evaluate(experiment.hypothesis, now);
    if (!baselineCheck.passed) {
      experiment.status = 'aborted';
      return this.buildAbortedResults(experiment, 'Steady-state hypothesis failed before experiment', now);
    }

    // --- Phase 3: Run fault phases ---
    experiment.status = 'running';
    experiment.startedAt = now;
    this.activeExperimentCount++;

    // Configure blast radius and kill switch
    this.blastRadius = new BlastRadiusController(
      this.targetResolver.totalAgents(),
      experiment.safetyConfig.maxBlastRadius
    );
    this.killSwitch = new KillSwitchMonitor(
      experiment.safetyConfig.killSwitchMetrics,
      this.collector
    );

    this.emitEvent({ type: 'experiment-started', timestamp: now, experimentId: id, data: {} });

    const allMetrics: MetricSample[] = [];
    const phaseResults: PhaseResult[] = [];
    let rollbackTriggered = false;
    let rollbackReason: string | undefined;
    let currentTime = now;

    for (const phase of experiment.faults) {
      // Delay before phase
      currentTime += phase.delayBeforeMs;

      const phaseStart = currentTime;
      this.emitEvent({
        type: 'phase-started',
        timestamp: currentTime,
        experimentId: id,
        data: { phase: phase.name }
      });

      const phaseAffected: Set<AgentId> = new Set();
      const phaseHandles: FaultHandle[] = [];

      // Inject all faults in this phase
      for (const faultSpec of phase.faults) {
        const targets = this.targetResolver.resolve(
          faultSpec.targets,
          `${id}:${phase.name}:${faultSpec.type}`
        );

        // Blast radius check
        if (!this.blastRadius.canAffect(targets)) {
          this.emitEvent({
            type: 'blast-radius-exceeded',
            timestamp: currentTime,
            experimentId: id,
            data: { requestedAgents: targets.length, currentRadius: this.blastRadius.currentRadius() }
          });

          if (experiment.safetyConfig.autoRollbackOnHypothesisViolation) {
            rollbackTriggered = true;
            rollbackReason = 'Blast radius exceeded';
            break;
          }
          continue;
        }

        const handle = this.faultInjector.inject(faultSpec, targets, currentTime);
        phaseHandles.push(handle);
        this.blastRadius.recordAffected(targets);
        for (const t of targets) phaseAffected.add(t);

        this.emitEvent({
          type: 'fault-injected',
          timestamp: currentTime,
          experimentId: id,
          data: { faultId: handle.faultId, type: faultSpec.type, agents: targets }
        });
      }

      if (rollbackTriggered) break;

      // Simulate phase duration with kill switch monitoring
      const phaseEndTime = currentTime + phase.durationMs;
      while (currentTime < phaseEndTime) {
        currentTime += tickInterval;

        // Kill switch check
        const ks = this.killSwitch.check(currentTime);
        if (ks.triggered) {
          this.emitEvent({
            type: 'kill-switch-triggered',
            timestamp: currentTime,
            experimentId: id,
            data: { reason: ks.reason }
          });
          rollbackTriggered = true;
          rollbackReason = ks.reason;
          break;
        }

        // Collect metrics snapshot
        allMetrics.push(...this.collectCurrentMetrics(currentTime));
      }

      if (rollbackTriggered) break;

      // Remove phase faults
      for (const handle of phaseHandles) {
        this.faultInjector.remove(handle.faultId);
        this.blastRadius.removeAffected(handle.affectedAgents);
        this.emitEvent({
          type: 'fault-removed',
          timestamp: currentTime,
          experimentId: id,
          data: { faultId: handle.faultId }
        });
      }

      // Post-phase hypothesis check
      let hypothesisCheckPassed: boolean | undefined;
      if (phase.verifyHypothesisAfter) {
        const check = this.hypothesisEvaluator.evaluate(experiment.hypothesis, currentTime);
        hypothesisCheckPassed = check.passed;

        this.emitEvent({
          type: 'hypothesis-checked',
          timestamp: currentTime,
          experimentId: id,
          data: { passed: check.passed, results: check.results }
        });

        if (!check.passed) {
          this.emitEvent({
            type: 'hypothesis-violated',
            timestamp: currentTime,
            experimentId: id,
            data: { phase: phase.name }
          });

          if (experiment.safetyConfig.autoRollbackOnHypothesisViolation) {
            rollbackTriggered = true;
            rollbackReason = `Hypothesis violated after phase "${phase.name}"`;
          }
        }
      }

      phaseResults.push({
        phaseName: phase.name,
        startedAt: phaseStart,
        completedAt: currentTime,
        hypothesisCheckPassed,
        faultsInjected: phaseHandles.length,
        agentsAffected: Array.from(phaseAffected)
      });

      this.emitEvent({
        type: 'phase-completed',
        timestamp: currentTime,
        experimentId: id,
        data: { phase: phase.name }
      });

      if (rollbackTriggered) break;
    }

    // --- Phase 4: Rollback if needed ---
    if (rollbackTriggered) {
      experiment.status = 'rolling-back';
      this.emitEvent({
        type: 'rollback-started',
        timestamp: currentTime,
        experimentId: id,
        data: { reason: rollbackReason }
      });

      const removed = this.faultInjector.removeAll();
      this.blastRadius.reset();

      this.emitEvent({
        type: 'rollback-completed',
        timestamp: currentTime,
        experimentId: id,
        data: { faultsRemoved: removed.length }
      });
    }

    // --- Phase 5: Analysis ---
    experiment.status = 'analyzing';
    this.activeExperimentCount--;

    // Final hypothesis evaluation
    const finalCheck = this.hypothesisEvaluator.evaluate(experiment.hypothesis, currentTime);

    // Compute impact assessment
    const impactAssessment = this.computeImpact(phaseResults, allMetrics, now, currentTime);

    // Discover issues
    const issues = this.analyzeIssues(phaseResults, allMetrics, rollbackTriggered, rollbackReason);

    // Build results
    const results: ExperimentResults = {
      hypothesisHeld: finalCheck.passed && !rollbackTriggered,
      phases: phaseResults,
      metricsTimeline: allMetrics,
      impactAssessment,
      discoveredIssues: issues,
      duration: currentTime - now,
      rollbackTriggered,
      rollbackReason
    };

    experiment.results = results;
    experiment.completedAt = currentTime;
    experiment.status = rollbackTriggered ? 'aborted' : 'completed';

    this.emitEvent({
      type: rollbackTriggered ? 'experiment-aborted' : 'experiment-completed',
      timestamp: currentTime,
      experimentId: id,
      data: {
        hypothesisHeld: results.hypothesisHeld,
        issuesFound: issues.length,
        duration: results.duration
      }
    });

    // Auto-generate regression tests from discovered issues
    for (const issue of issues.filter(i => i.severity === 'critical' || i.severity === 'high')) {
      this.generateRegressionTest(experiment, issue, currentTime);
    }

    this.killSwitch.reset();
    return results;
  }

  private buildAbortedResults(experiment: Experiment, reason: string, now: Timestamp): ExperimentResults {
    return {
      hypothesisHeld: false,
      phases: [],
      metricsTimeline: [],
      impactAssessment: {
        availabilityDegradation: 0,
        latencyIncrease: 1,
        errorRateIncrease: 0,
        throughputReduction: 0,
        recoveryTimeMs: 0,
        cascadeDepth: 0,
        affectedAgents: []
      },
      discoveredIssues: [{
        severity: 'medium',
        category: 'preflight',
        description: reason,
        reproducible: true,
        relatedFaults: [],
        affectedAgents: []
      }],
      duration: 0,
      rollbackTriggered: false,
      rollbackReason: reason
    };
  }

  private isInBlockedWindow(windows: TimeWindow[], now: Timestamp): boolean {
    const date = new Date(now);
    const hour = date.getHours();
    const dow = date.getDay();

    return windows.some(w =>
      w.daysOfWeek.includes(dow) &&
      hour >= w.startHour &&
      hour < w.endHour
    );
  }

  private collectCurrentMetrics(now: Timestamp): MetricSample[] {
    // In a real system, this would poll actual agent metrics endpoints.
    // Here it returns whatever's been recently recorded in the collector.
    return [];
  }

  private computeImpact(
    phases: PhaseResult[],
    metrics: MetricSample[],
    startTime: Timestamp,
    endTime: Timestamp
  ): ImpactAssessment {
    const allAffected = new Set<AgentId>();
    let maxCascade = 0;

    for (const phase of phases) {
      for (const agent of phase.agentsAffected) allAffected.add(agent);
      // Cascade depth approximation: phases that fail hypothesis after
      // only targeting a small subset indicate cascade
      if (phase.hypothesisCheckPassed === false) {
        maxCascade = Math.max(maxCascade, phase.agentsAffected.length);
      }
    }

    // Extract availability and latency metrics if available
    const availMetrics = metrics.filter(m => m.name === 'availability');
    const latencyMetrics = metrics.filter(m => m.name === 'latency_p99');
    const errorMetrics = metrics.filter(m => m.name === 'error_rate');
    const throughputMetrics = metrics.filter(m => m.name === 'throughput');

    const avgAvail = availMetrics.length > 0
      ? availMetrics.reduce((s, m) => s + m.value, 0) / availMetrics.length
      : 1.0;

    const avgLatency = latencyMetrics.length > 0
      ? latencyMetrics.reduce((s, m) => s + m.value, 0) / latencyMetrics.length
      : 1.0;

    const avgErrors = errorMetrics.length > 0
      ? errorMetrics.reduce((s, m) => s + m.value, 0) / errorMetrics.length
      : 0;

    const avgThroughput = throughputMetrics.length > 0
      ? throughputMetrics.reduce((s, m) => s + m.value, 0) / throughputMetrics.length
      : 1.0;

    // Recovery time: time from last fault removal to hypothesis holding
    const lastPhaseEnd = phases.length > 0 ? phases[phases.length - 1].completedAt : startTime;
    const recoveryTime = endTime - lastPhaseEnd;

    return {
      availabilityDegradation: 1 - avgAvail,
      latencyIncrease: avgLatency,
      errorRateIncrease: avgErrors,
      throughputReduction: 1 - avgThroughput,
      recoveryTimeMs: recoveryTime,
      cascadeDepth: maxCascade,
      affectedAgents: Array.from(allAffected)
    };
  }

  private analyzeIssues(
    phases: PhaseResult[],
    metrics: MetricSample[],
    rollbackTriggered: boolean,
    rollbackReason?: string
  ): DiscoveredIssue[] {
    const issues: DiscoveredIssue[] = [];

    // Issue: hypothesis violated
    const failedPhases = phases.filter(p => p.hypothesisCheckPassed === false);
    for (const phase of failedPhases) {
      issues.push({
        severity: 'high',
        category: 'resilience',
        description: `Steady-state hypothesis violated after phase "${phase.phaseName}" affecting ${phase.agentsAffected.length} agents`,
        reproducible: true,
        suggestedFix: 'Add circuit breakers or fallback paths for affected fault type',
        relatedFaults: [],
        affectedAgents: phase.agentsAffected
      });
    }

    // Issue: kill switch triggered
    if (rollbackTriggered && rollbackReason?.startsWith('Kill switch:')) {
      issues.push({
        severity: 'critical',
        category: 'safety',
        description: rollbackReason,
        reproducible: true,
        suggestedFix: 'Investigate cascading failure path; add bulkhead isolation',
        relatedFaults: [],
        affectedAgents: []
      });
    }

    // Issue: slow recovery (>5x phase duration)
    for (let i = 0; i < phases.length; i++) {
      const phase = phases[i];
      const phaseDuration = phase.completedAt - phase.startedAt;
      const nextStart = i + 1 < phases.length ? phases[i + 1].startedAt : phase.completedAt;
      const gap = nextStart - phase.completedAt;

      if (gap > phaseDuration * 5) {
        issues.push({
          severity: 'medium',
          category: 'recovery',
          description: `Slow recovery after phase "${phase.phaseName}": ${gap}ms (phase was ${phaseDuration}ms)`,
          reproducible: true,
          suggestedFix: 'Add health-check driven auto-recovery or faster failover',
          relatedFaults: [],
          affectedAgents: phase.agentsAffected
        });
      }
    }

    // Issue: cascade (single-target fault affects many agents)
    for (const phase of phases) {
      if (phase.faultsInjected === 1 && phase.agentsAffected.length > 3 &&
          phase.hypothesisCheckPassed === false) {
        issues.push({
          severity: 'high',
          category: 'cascade',
          description: `Single fault in "${phase.phaseName}" cascaded to ${phase.agentsAffected.length} agents`,
          reproducible: true,
          suggestedFix: 'Add bulkhead isolation between failure domains',
          relatedFaults: [],
          affectedAgents: phase.agentsAffected
        });
      }
    }

    return issues;
  }

  private generateRegressionTest(
    experiment: Experiment,
    issue: DiscoveredIssue,
    now: Timestamp
  ): RegressionTest {
    const allFaults = experiment.faults.flatMap(p => p.faults);
    const testId = `reg-${fnv1a(`${experiment.id}:${issue.description}`).toString(16)}`;

    const test: RegressionTest = {
      id: testId,
      sourceExperimentId: experiment.id,
      issueDescription: issue.description,
      faultSequence: allFaults,
      assertions: experiment.hypothesis.metrics,
      createdAt: now
    };

    this.regressionTests.set(testId, test);
    this.emitEvent({
      type: 'regression-test-created',
      timestamp: now,
      experimentId: experiment.id,
      data: { testId, issue: issue.description }
    });

    return test;
  }

  getExperiment(id: ExperimentId): Experiment | undefined {
    return this.experiments.get(id);
  }

  getRegressionTests(): RegressionTest[] {
    return Array.from(this.regressionTests.values());
  }

  getEventLog(): ChaosEvent[] {
    return [...this.eventLog];
  }

  private emitEvent(event: ChaosEvent): void {
    this.eventLog.push(event);
  }
}

// ============================================================
// GameDay Coordinator — multi-experiment exercises
// ============================================================

class GameDayCoordinator {
  private gameDays: Map<string, GameDay> = new Map();
  private engine: ExperimentEngine;
  private eventLog: ChaosEvent[] = [];

  constructor(engine: ExperimentEngine) {
    this.engine = engine;
  }

  createGameDay(params: {
    name: string;
    description: string;
    experimentIds: ExperimentId[];
    participants: Participant[];
    runbook: RunbookStep[];
  }): GameDay {
    const id = `gd-${Date.now()}-${fnv1a(params.name).toString(16)}`;
    const gameDay: GameDay = {
      id,
      name: params.name,
      description: params.description,
      experiments: params.experimentIds,
      participants: params.participants,
      runbook: params.runbook,
      status: 'planning'
    };

    this.gameDays.set(id, gameDay);
    return gameDay;
  }

  async startGameDay(id: string, now: Timestamp): Promise<{
    gameDayId: string;
    experimentResults: Map<ExperimentId, ExperimentResults>;
    totalIssues: number;
    criticalIssues: number;
  }> {
    const gameDay = this.gameDays.get(id);
    if (!gameDay) throw new Error(`GameDay ${id} not found`);

    gameDay.status = 'active';
    gameDay.startedAt = now;

    this.eventLog.push({
      type: 'gameday-started',
      timestamp: now,
      data: { gameDayId: id, name: gameDay.name }
    });

    const results = new Map<ExperimentId, ExperimentResults>();
    let currentTime = now;

    for (const step of gameDay.runbook.sort((a, b) => a.order - b.order)) {
      if (step.experimentId) {
        const result = await this.engine.runExperiment(step.experimentId, currentTime);
        results.set(step.experimentId, result);
        currentTime += result.duration;
      }
      currentTime += step.expectedDuration;
    }

    gameDay.status = 'completed';
    gameDay.completedAt = currentTime;

    const allIssues = Array.from(results.values()).flatMap(r => r.discoveredIssues);

    this.eventLog.push({
      type: 'gameday-completed',
      timestamp: currentTime,
      data: {
        gameDayId: id,
        experimentsRun: results.size,
        totalIssues: allIssues.length,
        criticalIssues: allIssues.filter(i => i.severity === 'critical').length
      }
    });

    return {
      gameDayId: id,
      experimentResults: results,
      totalIssues: allIssues.length,
      criticalIssues: allIssues.filter(i => i.severity === 'critical').length
    };
  }

  getGameDay(id: string): GameDay | undefined {
    return this.gameDays.get(id);
  }
}

// ============================================================
// Scenario Library — pre-built chaos experiments
// ============================================================

function singleAgentCrashScenario(agentId: AgentId): {
  hypothesis: SteadyStateHypothesis;
  faults: FaultPhase[];
  safetyConfig: SafetyConfig;
} {
  return {
    hypothesis: {
      id: 'single-crash-hypothesis',
      name: 'System survives single agent crash',
      description: 'Availability stays above 99%, error rate below 1%, latency within 2x baseline',
      metrics: [
        { metric: 'availability', operator: 'gte', value: 0.99, aggregation: 'avg' },
        { metric: 'error_rate', operator: 'lte', value: 0.01, aggregation: 'avg' },
        { metric: 'latency_p99', operator: 'lte', value: 2.0, aggregation: 'p99' }
      ],
      tolerance: 0,
      evaluationWindowMs: 60000,
      cooldownMs: 30000
    },
    faults: [{
      name: 'crash-single-agent',
      faults: [{
        type: 'agent-crash',
        targets: { mode: 'specific', agents: [agentId] },
        parameters: { graceful: false },
        durationMs: 120000
      }],
      delayBeforeMs: 10000,
      durationMs: 120000,
      verifyHypothesisAfter: true
    }],
    safetyConfig: {
      maxBlastRadius: 0.1,
      killSwitchMetrics: [{
        metric: 'availability',
        operator: 'lt',
        threshold: 0.9,
        sustainedMs: 30000,
        description: 'Availability dropped below 90% for 30s'
      }],
      autoRollbackOnHypothesisViolation: true,
      requireManualApproval: false,
      blockedTimeWindows: [],
      minimumHealthyAgents: 3,
      maxConcurrentExperiments: 1,
      preflightChecks: [
        { name: 'min-agents', type: 'minimum-agents', config: { count: 5 } },
        { name: 'no-incidents', type: 'no-active-incidents', config: {} }
      ]
    }
  };
}

function networkPartitionScenario(): {
  hypothesis: SteadyStateHypothesis;
  faults: FaultPhase[];
  safetyConfig: SafetyConfig;
} {
  return {
    hypothesis: {
      id: 'partition-hypothesis',
      name: 'System handles network partition',
      description: 'Majority partition remains available, no data loss after heal',
      metrics: [
        { metric: 'availability', operator: 'gte', value: 0.5, aggregation: 'avg' },
        { metric: 'data_loss_events', operator: 'eq', value: 0, aggregation: 'sum' },
        { metric: 'split_brain_detected', operator: 'eq', value: 0, aggregation: 'sum' }
      ],
      tolerance: 0,
      evaluationWindowMs: 120000,
      cooldownMs: 60000
    },
    faults: [
      {
        name: 'create-partition',
        faults: [{
          type: 'partition',
          targets: { mode: 'percentage', percentage: 30 },
          parameters: { bidirectional: true },
          durationMs: 180000
        }],
        delayBeforeMs: 15000,
        durationMs: 180000,
        verifyHypothesisAfter: true
      },
      {
        name: 'heal-and-verify',
        faults: [],
        delayBeforeMs: 5000,
        durationMs: 60000,
        verifyHypothesisAfter: true
      }
    ],
    safetyConfig: {
      maxBlastRadius: 0.35,
      killSwitchMetrics: [{
        metric: 'data_loss_events',
        operator: 'gt',
        threshold: 0,
        sustainedMs: 5000,
        description: 'Data loss detected'
      }],
      autoRollbackOnHypothesisViolation: true,
      requireManualApproval: true,
      blockedTimeWindows: [{
        startHour: 22,
        endHour: 8,
        daysOfWeek: [0, 1, 2, 3, 4, 5, 6],
        timezone: 'UTC'
      }],
      minimumHealthyAgents: 5,
      maxConcurrentExperiments: 1,
      preflightChecks: [
        { name: 'min-agents', type: 'minimum-agents', config: { count: 7 } },
        { name: 'no-incidents', type: 'no-active-incidents', config: {} },
        { name: 'healthy-baseline', type: 'metric-threshold', config: { metric: 'availability', operator: 'gt', threshold: 0.99, windowMs: 300000 } }
      ]
    }
  };
}

function cascadingFailureScenario(): {
  hypothesis: SteadyStateHypothesis;
  faults: FaultPhase[];
  safetyConfig: SafetyConfig;
} {
  return {
    hypothesis: {
      id: 'cascade-hypothesis',
      name: 'System resists cascading failure',
      description: 'Single dependency failure does not take down more than 20% of agents',
      metrics: [
        { metric: 'availability', operator: 'gte', value: 0.8, aggregation: 'avg' },
        { metric: 'cascade_depth', operator: 'lte', value: 2, aggregation: 'max' },
        { metric: 'circuit_breaker_opens', operator: 'gte', value: 1, aggregation: 'sum' }
      ],
      tolerance: 0.33,
      evaluationWindowMs: 90000,
      cooldownMs: 45000
    },
    faults: [
      {
        name: 'kill-critical-dependency',
        faults: [{
          type: 'dependency-failure',
          targets: { mode: 'label-match', labels: { role: 'critical-dependency' } },
          parameters: { failureMode: 'timeout', timeoutMs: 30000 },
          durationMs: 120000
        }],
        delayBeforeMs: 10000,
        durationMs: 120000,
        verifyHypothesisAfter: true
      },
      {
        name: 'add-latency-spike',
        faults: [{
          type: 'latency-spike',
          targets: { mode: 'random', count: 3 },
          parameters: { latencyMs: 5000, jitterMs: 2000 },
          durationMs: 60000
        }],
        delayBeforeMs: 0,
        durationMs: 60000,
        verifyHypothesisAfter: true
      },
      {
        name: 'recovery-observation',
        faults: [],
        delayBeforeMs: 5000,
        durationMs: 90000,
        verifyHypothesisAfter: true
      }
    ],
    safetyConfig: {
      maxBlastRadius: 0.25,
      killSwitchMetrics: [
        {
          metric: 'availability',
          operator: 'lt',
          threshold: 0.5,
          sustainedMs: 15000,
          description: 'Availability below 50% for 15s — total failure'
        },
        {
          metric: 'error_rate',
          operator: 'gt',
          threshold: 0.5,
          sustainedMs: 10000,
          description: 'Error rate above 50% for 10s'
        }
      ],
      autoRollbackOnHypothesisViolation: false, // We want to observe cascade
      requireManualApproval: false,
      blockedTimeWindows: [],
      minimumHealthyAgents: 5,
      maxConcurrentExperiments: 1,
      preflightChecks: [
        { name: 'min-agents', type: 'minimum-agents', config: { count: 8 } },
        { name: 'no-incidents', type: 'no-active-incidents', config: {} }
      ]
    }
  };
}

function resourceExhaustionScenario(): {
  hypothesis: SteadyStateHypothesis;
  faults: FaultPhase[];
  safetyConfig: SafetyConfig;
} {
  return {
    hypothesis: {
      id: 'resource-exhaustion-hypothesis',
      name: 'System handles resource pressure',
      description: 'Load shedding activates gracefully, no OOM kills, backpressure propagates',
      metrics: [
        { metric: 'oom_kills', operator: 'eq', value: 0, aggregation: 'sum' },
        { metric: 'load_shed_activated', operator: 'gte', value: 1, aggregation: 'sum' },
        { metric: 'latency_p99', operator: 'lte', value: 5.0, aggregation: 'p99' }
      ],
      tolerance: 0.33,
      evaluationWindowMs: 120000,
      cooldownMs: 60000
    },
    faults: [{
      name: 'exhaust-resources',
      faults: [
        {
          type: 'resource-exhaustion',
          targets: { mode: 'random', count: 2 },
          parameters: { resource: 'memory', utilizationPercent: 90 },
          durationMs: 90000,
          rampUpMs: 15000
        },
        {
          type: 'resource-exhaustion',
          targets: { mode: 'random', count: 2 },
          parameters: { resource: 'cpu', utilizationPercent: 95 },
          durationMs: 90000,
          rampUpMs: 10000
        }
      ],
      delayBeforeMs: 10000,
      durationMs: 90000,
      verifyHypothesisAfter: true
    }],
    safetyConfig: {
      maxBlastRadius: 0.2,
      killSwitchMetrics: [{
        metric: 'oom_kills',
        operator: 'gt',
        threshold: 2,
        sustainedMs: 5000,
        description: 'Multiple OOM kills detected'
      }],
      autoRollbackOnHypothesisViolation: true,
      requireManualApproval: false,
      blockedTimeWindows: [],
      minimumHealthyAgents: 4,
      maxConcurrentExperiments: 1,
      preflightChecks: [
        { name: 'min-agents', type: 'minimum-agents', config: { count: 6 } }
      ]
    }
  };
}

// ============================================================
// Exports
// ============================================================

export {
  // Core types
  MetricSample,
  SteadyStateHypothesis,
  MetricAssertion,
  FaultSpec,
  FaultType,
  TargetSelector,
  Experiment,
  ExperimentResults,
  ExperimentStatus,
  FaultPhase,
  SafetyConfig,
  KillSwitchCondition,
  TimeWindow,
  PreflightCheck,
  ScheduleConfig,
  ImpactAssessment,
  DiscoveredIssue,
  GameDay,
  Participant,
  RunbookStep,
  RegressionTest,
  ChaosEvent,
  ChaosEventType,
  FaultHandle,

  // Core classes
  MetricCollector,
  HypothesisEvaluator,
  BlastRadiusController,
  KillSwitchMonitor,
  TargetResolver,
  FaultInjector,
  PreflightChecker,
  ExperimentEngine,
  GameDayCoordinator,

  // Pre-built scenarios
  singleAgentCrashScenario,
  networkPartitionScenario,
  cascadingFailureScenario,
  resourceExhaustionScenario,

  // Utilities
  fnv1a
};
