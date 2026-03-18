import { fnv1aHash } from './shared-utils';
/**
 * Capability Health Monitor
 * 
 * Real-time health tracking for individual agent capabilities with
 * degradation detection, predictive failure analysis, and automatic
 * capability routing adjustments.
 * 
 * Key components:
 * - CapabilityProbe: Per-capability liveness/readiness/performance probes
 * - DegradationDetector: Multi-signal degradation classification
 * - FailurePredictor: Time-series trend analysis for proactive alerting
 * - CapabilityScorecard: Composite health scoring with SLA tracking
 * - RemediationEngine: Automated remediation action selection
 * - HealthFederator: Cross-agent capability health aggregation
 * 
 * @author Apex
 */

// ─── Utilities ────────────────────────────────────────────────────────────

interface WelfordState {
  count: number;
  mean: number;
  m2: number;
}

function welfordInit(): WelfordState {
  return { count: 0, mean: 0, m2: 0 };
}

function welfordUpdate(state: WelfordState, value: number): void {
  state.count++;
  const delta = value - state.mean;
  state.mean += delta / state.count;
  const delta2 = value - state.mean;
  state.m2 += delta * delta2;
}

function welfordVariance(state: WelfordState): number {
  return state.count < 2 ? 0 : state.m2 / (state.count - 1);
}

function welfordStdDev(state: WelfordState): number {
  return Math.sqrt(welfordVariance(state));
}

// ─── Types ────────────────────────────────────────────────────────────────

type ProbeType = 'liveness' | 'readiness' | 'performance' | 'saturation';
type HealthStatus = 'healthy' | 'degraded' | 'unhealthy' | 'unknown';
type DegradationLevel = 'none' | 'minor' | 'moderate' | 'severe' | 'critical';
type RemediationAction = 'none' | 'alert' | 'throttle' | 'reroute' | 'disable' | 'restart';

interface ProbeConfig {
  type: ProbeType;
  intervalMs: number;
  timeoutMs: number;
  failureThreshold: number;
  successThreshold: number;
  enabled: boolean;
}

interface ProbeResult {
  type: ProbeType;
  capabilityId: string;
  agentId: string;
  success: boolean;
  latencyMs: number;
  timestamp: number;
  metadata?: Record<string, unknown>;
}

interface CapabilityHealthState {
  capabilityId: string;
  agentId: string;
  status: HealthStatus;
  degradationLevel: DegradationLevel;
  score: number; // 0-1 composite health score
  lastProbeResults: Map<ProbeType, ProbeResult>;
  consecutiveFailures: Map<ProbeType, number>;
  consecutiveSuccesses: Map<ProbeType, number>;
  slaCompliance: SLACompliance;
  lastStatusChange: number;
  createdAt: number;
}

interface SLATarget {
  availabilityPercent: number;    // e.g., 99.9
  maxLatencyP50Ms: number;
  maxLatencyP99Ms: number;
  maxErrorRatePercent: number;
  evaluationWindowMs: number;
}

interface SLACompliance {
  availability: number;          // current availability percentage
  latencyP50: number;
  latencyP99: number;
  errorRate: number;
  withinSLA: boolean;
  violationCount: number;
  lastEvaluated: number;
}

interface DegradationSignal {
  type: 'latency_increase' | 'error_spike' | 'saturation_high' | 'throughput_drop' | 'partial_failure';
  severity: number;             // 0-1
  capabilityId: string;
  agentId: string;
  timestamp: number;
  details: string;
}

interface FailurePrediction {
  capabilityId: string;
  agentId: string;
  predictedFailureTime: number;
  confidence: number;           // 0-1
  basis: string;
  recommendedAction: RemediationAction;
}

interface RemediationRecord {
  id: string;
  capabilityId: string;
  agentId: string;
  action: RemediationAction;
  reason: string;
  timestamp: number;
  resolved: boolean;
  resolvedAt?: number;
}

interface HealthEvent {
  type: 'probe_result' | 'status_change' | 'degradation_detected' | 'failure_predicted' |
        'sla_violation' | 'remediation_applied' | 'capability_recovered' | 'health_report' |
        'federation_sync' | 'probe_timeout';
  timestamp: number;
  data: Record<string, unknown>;
}

// ─── Capability Probe ─────────────────────────────────────────────────────

/**
 * Per-capability health probes with configurable thresholds.
 * Tracks liveness (alive?), readiness (accepting work?),
 * performance (within latency bounds?), and saturation (resource pressure).
 */
class CapabilityProbe {
  private configs: Map<string, Map<ProbeType, ProbeConfig>> = new Map();
  private results: Map<string, ProbeResult[]> = new Map();
  private readonly maxResultsPerCapability: number;
  private latencyStats: Map<string, WelfordState> = new Map();
  private ewmaLatency: Map<string, number> = new Map();
  private readonly ewmaAlpha: number;

  constructor(
    private readonly defaultConfigs: Record<ProbeType, ProbeConfig>,
    maxResultsPerCapability: number = 1000,
    ewmaAlpha: number = 0.3
  ) {
    this.maxResultsPerCapability = maxResultsPerCapability;
    this.ewmaAlpha = ewmaAlpha;
  }

  configureProbe(capabilityId: string, agentId: string, config: ProbeConfig): void {
    const key = `${capabilityId}:${agentId}`;
    if (!this.configs.has(key)) {
      this.configs.set(key, new Map());
    }
    this.configs.get(key)!.set(config.type, config);
  }

  getConfig(capabilityId: string, agentId: string, probeType: ProbeType): ProbeConfig {
    const key = `${capabilityId}:${agentId}`;
    return this.configs.get(key)?.get(probeType) ?? this.defaultConfigs[probeType];
  }

  recordResult(result: ProbeResult): void {
    const key = `${result.capabilityId}:${result.agentId}`;
    
    if (!this.results.has(key)) {
      this.results.set(key, []);
    }
    
    const results = this.results.get(key)!;
    results.push(result);
    
    // Bounded retention
    if (results.length > this.maxResultsPerCapability) {
      results.splice(0, results.length - this.maxResultsPerCapability);
    }

    // Update latency tracking
    if (result.success) {
      const latencyKey = `${key}:${result.type}`;
      if (!this.latencyStats.has(latencyKey)) {
        this.latencyStats.set(latencyKey, welfordInit());
      }
      welfordUpdate(this.latencyStats.get(latencyKey)!, result.latencyMs);

      const prev = this.ewmaLatency.get(latencyKey) ?? result.latencyMs;
      this.ewmaLatency.set(latencyKey, this.ewmaAlpha * result.latencyMs + (1 - this.ewmaAlpha) * prev);
    }
  }

  getRecentResults(capabilityId: string, agentId: string, windowMs: number): ProbeResult[] {
    const key = `${capabilityId}:${agentId}`;
    const results = this.results.get(key) ?? [];
    const cutoff = Date.now() - windowMs;
    return results.filter(r => r.timestamp >= cutoff);
  }

  getLatencyStats(capabilityId: string, agentId: string, probeType: ProbeType): {
    mean: number; stdDev: number; ewma: number; count: number;
  } {
    const key = `${capabilityId}:${agentId}:${probeType}`;
    const stats = this.latencyStats.get(key);
    if (!stats || stats.count === 0) {
      return { mean: 0, stdDev: 0, ewma: 0, count: 0 };
    }
    return {
      mean: stats.mean,
      stdDev: welfordStdDev(stats),
      ewma: this.ewmaLatency.get(key) ?? stats.mean,
      count: stats.count
    };
  }

  computeAvailability(capabilityId: string, agentId: string, windowMs: number): number {
    const results = this.getRecentResults(capabilityId, agentId, windowMs);
    if (results.length === 0) return 1.0; // No data = assume healthy
    
    const successful = results.filter(r => r.success).length;
    return successful / results.length;
  }

  computePercentileLatency(capabilityId: string, agentId: string, percentile: number, windowMs: number): number {
    const results = this.getRecentResults(capabilityId, agentId, windowMs)
      .filter(r => r.success)
      .map(r => r.latencyMs)
      .sort((a, b) => a - b);
    
    if (results.length === 0) return 0;
    
    const index = Math.ceil(percentile / 100 * results.length) - 1;
    return results[Math.max(0, index)];
  }
}

// ─── Degradation Detector ────────────────────────────────────────────────

/**
 * Multi-signal degradation detection. Combines latency trends,
 * error rates, saturation levels, and throughput changes to classify
 * degradation severity with debouncing to prevent flapping.
 */
class DegradationDetector {
  private readonly latencyBaselineWindow: number;
  private readonly detectionWindow: number;
  private readonly latencyThresholds: { minor: number; moderate: number; severe: number; critical: number };
  private readonly errorThresholds: { minor: number; moderate: number; severe: number; critical: number };
  private readonly saturationThresholds: { minor: number; moderate: number; severe: number; critical: number };
  private readonly debounceMs: number;
  private lastSignals: Map<string, { level: DegradationLevel; timestamp: number }> = new Map();

  constructor(config: {
    latencyBaselineWindowMs?: number;
    detectionWindowMs?: number;
    latencyMultipliers?: { minor: number; moderate: number; severe: number; critical: number };
    errorPercents?: { minor: number; moderate: number; severe: number; critical: number };
    saturationPercents?: { minor: number; moderate: number; severe: number; critical: number };
    debounceMs?: number;
  } = {}) {
    this.latencyBaselineWindow = config.latencyBaselineWindowMs ?? 3600000;  // 1 hour
    this.detectionWindow = config.detectionWindowMs ?? 300000;              // 5 minutes
    this.latencyThresholds = config.latencyMultipliers ?? { minor: 1.5, moderate: 2.5, severe: 4.0, critical: 8.0 };
    this.errorThresholds = config.errorPercents ?? { minor: 1, moderate: 5, severe: 15, critical: 30 };
    this.saturationThresholds = config.saturationPercents ?? { minor: 60, moderate: 75, severe: 85, critical: 95 };
    this.debounceMs = config.debounceMs ?? 30000;
  }

  detect(
    probe: CapabilityProbe,
    capabilityId: string,
    agentId: string
  ): { level: DegradationLevel; signals: DegradationSignal[] } {
    const signals: DegradationSignal[] = [];
    const now = Date.now();
    const key = `${capabilityId}:${agentId}`;

    // 1. Latency degradation
    const baselineStats = probe.getLatencyStats(capabilityId, agentId, 'performance');
    if (baselineStats.count > 10) {
      const recentP50 = probe.computePercentileLatency(capabilityId, agentId, 50, this.detectionWindow);
      const ratio = baselineStats.mean > 0 ? recentP50 / baselineStats.mean : 0;
      
      if (ratio >= this.latencyThresholds.critical) {
        signals.push(this.makeSignal('latency_increase', 1.0, capabilityId, agentId, 
          `Latency ${ratio.toFixed(1)}x baseline (${recentP50.toFixed(0)}ms vs ${baselineStats.mean.toFixed(0)}ms)`));
      } else if (ratio >= this.latencyThresholds.severe) {
        signals.push(this.makeSignal('latency_increase', 0.75, capabilityId, agentId,
          `Latency ${ratio.toFixed(1)}x baseline`));
      } else if (ratio >= this.latencyThresholds.moderate) {
        signals.push(this.makeSignal('latency_increase', 0.5, capabilityId, agentId,
          `Latency ${ratio.toFixed(1)}x baseline`));
      } else if (ratio >= this.latencyThresholds.minor) {
        signals.push(this.makeSignal('latency_increase', 0.25, capabilityId, agentId,
          `Latency ${ratio.toFixed(1)}x baseline`));
      }
    }

    // 2. Error rate degradation
    const recentResults = probe.getRecentResults(capabilityId, agentId, this.detectionWindow);
    if (recentResults.length > 5) {
      const errorRate = (recentResults.filter(r => !r.success).length / recentResults.length) * 100;
      
      if (errorRate >= this.errorThresholds.critical) {
        signals.push(this.makeSignal('error_spike', 1.0, capabilityId, agentId,
          `Error rate ${errorRate.toFixed(1)}% in detection window`));
      } else if (errorRate >= this.errorThresholds.severe) {
        signals.push(this.makeSignal('error_spike', 0.75, capabilityId, agentId,
          `Error rate ${errorRate.toFixed(1)}%`));
      } else if (errorRate >= this.errorThresholds.moderate) {
        signals.push(this.makeSignal('error_spike', 0.5, capabilityId, agentId,
          `Error rate ${errorRate.toFixed(1)}%`));
      } else if (errorRate >= this.errorThresholds.minor) {
        signals.push(this.makeSignal('error_spike', 0.25, capabilityId, agentId,
          `Error rate ${errorRate.toFixed(1)}%`));
      }
    }

    // 3. Throughput degradation (compare recent vs baseline window rate)
    const baselineResults = probe.getRecentResults(capabilityId, agentId, this.latencyBaselineWindow);
    if (baselineResults.length > 20 && recentResults.length > 0) {
      const baselineRate = baselineResults.length / (this.latencyBaselineWindow / 60000); // per minute
      const recentRate = recentResults.length / (this.detectionWindow / 60000);
      
      if (baselineRate > 0) {
        const dropRatio = 1 - (recentRate / baselineRate);
        if (dropRatio > 0.7) {
          signals.push(this.makeSignal('throughput_drop', 0.9, capabilityId, agentId,
            `Throughput dropped ${(dropRatio * 100).toFixed(0)}% from baseline`));
        } else if (dropRatio > 0.4) {
          signals.push(this.makeSignal('throughput_drop', 0.5, capabilityId, agentId,
            `Throughput dropped ${(dropRatio * 100).toFixed(0)}%`));
        }
      }
    }

    // Composite degradation level with debouncing
    const level = this.classifyLevel(signals);
    
    // Debounce: only report escalation or after debounce period
    const lastSignal = this.lastSignals.get(key);
    if (lastSignal) {
      const levelOrder: Record<DegradationLevel, number> = { none: 0, minor: 1, moderate: 2, severe: 3, critical: 4 };
      const isEscalation = levelOrder[level] > levelOrder[lastSignal.level];
      const debounceExpired = now - lastSignal.timestamp > this.debounceMs;
      
      if (!isEscalation && !debounceExpired) {
        return { level: lastSignal.level, signals: [] };
      }
    }

    this.lastSignals.set(key, { level, timestamp: now });
    return { level, signals };
  }

  private classifyLevel(signals: DegradationSignal[]): DegradationLevel {
    if (signals.length === 0) return 'none';
    
    const maxSeverity = Math.max(...signals.map(s => s.severity));
    // Multiple moderate signals compound to higher severity
    const compoundSeverity = Math.min(1.0, maxSeverity + signals.length * 0.05);
    
    if (compoundSeverity >= 0.9) return 'critical';
    if (compoundSeverity >= 0.7) return 'severe';
    if (compoundSeverity >= 0.45) return 'moderate';
    if (compoundSeverity > 0) return 'minor';
    return 'none';
  }

  private makeSignal(
    type: DegradationSignal['type'],
    severity: number,
    capabilityId: string,
    agentId: string,
    details: string
  ): DegradationSignal {
    return { type, severity, capabilityId, agentId, timestamp: Date.now(), details };
  }
}

// ─── Failure Predictor ────────────────────────────────────────────────────

/**
 * Time-series trend analysis for proactive failure prediction.
 * Uses linear regression on error rates and latency trends to
 * estimate time-to-failure with confidence scoring.
 */
class FailurePredictor {
  private readonly windowCount: number;
  private readonly windowSizeMs: number;
  private readonly errorRateFailureThreshold: number;
  private readonly latencyFailureMultiplier: number;

  constructor(config: {
    windowCount?: number;
    windowSizeMs?: number;
    errorRateFailureThreshold?: number;
    latencyFailureMultiplier?: number;
  } = {}) {
    this.windowCount = config.windowCount ?? 12;
    this.windowSizeMs = config.windowSizeMs ?? 300000; // 5 min windows
    this.errorRateFailureThreshold = config.errorRateFailureThreshold ?? 50;
    this.latencyFailureMultiplier = config.latencyFailureMultiplier ?? 10;
  }

  predict(
    probe: CapabilityProbe,
    capabilityId: string,
    agentId: string
  ): FailurePrediction | null {
    const now = Date.now();
    const totalWindow = this.windowCount * this.windowSizeMs;
    const allResults = probe.getRecentResults(capabilityId, agentId, totalWindow);
    
    if (allResults.length < 20) return null; // Not enough data

    // Bucket results into windows
    const windows: { errorRate: number; avgLatency: number; timestamp: number }[] = [];
    
    for (let i = 0; i < this.windowCount; i++) {
      const windowStart = now - totalWindow + i * this.windowSizeMs;
      const windowEnd = windowStart + this.windowSizeMs;
      const windowResults = allResults.filter(r => r.timestamp >= windowStart && r.timestamp < windowEnd);
      
      if (windowResults.length === 0) continue;
      
      const errors = windowResults.filter(r => !r.success).length;
      const successes = windowResults.filter(r => r.success);
      const avgLat = successes.length > 0 
        ? successes.reduce((s, r) => s + r.latencyMs, 0) / successes.length 
        : 0;
      
      windows.push({
        errorRate: (errors / windowResults.length) * 100,
        avgLatency: avgLat,
        timestamp: (windowStart + windowEnd) / 2
      });
    }

    if (windows.length < 4) return null;

    // Linear regression on error rate
    const errorTrend = this.linearRegression(
      windows.map((w, i) => i),
      windows.map(w => w.errorRate)
    );

    // Linear regression on latency
    const baselineLatency = probe.getLatencyStats(capabilityId, agentId, 'performance');
    const latencyTrend = this.linearRegression(
      windows.map((w, i) => i),
      windows.map(w => w.avgLatency)
    );

    // Predict time to failure based on error rate trend
    if (errorTrend.slope > 0.1) { // Error rate increasing
      const currentErrorRate = windows[windows.length - 1].errorRate;
      const windowsToFailure = (this.errorRateFailureThreshold - currentErrorRate) / errorTrend.slope;
      
      if (windowsToFailure > 0 && windowsToFailure < this.windowCount * 2) {
        const predictedTime = now + windowsToFailure * this.windowSizeMs;
        const confidence = Math.min(0.95, errorTrend.r2 * (1 - 1 / windows.length));
        
        return {
          capabilityId,
          agentId,
          predictedFailureTime: predictedTime,
          confidence,
          basis: `Error rate trending at +${errorTrend.slope.toFixed(2)}%/window (R²=${errorTrend.r2.toFixed(3)}), ` +
                 `current ${currentErrorRate.toFixed(1)}%, threshold ${this.errorRateFailureThreshold}%`,
          recommendedAction: confidence > 0.7 ? 'reroute' : confidence > 0.4 ? 'throttle' : 'alert'
        };
      }
    }

    // Predict based on latency trend
    if (latencyTrend.slope > 0 && baselineLatency.mean > 0) {
      const currentLatency = windows[windows.length - 1].avgLatency;
      const failureLatency = baselineLatency.mean * this.latencyFailureMultiplier;
      const windowsToFailure = (failureLatency - currentLatency) / latencyTrend.slope;
      
      if (windowsToFailure > 0 && windowsToFailure < this.windowCount * 3) {
        const predictedTime = now + windowsToFailure * this.windowSizeMs;
        const confidence = Math.min(0.85, latencyTrend.r2 * 0.8);
        
        return {
          capabilityId,
          agentId,
          predictedFailureTime: predictedTime,
          confidence,
          basis: `Latency trending at +${latencyTrend.slope.toFixed(1)}ms/window (R²=${latencyTrend.r2.toFixed(3)}), ` +
                 `current ${currentLatency.toFixed(0)}ms, failure threshold ${failureLatency.toFixed(0)}ms`,
          recommendedAction: confidence > 0.6 ? 'throttle' : 'alert'
        };
      }
    }

    return null;
  }

  private linearRegression(x: number[], y: number[]): { slope: number; intercept: number; r2: number } {
    const n = x.length;
    if (n < 2) return { slope: 0, intercept: 0, r2: 0 };

    let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0, sumY2 = 0;
    for (let i = 0; i < n; i++) {
      sumX += x[i];
      sumY += y[i];
      sumXY += x[i] * y[i];
      sumX2 += x[i] * x[i];
      sumY2 += y[i] * y[i];
    }

    const denom = n * sumX2 - sumX * sumX;
    if (Math.abs(denom) < 1e-10) return { slope: 0, intercept: sumY / n, r2: 0 };

    const slope = (n * sumXY - sumX * sumY) / denom;
    const intercept = (sumY - slope * sumX) / n;

    // R² calculation
    const yMean = sumY / n;
    let ssRes = 0, ssTot = 0;
    for (let i = 0; i < n; i++) {
      const predicted = slope * x[i] + intercept;
      ssRes += (y[i] - predicted) ** 2;
      ssTot += (y[i] - yMean) ** 2;
    }
    const r2 = ssTot > 0 ? 1 - ssRes / ssTot : 0;

    return { slope, intercept, r2: Math.max(0, r2) };
  }
}

// ─── Capability Scorecard ─────────────────────────────────────────────────

/**
 * Composite health scoring with SLA compliance tracking.
 * Combines probe results, degradation signals, and failure predictions
 * into a single 0-1 health score per capability per agent.
 */
class CapabilityScorecard {
  private states: Map<string, CapabilityHealthState> = new Map();
  private readonly slaTargets: Map<string, SLATarget> = new Map();
  private readonly defaultSLA: SLATarget;
  private readonly weights: { availability: number; latency: number; errorRate: number; degradation: number };

  constructor(config: {
    defaultSLA?: SLATarget;
    weights?: { availability: number; latency: number; errorRate: number; degradation: number };
  } = {}) {
    this.defaultSLA = config.defaultSLA ?? {
      availabilityPercent: 99.5,
      maxLatencyP50Ms: 200,
      maxLatencyP99Ms: 1000,
      maxErrorRatePercent: 2,
      evaluationWindowMs: 3600000
    };
    this.weights = config.weights ?? { availability: 0.35, latency: 0.25, errorRate: 0.25, degradation: 0.15 };
  }

  setSLATarget(capabilityId: string, sla: SLATarget): void {
    this.slaTargets.set(capabilityId, sla);
  }

  evaluate(
    probe: CapabilityProbe,
    degradation: { level: DegradationLevel; signals: DegradationSignal[] },
    capabilityId: string,
    agentId: string
  ): CapabilityHealthState {
    const key = `${capabilityId}:${agentId}`;
    const sla = this.slaTargets.get(capabilityId) ?? this.defaultSLA;
    const now = Date.now();

    // Get or create state
    let state = this.states.get(key);
    if (!state) {
      state = {
        capabilityId,
        agentId,
        status: 'unknown',
        degradationLevel: 'none',
        score: 1.0,
        lastProbeResults: new Map(),
        consecutiveFailures: new Map(),
        consecutiveSuccesses: new Map(),
        slaCompliance: {
          availability: 100,
          latencyP50: 0,
          latencyP99: 0,
          errorRate: 0,
          withinSLA: true,
          violationCount: 0,
          lastEvaluated: now
        },
        lastStatusChange: now,
        createdAt: now
      };
      this.states.set(key, state);
    }

    // Compute metrics
    const availability = probe.computeAvailability(capabilityId, agentId, sla.evaluationWindowMs);
    const p50 = probe.computePercentileLatency(capabilityId, agentId, 50, sla.evaluationWindowMs);
    const p99 = probe.computePercentileLatency(capabilityId, agentId, 99, sla.evaluationWindowMs);
    const recentResults = probe.getRecentResults(capabilityId, agentId, sla.evaluationWindowMs);
    const errorRate = recentResults.length > 0
      ? (recentResults.filter(r => !r.success).length / recentResults.length) * 100
      : 0;

    // Score components (0-1, higher is better)
    const availabilityScore = Math.min(1, availability / (sla.availabilityPercent / 100));
    const latencyScore = sla.maxLatencyP50Ms > 0 
      ? Math.max(0, 1 - (p50 / sla.maxLatencyP50Ms - 1) * 0.5)
      : 1;
    const errorScore = sla.maxErrorRatePercent > 0
      ? Math.max(0, 1 - (errorRate / sla.maxErrorRatePercent - 1) * 0.3)
      : 1;
    
    const degradationPenalty: Record<DegradationLevel, number> = {
      none: 1.0, minor: 0.9, moderate: 0.7, severe: 0.4, critical: 0.1
    };
    const degradationScore = degradationPenalty[degradation.level];

    // Composite score
    const score = Math.max(0, Math.min(1,
      this.weights.availability * availabilityScore +
      this.weights.latency * Math.min(1, latencyScore) +
      this.weights.errorRate * Math.min(1, errorScore) +
      this.weights.degradation * degradationScore
    ));

    // Determine status
    const previousStatus = state.status;
    let newStatus: HealthStatus;
    if (score >= 0.9) newStatus = 'healthy';
    else if (score >= 0.6) newStatus = 'degraded';
    else if (score > 0) newStatus = 'unhealthy';
    else newStatus = 'unknown';

    // SLA compliance
    const withinSLA = 
      availability * 100 >= sla.availabilityPercent &&
      p50 <= sla.maxLatencyP50Ms &&
      p99 <= sla.maxLatencyP99Ms &&
      errorRate <= sla.maxErrorRatePercent;

    // Update state
    state.status = newStatus;
    state.degradationLevel = degradation.level;
    state.score = score;
    state.slaCompliance = {
      availability: availability * 100,
      latencyP50: p50,
      latencyP99: p99,
      errorRate,
      withinSLA,
      violationCount: withinSLA ? state.slaCompliance.violationCount : state.slaCompliance.violationCount + 1,
      lastEvaluated: now
    };

    if (newStatus !== previousStatus) {
      state.lastStatusChange = now;
    }

    return state;
  }

  getState(capabilityId: string, agentId: string): CapabilityHealthState | undefined {
    return this.states.get(`${capabilityId}:${agentId}`);
  }

  getAllStates(): CapabilityHealthState[] {
    return Array.from(this.states.values());
  }

  getAgentHealthSummary(agentId: string): {
    totalCapabilities: number;
    healthy: number;
    degraded: number;
    unhealthy: number;
    averageScore: number;
    slaViolations: number;
  } {
    const agentStates = Array.from(this.states.values()).filter(s => s.agentId === agentId);
    
    return {
      totalCapabilities: agentStates.length,
      healthy: agentStates.filter(s => s.status === 'healthy').length,
      degraded: agentStates.filter(s => s.status === 'degraded').length,
      unhealthy: agentStates.filter(s => s.status === 'unhealthy').length,
      averageScore: agentStates.length > 0
        ? agentStates.reduce((sum, s) => sum + s.score, 0) / agentStates.length
        : 1.0,
      slaViolations: agentStates.filter(s => !s.slaCompliance.withinSLA).length
    };
  }
}

// ─── Remediation Engine ──────────────────────────────────────────────────

/**
 * Automated remediation action selection based on degradation severity,
 * failure predictions, and configured escalation policies.
 * Includes cooldown to prevent remediation storms.
 */
class RemediationEngine {
  private readonly escalationPolicy: Map<DegradationLevel, RemediationAction>;
  private readonly cooldowns: Map<string, number> = new Map();
  private readonly cooldownMs: number;
  private readonly records: RemediationRecord[] = [];
  private readonly maxRecords: number;
  private nextId: number = 1;

  constructor(config: {
    escalationPolicy?: Map<DegradationLevel, RemediationAction>;
    cooldownMs?: number;
    maxRecords?: number;
  } = {}) {
    this.escalationPolicy = config.escalationPolicy ?? new Map([
      ['none', 'none' as RemediationAction],
      ['minor', 'alert' as RemediationAction],
      ['moderate', 'throttle' as RemediationAction],
      ['severe', 'reroute' as RemediationAction],
      ['critical', 'disable' as RemediationAction]
    ]);
    this.cooldownMs = config.cooldownMs ?? 60000;
    this.maxRecords = config.maxRecords ?? 500;
  }

  selectAction(
    state: CapabilityHealthState,
    prediction: FailurePrediction | null,
    degradation: { level: DegradationLevel; signals: DegradationSignal[] }
  ): { action: RemediationAction; reason: string } {
    const key = `${state.capabilityId}:${state.agentId}`;
    const now = Date.now();

    // Check cooldown
    const lastAction = this.cooldowns.get(key);
    if (lastAction && now - lastAction < this.cooldownMs) {
      return { action: 'none', reason: 'Cooldown active' };
    }

    // Priority 1: Predictive action (proactive)
    if (prediction && prediction.confidence > 0.6) {
      const timeToFailure = prediction.predictedFailureTime - now;
      if (timeToFailure < 600000) { // < 10 minutes to predicted failure
        this.cooldowns.set(key, now);
        return {
          action: prediction.recommendedAction,
          reason: `Predictive: ${prediction.basis}`
        };
      }
    }

    // Priority 2: Escalation policy based on degradation
    const action = this.escalationPolicy.get(degradation.level) ?? 'none';
    if (action !== 'none') {
      this.cooldowns.set(key, now);
      const signalSummary = degradation.signals.map(s => s.details).join('; ');
      return {
        action,
        reason: `Degradation ${degradation.level}: ${signalSummary}`
      };
    }

    // Priority 3: SLA violation override
    if (!state.slaCompliance.withinSLA && state.slaCompliance.violationCount >= 3) {
      this.cooldowns.set(key, now);
      return {
        action: 'throttle',
        reason: `SLA violated ${state.slaCompliance.violationCount} times consecutively`
      };
    }

    return { action: 'none', reason: 'No remediation needed' };
  }

  recordRemediation(
    capabilityId: string,
    agentId: string,
    action: RemediationAction,
    reason: string
  ): RemediationRecord {
    const record: RemediationRecord = {
      id: `rem-${this.nextId++}`,
      capabilityId,
      agentId,
      action,
      reason,
      timestamp: Date.now(),
      resolved: false
    };

    this.records.push(record);
    if (this.records.length > this.maxRecords) {
      this.records.splice(0, this.records.length - this.maxRecords);
    }

    return record;
  }

  resolveRemediation(id: string): boolean {
    const record = this.records.find(r => r.id === id);
    if (!record || record.resolved) return false;
    record.resolved = true;
    record.resolvedAt = Date.now();
    return true;
  }

  getActiveRemediations(capabilityId?: string, agentId?: string): RemediationRecord[] {
    return this.records.filter(r => {
      if (r.resolved) return false;
      if (capabilityId && r.capabilityId !== capabilityId) return false;
      if (agentId && r.agentId !== agentId) return false;
      return true;
    });
  }

  getRemediationStats(): {
    total: number;
    active: number;
    resolved: number;
    byAction: Record<string, number>;
    averageResolutionMs: number;
  } {
    const resolved = this.records.filter(r => r.resolved && r.resolvedAt);
    const byAction: Record<string, number> = {};
    
    for (const r of this.records) {
      byAction[r.action] = (byAction[r.action] ?? 0) + 1;
    }

    const avgResolution = resolved.length > 0
      ? resolved.reduce((sum, r) => sum + (r.resolvedAt! - r.timestamp), 0) / resolved.length
      : 0;

    return {
      total: this.records.length,
      active: this.records.filter(r => !r.resolved).length,
      resolved: resolved.length,
      byAction,
      averageResolutionMs: avgResolution
    };
  }
}

// ─── Health Federator ─────────────────────────────────────────────────────

/**
 * Cross-agent capability health aggregation. Maintains a federated
 * view of which agents can provide which capabilities and their
 * health status, enabling intelligent routing decisions.
 */
class HealthFederator {
  private capabilityIndex: Map<string, Map<string, {
    score: number;
    status: HealthStatus;
    lastUpdate: number;
    slaCompliant: boolean;
  }>> = new Map();

  private readonly staleThresholdMs: number;

  constructor(staleThresholdMs: number = 120000) {
    this.staleThresholdMs = staleThresholdMs;
  }

  updateHealth(capabilityId: string, agentId: string, state: CapabilityHealthState): void {
    if (!this.capabilityIndex.has(capabilityId)) {
      this.capabilityIndex.set(capabilityId, new Map());
    }
    
    this.capabilityIndex.get(capabilityId)!.set(agentId, {
      score: state.score,
      status: state.status,
      lastUpdate: Date.now(),
      slaCompliant: state.slaCompliance.withinSLA
    });
  }

  getHealthyProviders(capabilityId: string, minScore: number = 0.6): string[] {
    const providers = this.capabilityIndex.get(capabilityId);
    if (!providers) return [];

    const now = Date.now();
    return Array.from(providers.entries())
      .filter(([_, info]) => {
        if (now - info.lastUpdate > this.staleThresholdMs) return false;
        return info.score >= minScore && info.status !== 'unhealthy';
      })
      .sort((a, b) => b[1].score - a[1].score)
      .map(([agentId]) => agentId);
  }

  getBestProvider(capabilityId: string): string | null {
    const healthy = this.getHealthyProviders(capabilityId);
    return healthy.length > 0 ? healthy[0] : null;
  }

  getCapabilityAvailability(capabilityId: string): {
    totalProviders: number;
    healthyProviders: number;
    averageScore: number;
    anyAvailable: boolean;
  } {
    const providers = this.capabilityIndex.get(capabilityId);
    if (!providers || providers.size === 0) {
      return { totalProviders: 0, healthyProviders: 0, averageScore: 0, anyAvailable: false };
    }

    const now = Date.now();
    const active = Array.from(providers.values())
      .filter(p => now - p.lastUpdate <= this.staleThresholdMs);
    
    const healthy = active.filter(p => p.status === 'healthy' || p.status === 'degraded');
    const avgScore = active.length > 0
      ? active.reduce((sum, p) => sum + p.score, 0) / active.length
      : 0;

    return {
      totalProviders: active.length,
      healthyProviders: healthy.length,
      averageScore: avgScore,
      anyAvailable: healthy.length > 0
    };
  }

  getNetworkHealthReport(): {
    totalCapabilities: number;
    totalProviders: number;
    atRiskCapabilities: string[];
    healthDistribution: Record<HealthStatus, number>;
  } {
    const now = Date.now();
    const atRisk: string[] = [];
    const distribution: Record<HealthStatus, number> = { healthy: 0, degraded: 0, unhealthy: 0, unknown: 0 };
    let totalProviders = 0;

    for (const [capId, providers] of this.capabilityIndex) {
      const active = Array.from(providers.values())
        .filter(p => now - p.lastUpdate <= this.staleThresholdMs);
      
      totalProviders += active.length;
      
      for (const p of active) {
        distribution[p.status]++;
      }

      const healthy = active.filter(p => p.status === 'healthy' || p.status === 'degraded');
      if (healthy.length <= 1 && active.length > 0) {
        atRisk.push(capId);
      }
    }

    return {
      totalCapabilities: this.capabilityIndex.size,
      totalProviders,
      atRiskCapabilities: atRisk,
      healthDistribution: distribution
    };
  }

  pruneStale(): number {
    const now = Date.now();
    let pruned = 0;

    for (const [capId, providers] of this.capabilityIndex) {
      for (const [agentId, info] of providers) {
        if (now - info.lastUpdate > this.staleThresholdMs * 3) {
          providers.delete(agentId);
          pruned++;
        }
      }
      if (providers.size === 0) {
        this.capabilityIndex.delete(capId);
      }
    }

    return pruned;
  }
}

// ─── Capability Health Monitor (Orchestrator) ────────────────────────────

/**
 * Unified orchestrator combining all health monitoring components.
 * Provides a single entry point for probe recording, health evaluation,
 * failure prediction, and remediation with federated health views.
 */
class CapabilityHealthMonitor {
  private readonly probe: CapabilityProbe;
  private readonly degradationDetector: DegradationDetector;
  private readonly failurePredictor: FailurePredictor;
  private readonly scorecard: CapabilityScorecard;
  private readonly remediation: RemediationEngine;
  private readonly federator: HealthFederator;
  private readonly listeners: ((event: HealthEvent) => void)[] = [];

  constructor(config: {
    probeDefaults?: Record<ProbeType, ProbeConfig>;
    degradation?: ConstructorParameters<typeof DegradationDetector>[0];
    predictor?: ConstructorParameters<typeof FailurePredictor>[0];
    scorecard?: ConstructorParameters<typeof CapabilityScorecard>[0];
    remediation?: ConstructorParameters<typeof RemediationEngine>[0];
    staleThresholdMs?: number;
  } = {}) {
    const defaultProbes: Record<ProbeType, ProbeConfig> = config.probeDefaults ?? {
      liveness: { type: 'liveness', intervalMs: 10000, timeoutMs: 5000, failureThreshold: 3, successThreshold: 1, enabled: true },
      readiness: { type: 'readiness', intervalMs: 15000, timeoutMs: 5000, failureThreshold: 2, successThreshold: 2, enabled: true },
      performance: { type: 'performance', intervalMs: 30000, timeoutMs: 10000, failureThreshold: 5, successThreshold: 3, enabled: true },
      saturation: { type: 'saturation', intervalMs: 60000, timeoutMs: 10000, failureThreshold: 3, successThreshold: 2, enabled: true }
    };

    this.probe = new CapabilityProbe(defaultProbes);
    this.degradationDetector = new DegradationDetector(config.degradation);
    this.failurePredictor = new FailurePredictor(config.predictor);
    this.scorecard = new CapabilityScorecard(config.scorecard);
    this.remediation = new RemediationEngine(config.remediation);
    this.federator = new HealthFederator(config.staleThresholdMs);
  }

  on(listener: (event: HealthEvent) => void): void {
    this.listeners.push(listener);
  }

  private emit(event: HealthEvent): void {
    for (const listener of this.listeners) {
      try { listener(event); } catch {}
    }
  }

  /**
   * Record a probe result and run the full evaluation pipeline:
   * probe → degradation detection → failure prediction → scorecard → remediation → federation
   */
  recordProbe(result: ProbeResult): {
    state: CapabilityHealthState;
    prediction: FailurePrediction | null;
    remediation: { action: RemediationAction; reason: string };
  } {
    // 1. Record probe
    this.probe.recordResult(result);
    this.emit({
      type: 'probe_result',
      timestamp: Date.now(),
      data: { ...result }
    });

    // 2. Detect degradation
    const degradation = this.degradationDetector.detect(
      this.probe, result.capabilityId, result.agentId
    );

    if (degradation.signals.length > 0) {
      this.emit({
        type: 'degradation_detected',
        timestamp: Date.now(),
        data: { 
          capabilityId: result.capabilityId, 
          agentId: result.agentId,
          level: degradation.level,
          signalCount: degradation.signals.length
        }
      });
    }

    // 3. Predict failures
    const prediction = this.failurePredictor.predict(
      this.probe, result.capabilityId, result.agentId
    );

    if (prediction) {
      this.emit({
        type: 'failure_predicted',
        timestamp: Date.now(),
        data: { ...prediction }
      });
    }

    // 4. Evaluate scorecard
    const previousState = this.scorecard.getState(result.capabilityId, result.agentId);
    const state = this.scorecard.evaluate(
      this.probe, degradation, result.capabilityId, result.agentId
    );

    if (previousState && previousState.status !== state.status) {
      this.emit({
        type: 'status_change',
        timestamp: Date.now(),
        data: {
          capabilityId: result.capabilityId,
          agentId: result.agentId,
          previousStatus: previousState.status,
          newStatus: state.status,
          score: state.score
        }
      });

      if (previousState.status === 'unhealthy' && 
          (state.status === 'healthy' || state.status === 'degraded')) {
        this.emit({
          type: 'capability_recovered',
          timestamp: Date.now(),
          data: { capabilityId: result.capabilityId, agentId: result.agentId }
        });
      }
    }

    if (!state.slaCompliance.withinSLA) {
      this.emit({
        type: 'sla_violation',
        timestamp: Date.now(),
        data: {
          capabilityId: result.capabilityId,
          agentId: result.agentId,
          compliance: { ...state.slaCompliance }
        }
      });
    }

    // 5. Select remediation
    const remediationResult = this.remediation.selectAction(state, prediction, degradation);

    if (remediationResult.action !== 'none') {
      this.remediation.recordRemediation(
        result.capabilityId, result.agentId,
        remediationResult.action, remediationResult.reason
      );
      this.emit({
        type: 'remediation_applied',
        timestamp: Date.now(),
        data: {
          capabilityId: result.capabilityId,
          agentId: result.agentId,
          action: remediationResult.action,
          reason: remediationResult.reason
        }
      });
    }

    // 6. Update federation view
    this.federator.updateHealth(result.capabilityId, result.agentId, state);

    return { state, prediction, remediation: remediationResult };
  }

  // ─── Query Methods ──────────────────────────────────────────────

  getCapabilityHealth(capabilityId: string, agentId: string): CapabilityHealthState | undefined {
    return this.scorecard.getState(capabilityId, agentId);
  }

  getAgentSummary(agentId: string): ReturnType<CapabilityScorecard['getAgentHealthSummary']> {
    return this.scorecard.getAgentHealthSummary(agentId);
  }

  getHealthyProviders(capabilityId: string, minScore?: number): string[] {
    return this.federator.getHealthyProviders(capabilityId, minScore);
  }

  getBestProvider(capabilityId: string): string | null {
    return this.federator.getBestProvider(capabilityId);
  }

  getNetworkReport(): ReturnType<HealthFederator['getNetworkHealthReport']> {
    return this.federator.getNetworkHealthReport();
  }

  getRemediationStats(): ReturnType<RemediationEngine['getRemediationStats']> {
    return this.remediation.getRemediationStats();
  }

  getActiveRemediations(capabilityId?: string, agentId?: string): RemediationRecord[] {
    return this.remediation.getActiveRemediations(capabilityId, agentId);
  }

  pruneStaleData(): { stalePruned: number } {
    return { stalePruned: this.federator.pruneStale() };
  }

  generateHealthReport(): {
    network: ReturnType<HealthFederator['getNetworkHealthReport']>;
    remediations: ReturnType<RemediationEngine['getRemediationStats']>;
    allStates: CapabilityHealthState[];
    timestamp: number;
  } {
    const report = {
      network: this.federator.getNetworkHealthReport(),
      remediations: this.remediation.getRemediationStats(),
      allStates: this.scorecard.getAllStates(),
      timestamp: Date.now()
    };

    this.emit({
      type: 'health_report',
      timestamp: Date.now(),
      data: {
        totalCapabilities: report.network.totalCapabilities,
        totalProviders: report.network.totalProviders,
        atRiskCount: report.network.atRiskCapabilities.length,
        activeRemediations: report.remediations.active
      }
    });

    return report;
  }
}

// ─── Presets ──────────────────────────────────────────────────────────────

const PRESETS = {
  'real-time-api': {
    probeDefaults: {
      liveness: { type: 'liveness' as ProbeType, intervalMs: 5000, timeoutMs: 2000, failureThreshold: 2, successThreshold: 1, enabled: true },
      readiness: { type: 'readiness' as ProbeType, intervalMs: 10000, timeoutMs: 3000, failureThreshold: 2, successThreshold: 1, enabled: true },
      performance: { type: 'performance' as ProbeType, intervalMs: 15000, timeoutMs: 5000, failureThreshold: 3, successThreshold: 2, enabled: true },
      saturation: { type: 'saturation' as ProbeType, intervalMs: 30000, timeoutMs: 5000, failureThreshold: 2, successThreshold: 2, enabled: true }
    },
    degradation: {
      detectionWindowMs: 120000,
      latencyMultipliers: { minor: 1.3, moderate: 2.0, severe: 3.0, critical: 5.0 },
      debounceMs: 15000
    },
    scorecard: {
      defaultSLA: {
        availabilityPercent: 99.9,
        maxLatencyP50Ms: 100,
        maxLatencyP99Ms: 500,
        maxErrorRatePercent: 0.5,
        evaluationWindowMs: 1800000
      }
    }
  },

  'batch-processing': {
    probeDefaults: {
      liveness: { type: 'liveness' as ProbeType, intervalMs: 30000, timeoutMs: 10000, failureThreshold: 5, successThreshold: 1, enabled: true },
      readiness: { type: 'readiness' as ProbeType, intervalMs: 60000, timeoutMs: 15000, failureThreshold: 3, successThreshold: 2, enabled: true },
      performance: { type: 'performance' as ProbeType, intervalMs: 120000, timeoutMs: 30000, failureThreshold: 5, successThreshold: 3, enabled: true },
      saturation: { type: 'saturation' as ProbeType, intervalMs: 120000, timeoutMs: 15000, failureThreshold: 3, successThreshold: 2, enabled: true }
    },
    degradation: {
      detectionWindowMs: 600000,
      latencyMultipliers: { minor: 2.0, moderate: 4.0, severe: 8.0, critical: 15.0 },
      debounceMs: 120000
    },
    scorecard: {
      defaultSLA: {
        availabilityPercent: 99.0,
        maxLatencyP50Ms: 5000,
        maxLatencyP99Ms: 30000,
        maxErrorRatePercent: 5,
        evaluationWindowMs: 7200000
      }
    }
  },

  'agent-mesh': {
    probeDefaults: {
      liveness: { type: 'liveness' as ProbeType, intervalMs: 15000, timeoutMs: 5000, failureThreshold: 3, successThreshold: 1, enabled: true },
      readiness: { type: 'readiness' as ProbeType, intervalMs: 20000, timeoutMs: 8000, failureThreshold: 3, successThreshold: 2, enabled: true },
      performance: { type: 'performance' as ProbeType, intervalMs: 30000, timeoutMs: 10000, failureThreshold: 4, successThreshold: 2, enabled: true },
      saturation: { type: 'saturation' as ProbeType, intervalMs: 60000, timeoutMs: 10000, failureThreshold: 3, successThreshold: 2, enabled: true }
    },
    degradation: {
      detectionWindowMs: 300000,
      latencyMultipliers: { minor: 1.5, moderate: 2.5, severe: 4.0, critical: 8.0 },
      debounceMs: 30000
    },
    scorecard: {
      defaultSLA: {
        availabilityPercent: 99.5,
        maxLatencyP50Ms: 200,
        maxLatencyP99Ms: 1000,
        maxErrorRatePercent: 2,
        evaluationWindowMs: 3600000
      }
    }
  }
};

export {
  CapabilityProbe,
  DegradationDetector,
  FailurePredictor,
  CapabilityScorecard,
  RemediationEngine,
  HealthFederator,
  CapabilityHealthMonitor,
  PRESETS,
  // Types
  ProbeType,
  HealthStatus,
  DegradationLevel,
  RemediationAction,
  ProbeConfig,
  ProbeResult,
  CapabilityHealthState,
  SLATarget,
  SLACompliance,
  DegradationSignal,
  FailurePrediction,
  RemediationRecord,
  HealthEvent
};
