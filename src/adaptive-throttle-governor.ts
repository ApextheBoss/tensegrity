import { fnv1aHash, WelfordStats, EWMATracker } from './shared-utils';
/**
 * Adaptive Throttle Governor for Agent Networks
 * 
 * Dynamic rate control that adjusts throughput based on downstream health signals,
 * queue depth, and latency trends. Unlike static rate limiters, this governor
 * continuously adapts its limits using AIMD (Additive Increase, Multiplicative
 * Decrease) combined with gradient-based optimization.
 * 
 * Key algorithms:
 * - AIMD with configurable increase/decrease factors
 * - Vegas-style latency-based congestion detection (RTT gradient)
 * - CoDel-inspired sojourn time queue management
 * - Proportional-Integral (PI) controller for steady-state convergence
 * - Multi-tenant fairness with weighted max-min allocation
 * - Coordinated throttling across agent clusters via gossip
 */

// ─── Types ───────────────────────────────────────────────────────────────────

interface ThrottleConfig {
  initialRate: number;          // Starting requests/second
  minRate: number;              // Floor rate (never go below)
  maxRate: number;              // Ceiling rate
  aimdIncrease: number;         // Additive increase per interval
  aimdDecrease: number;         // Multiplicative decrease factor (0-1)
  targetLatencyMs: number;      // Desired p95 latency
  maxQueueDepth: number;        // CoDel target queue depth
  sojournTargetMs: number;      // CoDel sojourn time target
  piKp: number;                 // Proportional gain
  piKi: number;                 // Integral gain
  updateIntervalMs: number;     // Control loop interval
  warmupRequests: number;       // Requests before adapting
  historyWindowSize: number;    // Sliding window for metrics
}

interface TenantConfig {
  id: string;
  weight: number;               // Relative share (higher = more)
  minGuaranteedRate: number;    // Floor allocation
  maxBurstRate: number;         // Ceiling allocation
  priority: number;             // 0=highest
}

interface LatencySample {
  timestamp: number;
  durationMs: number;
  success: boolean;
  tenantId: string;
}

interface QueueMetrics {
  depth: number;
  oldestEntryAgeMs: number;
  enqueueRate: number;
  dequeueRate: number;
}

interface ThrottleState {
  currentRate: number;
  effectiveRate: number;
  congestionLevel: number;      // 0-1
  mode: 'warmup' | 'probing' | 'steady' | 'backoff' | 'recovery';
  tenantAllocations: Map<string, number>;
}

interface ThrottleEvent {
  type: 'rate-adjusted' | 'congestion-detected' | 'backoff-triggered' |
        'recovery-started' | 'tenant-throttled' | 'tenant-unthrottled' |
        'queue-pressure' | 'latency-spike' | 'fairness-rebalance' |
        'coordinated-update';
  timestamp: number;
  data: Record<string, unknown>;
}

// ─── FNV-1a Hash ─────────────────────────────────────────────────────────────

// WelfordStats imported from shared-utils

// ─── Sliding Window ──────────────────────────────────────────────────────────

class SlidingWindow<T> {
  private items: T[] = [];
  private readonly maxSize: number;

  constructor(maxSize: number) {
    this.maxSize = maxSize;
  }

  push(item: T): void {
    this.items.push(item);
    if (this.items.length > this.maxSize) {
      this.items.shift();
    }
  }

  getAll(): T[] { return [...this.items]; }
  get length(): number { return this.items.length; }
  clear(): void { this.items = []; }
}

// EWMATracker imported from shared-utils

// ─── Latency Gradient Detector (Vegas-style) ─────────────────────────────────

class LatencyGradientDetector {
  private baselineRtt: number | null = null;
  private readonly samples: SlidingWindow<number>;
  private readonly ewma: EWMATracker;
  private readonly baselineAlpha: number;

  constructor(windowSize: number = 50, baselineAlpha: number = 0.01) {
    this.samples = new SlidingWindow(windowSize);
    this.ewma = new EWMATracker(0.2);
    this.baselineAlpha = baselineAlpha;
  }

  addSample(latencyMs: number): {
    congestionSignal: number;   // 0 = no congestion, 1 = severe
    gradient: number;           // Rate of change
    baselineRtt: number;
    currentRtt: number;
  } {
    this.samples.push(latencyMs);
    const smoothed = this.ewma.update(latencyMs);

    // Track minimum RTT as baseline (like TCP Vegas)
    if (this.baselineRtt === null || latencyMs < this.baselineRtt) {
      this.baselineRtt = latencyMs;
    } else {
      // Slowly inflate baseline to account for legitimate changes
      this.baselineRtt = this.baselineRtt * (1 + this.baselineAlpha);
    }

    // Vegas-style: congestion = (current - baseline) / baseline
    const diff = smoothed - this.baselineRtt;
    const congestionSignal = Math.max(0, Math.min(1, diff / Math.max(this.baselineRtt, 1)));

    // Compute gradient from recent samples
    const allSamples = this.samples.getAll();
    let gradient = 0;
    if (allSamples.length >= 5) {
      const recent = allSamples.slice(-5);
      const older = allSamples.slice(-10, -5);
      if (older.length > 0) {
        const recentAvg = recent.reduce((a, b) => a + b, 0) / recent.length;
        const olderAvg = older.reduce((a, b) => a + b, 0) / older.length;
        gradient = (recentAvg - olderAvg) / Math.max(olderAvg, 1);
      }
    }

    return {
      congestionSignal,
      gradient,
      baselineRtt: this.baselineRtt,
      currentRtt: smoothed
    };
  }

  reset(): void {
    this.baselineRtt = null;
    this.samples.clear();
    this.ewma.reset();
  }
}

// ─── CoDel-Inspired Queue Manager ───────────────────────────────────────────

class CoDelQueueManager {
  private readonly targetMs: number;
  private readonly intervalMs: number;
  private firstAboveTime: number | null = null;
  private dropNext: number = 0;
  private dropCount: number = 0;
  private dropping: boolean = false;

  constructor(targetMs: number = 5, intervalMs: number = 100) {
    this.targetMs = targetMs;
    this.intervalMs = intervalMs;
  }

  /**
   * Evaluate whether queue pressure requires action.
   * Returns drop probability (0-1) and whether we're in dropping state.
   */
  evaluate(sojournTimeMs: number, now: number): {
    shouldDrop: boolean;
    dropProbability: number;
    dropping: boolean;
  } {
    if (sojournTimeMs < this.targetMs) {
      this.firstAboveTime = null;
      this.dropping = false;
      this.dropCount = 0;
      return { shouldDrop: false, dropProbability: 0, dropping: false };
    }

    if (this.firstAboveTime === null) {
      this.firstAboveTime = now + this.intervalMs;
      return { shouldDrop: false, dropProbability: 0, dropping: false };
    }

    if (!this.dropping) {
      if (now >= this.firstAboveTime) {
        this.dropping = true;
        this.dropCount = 1;
        // Schedule next drop using inverse-sqrt spacing
        this.dropNext = now + this.intervalMs / Math.sqrt(this.dropCount);
        return { shouldDrop: true, dropProbability: 0.5, dropping: true };
      }
      return { shouldDrop: false, dropProbability: 0.2, dropping: false };
    }

    // In dropping state
    if (now >= this.dropNext) {
      this.dropCount++;
      this.dropNext = now + this.intervalMs / Math.sqrt(this.dropCount);
      const prob = Math.min(1, this.dropCount / 10);
      return { shouldDrop: true, dropProbability: prob, dropping: true };
    }

    return { shouldDrop: false, dropProbability: 0.3, dropping: true };
  }

  reset(): void {
    this.firstAboveTime = null;
    this.dropNext = 0;
    this.dropCount = 0;
    this.dropping = false;
  }
}

// ─── PI Controller ───────────────────────────────────────────────────────────

class PIController {
  private integral: number = 0;
  private lastError: number = 0;
  private readonly kp: number;
  private readonly ki: number;
  private readonly integralClamp: number;

  constructor(kp: number, ki: number, integralClamp: number = 100) {
    this.kp = kp;
    this.ki = ki;
    this.integralClamp = integralClamp;
  }

  /**
   * Compute control output given setpoint and measured value.
   * Returns adjustment to apply to rate.
   */
  compute(setpoint: number, measured: number): number {
    const error = setpoint - measured;

    // Anti-windup: clamp integral
    this.integral = Math.max(
      -this.integralClamp,
      Math.min(this.integralClamp, this.integral + error)
    );

    const output = this.kp * error + this.ki * this.integral;
    this.lastError = error;
    return output;
  }

  reset(): void {
    this.integral = 0;
    this.lastError = 0;
  }
}

// ─── AIMD Rate Controller ────────────────────────────────────────────────────

class AIMDController {
  private rate: number;
  private readonly minRate: number;
  private readonly maxRate: number;
  private readonly increase: number;
  private readonly decreaseFactor: number;
  private consecutiveIncreases: number = 0;
  private lastDecreaseTime: number = 0;
  private readonly decreaseCooldownMs: number;

  constructor(
    initialRate: number,
    minRate: number,
    maxRate: number,
    increase: number = 1,
    decreaseFactor: number = 0.5,
    decreaseCooldownMs: number = 1000
  ) {
    this.rate = initialRate;
    this.minRate = minRate;
    this.maxRate = maxRate;
    this.increase = increase;
    this.decreaseFactor = decreaseFactor;
    this.decreaseCooldownMs = decreaseCooldownMs;
  }

  /**
   * Additive increase — steady ramp-up.
   */
  additiveIncrease(): number {
    this.consecutiveIncreases++;
    // Slow start: double rate when consecutive increases are high
    const factor = this.consecutiveIncreases > 10 ? 2 : 1;
    this.rate = Math.min(this.maxRate, this.rate + this.increase * factor);
    return this.rate;
  }

  /**
   * Multiplicative decrease — sharp cutback on congestion.
   */
  multiplicativeDecrease(now: number): number {
    if (now - this.lastDecreaseTime < this.decreaseCooldownMs) {
      return this.rate; // Cooldown active
    }
    this.consecutiveIncreases = 0;
    this.lastDecreaseTime = now;
    this.rate = Math.max(this.minRate, this.rate * this.decreaseFactor);
    return this.rate;
  }

  get currentRate(): number { return this.rate; }

  setRate(rate: number): void {
    this.rate = Math.max(this.minRate, Math.min(this.maxRate, rate));
  }
}

// ─── Tenant Fair Share Allocator ─────────────────────────────────────────────

class TenantFairShareAllocator {
  private tenants: Map<string, TenantConfig> = new Map();
  private allocations: Map<string, number> = new Map();
  private usage: Map<string, EWMATracker> = new Map();

  addTenant(config: TenantConfig): void {
    this.tenants.set(config.id, config);
    this.usage.set(config.id, new EWMATracker(0.3));
  }

  removeTenant(id: string): void {
    this.tenants.delete(id);
    this.allocations.delete(id);
    this.usage.delete(id);
  }

  /**
   * Weighted max-min fair allocation.
   * Guarantees minimums, distributes surplus by weight, caps at max burst.
   */
  allocate(totalRate: number): Map<string, number> {
    const tenantList = Array.from(this.tenants.values());
    if (tenantList.length === 0) return new Map();

    // Sort by priority (lower = higher priority)
    tenantList.sort((a, b) => a.priority - b.priority);

    const totalWeight = tenantList.reduce((sum, t) => sum + t.weight, 0);
    const allocs = new Map<string, number>();

    // Phase 1: Guarantee minimums
    let remaining = totalRate;
    for (const tenant of tenantList) {
      const minAlloc = Math.min(tenant.minGuaranteedRate, remaining);
      allocs.set(tenant.id, minAlloc);
      remaining -= minAlloc;
    }

    // Phase 2: Distribute surplus proportionally by weight
    if (remaining > 0) {
      // Iterative: some tenants may hit their max burst
      let saturated = new Set<string>();
      let iterations = 0;

      while (remaining > 0.01 && saturated.size < tenantList.length && iterations < 10) {
        iterations++;
        const activeWeight = tenantList
          .filter(t => !saturated.has(t.id))
          .reduce((sum, t) => sum + t.weight, 0);

        if (activeWeight <= 0) break;

        let distributed = 0;
        for (const tenant of tenantList) {
          if (saturated.has(tenant.id)) continue;

          const share = (tenant.weight / activeWeight) * remaining;
          const current = allocs.get(tenant.id) || 0;
          const maxAdd = tenant.maxBurstRate - current;

          if (share >= maxAdd) {
            allocs.set(tenant.id, current + maxAdd);
            distributed += maxAdd;
            saturated.add(tenant.id);
          } else {
            allocs.set(tenant.id, current + share);
            distributed += share;
          }
        }
        remaining -= distributed;
      }
    }

    // Phase 3: Check for unused allocation from inactive tenants
    // Redistribute from tenants using < 50% of allocation
    const redistributable: { id: string; excess: number }[] = [];
    for (const [id, alloc] of Array.from(allocs)) {
      const tracker = this.usage.get(id);
      if (tracker && tracker.current < alloc * 0.5) {
        const excess = alloc - tracker.current * 1.2; // Keep 20% headroom
        if (excess > 0) {
          redistributable.push({ id, excess: excess * 0.5 }); // Only reclaim half
        }
      }
    }

    if (redistributable.length > 0) {
      const totalExcess = redistributable.reduce((s, r) => s + r.excess, 0);
      const activeNeeders = tenantList.filter(t => {
        const tracker = this.usage.get(t.id);
        const alloc = allocs.get(t.id) || 0;
        return tracker && tracker.current > alloc * 0.8;
      });

      if (activeNeeders.length > 0) {
        const perNeeder = totalExcess / activeNeeders.length;
        for (const needer of activeNeeders) {
          const current = allocs.get(needer.id) || 0;
          allocs.set(needer.id, Math.min(needer.maxBurstRate, current + perNeeder));
        }
        for (const { id, excess } of redistributable) {
          allocs.set(id, (allocs.get(id) || 0) - excess);
        }
      }
    }

    this.allocations = allocs;
    return new Map(allocs);
  }

  recordUsage(tenantId: string, rate: number): void {
    this.usage.get(tenantId)?.update(rate);
  }

  getAllocations(): Map<string, number> { return new Map(this.allocations); }
}

// ─── Coordinated Throttle Gossip ─────────────────────────────────────────────

interface GossipState {
  nodeId: string;
  rate: number;
  congestionLevel: number;
  timestamp: number;
}

class CoordinatedThrottleGossip {
  private peers: Map<string, GossipState> = new Map();
  private readonly nodeId: string;
  private readonly stalePeerMs: number;

  constructor(nodeId: string, stalePeerMs: number = 30000) {
    this.nodeId = nodeId;
    this.stalePeerMs = stalePeerMs;
  }

  /**
   * Generate local state for gossip dissemination.
   */
  getLocalState(rate: number, congestionLevel: number): GossipState {
    return {
      nodeId: this.nodeId,
      rate,
      congestionLevel,
      timestamp: Date.now()
    };
  }

  /**
   * Receive peer state updates.
   */
  receivePeerState(state: GossipState): void {
    if (state.nodeId === this.nodeId) return;
    const existing = this.peers.get(state.nodeId);
    if (!existing || state.timestamp > existing.timestamp) {
      this.peers.set(state.nodeId, state);
    }
  }

  /**
   * Get recommended rate adjustment based on peer states.
   * If peers are heavily congested, preemptively reduce local rate.
   */
  getCoordinatedAdjustment(): {
    recommendedMultiplier: number;
    peerCongestion: number;
    activePeers: number;
  } {
    const now = Date.now();
    // Prune stale peers
    for (const [id, state] of Array.from(this.peers)) {
      if (now - state.timestamp > this.stalePeerMs) {
        this.peers.delete(id);
      }
    }

    const peers = Array.from(this.peers.values());
    if (peers.length === 0) {
      return { recommendedMultiplier: 1.0, peerCongestion: 0, activePeers: 0 };
    }

    // Weighted average congestion of peers
    const avgCongestion = peers.reduce((s, p) => s + p.congestionLevel, 0) / peers.length;

    // If peers are congested, reduce our rate proportionally
    // This prevents thundering herd when shared downstream is overloaded
    const multiplier = avgCongestion > 0.5
      ? 1.0 - (avgCongestion - 0.5) * 0.6  // Linear reduction: 0.5 cong → 1.0x, 1.0 cong → 0.7x
      : 1.0;

    return {
      recommendedMultiplier: Math.max(0.3, multiplier),
      peerCongestion: avgCongestion,
      activePeers: peers.length
    };
  }
}

// ─── Backoff Scheduler ───────────────────────────────────────────────────────

class BackoffScheduler {
  private attempt: number = 0;
  private readonly baseMs: number;
  private readonly maxMs: number;
  private readonly jitterFactor: number;
  private seed: number;

  constructor(baseMs: number = 1000, maxMs: number = 60000, jitterFactor: number = 0.25) {
    this.baseMs = baseMs;
    this.maxMs = maxMs;
    this.jitterFactor = jitterFactor;
    this.seed = fnv1aHash(`backoff-${Date.now()}`);
  }

  nextDelay(): number {
    const exponential = Math.min(this.maxMs, this.baseMs * Math.pow(2, this.attempt));
    // Decorrelated jitter
    this.seed = (this.seed * 1103515245 + 12345) >>> 0;
    const jitter = (this.seed / 0xffffffff) * 2 - 1; // -1 to 1
    const delay = exponential * (1 + jitter * this.jitterFactor);
    this.attempt++;
    return Math.max(this.baseMs, Math.min(this.maxMs, delay));
  }

  reset(): void { this.attempt = 0; }
  get attempts(): number { return this.attempt; }
}

// ─── Adaptive Throttle Governor ──────────────────────────────────────────────

class AdaptiveThrottleGovernor {
  private readonly config: ThrottleConfig;
  private readonly aimd: AIMDController;
  private readonly latencyDetector: LatencyGradientDetector;
  private readonly queueManager: CoDelQueueManager;
  private readonly piController: PIController;
  private readonly tenantAllocator: TenantFairShareAllocator;
  private readonly gossip: CoordinatedThrottleGossip;
  private readonly backoff: BackoffScheduler;
  private readonly latencyWindow: SlidingWindow<LatencySample>;
  private readonly events: ThrottleEvent[] = [];

  private mode: ThrottleState['mode'] = 'warmup';
  private requestCount: number = 0;
  private lastUpdateTime: number = 0;
  private congestionLevel: number = 0;
  private effectiveRate: number;
  private consecutiveBackoffs: number = 0;

  constructor(config: ThrottleConfig, nodeId: string = 'default') {
    this.config = config;
    this.effectiveRate = config.initialRate;

    this.aimd = new AIMDController(
      config.initialRate,
      config.minRate,
      config.maxRate,
      config.aimdIncrease,
      config.aimdDecrease
    );

    this.latencyDetector = new LatencyGradientDetector(config.historyWindowSize);
    this.queueManager = new CoDelQueueManager(config.sojournTargetMs);
    this.piController = new PIController(config.piKp, config.piKi);
    this.tenantAllocator = new TenantFairShareAllocator();
    this.gossip = new CoordinatedThrottleGossip(nodeId);
    this.backoff = new BackoffScheduler();
    this.latencyWindow = new SlidingWindow(config.historyWindowSize);
  }

  /**
   * Record a completed request for adaptation.
   */
  recordRequest(sample: LatencySample): void {
    this.requestCount++;
    this.latencyWindow.push(sample);

    // Update per-tenant usage
    this.tenantAllocator.recordUsage(sample.tenantId, 1);

    // Feed latency detector
    const latencySignal = this.latencyDetector.addSample(sample.durationMs);

    // Check if we should update rate
    const now = sample.timestamp;
    if (now - this.lastUpdateTime >= this.config.updateIntervalMs) {
      this.updateRate(now, latencySignal);
      this.lastUpdateTime = now;
    }
  }

  /**
   * Core control loop: combine signals and adjust rate.
   */
  private updateRate(
    now: number,
    latencySignal: { congestionSignal: number; gradient: number; baselineRtt: number; currentRtt: number }
  ): void {
    // Phase check: are we still warming up?
    if (this.mode === 'warmup' && this.requestCount < this.config.warmupRequests) {
      return;
    }
    if (this.mode === 'warmup') {
      this.mode = 'probing';
      this.emit({
        type: 'rate-adjusted',
        timestamp: now,
        data: { mode: 'probing', reason: 'warmup-complete', rate: this.effectiveRate }
      });
    }

    // Compute queue pressure
    const recentSamples = this.latencyWindow.getAll();
    const queuePressure = this.computeQueuePressure(recentSamples, now);

    // Compute error rate
    const errorRate = this.computeErrorRate(recentSamples);

    // Combine signals into congestion score
    this.congestionLevel = this.computeCongestionLevel(
      latencySignal.congestionSignal,
      latencySignal.gradient,
      queuePressure,
      errorRate
    );

    // Get coordinated adjustment from peers
    const { recommendedMultiplier, peerCongestion } = this.gossip.getCoordinatedAdjustment();

    // Decision: increase or decrease?
    if (this.congestionLevel > 0.7 || errorRate > 0.1) {
      // Backoff
      this.aimd.multiplicativeDecrease(now);
      this.consecutiveBackoffs++;
      this.mode = 'backoff';

      this.emit({
        type: 'backoff-triggered',
        timestamp: now,
        data: {
          congestion: this.congestionLevel,
          errorRate,
          latencySignal: latencySignal.congestionSignal,
          gradient: latencySignal.gradient,
          newRate: this.aimd.currentRate,
          consecutiveBackoffs: this.consecutiveBackoffs
        }
      });

      if (errorRate > 0.1) {
        this.emit({
          type: 'congestion-detected',
          timestamp: now,
          data: { errorRate, congestionLevel: this.congestionLevel }
        });
      }
    } else if (this.congestionLevel < 0.3 && errorRate < 0.02) {
      // Increase
      this.aimd.additiveIncrease();
      this.consecutiveBackoffs = 0;

      if (this.mode === 'backoff' || this.mode === 'recovery') {
        this.mode = 'recovery';
        this.emit({
          type: 'recovery-started',
          timestamp: now,
          data: { rate: this.aimd.currentRate, congestion: this.congestionLevel }
        });
      } else {
        this.mode = this.congestionLevel < 0.1 ? 'steady' : 'probing';
      }
    } else {
      // Steady state: use PI controller for fine adjustment
      const piAdjustment = this.piController.compute(
        this.config.targetLatencyMs,
        latencySignal.currentRtt
      );

      const adjustedRate = this.aimd.currentRate + piAdjustment * 0.1;
      this.aimd.setRate(adjustedRate);
      this.mode = 'steady';
    }

    // Apply coordinated multiplier
    this.effectiveRate = this.aimd.currentRate * recommendedMultiplier;

    // Reallocate tenant shares
    this.tenantAllocator.allocate(this.effectiveRate);

    // Emit coordinated update if peers affected us
    if (recommendedMultiplier < 0.95) {
      this.emit({
        type: 'coordinated-update',
        timestamp: now,
        data: {
          multiplier: recommendedMultiplier,
          peerCongestion,
          effectiveRate: this.effectiveRate
        }
      });
    }
  }

  /**
   * Compute queue pressure from recent latency samples.
   */
  private computeQueuePressure(samples: LatencySample[], now: number): number {
    if (samples.length < 5) return 0;

    // Use sojourn time approximation: how long requests spend "in system"
    const recent = samples.slice(-10);
    const avgLatency = recent.reduce((s, r) => s + r.durationMs, 0) / recent.length;

    const codelResult = this.queueManager.evaluate(avgLatency, now);

    if (codelResult.dropping) {
      this.emit({
        type: 'queue-pressure',
        timestamp: now,
        data: {
          avgLatency,
          dropProbability: codelResult.dropProbability,
          dropping: true
        }
      });
    }

    return codelResult.dropProbability;
  }

  /**
   * Compute error rate from recent samples.
   */
  private computeErrorRate(samples: LatencySample[]): number {
    if (samples.length === 0) return 0;
    const recent = samples.slice(-20);
    const errors = recent.filter(s => !s.success).length;
    return errors / recent.length;
  }

  /**
   * Combine multiple signals into unified congestion score.
   * Weighted combination with non-linear amplification.
   */
  private computeCongestionLevel(
    latencyCongestion: number,
    latencyGradient: number,
    queuePressure: number,
    errorRate: number
  ): number {
    // Weights: errors are the strongest signal
    const weights = {
      latency: 0.3,
      gradient: 0.15,
      queue: 0.25,
      error: 0.3
    };

    // Normalize gradient to 0-1
    const normalizedGradient = Math.max(0, Math.min(1, latencyGradient * 2));

    // Normalize error rate (10% error = full congestion)
    const normalizedError = Math.min(1, errorRate * 10);

    const raw =
      weights.latency * latencyCongestion +
      weights.gradient * normalizedGradient +
      weights.queue * queuePressure +
      weights.error * normalizedError;

    // Amplify: square root makes us more sensitive to early congestion
    return Math.min(1, Math.sqrt(raw));
  }

  // ─── Public API ──────────────────────────────────────────────────────────

  /**
   * Check if a request should be allowed (simple admission control).
   */
  shouldAllow(tenantId: string): boolean {
    const allocs = this.tenantAllocator.getAllocations();
    const tenantRate = allocs.get(tenantId);
    if (tenantRate === undefined) return this.effectiveRate > 0;
    return tenantRate > 0;
  }

  /**
   * Add a tenant for fair-share allocation.
   */
  addTenant(config: TenantConfig): void {
    this.tenantAllocator.addTenant(config);
    this.tenantAllocator.allocate(this.effectiveRate);
  }

  /**
   * Remove a tenant.
   */
  removeTenant(id: string): void {
    this.tenantAllocator.removeTenant(id);
    this.tenantAllocator.allocate(this.effectiveRate);
  }

  /**
   * Receive gossip state from a peer.
   */
  receivePeerState(state: GossipState): void {
    this.gossip.receivePeerState(state);
  }

  /**
   * Get local state for gossip dissemination.
   */
  getGossipState(): GossipState {
    return this.gossip.getLocalState(this.effectiveRate, this.congestionLevel);
  }

  /**
   * Get current throttle state.
   */
  getState(): ThrottleState {
    return {
      currentRate: this.aimd.currentRate,
      effectiveRate: this.effectiveRate,
      congestionLevel: this.congestionLevel,
      mode: this.mode,
      tenantAllocations: this.tenantAllocator.getAllocations()
    };
  }

  /**
   * Get recent events (drains on read).
   */
  drainEvents(): ThrottleEvent[] {
    return this.events.splice(0);
  }

  private emit(event: ThrottleEvent): void {
    this.events.push(event);
    if (this.events.length > 200) this.events.shift();
  }

  /**
   * Force a rate (for manual override or emergency).
   */
  overrideRate(rate: number): void {
    this.aimd.setRate(rate);
    this.effectiveRate = rate;
    this.tenantAllocator.allocate(rate);
  }

  /**
   * Get p95 latency from recent window.
   */
  getP95Latency(): number {
    const samples = this.latencyWindow.getAll();
    if (samples.length === 0) return 0;
    const sorted = samples.map(s => s.durationMs).sort((a, b) => a - b);
    const idx = Math.floor(sorted.length * 0.95);
    return sorted[Math.min(idx, sorted.length - 1)];
  }

  /**
   * Get latency statistics.
   */
  getLatencyStats(): { mean: number; p50: number; p95: number; p99: number; stddev: number } {
    const samples = this.latencyWindow.getAll();
    if (samples.length === 0) return { mean: 0, p50: 0, p95: 0, p99: 0, stddev: 0 };

    const stats = new WelfordStats();
    const values: number[] = [];
    for (const s of samples) {
      stats.add(s.durationMs);
      values.push(s.durationMs);
    }
    values.sort((a, b) => a - b);

    return {
      mean: stats.mean,
      p50: values[Math.floor(values.length * 0.5)] || 0,
      p95: values[Math.floor(values.length * 0.95)] || 0,
      p99: values[Math.floor(values.length * 0.99)] || 0,
      stddev: stats.stddev
    };
  }
}

// ─── Presets ──────────────────────────────────────────────────────────────────

const PRESETS = {
  'api-gateway': {
    initialRate: 100,
    minRate: 10,
    maxRate: 10000,
    aimdIncrease: 5,
    aimdDecrease: 0.5,
    targetLatencyMs: 50,
    maxQueueDepth: 1000,
    sojournTargetMs: 10,
    piKp: 0.5,
    piKi: 0.1,
    updateIntervalMs: 1000,
    warmupRequests: 50,
    historyWindowSize: 200
  } as ThrottleConfig,

  'agent-to-agent': {
    initialRate: 20,
    minRate: 1,
    maxRate: 500,
    aimdIncrease: 2,
    aimdDecrease: 0.6,
    targetLatencyMs: 200,
    maxQueueDepth: 100,
    sojournTargetMs: 50,
    piKp: 0.3,
    piKi: 0.05,
    updateIntervalMs: 5000,
    warmupRequests: 20,
    historyWindowSize: 100
  } as ThrottleConfig,

  'batch-processing': {
    initialRate: 50,
    minRate: 5,
    maxRate: 2000,
    aimdIncrease: 10,
    aimdDecrease: 0.4,
    targetLatencyMs: 500,
    maxQueueDepth: 5000,
    sojournTargetMs: 100,
    piKp: 0.8,
    piKi: 0.2,
    updateIntervalMs: 10000,
    warmupRequests: 30,
    historyWindowSize: 300
  } as ThrottleConfig
};

export {
  AdaptiveThrottleGovernor,
  AIMDController,
  LatencyGradientDetector,
  CoDelQueueManager,
  PIController,
  TenantFairShareAllocator,
  CoordinatedThrottleGossip,
  BackoffScheduler,
  WelfordStats,
  SlidingWindow,
  EWMATracker,
  PRESETS,
  ThrottleConfig,
  TenantConfig,
  LatencySample,
  QueueMetrics,
  ThrottleState,
  ThrottleEvent,
  GossipState
};
