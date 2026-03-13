/**
 * Distributed Barrier Synchronizer
 * 
 * Multi-phase barrier synchronization for coordinating agent groups through
 * checkpoint-based execution gates. Implements tree-based aggregation for
 * scalable barrier operations, sense-reversal for consecutive barriers without
 * reset, fuzzy barriers for approximate synchronization, and adaptive timeout
 * with stragglers detection.
 *
 * Components:
 * - BarrierRegistry: Named barrier lifecycle management with participant tracking
 * - TreeAggregator: O(log N) tree-reduction for scalable arrival notification
 * - SenseReversalController: Alternating sense bits for back-to-back barriers
 * - FuzzyBarrierManager: Approximate sync with configurable slack tolerance
 * - StragglerDetector: Statistical identification of slow participants
 * - AdaptiveTimeoutCalculator: History-driven timeout adjustment
 * - BarrierChainOrchestrator: Sequential multi-barrier pipeline execution
 * - DistributedBarrierSynchronizer: Unified orchestrator
 */

// ─── Utilities ───────────────────────────────────────────────────────────

function fnv1aHash(input: string): number {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = (hash * 16777619) >>> 0;
  }
  return hash;
}

interface EWMATracker {
  value: number;
  alpha: number;
  count: number;
}

function createEWMA(alpha: number): EWMATracker {
  return { value: 0, alpha, count: 0 };
}

function updateEWMA(tracker: EWMATracker, sample: number): number {
  if (tracker.count === 0) {
    tracker.value = sample;
  } else {
    tracker.value = tracker.alpha * sample + (1 - tracker.alpha) * tracker.value;
  }
  tracker.count++;
  return tracker.value;
}

interface WelfordStats {
  count: number;
  mean: number;
  m2: number;
}

function createWelford(): WelfordStats {
  return { count: 0, mean: 0, m2: 0 };
}

function updateWelford(stats: WelfordStats, value: number): void {
  stats.count++;
  const delta = value - stats.mean;
  stats.mean += delta / stats.count;
  const delta2 = value - stats.mean;
  stats.m2 += delta * delta2;
}

function getVariance(stats: WelfordStats): number {
  return stats.count < 2 ? 0 : stats.m2 / (stats.count - 1);
}

function getStdDev(stats: WelfordStats): number {
  return Math.sqrt(getVariance(stats));
}

// ─── Types ───────────────────────────────────────────────────────────────

type BarrierState = 'created' | 'open' | 'gathering' | 'satisfied' | 'released' | 'timed_out' | 'cancelled';
type ParticipantStatus = 'pending' | 'arrived' | 'released' | 'timed_out' | 'detached';
type StragglerSeverity = 'normal' | 'slow' | 'straggler' | 'critical';

interface BarrierConfig {
  name: string;
  expectedParticipants: number;
  timeoutMs: number;
  fuzzySlackMs?: number;
  senseReversal?: boolean;
  autoRelease?: boolean;
  minParticipants?: number;
  tags?: string[];
  metadata?: Record<string, unknown>;
}

interface Participant {
  id: string;
  status: ParticipantStatus;
  arrivalTime?: number;
  releaseTime?: number;
  sense: boolean;
  arrivalLatencyMs?: number;
  payload?: unknown;
  tags?: string[];
}

interface Barrier {
  id: string;
  config: BarrierConfig;
  state: BarrierState;
  participants: Map<string, Participant>;
  createdAt: number;
  openedAt?: number;
  satisfiedAt?: number;
  releasedAt?: number;
  generation: number;
  currentSense: boolean;
  arrivalOrder: string[];
  childBarriers?: string[];
  parentBarrier?: string;
}

interface TreeNode {
  id: string;
  children: string[];
  parent?: string;
  arrivedCount: number;
  totalDescendants: number;
  satisfied: boolean;
  level: number;
}

interface StragglerReport {
  participantId: string;
  severity: StragglerSeverity;
  elapsedMs: number;
  expectedMs: number;
  deviationSigma: number;
  recommendation: 'wait' | 'warn' | 'proceed_without' | 'cancel';
}

interface BarrierMetrics {
  totalBarriers: number;
  activeBarriers: number;
  satisfiedBarriers: number;
  timedOutBarriers: number;
  averageSyncTimeMs: number;
  stragglerRate: number;
  fuzzyReleasedEarly: number;
  chainCompletions: number;
}

interface BarrierChain {
  id: string;
  name: string;
  barrierIds: string[];
  currentIndex: number;
  state: 'pending' | 'running' | 'completed' | 'failed';
  createdAt: number;
  completedAt?: number;
  results: Map<string, unknown>;
}

interface BarrierEvent {
  type: 'barrier_created' | 'barrier_opened' | 'participant_arrived' | 'barrier_satisfied' |
        'barrier_released' | 'barrier_timed_out' | 'barrier_cancelled' | 'straggler_detected' |
        'fuzzy_early_release' | 'sense_flipped' | 'chain_advanced' | 'chain_completed';
  barrierId: string;
  timestamp: number;
  data: Record<string, unknown>;
}

// ─── BarrierRegistry ─────────────────────────────────────────────────────

class BarrierRegistry {
  private barriers: Map<string, Barrier> = new Map();
  private nameIndex: Map<string, string[]> = new Map();
  private tagIndex: Map<string, Set<string>> = new Map();
  private barrierCounter = 0;

  create(config: BarrierConfig, now: number): Barrier {
    const id = `barrier_${++this.barrierCounter}_${fnv1aHash(config.name + now).toString(16)}`;
    const barrier: Barrier = {
      id,
      config,
      state: 'created',
      participants: new Map(),
      createdAt: now,
      generation: 0,
      currentSense: true,
      arrivalOrder: [],
    };
    this.barriers.set(id, barrier);

    const existing = this.nameIndex.get(config.name) || [];
    existing.push(id);
    this.nameIndex.set(config.name, existing);

    if (config.tags) {
      for (const tag of config.tags) {
        if (!this.tagIndex.has(tag)) this.tagIndex.set(tag, new Set());
        this.tagIndex.get(tag)!.add(id);
      }
    }
    return barrier;
  }

  get(id: string): Barrier | undefined {
    return this.barriers.get(id);
  }

  getByName(name: string): Barrier[] {
    const ids = this.nameIndex.get(name) || [];
    return ids.map(id => this.barriers.get(id)!).filter(Boolean);
  }

  getByTag(tag: string): Barrier[] {
    const ids = this.tagIndex.get(tag);
    if (!ids) return [];
    return [...ids].map(id => this.barriers.get(id)!).filter(Boolean);
  }

  getActive(): Barrier[] {
    return [...this.barriers.values()].filter(b =>
      b.state === 'open' || b.state === 'gathering' || b.state === 'satisfied'
    );
  }

  remove(id: string): boolean {
    const barrier = this.barriers.get(id);
    if (!barrier) return false;
    this.barriers.delete(id);

    const nameList = this.nameIndex.get(barrier.config.name);
    if (nameList) {
      const idx = nameList.indexOf(id);
      if (idx >= 0) nameList.splice(idx, 1);
      if (nameList.length === 0) this.nameIndex.delete(barrier.config.name);
    }

    if (barrier.config.tags) {
      for (const tag of barrier.config.tags) {
        this.tagIndex.get(tag)?.delete(id);
      }
    }
    return true;
  }

  prune(maxAge: number, now: number): number {
    let pruned = 0;
    for (const [id, barrier] of this.barriers) {
      if ((barrier.state === 'released' || barrier.state === 'timed_out' || barrier.state === 'cancelled') &&
          now - (barrier.releasedAt || barrier.createdAt) > maxAge) {
        this.remove(id);
        pruned++;
      }
    }
    return pruned;
  }

  stats(): { total: number; active: number; byState: Record<string, number> } {
    const byState: Record<string, number> = {};
    for (const b of this.barriers.values()) {
      byState[b.state] = (byState[b.state] || 0) + 1;
    }
    return {
      total: this.barriers.size,
      active: this.getActive().length,
      byState,
    };
  }
}

// ─── TreeAggregator ──────────────────────────────────────────────────────

class TreeAggregator {
  private trees: Map<string, Map<string, TreeNode>> = new Map();
  private readonly branchingFactor: number;

  constructor(branchingFactor: number = 4) {
    this.branchingFactor = branchingFactor;
  }

  buildTree(barrierId: string, participantIds: string[]): void {
    const nodes = new Map<string, TreeNode>();
    const sorted = [...participantIds].sort((a, b) =>
      fnv1aHash(barrierId + a) - fnv1aHash(barrierId + b)
    );

    // Build balanced k-ary tree
    for (let i = 0; i < sorted.length; i++) {
      nodes.set(sorted[i], {
        id: sorted[i],
        children: [],
        arrivedCount: 0,
        totalDescendants: 0,
        satisfied: false,
        level: 0,
      });
    }

    // Assign parent-child relationships
    for (let i = 0; i < sorted.length; i++) {
      const parentIdx = i === 0 ? -1 : Math.floor((i - 1) / this.branchingFactor);
      if (parentIdx >= 0) {
        const parentNode = nodes.get(sorted[parentIdx])!;
        const childNode = nodes.get(sorted[i])!;
        parentNode.children.push(sorted[i]);
        childNode.parent = sorted[parentIdx];
        childNode.level = parentNode.level + 1;
      }
    }

    // Compute total descendants bottom-up
    for (let i = sorted.length - 1; i >= 0; i--) {
      const node = nodes.get(sorted[i])!;
      node.totalDescendants = node.children.reduce((sum, cid) => {
        const child = nodes.get(cid)!;
        return sum + 1 + child.totalDescendants;
      }, 0);
    }

    this.trees.set(barrierId, nodes);
  }

  recordArrival(barrierId: string, participantId: string): string[] {
    const tree = this.trees.get(barrierId);
    if (!tree) return [];

    const notified: string[] = [];
    let current = participantId;
    const node = tree.get(current);
    if (!node) return [];

    node.satisfied = true;

    // Propagate up the tree
    while (node.parent) {
      const parent = tree.get(node.parent)!;
      parent.arrivedCount++;

      // Check if all children and their subtrees are satisfied
      const allChildrenSatisfied = parent.children.every(cid => {
        const child = tree.get(cid)!;
        return child.satisfied && child.arrivedCount >= child.totalDescendants;
      });

      if (allChildrenSatisfied && parent.arrivedCount >= parent.totalDescendants) {
        parent.satisfied = true;
        notified.push(parent.id);
      }
      current = parent.id;
      break;
    }

    return notified;
  }

  isRootSatisfied(barrierId: string, participantIds: string[]): boolean {
    const tree = this.trees.get(barrierId);
    if (!tree) return false;
    const sorted = [...participantIds].sort((a, b) =>
      fnv1aHash(barrierId + a) - fnv1aHash(barrierId + b)
    );
    if (sorted.length === 0) return false;
    const root = tree.get(sorted[0]);
    return root ? root.satisfied : false;
  }

  removeTree(barrierId: string): void {
    this.trees.delete(barrierId);
  }

  getTreeDepth(barrierId: string): number {
    const tree = this.trees.get(barrierId);
    if (!tree) return 0;
    let maxLevel = 0;
    for (const node of tree.values()) {
      maxLevel = Math.max(maxLevel, node.level);
    }
    return maxLevel + 1;
  }
}

// ─── SenseReversalController ─────────────────────────────────────────────

class SenseReversalController {
  private senseMap: Map<string, boolean> = new Map();

  getCurrentSense(barrierId: string): boolean {
    if (!this.senseMap.has(barrierId)) {
      this.senseMap.set(barrierId, true);
    }
    return this.senseMap.get(barrierId)!;
  }

  flipSense(barrierId: string): boolean {
    const current = this.getCurrentSense(barrierId);
    const next = !current;
    this.senseMap.set(barrierId, next);
    return next;
  }

  checkParticipantSense(barrierId: string, participantSense: boolean): boolean {
    return this.getCurrentSense(barrierId) === participantSense;
  }

  reset(barrierId: string): void {
    this.senseMap.delete(barrierId);
  }
}

// ─── FuzzyBarrierManager ─────────────────────────────────────────────────

class FuzzyBarrierManager {
  private arrivalTrackers: Map<string, { times: number[]; threshold: number }> = new Map();

  initBarrier(barrierId: string, slackMs: number): void {
    this.arrivalTrackers.set(barrierId, { times: [], threshold: slackMs });
  }

  recordArrival(barrierId: string, arrivalTime: number): void {
    const tracker = this.arrivalTrackers.get(barrierId);
    if (tracker) {
      tracker.times.push(arrivalTime);
    }
  }

  /**
   * Check if enough participants have arrived within the slack window
   * to consider the barrier "fuzzy satisfied"
   */
  checkFuzzySatisfied(
    barrierId: string,
    minParticipants: number,
    totalExpected: number,
    now: number
  ): { satisfied: boolean; arrivedCount: number; withinSlack: number } {
    const tracker = this.arrivalTrackers.get(barrierId);
    if (!tracker) return { satisfied: false, arrivedCount: 0, withinSlack: 0 };

    const arrivedCount = tracker.times.length;
    if (arrivedCount < minParticipants) {
      return { satisfied: false, arrivedCount, withinSlack: 0 };
    }

    // Check how many arrived within slack of the first arrival
    const firstArrival = Math.min(...tracker.times);
    const withinSlack = tracker.times.filter(t => t - firstArrival <= tracker.threshold).length;

    // Fuzzy satisfaction: enough within slack window OR a high enough fraction
    const fractionArrived = arrivedCount / totalExpected;
    const satisfied = withinSlack >= minParticipants ||
      (fractionArrived >= 0.8 && arrivedCount >= minParticipants);

    return { satisfied, arrivedCount, withinSlack };
  }

  getArrivalSpread(barrierId: string): number {
    const tracker = this.arrivalTrackers.get(barrierId);
    if (!tracker || tracker.times.length < 2) return 0;
    return Math.max(...tracker.times) - Math.min(...tracker.times);
  }

  removeBarrier(barrierId: string): void {
    this.arrivalTrackers.delete(barrierId);
  }
}

// ─── StragglerDetector ───────────────────────────────────────────────────

class StragglerDetector {
  private arrivalStats: Map<string, WelfordStats> = new Map();
  private perParticipantStats: Map<string, WelfordStats> = new Map();
  private readonly slowThresholdSigma: number;
  private readonly stragglerThresholdSigma: number;
  private readonly criticalThresholdSigma: number;

  constructor(
    slowSigma: number = 1.5,
    stragglerSigma: number = 2.5,
    criticalSigma: number = 4.0
  ) {
    this.slowThresholdSigma = slowSigma;
    this.stragglerThresholdSigma = stragglerSigma;
    this.criticalThresholdSigma = criticalSigma;
  }

  recordArrivalLatency(barrierId: string, participantId: string, latencyMs: number): void {
    if (!this.arrivalStats.has(barrierId)) {
      this.arrivalStats.set(barrierId, createWelford());
    }
    updateWelford(this.arrivalStats.get(barrierId)!, latencyMs);

    const key = `${barrierId}:${participantId}`;
    if (!this.perParticipantStats.has(key)) {
      this.perParticipantStats.set(key, createWelford());
    }
    updateWelford(this.perParticipantStats.get(key)!, latencyMs);
  }

  detectStragglers(
    barrierId: string,
    pendingParticipants: string[],
    elapsedMs: number,
    barrierOpenTime: number
  ): StragglerReport[] {
    const stats = this.arrivalStats.get(barrierId);
    if (!stats || stats.count < 3) {
      // Not enough data for statistical detection
      return pendingParticipants.map(pid => ({
        participantId: pid,
        severity: 'normal' as StragglerSeverity,
        elapsedMs,
        expectedMs: 0,
        deviationSigma: 0,
        recommendation: 'wait' as const,
      }));
    }

    const mean = stats.mean;
    const stdDev = getStdDev(stats);
    const reports: StragglerReport[] = [];

    for (const pid of pendingParticipants) {
      const deviation = stdDev > 0 ? (elapsedMs - mean) / stdDev : 0;
      let severity: StragglerSeverity = 'normal';
      let recommendation: 'wait' | 'warn' | 'proceed_without' | 'cancel' = 'wait';

      if (deviation >= this.criticalThresholdSigma) {
        severity = 'critical';
        recommendation = 'cancel';
      } else if (deviation >= this.stragglerThresholdSigma) {
        severity = 'straggler';
        recommendation = 'proceed_without';
      } else if (deviation >= this.slowThresholdSigma) {
        severity = 'slow';
        recommendation = 'warn';
      }

      // Check participant-specific history
      const key = `${barrierId}:${pid}`;
      const pStats = this.perParticipantStats.get(key);
      if (pStats && pStats.count >= 3) {
        const pMean = pStats.mean;
        // If this participant is historically slow, be more tolerant
        if (pMean > mean * 1.5 && severity === 'straggler') {
          severity = 'slow';
          recommendation = 'warn';
        }
      }

      reports.push({
        participantId: pid,
        severity,
        elapsedMs,
        expectedMs: mean,
        deviationSigma: deviation,
        recommendation,
      });
    }

    return reports;
  }

  getParticipantProfile(barrierId: string, participantId: string): {
    meanLatency: number;
    stdDev: number;
    samples: number;
  } | null {
    const key = `${barrierId}:${participantId}`;
    const stats = this.perParticipantStats.get(key);
    if (!stats || stats.count === 0) return null;
    return {
      meanLatency: stats.mean,
      stdDev: getStdDev(stats),
      samples: stats.count,
    };
  }

  pruneStats(barrierId: string): void {
    this.arrivalStats.delete(barrierId);
    for (const key of this.perParticipantStats.keys()) {
      if (key.startsWith(barrierId + ':')) {
        this.perParticipantStats.delete(key);
      }
    }
  }
}

// ─── AdaptiveTimeoutCalculator ───────────────────────────────────────────

class AdaptiveTimeoutCalculator {
  private completionTimes: Map<string, EWMATracker> = new Map();
  private timeoutHistory: Map<string, { timeouts: number; total: number }> = new Map();
  private readonly minTimeoutMs: number;
  private readonly maxTimeoutMs: number;
  private readonly targetTimeoutRate: number;

  constructor(minTimeoutMs: number = 100, maxTimeoutMs: number = 300000, targetTimeoutRate: number = 0.01) {
    this.minTimeoutMs = minTimeoutMs;
    this.maxTimeoutMs = maxTimeoutMs;
    this.targetTimeoutRate = targetTimeoutRate;
  }

  recordCompletion(barrierName: string, durationMs: number): void {
    if (!this.completionTimes.has(barrierName)) {
      this.completionTimes.set(barrierName, createEWMA(0.2));
    }
    updateEWMA(this.completionTimes.get(barrierName)!, durationMs);

    if (!this.timeoutHistory.has(barrierName)) {
      this.timeoutHistory.set(barrierName, { timeouts: 0, total: 0 });
    }
    this.timeoutHistory.get(barrierName)!.total++;
  }

  recordTimeout(barrierName: string): void {
    if (!this.timeoutHistory.has(barrierName)) {
      this.timeoutHistory.set(barrierName, { timeouts: 0, total: 0 });
    }
    const hist = this.timeoutHistory.get(barrierName)!;
    hist.timeouts++;
    hist.total++;
  }

  calculateTimeout(barrierName: string, baseTimeoutMs: number): number {
    const tracker = this.completionTimes.get(barrierName);
    if (!tracker || tracker.count < 3) return baseTimeoutMs;

    const hist = this.timeoutHistory.get(barrierName);
    const timeoutRate = hist ? hist.timeouts / Math.max(hist.total, 1) : 0;

    // Adaptive multiplier based on timeout rate vs target
    let multiplier = 1.0;
    if (timeoutRate > this.targetTimeoutRate * 2) {
      // Too many timeouts — increase timeout
      multiplier = 1.5;
    } else if (timeoutRate < this.targetTimeoutRate * 0.5) {
      // Few timeouts — can tighten
      multiplier = 0.85;
    }

    // Base timeout on EWMA of completion times with headroom
    const adaptiveTimeout = tracker.value * 2.5 * multiplier;

    return Math.max(this.minTimeoutMs, Math.min(this.maxTimeoutMs, adaptiveTimeout));
  }

  getStats(barrierName: string): {
    avgCompletionMs: number;
    timeoutRate: number;
    samples: number;
  } | null {
    const tracker = this.completionTimes.get(barrierName);
    const hist = this.timeoutHistory.get(barrierName);
    if (!tracker) return null;
    return {
      avgCompletionMs: tracker.value,
      timeoutRate: hist ? hist.timeouts / Math.max(hist.total, 1) : 0,
      samples: tracker.count,
    };
  }
}

// ─── BarrierChainOrchestrator ────────────────────────────────────────────

class BarrierChainOrchestrator {
  private chains: Map<string, BarrierChain> = new Map();
  private chainCounter = 0;

  createChain(name: string, barrierIds: string[], now: number): BarrierChain {
    const id = `chain_${++this.chainCounter}_${fnv1aHash(name + now).toString(16)}`;
    const chain: BarrierChain = {
      id,
      name,
      barrierIds,
      currentIndex: 0,
      state: 'pending',
      createdAt: now,
      results: new Map(),
    };
    this.chains.set(id, chain);
    return chain;
  }

  startChain(chainId: string): string | null {
    const chain = this.chains.get(chainId);
    if (!chain || chain.state !== 'pending') return null;
    chain.state = 'running';
    return chain.barrierIds[0] || null;
  }

  advanceChain(chainId: string, completedBarrierId: string, result: unknown, now: number): {
    nextBarrierId: string | null;
    chainCompleted: boolean;
  } {
    const chain = this.chains.get(chainId);
    if (!chain || chain.state !== 'running') {
      return { nextBarrierId: null, chainCompleted: false };
    }

    const currentBarrierId = chain.barrierIds[chain.currentIndex];
    if (currentBarrierId !== completedBarrierId) {
      return { nextBarrierId: null, chainCompleted: false };
    }

    chain.results.set(completedBarrierId, result);
    chain.currentIndex++;

    if (chain.currentIndex >= chain.barrierIds.length) {
      chain.state = 'completed';
      chain.completedAt = now;
      return { nextBarrierId: null, chainCompleted: true };
    }

    return { nextBarrierId: chain.barrierIds[chain.currentIndex], chainCompleted: false };
  }

  failChain(chainId: string, reason: string): void {
    const chain = this.chains.get(chainId);
    if (chain) {
      chain.state = 'failed';
      chain.results.set('_failure_reason', reason);
    }
  }

  getChain(chainId: string): BarrierChain | undefined {
    return this.chains.get(chainId);
  }

  getActiveChains(): BarrierChain[] {
    return [...this.chains.values()].filter(c => c.state === 'running');
  }

  pruneChains(maxAge: number, now: number): number {
    let pruned = 0;
    for (const [id, chain] of this.chains) {
      if ((chain.state === 'completed' || chain.state === 'failed') &&
          now - (chain.completedAt || chain.createdAt) > maxAge) {
        this.chains.delete(id);
        pruned++;
      }
    }
    return pruned;
  }
}

// ─── DistributedBarrierSynchronizer ──────────────────────────────────────

interface BarrierSyncConfig {
  defaultTimeoutMs: number;
  maxBarriers: number;
  pruneIntervalMs: number;
  pruneMaxAge: number;
  treeBranchingFactor: number;
  defaultFuzzySlackMs: number;
  enableSenseReversal: boolean;
  stragglerSigmaThresholds: { slow: number; straggler: number; critical: number };
  adaptiveTimeoutRange: { min: number; max: number };
  targetTimeoutRate: number;
}

class DistributedBarrierSynchronizer {
  private registry: BarrierRegistry;
  private treeAggregator: TreeAggregator;
  private senseController: SenseReversalController;
  private fuzzyManager: FuzzyBarrierManager;
  private stragglerDetector: StragglerDetector;
  private timeoutCalculator: AdaptiveTimeoutCalculator;
  private chainOrchestrator: BarrierChainOrchestrator;
  private config: BarrierSyncConfig;
  private events: BarrierEvent[] = [];
  private metrics: BarrierMetrics;
  private syncTimeTracker: EWMATracker;
  private lastPrune: number = 0;

  constructor(config: Partial<BarrierSyncConfig> = {}) {
    this.config = {
      defaultTimeoutMs: 30000,
      maxBarriers: 1000,
      pruneIntervalMs: 60000,
      pruneMaxAge: 300000,
      treeBranchingFactor: 4,
      defaultFuzzySlackMs: 500,
      enableSenseReversal: true,
      stragglerSigmaThresholds: { slow: 1.5, straggler: 2.5, critical: 4.0 },
      adaptiveTimeoutRange: { min: 100, max: 300000 },
      targetTimeoutRate: 0.01,
      ...config,
    };

    this.registry = new BarrierRegistry();
    this.treeAggregator = new TreeAggregator(this.config.treeBranchingFactor);
    this.senseController = new SenseReversalController();
    this.fuzzyManager = new FuzzyBarrierManager();
    this.stragglerDetector = new StragglerDetector(
      this.config.stragglerSigmaThresholds.slow,
      this.config.stragglerSigmaThresholds.straggler,
      this.config.stragglerSigmaThresholds.critical
    );
    this.timeoutCalculator = new AdaptiveTimeoutCalculator(
      this.config.adaptiveTimeoutRange.min,
      this.config.adaptiveTimeoutRange.max,
      this.config.targetTimeoutRate
    );
    this.chainOrchestrator = new BarrierChainOrchestrator();
    this.syncTimeTracker = createEWMA(0.15);

    this.metrics = {
      totalBarriers: 0,
      activeBarriers: 0,
      satisfiedBarriers: 0,
      timedOutBarriers: 0,
      averageSyncTimeMs: 0,
      stragglerRate: 0,
      fuzzyReleasedEarly: 0,
      chainCompletions: 0,
    };
  }

  // ── Barrier Lifecycle ──────────────────────────────────────────────

  createBarrier(config: BarrierConfig, now: number): Barrier {
    const effectiveTimeout = this.timeoutCalculator.calculateTimeout(
      config.name,
      config.timeoutMs || this.config.defaultTimeoutMs
    );
    const effectiveConfig = { ...config, timeoutMs: effectiveTimeout };

    const barrier = this.registry.create(effectiveConfig, now);

    if (config.fuzzySlackMs !== undefined) {
      this.fuzzyManager.initBarrier(barrier.id, config.fuzzySlackMs);
    } else if (this.config.defaultFuzzySlackMs > 0) {
      this.fuzzyManager.initBarrier(barrier.id, this.config.defaultFuzzySlackMs);
    }

    this.metrics.totalBarriers++;
    this.emitEvent({
      type: 'barrier_created',
      barrierId: barrier.id,
      timestamp: now,
      data: { name: config.name, expectedParticipants: config.expectedParticipants, timeoutMs: effectiveTimeout },
    });

    return barrier;
  }

  openBarrier(barrierId: string, participantIds: string[], now: number): boolean {
    const barrier = this.registry.get(barrierId);
    if (!barrier || barrier.state !== 'created') return false;

    barrier.state = 'open';
    barrier.openedAt = now;

    for (const pid of participantIds) {
      barrier.participants.set(pid, {
        id: pid,
        status: 'pending',
        sense: barrier.currentSense,
      });
    }

    // Build aggregation tree for scalable arrival tracking
    this.treeAggregator.buildTree(barrierId, participantIds);
    this.metrics.activeBarriers++;

    this.emitEvent({
      type: 'barrier_opened',
      barrierId,
      timestamp: now,
      data: { participants: participantIds.length },
    });

    return true;
  }

  arrive(barrierId: string, participantId: string, now: number, payload?: unknown): {
    accepted: boolean;
    barrierSatisfied: boolean;
    reason?: string;
  } {
    const barrier = this.registry.get(barrierId);
    if (!barrier) return { accepted: false, barrierSatisfied: false, reason: 'barrier_not_found' };
    if (barrier.state !== 'open' && barrier.state !== 'gathering') {
      return { accepted: false, barrierSatisfied: false, reason: `barrier_state_${barrier.state}` };
    }

    const participant = barrier.participants.get(participantId);
    if (!participant) return { accepted: false, barrierSatisfied: false, reason: 'not_participant' };
    if (participant.status === 'arrived') return { accepted: false, barrierSatisfied: false, reason: 'already_arrived' };

    // Sense reversal check
    if (this.config.enableSenseReversal && barrier.config.senseReversal) {
      if (!this.senseController.checkParticipantSense(barrierId, participant.sense)) {
        return { accepted: false, barrierSatisfied: false, reason: 'wrong_sense' };
      }
    }

    // Record arrival
    participant.status = 'arrived';
    participant.arrivalTime = now;
    participant.payload = payload;
    participant.arrivalLatencyMs = barrier.openedAt ? now - barrier.openedAt : 0;
    barrier.arrivalOrder.push(participantId);

    if (barrier.state === 'open') barrier.state = 'gathering';

    // Record in tree aggregator
    this.treeAggregator.recordArrival(barrierId, participantId);

    // Record in fuzzy manager
    this.fuzzyManager.recordArrival(barrierId, now);

    // Record straggler stats
    if (participant.arrivalLatencyMs !== undefined) {
      this.stragglerDetector.recordArrivalLatency(barrierId, participantId, participant.arrivalLatencyMs);
    }

    this.emitEvent({
      type: 'participant_arrived',
      barrierId,
      timestamp: now,
      data: { participantId, latencyMs: participant.arrivalLatencyMs, arrivalOrder: barrier.arrivalOrder.length },
    });

    // Check if barrier is satisfied
    const arrivedCount = [...barrier.participants.values()].filter(p => p.status === 'arrived').length;
    const barrierSatisfied = arrivedCount >= barrier.config.expectedParticipants;

    if (barrierSatisfied) {
      this.satisfyBarrier(barrier, now);
    } else {
      // Check fuzzy satisfaction
      const minParts = barrier.config.minParticipants || barrier.config.expectedParticipants;
      const fuzzyResult = this.fuzzyManager.checkFuzzySatisfied(
        barrierId, minParts, barrier.config.expectedParticipants, now
      );
      if (fuzzyResult.satisfied && arrivedCount >= minParts) {
        this.metrics.fuzzyReleasedEarly++;
        this.emitEvent({
          type: 'fuzzy_early_release',
          barrierId,
          timestamp: now,
          data: { arrivedCount, withinSlack: fuzzyResult.withinSlack, total: barrier.config.expectedParticipants },
        });
        this.satisfyBarrier(barrier, now);
      }
    }

    return { accepted: true, barrierSatisfied: (barrier.state as string) === 'satisfied' || (barrier.state as string) === 'released' };
  }

  private satisfyBarrier(barrier: Barrier, now: number): void {
    barrier.state = 'satisfied';
    barrier.satisfiedAt = now;

    const duration = barrier.openedAt ? now - barrier.openedAt : 0;
    this.timeoutCalculator.recordCompletion(barrier.config.name, duration);
    updateEWMA(this.syncTimeTracker, duration);
    this.metrics.averageSyncTimeMs = this.syncTimeTracker.value;
    this.metrics.satisfiedBarriers++;

    this.emitEvent({
      type: 'barrier_satisfied',
      barrierId: barrier.id,
      timestamp: now,
      data: { durationMs: duration, generation: barrier.generation },
    });

    if (barrier.config.autoRelease) {
      this.releaseBarrier(barrier.id, now);
    }
  }

  releaseBarrier(barrierId: string, now: number): boolean {
    const barrier = this.registry.get(barrierId);
    if (!barrier || barrier.state !== 'satisfied') return false;

    barrier.state = 'released';
    barrier.releasedAt = now;
    this.metrics.activeBarriers = Math.max(0, this.metrics.activeBarriers - 1);

    for (const p of barrier.participants.values()) {
      if (p.status === 'arrived') {
        p.status = 'released';
        p.releaseTime = now;
      }
    }

    // Sense reversal for reusable barriers
    if (this.config.enableSenseReversal && barrier.config.senseReversal) {
      const newSense = this.senseController.flipSense(barrierId);
      barrier.currentSense = newSense;
      barrier.generation++;
      this.emitEvent({
        type: 'sense_flipped',
        barrierId,
        timestamp: now,
        data: { newSense, generation: barrier.generation },
      });
    }

    this.emitEvent({
      type: 'barrier_released',
      barrierId,
      timestamp: now,
      data: { generation: barrier.generation },
    });

    // Clean up
    this.treeAggregator.removeTree(barrierId);
    this.fuzzyManager.removeBarrier(barrierId);

    return true;
  }

  cancelBarrier(barrierId: string, reason: string, now: number): boolean {
    const barrier = this.registry.get(barrierId);
    if (!barrier || barrier.state === 'released' || barrier.state === 'cancelled') return false;

    barrier.state = 'cancelled';
    barrier.releasedAt = now;
    this.metrics.activeBarriers = Math.max(0, this.metrics.activeBarriers - 1);

    this.treeAggregator.removeTree(barrierId);
    this.fuzzyManager.removeBarrier(barrierId);

    this.emitEvent({
      type: 'barrier_cancelled',
      barrierId,
      timestamp: now,
      data: { reason },
    });

    return true;
  }

  detachParticipant(barrierId: string, participantId: string, now: number): boolean {
    const barrier = this.registry.get(barrierId);
    if (!barrier) return false;

    const participant = barrier.participants.get(participantId);
    if (!participant || participant.status === 'released') return false;

    participant.status = 'detached';

    // Reduce expected count
    if (barrier.config.expectedParticipants > (barrier.config.minParticipants || 1)) {
      barrier.config.expectedParticipants--;

      // Re-check satisfaction
      const arrivedCount = [...barrier.participants.values()].filter(p => p.status === 'arrived').length;
      if (arrivedCount >= barrier.config.expectedParticipants && barrier.state === 'gathering') {
        this.satisfyBarrier(barrier, now);
      }
    }

    return true;
  }

  // ── Chain Management ───────────────────────────────────────────────

  createChain(name: string, barrierConfigs: BarrierConfig[], now: number): {
    chainId: string;
    barrierIds: string[];
  } {
    const barriers = barrierConfigs.map(config => this.createBarrier(config, now));
    const barrierIds = barriers.map(b => b.id);

    // Link barriers
    for (let i = 0; i < barriers.length; i++) {
      if (i > 0) barriers[i].parentBarrier = barriers[i - 1].id;
      if (i < barriers.length - 1) {
        barriers[i].childBarriers = [barriers[i + 1].id];
      }
    }

    const chain = this.chainOrchestrator.createChain(name, barrierIds, now);
    return { chainId: chain.id, barrierIds };
  }

  startChain(chainId: string, participantIds: string[], now: number): boolean {
    const firstBarrierId = this.chainOrchestrator.startChain(chainId);
    if (!firstBarrierId) return false;
    return this.openBarrier(firstBarrierId, participantIds, now);
  }

  // ── Tick Processing ────────────────────────────────────────────────

  tick(now: number): {
    timedOut: string[];
    stragglers: StragglerReport[];
    chainsAdvanced: string[];
    pruned: number;
  } {
    const timedOut: string[] = [];
    const allStragglers: StragglerReport[] = [];
    const chainsAdvanced: string[] = [];

    // Phase 1: Check timeouts
    for (const barrier of this.registry.getActive()) {
      if (!barrier.openedAt) continue;
      const elapsed = now - barrier.openedAt;

      if (elapsed > barrier.config.timeoutMs && barrier.state !== 'satisfied') {
        barrier.state = 'timed_out';
        barrier.releasedAt = now;
        this.metrics.timedOutBarriers++;
        this.metrics.activeBarriers = Math.max(0, this.metrics.activeBarriers - 1);
        this.timeoutCalculator.recordTimeout(barrier.config.name);

        this.treeAggregator.removeTree(barrier.id);
        this.fuzzyManager.removeBarrier(barrier.id);
        timedOut.push(barrier.id);

        this.emitEvent({
          type: 'barrier_timed_out',
          barrierId: barrier.id,
          timestamp: now,
          data: { elapsedMs: elapsed, timeoutMs: barrier.config.timeoutMs },
        });

        // Fail any chain containing this barrier
        for (const chain of this.chainOrchestrator.getActiveChains()) {
          if (chain.barrierIds.includes(barrier.id)) {
            this.chainOrchestrator.failChain(chain.id, `barrier_timeout:${barrier.id}`);
          }
        }
      }
    }

    // Phase 2: Straggler detection
    for (const barrier of this.registry.getActive()) {
      if (barrier.state !== 'gathering' || !barrier.openedAt) continue;
      const elapsed = now - barrier.openedAt;

      const pending = [...barrier.participants.entries()]
        .filter(([, p]) => p.status === 'pending')
        .map(([id]) => id);

      if (pending.length > 0) {
        const reports = this.stragglerDetector.detectStragglers(
          barrier.id, pending, elapsed, barrier.openedAt
        );
        const significantStragglers = reports.filter(r => r.severity !== 'normal');
        if (significantStragglers.length > 0) {
          allStragglers.push(...significantStragglers);
          for (const report of significantStragglers) {
            this.emitEvent({
              type: 'straggler_detected',
              barrierId: barrier.id,
              timestamp: now,
              data: { ...report },
            });
          }
        }
      }
    }

    // Phase 3: Chain advancement
    for (const chain of this.chainOrchestrator.getActiveChains()) {
      const currentBarrierId = chain.barrierIds[chain.currentIndex];
      const barrier = this.registry.get(currentBarrierId);
      if (barrier && barrier.state === 'released') {
        const payloads = [...barrier.participants.values()]
          .filter(p => p.payload !== undefined)
          .map(p => p.payload);

        const { nextBarrierId, chainCompleted } = this.chainOrchestrator.advanceChain(
          chain.id, currentBarrierId, payloads, now
        );

        chainsAdvanced.push(chain.id);

        this.emitEvent({
          type: 'chain_advanced',
          barrierId: currentBarrierId,
          timestamp: now,
          data: { chainId: chain.id, nextBarrierId, chainCompleted },
        });

        if (chainCompleted) {
          this.metrics.chainCompletions++;
          this.emitEvent({
            type: 'chain_completed',
            barrierId: currentBarrierId,
            timestamp: now,
            data: { chainId: chain.id },
          });
        } else if (nextBarrierId) {
          // Auto-open next barrier with same participants
          const participantIds = [...barrier.participants.keys()];
          this.openBarrier(nextBarrierId, participantIds, now);
        }
      }
    }

    // Phase 4: Prune old barriers and chains
    let pruned = 0;
    if (now - this.lastPrune > this.config.pruneIntervalMs) {
      pruned += this.registry.prune(this.config.pruneMaxAge, now);
      pruned += this.chainOrchestrator.pruneChains(this.config.pruneMaxAge, now);
      this.lastPrune = now;
    }

    // Update straggler rate metric
    const activeBarriers = this.registry.getActive();
    if (activeBarriers.length > 0) {
      const totalPending = activeBarriers.reduce((sum, b) =>
        sum + [...b.participants.values()].filter(p => p.status === 'pending').length, 0
      );
      const totalParticipants = activeBarriers.reduce((sum, b) => sum + b.participants.size, 0);
      this.metrics.stragglerRate = totalParticipants > 0 ? totalPending / totalParticipants : 0;
    }

    return { timedOut, stragglers: allStragglers, chainsAdvanced, pruned };
  }

  // ── Queries ────────────────────────────────────────────────────────

  getBarrier(barrierId: string): Barrier | undefined {
    return this.registry.get(barrierId);
  }

  getBarriersByName(name: string): Barrier[] {
    return this.registry.getByName(name);
  }

  getBarrierStatus(barrierId: string): {
    state: BarrierState;
    arrived: number;
    expected: number;
    pending: string[];
    elapsed: number;
    spread: number;
  } | null {
    const barrier = this.registry.get(barrierId);
    if (!barrier) return null;

    const arrived = [...barrier.participants.values()].filter(p => p.status === 'arrived').length;
    const pending = [...barrier.participants.entries()]
      .filter(([, p]) => p.status === 'pending')
      .map(([id]) => id);

    return {
      state: barrier.state,
      arrived,
      expected: barrier.config.expectedParticipants,
      pending,
      elapsed: barrier.openedAt ? Date.now() - barrier.openedAt : 0,
      spread: this.fuzzyManager.getArrivalSpread(barrierId),
    };
  }

  getMetrics(): BarrierMetrics {
    return { ...this.metrics };
  }

  getChain(chainId: string): BarrierChain | undefined {
    return this.chainOrchestrator.getChain(chainId);
  }

  getRecentEvents(limit: number = 50): BarrierEvent[] {
    return this.events.slice(-limit);
  }

  dashboard(): {
    metrics: BarrierMetrics;
    registryStats: ReturnType<BarrierRegistry['stats']>;
    activeBarriers: Array<{
      id: string;
      name: string;
      state: BarrierState;
      arrived: number;
      expected: number;
      elapsedMs: number;
    }>;
    activeChains: Array<{
      id: string;
      name: string;
      progress: string;
      state: string;
    }>;
  } {
    const active = this.registry.getActive();
    return {
      metrics: this.getMetrics(),
      registryStats: this.registry.stats(),
      activeBarriers: active.map(b => ({
        id: b.id,
        name: b.config.name,
        state: b.state,
        arrived: [...b.participants.values()].filter(p => p.status === 'arrived').length,
        expected: b.config.expectedParticipants,
        elapsedMs: b.openedAt ? Date.now() - b.openedAt : 0,
      })),
      activeChains: this.chainOrchestrator.getActiveChains().map(c => ({
        id: c.id,
        name: c.name,
        progress: `${c.currentIndex + 1}/${c.barrierIds.length}`,
        state: c.state,
      })),
    };
  }

  // ── Internal ───────────────────────────────────────────────────────

  private emitEvent(event: BarrierEvent): void {
    this.events.push(event);
    if (this.events.length > 10000) {
      this.events = this.events.slice(-5000);
    }
  }
}

// ─── Presets ─────────────────────────────────────────────────────────────

const PRESETS = {
  'interactive-agents': {
    defaultTimeoutMs: 5000,
    maxBarriers: 500,
    pruneIntervalMs: 30000,
    pruneMaxAge: 120000,
    treeBranchingFactor: 4,
    defaultFuzzySlackMs: 200,
    enableSenseReversal: true,
    stragglerSigmaThresholds: { slow: 1.0, straggler: 2.0, critical: 3.0 },
    adaptiveTimeoutRange: { min: 100, max: 30000 },
    targetTimeoutRate: 0.02,
  },
  'batch-pipeline': {
    defaultTimeoutMs: 60000,
    maxBarriers: 200,
    pruneIntervalMs: 120000,
    pruneMaxAge: 600000,
    treeBranchingFactor: 8,
    defaultFuzzySlackMs: 5000,
    enableSenseReversal: false,
    stragglerSigmaThresholds: { slow: 2.0, straggler: 3.0, critical: 5.0 },
    adaptiveTimeoutRange: { min: 1000, max: 300000 },
    targetTimeoutRate: 0.005,
  },
  'agent-swarm': {
    defaultTimeoutMs: 15000,
    maxBarriers: 1000,
    pruneIntervalMs: 60000,
    pruneMaxAge: 300000,
    treeBranchingFactor: 4,
    defaultFuzzySlackMs: 1000,
    enableSenseReversal: true,
    stragglerSigmaThresholds: { slow: 1.5, straggler: 2.5, critical: 4.0 },
    adaptiveTimeoutRange: { min: 500, max: 120000 },
    targetTimeoutRate: 0.01,
  },
};

export {
  DistributedBarrierSynchronizer,
  BarrierRegistry,
  TreeAggregator,
  SenseReversalController,
  FuzzyBarrierManager,
  StragglerDetector,
  AdaptiveTimeoutCalculator,
  BarrierChainOrchestrator,
  PRESETS,
};
