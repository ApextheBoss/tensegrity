/**
 * Consensus View Synchronizer
 * 
 * View synchronization for BFT consensus protocols — ensures all honest agents
 * converge on the same view (round) despite asynchrony and Byzantine faults.
 * 
 * Components:
 * - PacemakerTimer: Adaptive timeout management with exponential backoff and leader reputation
 * - ViewChangeCollector: Aggregates view-change messages with quorum detection
 * - HighestCertificateTracker: Tracks and forwards highest QC/TC across views
 * - LeaderScheduler: Deterministic leader rotation with reputation-weighted selection
 * - CatchUpProtocol: Helps lagging agents sync to current view
 * - OptimisticViewAdvance: Advances view on receiving f+1 matching next-view messages
 * - TimeoutCertificateBuilder: Constructs timeout certificates from timeout votes
 * - ViewDivergenceDetector: Detects when agents are stuck in different views
 */

// ─── FNV-1a Hash ────────────────────────────────────────────────────────────
function fnv1a(input: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    h ^= input.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

// ─── Types ──────────────────────────────────────────────────────────────────
interface ViewEvent {
  type: string;
  timestamp: number;
  view: number;
  agentId?: string;
  data?: Record<string, unknown>;
}

interface QuorumCertificate {
  view: number;
  blockHash: string;
  signatures: Map<string, string>;
  aggregateWeight: number;
  createdAt: number;
}

interface TimeoutCertificate {
  view: number;
  timeoutVotes: Map<string, { highestQCView: number; signature: string }>;
  aggregateWeight: number;
  createdAt: number;
}

interface ViewChangeMessage {
  fromAgent: string;
  targetView: number;
  highestQC: QuorumCertificate | null;
  highestTC: TimeoutCertificate | null;
  reason: 'timeout' | 'qc_received' | 'tc_received' | 'catch_up';
  timestamp: number;
}

interface AgentViewState {
  agentId: string;
  currentView: number;
  lastUpdate: number;
  weight: number;
  reputation: number;
  consecutiveTimeouts: number;
  viewHistory: Array<{ view: number; enteredAt: number; exitedAt: number }>;
}

interface ViewSyncConfig {
  baseTimeoutMs: number;
  maxTimeoutMs: number;
  timeoutMultiplier: number;
  quorumThreshold: number; // fraction, e.g. 0.667
  maxByzantine: number;
  catchUpThreshold: number; // views behind to trigger catch-up
  viewHistoryLimit: number;
  leaderRotation: 'round-robin' | 'reputation-weighted' | 'sticky';
  stickyLeaderViews: number;
  optimisticAdvanceThreshold: number; // f+1 fraction
  maxViewGap: number; // max gap before forced catch-up
  divergenceWindowMs: number;
}

type EventHandler = (event: ViewEvent) => void;

// ─── PacemakerTimer ─────────────────────────────────────────────────────────
/**
 * Adaptive timeout management for view changes.
 * Uses exponential backoff on consecutive timeouts and leader reputation feedback.
 */
class PacemakerTimer {
  private currentTimeout: number;
  private consecutiveTimeouts: number = 0;
  private viewStartTime: number = 0;
  private leaderReputations: Map<string, number> = new Map(); // EWMA success rate
  private readonly alpha = 0.3;

  constructor(private config: ViewSyncConfig) {
    this.currentTimeout = config.baseTimeoutMs;
  }

  startView(view: number, now: number, leaderId: string): void {
    this.viewStartTime = now;
    const rep = this.leaderReputations.get(leaderId) ?? 0.5;
    // Adjust timeout based on leader reputation: unreliable leaders get longer timeouts
    const repFactor = 1 + (1 - rep) * 0.5; // 1.0x for perfect, 1.5x for terrible
    const backoff = Math.pow(this.config.timeoutMultiplier, this.consecutiveTimeouts);
    this.currentTimeout = Math.min(
      this.config.baseTimeoutMs * backoff * repFactor,
      this.config.maxTimeoutMs
    );
  }

  isTimedOut(now: number): boolean {
    return now - this.viewStartTime >= this.currentTimeout;
  }

  recordTimeout(): void {
    this.consecutiveTimeouts++;
  }

  recordSuccess(leaderId: string): void {
    this.consecutiveTimeouts = 0;
    const prev = this.leaderReputations.get(leaderId) ?? 0.5;
    this.leaderReputations.set(leaderId, prev * (1 - this.alpha) + 1.0 * this.alpha);
  }

  recordFailure(leaderId: string): void {
    const prev = this.leaderReputations.get(leaderId) ?? 0.5;
    this.leaderReputations.set(leaderId, prev * (1 - this.alpha) + 0.0 * this.alpha);
  }

  getLeaderReputation(leaderId: string): number {
    return this.leaderReputations.get(leaderId) ?? 0.5;
  }

  getCurrentTimeout(): number {
    return this.currentTimeout;
  }

  getConsecutiveTimeouts(): number {
    return this.consecutiveTimeouts;
  }

  reset(): void {
    this.consecutiveTimeouts = 0;
    this.currentTimeout = this.config.baseTimeoutMs;
  }
}

// ─── ViewChangeCollector ────────────────────────────────────────────────────
/**
 * Aggregates view-change messages and detects quorum for view advancement.
 * Tracks per-view vote weight and extracts highest QC across all messages.
 */
class ViewChangeCollector {
  private messages: Map<number, Map<string, ViewChangeMessage>> = new Map(); // view -> agent -> msg
  private agentWeights: Map<string, number> = new Map();

  constructor(private config: ViewSyncConfig) {}

  registerAgent(agentId: string, weight: number): void {
    this.agentWeights.set(agentId, weight);
  }

  addMessage(msg: ViewChangeMessage): { quorumReached: boolean; totalWeight: number } {
    if (!this.messages.has(msg.targetView)) {
      this.messages.set(msg.targetView, new Map());
    }
    const viewMsgs = this.messages.get(msg.targetView)!;
    viewMsgs.set(msg.fromAgent, msg);

    const totalWeight = this.computeWeight(msg.targetView);
    const totalSystemWeight = this.getTotalSystemWeight();
    const quorumReached = totalWeight / totalSystemWeight >= this.config.quorumThreshold;

    return { quorumReached, totalWeight };
  }

  getHighestQCForView(targetView: number): QuorumCertificate | null {
    const viewMsgs = this.messages.get(targetView);
    if (!viewMsgs) return null;

    let highest: QuorumCertificate | null = null;
    for (const msg of viewMsgs.values()) {
      if (msg.highestQC && (!highest || msg.highestQC.view > highest.view)) {
        highest = msg.highestQC;
      }
    }
    return highest;
  }

  getHighestTCForView(targetView: number): TimeoutCertificate | null {
    const viewMsgs = this.messages.get(targetView);
    if (!viewMsgs) return null;

    let highest: TimeoutCertificate | null = null;
    for (const msg of viewMsgs.values()) {
      if (msg.highestTC && (!highest || msg.highestTC.view > highest.view)) {
        highest = msg.highestTC;
      }
    }
    return highest;
  }

  getVoterCount(targetView: number): number {
    return this.messages.get(targetView)?.size ?? 0;
  }

  pruneBelow(view: number): number {
    let pruned = 0;
    for (const [v] of this.messages) {
      if (v < view) {
        this.messages.delete(v);
        pruned++;
      }
    }
    return pruned;
  }

  private computeWeight(view: number): number {
    const viewMsgs = this.messages.get(view);
    if (!viewMsgs) return 0;
    let total = 0;
    for (const agentId of viewMsgs.keys()) {
      total += this.agentWeights.get(agentId) ?? 1;
    }
    return total;
  }

  private getTotalSystemWeight(): number {
    let total = 0;
    for (const w of this.agentWeights.values()) total += w;
    return total || 1;
  }
}

// ─── HighestCertificateTracker ──────────────────────────────────────────────
/**
 * Tracks the highest QC and TC seen across all views.
 * Ensures safety by always forwarding the highest certified state.
 */
class HighestCertificateTracker {
  private highestQC: QuorumCertificate | null = null;
  private highestTC: TimeoutCertificate | null = null;
  private qcHistory: QuorumCertificate[] = [];
  private tcHistory: TimeoutCertificate[] = [];
  private readonly maxHistory: number;

  constructor(maxHistory: number = 100) {
    this.maxHistory = maxHistory;
  }

  updateQC(qc: QuorumCertificate): boolean {
    if (!this.highestQC || qc.view > this.highestQC.view) {
      this.highestQC = qc;
      this.qcHistory.push(qc);
      if (this.qcHistory.length > this.maxHistory) {
        this.qcHistory = this.qcHistory.slice(-this.maxHistory);
      }
      return true;
    }
    return false;
  }

  updateTC(tc: TimeoutCertificate): boolean {
    if (!this.highestTC || tc.view > this.highestTC.view) {
      this.highestTC = tc;
      this.tcHistory.push(tc);
      if (this.tcHistory.length > this.maxHistory) {
        this.tcHistory = this.tcHistory.slice(-this.maxHistory);
      }
      return true;
    }
    return false;
  }

  getHighestQC(): QuorumCertificate | null {
    return this.highestQC;
  }

  getHighestTC(): TimeoutCertificate | null {
    return this.highestTC;
  }

  getQCForView(view: number): QuorumCertificate | undefined {
    return this.qcHistory.find(qc => qc.view === view);
  }

  getHighestCertifiedView(): number {
    const qcView = this.highestQC?.view ?? -1;
    const tcView = this.highestTC?.view ?? -1;
    return Math.max(qcView, tcView);
  }

  getProgressRate(windowSize: number): number {
    if (this.qcHistory.length < 2) return 0;
    const recent = this.qcHistory.slice(-windowSize);
    if (recent.length < 2) return 0;
    const elapsed = recent[recent.length - 1].createdAt - recent[0].createdAt;
    if (elapsed <= 0) return 0;
    return (recent.length - 1) / (elapsed / 1000); // QCs per second
  }
}

// ─── LeaderScheduler ────────────────────────────────────────────────────────
/**
 * Deterministic leader selection with multiple rotation strategies.
 * - round-robin: Simple modular rotation
 * - reputation-weighted: Higher reputation = higher chance of being leader
 * - sticky: Same leader for N consecutive views for amortized overhead
 */
class LeaderScheduler {
  private agents: AgentViewState[] = [];
  private agentIndex: Map<string, number> = new Map();

  constructor(private config: ViewSyncConfig) {}

  registerAgent(state: AgentViewState): void {
    if (!this.agentIndex.has(state.agentId)) {
      this.agentIndex.set(state.agentId, this.agents.length);
      this.agents.push(state);
    }
  }

  removeAgent(agentId: string): void {
    const idx = this.agentIndex.get(agentId);
    if (idx === undefined) return;
    this.agents.splice(idx, 1);
    this.agentIndex.clear();
    this.agents.forEach((a, i) => this.agentIndex.set(a.agentId, i));
  }

  getLeader(view: number): string | null {
    if (this.agents.length === 0) return null;

    switch (this.config.leaderRotation) {
      case 'round-robin':
        return this.roundRobinLeader(view);
      case 'reputation-weighted':
        return this.reputationWeightedLeader(view);
      case 'sticky':
        return this.stickyLeader(view);
      default:
        return this.roundRobinLeader(view);
    }
  }

  private roundRobinLeader(view: number): string {
    // Sorted by agent ID for determinism
    const sorted = [...this.agents].sort((a, b) => {
      const ha = fnv1a(a.agentId);
      const hb = fnv1a(b.agentId);
      return ha - hb;
    });
    return sorted[view % sorted.length].agentId;
  }

  private reputationWeightedLeader(view: number): string {
    // Weight by reputation, use view as seed for deterministic selection
    const sorted = [...this.agents].sort((a, b) => fnv1a(a.agentId) - fnv1a(b.agentId));
    const totalRep = sorted.reduce((sum, a) => sum + Math.max(a.reputation, 0.01), 0);
    
    // Deterministic pseudo-random from view number
    const seed = fnv1a(`view-${view}`);
    const target = (seed % 10000) / 10000 * totalRep;
    
    let cumulative = 0;
    for (const agent of sorted) {
      cumulative += Math.max(agent.reputation, 0.01);
      if (cumulative >= target) return agent.agentId;
    }
    return sorted[sorted.length - 1].agentId;
  }

  private stickyLeader(view: number): string {
    const epoch = Math.floor(view / this.config.stickyLeaderViews);
    return this.roundRobinLeader(epoch);
  }

  getNextLeaders(currentView: number, count: number): string[] {
    const leaders: string[] = [];
    for (let i = 0; i < count; i++) {
      const leader = this.getLeader(currentView + i);
      if (leader) leaders.push(leader);
    }
    return leaders;
  }
}

// ─── CatchUpProtocol ────────────────────────────────────────────────────────
/**
 * Helps lagging agents synchronize to the current view.
 * Sends compressed view-jump packages with required certificates.
 */
class CatchUpProtocol {
  private pendingCatchUps: Map<string, {
    fromView: number;
    targetView: number;
    certificates: QuorumCertificate[];
    requestedAt: number;
    fulfilled: boolean;
  }> = new Map();

  constructor(private config: ViewSyncConfig) {}

  needsCatchUp(agentView: number, currentView: number): boolean {
    return currentView - agentView >= this.config.catchUpThreshold;
  }

  requestCatchUp(
    agentId: string,
    fromView: number,
    targetView: number,
    now: number
  ): void {
    this.pendingCatchUps.set(agentId, {
      fromView,
      targetView,
      certificates: [],
      requestedAt: now,
      fulfilled: false,
    });
  }

  buildCatchUpPackage(
    agentId: string,
    certTracker: HighestCertificateTracker
  ): {
    targetView: number;
    highestQC: QuorumCertificate | null;
    highestTC: TimeoutCertificate | null;
  } | null {
    const pending = this.pendingCatchUps.get(agentId);
    if (!pending || pending.fulfilled) return null;

    pending.fulfilled = true;
    return {
      targetView: pending.targetView,
      highestQC: certTracker.getHighestQC(),
      highestTC: certTracker.getHighestTC(),
    };
  }

  fulfillCatchUp(agentId: string): void {
    const pending = this.pendingCatchUps.get(agentId);
    if (pending) pending.fulfilled = true;
  }

  pruneCompleted(): number {
    let pruned = 0;
    for (const [id, state] of this.pendingCatchUps) {
      if (state.fulfilled) {
        this.pendingCatchUps.delete(id);
        pruned++;
      }
    }
    return pruned;
  }

  getPendingCount(): number {
    let count = 0;
    for (const state of this.pendingCatchUps.values()) {
      if (!state.fulfilled) count++;
    }
    return count;
  }
}

// ─── OptimisticViewAdvance ──────────────────────────────────────────────────
/**
 * Allows early view advancement when f+1 agents signal readiness for next view.
 * Faster than waiting for full quorum timeout in optimistic case.
 */
class OptimisticViewAdvance {
  private nextViewSignals: Map<number, Set<string>> = new Map(); // view -> signalers
  private agentWeights: Map<string, number> = new Map();

  constructor(private config: ViewSyncConfig) {}

  registerAgent(agentId: string, weight: number): void {
    this.agentWeights.set(agentId, weight);
  }

  signalNextView(agentId: string, targetView: number): boolean {
    if (!this.nextViewSignals.has(targetView)) {
      this.nextViewSignals.set(targetView, new Set());
    }
    this.nextViewSignals.get(targetView)!.add(agentId);
    return this.shouldAdvance(targetView);
  }

  shouldAdvance(targetView: number): boolean {
    const signals = this.nextViewSignals.get(targetView);
    if (!signals) return false;

    let signalWeight = 0;
    for (const agentId of signals) {
      signalWeight += this.agentWeights.get(agentId) ?? 1;
    }

    let totalWeight = 0;
    for (const w of this.agentWeights.values()) totalWeight += w;

    // f+1 threshold: need more than maxByzantine fraction
    return signalWeight / (totalWeight || 1) >= this.config.optimisticAdvanceThreshold;
  }

  getSignalCount(targetView: number): number {
    return this.nextViewSignals.get(targetView)?.size ?? 0;
  }

  pruneBelow(view: number): void {
    for (const [v] of this.nextViewSignals) {
      if (v < view) this.nextViewSignals.delete(v);
    }
  }
}

// ─── TimeoutCertificateBuilder ──────────────────────────────────────────────
/**
 * Constructs timeout certificates from individual timeout votes.
 * A TC proves that enough agents timed out in a view, justifying view change.
 */
class TimeoutCertificateBuilder {
  private timeoutVotes: Map<number, Map<string, { highestQCView: number; signature: string }>> = new Map();
  private agentWeights: Map<string, number> = new Map();

  constructor(private config: ViewSyncConfig) {}

  registerAgent(agentId: string, weight: number): void {
    this.agentWeights.set(agentId, weight);
  }

  addTimeoutVote(
    view: number,
    agentId: string,
    highestQCView: number,
    now: number
  ): TimeoutCertificate | null {
    if (!this.timeoutVotes.has(view)) {
      this.timeoutVotes.set(view, new Map());
    }
    
    const viewVotes = this.timeoutVotes.get(view)!;
    const signature = fnv1a(`timeout-${view}-${agentId}-${now}`).toString(16);
    viewVotes.set(agentId, { highestQCView, signature });

    // Check if we have enough weight for a TC
    let totalVoteWeight = 0;
    for (const voterId of viewVotes.keys()) {
      totalVoteWeight += this.agentWeights.get(voterId) ?? 1;
    }

    let totalSystemWeight = 0;
    for (const w of this.agentWeights.values()) totalSystemWeight += w;

    if (totalVoteWeight / (totalSystemWeight || 1) >= this.config.quorumThreshold) {
      return {
        view,
        timeoutVotes: new Map(viewVotes),
        aggregateWeight: totalVoteWeight,
        createdAt: now,
      };
    }
    return null;
  }

  pruneBelow(view: number): void {
    for (const [v] of this.timeoutVotes) {
      if (v < view) this.timeoutVotes.delete(v);
    }
  }

  getVoteCount(view: number): number {
    return this.timeoutVotes.get(view)?.size ?? 0;
  }
}

// ─── ViewDivergenceDetector ─────────────────────────────────────────────────
/**
 * Detects when agents are stuck in divergent views and recommends resolution.
 * Monitors view distribution and triggers convergence actions.
 */
class ViewDivergenceDetector {
  private agentViews: Map<string, { view: number; lastUpdate: number }> = new Map();
  private divergenceAlerts: Array<{
    timestamp: number;
    viewDistribution: Map<number, number>;
    severity: 'low' | 'medium' | 'high' | 'critical';
    recommendation: string;
  }> = [];

  constructor(private config: ViewSyncConfig) {}

  updateAgentView(agentId: string, view: number, now: number): void {
    this.agentViews.set(agentId, { view, lastUpdate: now });
  }

  checkDivergence(now: number): {
    divergent: boolean;
    severity: 'low' | 'medium' | 'high' | 'critical';
    viewDistribution: Map<number, number>;
    recommendation: string;
  } {
    const distribution = new Map<number, number>();
    let activeCount = 0;

    for (const [, state] of this.agentViews) {
      if (now - state.lastUpdate < this.config.divergenceWindowMs * 2) {
        distribution.set(state.view, (distribution.get(state.view) ?? 0) + 1);
        activeCount++;
      }
    }

    if (activeCount === 0 || distribution.size <= 1) {
      return {
        divergent: false,
        severity: 'low',
        viewDistribution: distribution,
        recommendation: 'none',
      };
    }

    // Find max and min views
    const views = [...distribution.keys()].sort((a, b) => a - b);
    const maxGap = views[views.length - 1] - views[0];
    
    // Find majority view
    let majorityView = views[0];
    let majorityCount = 0;
    for (const [v, count] of distribution) {
      if (count > majorityCount) {
        majorityCount = count;
        majorityView = v;
      }
    }
    
    const majorityFraction = majorityCount / activeCount;

    let severity: 'low' | 'medium' | 'high' | 'critical';
    let recommendation: string;

    if (maxGap <= 1 && majorityFraction >= 0.8) {
      severity = 'low';
      recommendation = 'normal-progress';
    } else if (maxGap <= this.config.catchUpThreshold) {
      severity = 'medium';
      recommendation = 'wait-for-stragglers';
    } else if (majorityFraction >= 0.667) {
      severity = 'high';
      recommendation = 'force-catch-up-minority';
    } else {
      severity = 'critical';
      recommendation = 'emergency-view-sync';
    }

    const divergent = severity !== 'low';

    if (divergent) {
      this.divergenceAlerts.push({
        timestamp: now,
        viewDistribution: new Map(distribution),
        severity,
        recommendation,
      });
      if (this.divergenceAlerts.length > 100) {
        this.divergenceAlerts = this.divergenceAlerts.slice(-50);
      }
    }

    return { divergent, severity, viewDistribution: distribution, recommendation };
  }

  getMajorityView(): number {
    const distribution = new Map<number, number>();
    for (const state of this.agentViews.values()) {
      distribution.set(state.view, (distribution.get(state.view) ?? 0) + 1);
    }
    
    let majorityView = 0;
    let majorityCount = 0;
    for (const [v, count] of distribution) {
      if (count > majorityCount) {
        majorityCount = count;
        majorityView = v;
      }
    }
    return majorityView;
  }

  getRecentAlerts(count: number): typeof this.divergenceAlerts {
    return this.divergenceAlerts.slice(-count);
  }

  removeAgent(agentId: string): void {
    this.agentViews.delete(agentId);
  }
}

// ─── ConsensusViewSynchronizer ──────────────────────────────────────────────
/**
 * Unified orchestrator for view synchronization in BFT consensus.
 * 
 * Tick pipeline:
 * 1. Check pacemaker timeout → trigger view change if expired
 * 2. Process pending catch-ups
 * 3. Check view divergence
 * 4. Prune old state
 * 
 * View advancement triggers:
 * - QC received for current view → advance to view+1
 * - TC constructed for current view → advance to view+1
 * - Optimistic f+1 signals for next view → advance early
 * - Catch-up package received → jump to target view
 */
class ConsensusViewSynchronizer {
  private currentView: number = 0;
  private agentStates: Map<string, AgentViewState> = new Map();
  private events: ViewEvent[] = [];
  private eventHandlers: EventHandler[] = [];

  private pacemaker: PacemakerTimer;
  private viewChangeCollector: ViewChangeCollector;
  private certTracker: HighestCertificateTracker;
  private leaderScheduler: LeaderScheduler;
  private catchUpProtocol: CatchUpProtocol;
  private optimisticAdvance: OptimisticViewAdvance;
  private tcBuilder: TimeoutCertificateBuilder;
  private divergenceDetector: ViewDivergenceDetector;

  constructor(private config: ViewSyncConfig) {
    this.pacemaker = new PacemakerTimer(config);
    this.viewChangeCollector = new ViewChangeCollector(config);
    this.certTracker = new HighestCertificateTracker();
    this.leaderScheduler = new LeaderScheduler(config);
    this.catchUpProtocol = new CatchUpProtocol(config);
    this.optimisticAdvance = new OptimisticViewAdvance(config);
    this.tcBuilder = new TimeoutCertificateBuilder(config);
    this.divergenceDetector = new ViewDivergenceDetector(config);
  }

  // ── Agent Management ────────────────────────────────────────────────────
  registerAgent(agentId: string, weight: number = 1): void {
    const state: AgentViewState = {
      agentId,
      currentView: this.currentView,
      lastUpdate: Date.now(),
      weight,
      reputation: 0.5,
      consecutiveTimeouts: 0,
      viewHistory: [],
    };
    this.agentStates.set(agentId, state);
    this.viewChangeCollector.registerAgent(agentId, weight);
    this.optimisticAdvance.registerAgent(agentId, weight);
    this.tcBuilder.registerAgent(agentId, weight);
    this.leaderScheduler.registerAgent(state);
    this.divergenceDetector.updateAgentView(agentId, this.currentView, Date.now());

    this.emit({
      type: 'agent_registered',
      timestamp: Date.now(),
      view: this.currentView,
      agentId,
      data: { weight },
    });
  }

  removeAgent(agentId: string): void {
    this.agentStates.delete(agentId);
    this.leaderScheduler.removeAgent(agentId);
    this.divergenceDetector.removeAgent(agentId);
    this.emit({
      type: 'agent_removed',
      timestamp: Date.now(),
      view: this.currentView,
      agentId,
    });
  }

  // ── View Advancement ────────────────────────────────────────────────────
  /**
   * Called when a QC is received for the current view.
   * This is the normal-case view advancement path.
   */
  receiveQC(qc: QuorumCertificate): void {
    const now = Date.now();
    const updated = this.certTracker.updateQC(qc);
    
    if (updated && qc.view >= this.currentView) {
      const leader = this.leaderScheduler.getLeader(qc.view);
      if (leader) this.pacemaker.recordSuccess(leader);

      this.advanceToView(qc.view + 1, now, 'qc_received');
    }
  }

  /**
   * Called when an agent times out in the current view.
   * Generates a timeout vote and potentially constructs a TC.
   */
  handleTimeout(agentId: string): void {
    const now = Date.now();
    const state = this.agentStates.get(agentId);
    if (!state) return;

    state.consecutiveTimeouts++;
    const leader = this.leaderScheduler.getLeader(this.currentView);
    if (leader) this.pacemaker.recordFailure(leader);
    this.pacemaker.recordTimeout();

    const highestQCView = this.certTracker.getHighestQC()?.view ?? -1;
    const tc = this.tcBuilder.addTimeoutVote(this.currentView, agentId, highestQCView, now);

    this.emit({
      type: 'timeout_vote',
      timestamp: now,
      view: this.currentView,
      agentId,
      data: { consecutiveTimeouts: state.consecutiveTimeouts, tcFormed: !!tc },
    });

    if (tc) {
      this.certTracker.updateTC(tc);
      this.advanceToView(tc.view + 1, now, 'tc_received');
    }
  }

  /**
   * Signal readiness for the next view (optimistic fast path).
   */
  signalNextView(agentId: string): void {
    const targetView = this.currentView + 1;
    const shouldAdvance = this.optimisticAdvance.signalNextView(agentId, targetView);

    if (shouldAdvance) {
      this.advanceToView(targetView, Date.now(), 'qc_received');
    }
  }

  /**
   * Process a view-change message from another agent.
   */
  receiveViewChange(msg: ViewChangeMessage): void {
    const now = Date.now();

    // Update agent view tracking
    this.divergenceDetector.updateAgentView(msg.fromAgent, msg.targetView, now);

    // Forward any certificates
    if (msg.highestQC) this.certTracker.updateQC(msg.highestQC);
    if (msg.highestTC) this.certTracker.updateTC(msg.highestTC);

    // Add to collector
    const { quorumReached } = this.viewChangeCollector.addMessage(msg);

    if (quorumReached && msg.targetView > this.currentView) {
      this.advanceToView(msg.targetView, now, msg.reason);
    }
  }

  /**
   * Apply catch-up package from a more-advanced agent.
   */
  applyCatchUp(targetView: number, qc: QuorumCertificate | null, tc: TimeoutCertificate | null): void {
    const now = Date.now();
    if (targetView <= this.currentView) return;

    if (qc) this.certTracker.updateQC(qc);
    if (tc) this.certTracker.updateTC(tc);

    this.advanceToView(targetView, now, 'catch_up');
  }

  private advanceToView(newView: number, now: number, reason: string): void {
    if (newView <= this.currentView) return;

    const oldView = this.currentView;
    this.currentView = newView;

    // Update all agent states
    for (const state of this.agentStates.values()) {
      state.viewHistory.push({
        view: oldView,
        enteredAt: state.lastUpdate,
        exitedAt: now,
      });
      if (state.viewHistory.length > this.config.viewHistoryLimit) {
        state.viewHistory = state.viewHistory.slice(-this.config.viewHistoryLimit);
      }
      state.currentView = newView;
      state.lastUpdate = now;
    }

    // Reset pacemaker for new view
    const newLeader = this.leaderScheduler.getLeader(newView);
    if (newLeader) {
      this.pacemaker.startView(newView, now, newLeader);
    }

    // Prune old data
    this.viewChangeCollector.pruneBelow(oldView);
    this.optimisticAdvance.pruneBelow(oldView);
    this.tcBuilder.pruneBelow(oldView);

    this.emit({
      type: 'view_advanced',
      timestamp: now,
      view: newView,
      data: {
        fromView: oldView,
        reason,
        leader: newLeader,
        timeout: this.pacemaker.getCurrentTimeout(),
        highestQCView: this.certTracker.getHighestQC()?.view ?? -1,
        highestTCView: this.certTracker.getHighestTC()?.view ?? -1,
      },
    });
  }

  // ── Tick ─────────────────────────────────────────────────────────────────
  tick(now: number): ViewEvent[] {
    const tickEvents: ViewEvent[] = [];

    // Phase 1: Check pacemaker timeout
    if (this.pacemaker.isTimedOut(now)) {
      const leader = this.leaderScheduler.getLeader(this.currentView);
      tickEvents.push({
        type: 'view_timeout',
        timestamp: now,
        view: this.currentView,
        data: {
          leader,
          timeout: this.pacemaker.getCurrentTimeout(),
          consecutiveTimeouts: this.pacemaker.getConsecutiveTimeouts(),
        },
      });
    }

    // Phase 2: Check for catch-up needs
    for (const [agentId, state] of this.agentStates) {
      if (this.catchUpProtocol.needsCatchUp(state.currentView, this.currentView)) {
        this.catchUpProtocol.requestCatchUp(agentId, state.currentView, this.currentView, now);
        tickEvents.push({
          type: 'catch_up_triggered',
          timestamp: now,
          view: this.currentView,
          agentId,
          data: { agentView: state.currentView, gap: this.currentView - state.currentView },
        });
      }
    }

    // Phase 3: Check view divergence
    for (const [agentId] of this.agentStates) {
      this.divergenceDetector.updateAgentView(agentId, this.currentView, now);
    }
    const divergence = this.divergenceDetector.checkDivergence(now);
    if (divergence.divergent) {
      tickEvents.push({
        type: 'view_divergence_detected',
        timestamp: now,
        view: this.currentView,
        data: {
          severity: divergence.severity,
          recommendation: divergence.recommendation,
          viewCount: divergence.viewDistribution.size,
        },
      });
    }

    // Phase 4: Prune catch-ups
    this.catchUpProtocol.pruneCompleted();

    // Phase 5: Prune old events
    if (this.events.length > 5000) {
      this.events = this.events.slice(-2500);
    }

    for (const e of tickEvents) this.emit(e);
    return tickEvents;
  }

  // ── Queries ─────────────────────────────────────────────────────────────
  getCurrentView(): number {
    return this.currentView;
  }

  getCurrentLeader(): string | null {
    return this.leaderScheduler.getLeader(this.currentView);
  }

  getNextLeaders(count: number): string[] {
    return this.leaderScheduler.getNextLeaders(this.currentView + 1, count);
  }

  getHighestQC(): QuorumCertificate | null {
    return this.certTracker.getHighestQC();
  }

  getHighestTC(): TimeoutCertificate | null {
    return this.certTracker.getHighestTC();
  }

  getProgressRate(windowSize: number = 10): number {
    return this.certTracker.getProgressRate(windowSize);
  }

  getAgentState(agentId: string): AgentViewState | undefined {
    return this.agentStates.get(agentId);
  }

  getStatus(): {
    currentView: number;
    leader: string | null;
    agentCount: number;
    timeout: number;
    consecutiveTimeouts: number;
    highestQCView: number;
    highestTCView: number;
    progressRate: number;
    pendingCatchUps: number;
  } {
    return {
      currentView: this.currentView,
      leader: this.getCurrentLeader(),
      agentCount: this.agentStates.size,
      timeout: this.pacemaker.getCurrentTimeout(),
      consecutiveTimeouts: this.pacemaker.getConsecutiveTimeouts(),
      highestQCView: this.certTracker.getHighestQC()?.view ?? -1,
      highestTCView: this.certTracker.getHighestTC()?.view ?? -1,
      progressRate: this.getProgressRate(),
      pendingCatchUps: this.catchUpProtocol.getPendingCount(),
    };
  }

  // ── Events ──────────────────────────────────────────────────────────────
  onEvent(handler: EventHandler): void {
    this.eventHandlers.push(handler);
  }

  private emit(event: ViewEvent): void {
    this.events.push(event);
    for (const handler of this.eventHandlers) handler(event);
  }

  getEvents(limit?: number): ViewEvent[] {
    return limit ? this.events.slice(-limit) : [...this.events];
  }
}

// ─── Presets ────────────────────────────────────────────────────────────────
const PRESETS = {
  'fast-consensus': {
    baseTimeoutMs: 500,
    maxTimeoutMs: 8000,
    timeoutMultiplier: 1.5,
    quorumThreshold: 0.667,
    maxByzantine: 1,
    catchUpThreshold: 3,
    viewHistoryLimit: 50,
    leaderRotation: 'round-robin' as const,
    stickyLeaderViews: 1,
    optimisticAdvanceThreshold: 0.34,
    maxViewGap: 10,
    divergenceWindowMs: 5000,
  },
  'byzantine-tolerant': {
    baseTimeoutMs: 2000,
    maxTimeoutMs: 30000,
    timeoutMultiplier: 2.0,
    quorumThreshold: 0.75,
    maxByzantine: 3,
    catchUpThreshold: 5,
    viewHistoryLimit: 100,
    leaderRotation: 'reputation-weighted' as const,
    stickyLeaderViews: 1,
    optimisticAdvanceThreshold: 0.4,
    maxViewGap: 20,
    divergenceWindowMs: 15000,
  },
  'high-throughput': {
    baseTimeoutMs: 1000,
    maxTimeoutMs: 16000,
    timeoutMultiplier: 1.5,
    quorumThreshold: 0.667,
    maxByzantine: 2,
    catchUpThreshold: 3,
    viewHistoryLimit: 30,
    leaderRotation: 'sticky' as const,
    stickyLeaderViews: 5,
    optimisticAdvanceThreshold: 0.34,
    maxViewGap: 15,
    divergenceWindowMs: 10000,
  },
};

export {
  ConsensusViewSynchronizer,
  PacemakerTimer,
  ViewChangeCollector,
  HighestCertificateTracker,
  LeaderScheduler,
  CatchUpProtocol,
  OptimisticViewAdvance,
  TimeoutCertificateBuilder,
  ViewDivergenceDetector,
  PRESETS,
};

export type {
  ViewEvent,
  QuorumCertificate,
  TimeoutCertificate,
  ViewChangeMessage,
  AgentViewState,
  ViewSyncConfig,
};
