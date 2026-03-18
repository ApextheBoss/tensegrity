import { fnv1aHash } from './shared-utils';
/**
 * Resource Contention Arbiter for Agent Networks
 * 
 * Resolves contention when multiple agents compete for shared resources
 * using game-theoretic fair allocation, priority auction, and cooperative
 * bargaining mechanisms.
 * 
 * Core components:
 * - ResourceDemandTracker: EWMA demand profiling per agent per resource
 * - AuctionEngine: Sealed-bid second-price (Vickrey) auctions for scarce resources
 * - CooperativeBargainer: Nash Bargaining Solution for multi-party splits
 * - StarvationDetector: Gini coefficient monitoring with corrective reallocation
 * - ContentionPredictor: Linear regression trend on demand history
 * - WaitDieProtocol: Deadlock prevention via timestamp ordering
 * - ResourceBudgetPlanner: Token-based usage budgets with burst allowances
 * - ResourceContentionArbiter: Unified orchestrator
 */

// ── Utilities ──────────────────────────────────────────────────────────

function ewmaUpdate(prev: number, sample: number, alpha: number): number {
  return alpha * sample + (1 - alpha) * prev;
}

function linearRegression(points: { x: number; y: number }[]): { slope: number; intercept: number; r2: number } {
  const n = points.length;
  if (n < 2) return { slope: 0, intercept: points[0]?.y ?? 0, r2: 0 };
  
  let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0, sumY2 = 0;
  for (const p of points) {
    sumX += p.x;
    sumY += p.y;
    sumXY += p.x * p.y;
    sumX2 += p.x * p.x;
    sumY2 += p.y * p.y;
  }
  
  const denom = n * sumX2 - sumX * sumX;
  if (Math.abs(denom) < 1e-12) return { slope: 0, intercept: sumY / n, r2: 0 };
  
  const slope = (n * sumXY - sumX * sumY) / denom;
  const intercept = (sumY - slope * sumX) / n;
  
  const meanY = sumY / n;
  let ssTot = 0, ssRes = 0;
  for (const p of points) {
    ssTot += (p.y - meanY) ** 2;
    ssRes += (p.y - (slope * p.x + intercept)) ** 2;
  }
  const r2 = ssTot > 0 ? 1 - ssRes / ssTot : 0;
  
  return { slope, intercept, r2 };
}

function giniCoefficient(values: number[]): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const n = sorted.length;
  const mean = sorted.reduce((s, v) => s + v, 0) / n;
  if (mean === 0) return 0;
  
  let sumDiff = 0;
  for (let i = 0; i < n; i++) {
    for (let j = 0; j < n; j++) {
      sumDiff += Math.abs(sorted[i] - sorted[j]);
    }
  }
  return sumDiff / (2 * n * n * mean);
}

// ── Types ──────────────────────────────────────────────────────────────

interface ResourceDescriptor {
  id: string;
  capacity: number;
  divisible: boolean; // can be partially allocated
  preemptible: boolean; // can be taken from lower-priority holder
  category: string;
}

interface AgentDemand {
  agentId: string;
  resourceId: string;
  quantity: number;
  priority: number; // 0-10, higher = more important
  deadline?: number; // timestamp, undefined = no deadline
  flexibility: number; // 0-1, how much quantity can be reduced
  utilityPerUnit: number; // value derived per unit
}

interface Allocation {
  agentId: string;
  resourceId: string;
  quantity: number;
  grantedAt: number;
  expiresAt?: number;
  preemptible: boolean;
}

interface AuctionBid {
  agentId: string;
  resourceId: string;
  bidAmount: number;
  quantity: number;
  maxPrice: number;
  timestamp: number;
}

interface AuctionResult {
  resourceId: string;
  winnerId: string;
  winningBid: number;
  priceCharged: number; // second-price
  quantity: number;
  round: number;
}

interface BargainingSolution {
  resourceId: string;
  allocations: Map<string, number>;
  nashProduct: number;
  paretoOptimal: boolean;
}

interface ContentionEvent {
  type: 'contention-detected' | 'auction-completed' | 'bargain-reached' |
        'starvation-detected' | 'starvation-corrected' | 'preemption' |
        'budget-exhausted' | 'demand-spike' | 'deadlock-prevented' |
        'reallocation';
  resourceId: string;
  agents: string[];
  details: Record<string, unknown>;
  timestamp: number;
}

// ── Resource Demand Tracker ────────────────────────────────────────────

interface DemandProfile {
  agentId: string;
  resourceId: string;
  ewmaDemand: number;
  peakDemand: number;
  requestCount: number;
  lastRequestAt: number;
  history: { timestamp: number; quantity: number }[];
  satisfactionRate: number; // 0-1, how often fully satisfied
}

class ResourceDemandTracker {
  private profiles = new Map<string, DemandProfile>();
  private readonly alpha: number;
  private readonly historyLimit: number;
  
  constructor(
    private config: {
      ewmaAlpha?: number;
      historyLimit?: number;
    } = {}
  ) {
    this.alpha = config.ewmaAlpha ?? 0.3;
    this.historyLimit = config.historyLimit ?? 100;
  }
  
  private key(agentId: string, resourceId: string): string {
    return `${agentId}::${resourceId}`;
  }
  
  recordDemand(agentId: string, resourceId: string, quantity: number, satisfied: boolean): void {
    const k = this.key(agentId, resourceId);
    let profile = this.profiles.get(k);
    
    if (!profile) {
      profile = {
        agentId,
        resourceId,
        ewmaDemand: quantity,
        peakDemand: quantity,
        requestCount: 0,
        lastRequestAt: Date.now(),
        history: [],
        satisfactionRate: 1,
      };
      this.profiles.set(k, profile);
    }
    
    profile.ewmaDemand = ewmaUpdate(profile.ewmaDemand, quantity, this.alpha);
    profile.peakDemand = Math.max(profile.peakDemand, quantity);
    profile.requestCount++;
    profile.lastRequestAt = Date.now();
    profile.satisfactionRate = ewmaUpdate(
      profile.satisfactionRate,
      satisfied ? 1 : 0,
      0.1
    );
    
    profile.history.push({ timestamp: Date.now(), quantity });
    if (profile.history.length > this.historyLimit) {
      profile.history = profile.history.slice(-this.historyLimit);
    }
  }
  
  getProfile(agentId: string, resourceId: string): DemandProfile | undefined {
    return this.profiles.get(this.key(agentId, resourceId));
  }
  
  getResourceDemand(resourceId: string): DemandProfile[] {
    const result: DemandProfile[] = [];
    for (const [, profile] of this.profiles) {
      if (profile.resourceId === resourceId) result.push(profile);
    }
    return result;
  }
  
  getTotalDemand(resourceId: string): number {
    return this.getResourceDemand(resourceId)
      .reduce((sum, p) => sum + p.ewmaDemand, 0);
  }
  
  getMostStarved(resourceId: string): DemandProfile | undefined {
    const profiles = this.getResourceDemand(resourceId);
    let worst: DemandProfile | undefined;
    for (const p of profiles) {
      if (!worst || p.satisfactionRate < worst.satisfactionRate) worst = p;
    }
    return worst;
  }
}

// ── Auction Engine (Vickrey Second-Price) ──────────────────────────────

class AuctionEngine {
  private activeAuctions = new Map<string, {
    resourceId: string;
    bids: AuctionBid[];
    startedAt: number;
    round: number;
    reservePrice: number;
  }>();
  private completedAuctions: AuctionResult[] = [];
  private roundCounter = 0;
  
  constructor(
    private config: {
      defaultReservePrice?: number;
      maxBidsPerAuction?: number;
      auctionTimeoutMs?: number;
    } = {}
  ) {}
  
  startAuction(resourceId: string, reservePrice?: number): string {
    const auctionId = `auction-${resourceId}-${++this.roundCounter}`;
    this.activeAuctions.set(auctionId, {
      resourceId,
      bids: [],
      startedAt: Date.now(),
      round: this.roundCounter,
      reservePrice: reservePrice ?? this.config.defaultReservePrice ?? 0,
    });
    return auctionId;
  }
  
  submitBid(auctionId: string, bid: AuctionBid): boolean {
    const auction = this.activeAuctions.get(auctionId);
    if (!auction) return false;
    
    const maxBids = this.config.maxBidsPerAuction ?? 50;
    if (auction.bids.length >= maxBids) return false;
    
    // Sealed bid - no duplicate agents
    if (auction.bids.some(b => b.agentId === bid.agentId)) return false;
    
    auction.bids.push({ ...bid, timestamp: Date.now() });
    return true;
  }
  
  resolveAuction(auctionId: string): AuctionResult | null {
    const auction = this.activeAuctions.get(auctionId);
    if (!auction || auction.bids.length === 0) {
      this.activeAuctions.delete(auctionId);
      return null;
    }
    
    // Filter bids above reserve price
    const validBids = auction.bids
      .filter(b => b.bidAmount >= auction.reservePrice)
      .sort((a, b) => {
        if (b.bidAmount !== a.bidAmount) return b.bidAmount - a.bidAmount;
        return a.timestamp - b.timestamp; // earlier bid wins ties
      });
    
    if (validBids.length === 0) {
      this.activeAuctions.delete(auctionId);
      return null;
    }
    
    const winner = validBids[0];
    // Vickrey: winner pays second-highest price (or reserve if only one bid)
    const secondPrice = validBids.length > 1
      ? validBids[1].bidAmount
      : auction.reservePrice;
    
    const result: AuctionResult = {
      resourceId: auction.resourceId,
      winnerId: winner.agentId,
      winningBid: winner.bidAmount,
      priceCharged: secondPrice,
      quantity: winner.quantity,
      round: auction.round,
    };
    
    this.completedAuctions.push(result);
    if (this.completedAuctions.length > 200) {
      this.completedAuctions = this.completedAuctions.slice(-100);
    }
    
    this.activeAuctions.delete(auctionId);
    return result;
  }
  
  getAuctionHistory(resourceId: string): AuctionResult[] {
    return this.completedAuctions.filter(r => r.resourceId === resourceId);
  }
  
  getMarketPrice(resourceId: string): number {
    const history = this.getAuctionHistory(resourceId);
    if (history.length === 0) return 0;
    const recent = history.slice(-10);
    return recent.reduce((s, r) => s + r.priceCharged, 0) / recent.length;
  }
}

// ── Cooperative Bargainer (Nash Bargaining Solution) ───────────────────

class CooperativeBargainer {
  /**
   * Compute Nash Bargaining Solution for multi-party resource splits.
   * Maximizes the product of (utility - disagreement_point) across all parties.
   */
  solve(
    resourceId: string,
    capacity: number,
    demands: AgentDemand[],
    disagreementPoints?: Map<string, number>
  ): BargainingSolution {
    if (demands.length === 0) {
      return {
        resourceId,
        allocations: new Map(),
        nashProduct: 0,
        paretoOptimal: true,
      };
    }
    
    const totalDemand = demands.reduce((s, d) => s + d.quantity, 0);
    const allocations = new Map<string, number>();
    
    if (totalDemand <= capacity) {
      // No contention — everyone gets what they want
      for (const d of demands) allocations.set(d.agentId, d.quantity);
      return {
        resourceId,
        allocations,
        nashProduct: this.computeNashProduct(demands, allocations, disagreementPoints),
        paretoOptimal: true,
      };
    }
    
    // Weighted proportional allocation based on utility-per-unit * priority
    // This approximates the Nash Bargaining Solution for linear utilities
    const weights = new Map<string, number>();
    let totalWeight = 0;
    
    for (const d of demands) {
      const dp = disagreementPoints?.get(d.agentId) ?? 0;
      const marginalUtility = d.utilityPerUnit * (1 + d.priority / 10);
      const weight = marginalUtility * Math.max(0.01, 1 - dp / (d.quantity * d.utilityPerUnit));
      weights.set(d.agentId, weight);
      totalWeight += weight;
    }
    
    // Initial proportional allocation
    let allocated = 0;
    for (const d of demands) {
      const w = weights.get(d.agentId)!;
      const share = Math.min(d.quantity, capacity * (w / totalWeight));
      allocations.set(d.agentId, share);
      allocated += share;
    }
    
    // Redistribute any surplus (from agents capped at their demand)
    if (allocated < capacity - 0.001) {
      const surplus = capacity - allocated;
      const unsatisfied = demands.filter(d => {
        const alloc = allocations.get(d.agentId)!;
        return alloc < d.quantity - 0.001;
      });
      
      if (unsatisfied.length > 0) {
        const unsatWeight = unsatisfied.reduce(
          (s, d) => s + (weights.get(d.agentId) ?? 0), 0
        );
        for (const d of unsatisfied) {
          const w = weights.get(d.agentId)!;
          const extra = Math.min(
            d.quantity - allocations.get(d.agentId)!,
            surplus * (w / unsatWeight)
          );
          allocations.set(d.agentId, allocations.get(d.agentId)! + extra);
        }
      }
    }
    
    // Apply flexibility — agents with high flexibility yield to those with low
    this.applyFlexibility(demands, allocations, capacity);
    
    return {
      resourceId,
      allocations,
      nashProduct: this.computeNashProduct(demands, allocations, disagreementPoints),
      paretoOptimal: this.checkParetoOptimal(demands, allocations, capacity),
    };
  }
  
  private applyFlexibility(
    demands: AgentDemand[],
    allocations: Map<string, number>,
    capacity: number
  ): void {
    // Sort by flexibility descending — most flexible agents yield first
    const sorted = [...demands].sort((a, b) => b.flexibility - a.flexibility);
    
    let totalAlloc = 0;
    for (const [, v] of allocations) totalAlloc += v;
    
    if (totalAlloc <= capacity + 0.001) return;
    
    let excess = totalAlloc - capacity;
    for (const d of sorted) {
      if (excess <= 0.001) break;
      const current = allocations.get(d.agentId)!;
      const minAcceptable = current * (1 - d.flexibility);
      const canYield = current - minAcceptable;
      const yielded = Math.min(canYield, excess);
      allocations.set(d.agentId, current - yielded);
      excess -= yielded;
    }
  }
  
  private computeNashProduct(
    demands: AgentDemand[],
    allocations: Map<string, number>,
    disagreementPoints?: Map<string, number>
  ): number {
    let product = 1;
    for (const d of demands) {
      const alloc = allocations.get(d.agentId) ?? 0;
      const dp = disagreementPoints?.get(d.agentId) ?? 0;
      const utility = alloc * d.utilityPerUnit - dp;
      if (utility <= 0) return 0;
      product *= utility;
    }
    return product;
  }
  
  private checkParetoOptimal(
    demands: AgentDemand[],
    allocations: Map<string, number>,
    capacity: number
  ): boolean {
    let totalAlloc = 0;
    for (const [, v] of allocations) totalAlloc += v;
    // Pareto optimal if all capacity is used or all demands are met
    const allMet = demands.every(d => {
      const alloc = allocations.get(d.agentId) ?? 0;
      return alloc >= d.quantity - 0.001;
    });
    return allMet || Math.abs(totalAlloc - capacity) < 0.001;
  }
}

// ── Starvation Detector ────────────────────────────────────────────────

interface StarvationReport {
  resourceId: string;
  giniCoefficient: number;
  starvedAgents: { agentId: string; satisfactionRate: number; avgWait: number }[];
  severity: 'none' | 'mild' | 'moderate' | 'severe';
  correctionNeeded: boolean;
}

class StarvationDetector {
  private allocationHistory = new Map<string, {
    agentId: string;
    resourceId: string;
    allocated: number;
    timestamp: number;
  }[]>();
  
  constructor(
    private config: {
      giniThreshold?: number; // above this = unfair
      starvationThreshold?: number; // satisfaction below this = starved
      windowMs?: number;
    } = {}
  ) {}
  
  recordAllocation(agentId: string, resourceId: string, allocated: number): void {
    const key = resourceId;
    if (!this.allocationHistory.has(key)) {
      this.allocationHistory.set(key, []);
    }
    
    const history = this.allocationHistory.get(key)!;
    history.push({ agentId, resourceId, allocated, timestamp: Date.now() });
    
    const windowMs = this.config.windowMs ?? 3600000;
    const cutoff = Date.now() - windowMs;
    const trimmed = history.filter(h => h.timestamp > cutoff);
    this.allocationHistory.set(key, trimmed);
  }
  
  analyze(resourceId: string, demandTracker: ResourceDemandTracker): StarvationReport {
    const profiles = demandTracker.getResourceDemand(resourceId);
    const satisfactions = profiles.map(p => p.satisfactionRate);
    const gini = giniCoefficient(satisfactions);
    
    const starvationThreshold = this.config.starvationThreshold ?? 0.3;
    const starvedAgents = profiles
      .filter(p => p.satisfactionRate < starvationThreshold)
      .map(p => ({
        agentId: p.agentId,
        satisfactionRate: p.satisfactionRate,
        avgWait: p.history.length > 0
          ? (Date.now() - p.history[p.history.length - 1].timestamp) / 1000
          : 0,
      }));
    
    const giniThreshold = this.config.giniThreshold ?? 0.4;
    let severity: StarvationReport['severity'] = 'none';
    if (gini > giniThreshold * 1.5 || starvedAgents.length > profiles.length * 0.3) {
      severity = 'severe';
    } else if (gini > giniThreshold || starvedAgents.length > 0) {
      severity = 'moderate';
    } else if (gini > giniThreshold * 0.7) {
      severity = 'mild';
    }
    
    return {
      resourceId,
      giniCoefficient: gini,
      starvedAgents,
      severity,
      correctionNeeded: severity === 'moderate' || severity === 'severe',
    };
  }
  
  getAgentAllocations(resourceId: string, agentId: string): number {
    const history = this.allocationHistory.get(resourceId) ?? [];
    const agentAllocs = history.filter(h => h.agentId === agentId);
    return agentAllocs.reduce((s, h) => s + h.allocated, 0);
  }
}

// ── Contention Predictor ───────────────────────────────────────────────

interface ContentionForecast {
  resourceId: string;
  currentDemand: number;
  predictedDemand: number;
  capacity: number;
  contentionRatio: number; // predicted demand / capacity
  timeToContention: number | null; // ms until demand exceeds capacity, null if not trending
  confidence: number; // R² of regression
  trending: 'rising' | 'falling' | 'stable';
}

class ContentionPredictor {
  predict(
    resourceId: string,
    capacity: number,
    demandTracker: ResourceDemandTracker,
    horizonMs: number = 300000 // 5 min ahead
  ): ContentionForecast {
    const profiles = demandTracker.getResourceDemand(resourceId);
    const currentDemand = profiles.reduce((s, p) => s + p.ewmaDemand, 0);
    
    // Aggregate demand history across all agents
    const points: { x: number; y: number }[] = [];
    const allHistory: { timestamp: number; quantity: number }[] = [];
    
    for (const profile of profiles) {
      allHistory.push(...profile.history);
    }
    
    // Bucket by time windows (30s)
    const bucketMs = 30000;
    const buckets = new Map<number, number>();
    for (const h of allHistory) {
      const bucket = Math.floor(h.timestamp / bucketMs) * bucketMs;
      buckets.set(bucket, (buckets.get(bucket) ?? 0) + h.quantity);
    }
    
    const sortedBuckets = [...buckets.entries()].sort((a, b) => a[0] - b[0]);
    const baseTime = sortedBuckets[0]?.[0] ?? Date.now();
    for (const [ts, qty] of sortedBuckets) {
      points.push({ x: (ts - baseTime) / 1000, y: qty });
    }
    
    if (points.length < 3) {
      return {
        resourceId,
        currentDemand,
        predictedDemand: currentDemand,
        capacity,
        contentionRatio: currentDemand / capacity,
        timeToContention: currentDemand > capacity ? 0 : null,
        confidence: 0,
        trending: 'stable',
      };
    }
    
    const { slope, intercept, r2 } = linearRegression(points);
    const futureX = (Date.now() - baseTime + horizonMs) / 1000;
    const predictedDemand = Math.max(0, slope * futureX + intercept);
    
    let timeToContention: number | null = null;
    if (slope > 0 && currentDemand < capacity) {
      const xAtCapacity = (capacity - intercept) / slope;
      const currentX = (Date.now() - baseTime) / 1000;
      if (xAtCapacity > currentX) {
        timeToContention = (xAtCapacity - currentX) * 1000;
      }
    } else if (currentDemand >= capacity) {
      timeToContention = 0;
    }
    
    const trending = slope > 0.01 ? 'rising' : slope < -0.01 ? 'falling' : 'stable';
    
    return {
      resourceId,
      currentDemand,
      predictedDemand,
      capacity,
      contentionRatio: predictedDemand / capacity,
      timeToContention,
      confidence: r2,
      trending,
    };
  }
}

// ── Wait-Die Protocol (Deadlock Prevention) ────────────────────────────

interface WaitDieDecision {
  action: 'wait' | 'die' | 'granted';
  agentId: string;
  resourceId: string;
  reason: string;
}

class WaitDieProtocol {
  // Older agents wait, younger agents die (abort and retry)
  private agentTimestamps = new Map<string, number>();
  private resourceHolders = new Map<string, { agentId: string; timestamp: number }>();
  private waitQueues = new Map<string, string[]>();
  
  registerAgent(agentId: string): void {
    if (!this.agentTimestamps.has(agentId)) {
      this.agentTimestamps.set(agentId, Date.now());
    }
  }
  
  requestResource(agentId: string, resourceId: string): WaitDieDecision {
    this.registerAgent(agentId);
    
    const holder = this.resourceHolders.get(resourceId);
    if (!holder) {
      // Resource free — grant immediately
      this.resourceHolders.set(resourceId, {
        agentId,
        timestamp: this.agentTimestamps.get(agentId)!,
      });
      return { action: 'granted', agentId, resourceId, reason: 'Resource available' };
    }
    
    if (holder.agentId === agentId) {
      return { action: 'granted', agentId, resourceId, reason: 'Already holding' };
    }
    
    const requesterTs = this.agentTimestamps.get(agentId)!;
    const holderTs = holder.timestamp;
    
    if (requesterTs < holderTs) {
      // Requester is older — wait
      const queue = this.waitQueues.get(resourceId) ?? [];
      if (!queue.includes(agentId)) {
        queue.push(agentId);
        this.waitQueues.set(resourceId, queue);
      }
      return {
        action: 'wait',
        agentId,
        resourceId,
        reason: `Older agent (${requesterTs}) waits for younger holder (${holderTs})`,
      };
    } else {
      // Requester is younger — die (abort)
      return {
        action: 'die',
        agentId,
        resourceId,
        reason: `Younger agent (${requesterTs}) must abort and retry vs older holder (${holderTs})`,
      };
    }
  }
  
  releaseResource(agentId: string, resourceId: string): string | null {
    const holder = this.resourceHolders.get(resourceId);
    if (!holder || holder.agentId !== agentId) return null;
    
    this.resourceHolders.delete(resourceId);
    
    // Grant to longest-waiting agent (oldest timestamp in queue)
    const queue = this.waitQueues.get(resourceId) ?? [];
    if (queue.length > 0) {
      // Sort by registration timestamp — oldest first
      queue.sort((a, b) => {
        const tsA = this.agentTimestamps.get(a) ?? Infinity;
        const tsB = this.agentTimestamps.get(b) ?? Infinity;
        return tsA - tsB;
      });
      
      const nextAgent = queue.shift()!;
      this.waitQueues.set(resourceId, queue);
      this.resourceHolders.set(resourceId, {
        agentId: nextAgent,
        timestamp: this.agentTimestamps.get(nextAgent)!,
      });
      return nextAgent;
    }
    
    return null;
  }
  
  getWaitingAgents(resourceId: string): string[] {
    return [...(this.waitQueues.get(resourceId) ?? [])];
  }
  
  isHolding(agentId: string, resourceId: string): boolean {
    return this.resourceHolders.get(resourceId)?.agentId === agentId;
  }
}

// ── Resource Budget Planner ────────────────────────────────────────────

interface ResourceBudget {
  agentId: string;
  resourceId: string;
  allocated: number;
  used: number;
  burstAllowance: number;
  burstUsed: number;
  periodMs: number;
  periodStartAt: number;
  totalPeriodsUsed: number;
}

class ResourceBudgetPlanner {
  private budgets = new Map<string, ResourceBudget>();
  
  constructor(
    private config: {
      defaultPeriodMs?: number;
      defaultBurstMultiplier?: number;
    } = {}
  ) {}
  
  private key(agentId: string, resourceId: string): string {
    return `${agentId}::${resourceId}`;
  }
  
  setBudget(
    agentId: string,
    resourceId: string,
    allocated: number,
    burstAllowance?: number
  ): void {
    const periodMs = this.config.defaultPeriodMs ?? 60000;
    const burst = burstAllowance ?? allocated * (this.config.defaultBurstMultiplier ?? 0.5);
    
    this.budgets.set(this.key(agentId, resourceId), {
      agentId,
      resourceId,
      allocated,
      used: 0,
      burstAllowance: burst,
      burstUsed: 0,
      periodMs,
      periodStartAt: Date.now(),
      totalPeriodsUsed: 0,
    });
  }
  
  tryConsume(agentId: string, resourceId: string, amount: number): {
    allowed: boolean;
    fromBurst: boolean;
    remaining: number;
    burstRemaining: number;
  } {
    const budget = this.budgets.get(this.key(agentId, resourceId));
    if (!budget) return { allowed: false, fromBurst: false, remaining: 0, burstRemaining: 0 };
    
    this.maybeResetPeriod(budget);
    
    const regularRemaining = budget.allocated - budget.used;
    const burstRemaining = budget.burstAllowance - budget.burstUsed;
    
    if (amount <= regularRemaining) {
      budget.used += amount;
      return {
        allowed: true,
        fromBurst: false,
        remaining: budget.allocated - budget.used,
        burstRemaining,
      };
    }
    
    // Try using burst
    const neededFromBurst = amount - regularRemaining;
    if (neededFromBurst <= burstRemaining) {
      budget.used = budget.allocated; // exhaust regular
      budget.burstUsed += neededFromBurst;
      return {
        allowed: true,
        fromBurst: true,
        remaining: 0,
        burstRemaining: budget.burstAllowance - budget.burstUsed,
      };
    }
    
    return {
      allowed: false,
      fromBurst: false,
      remaining: regularRemaining,
      burstRemaining,
    };
  }
  
  private maybeResetPeriod(budget: ResourceBudget): void {
    const elapsed = Date.now() - budget.periodStartAt;
    if (elapsed >= budget.periodMs) {
      budget.used = 0;
      budget.burstUsed = 0;
      budget.periodStartAt = Date.now();
      budget.totalPeriodsUsed++;
    }
  }
  
  getBudget(agentId: string, resourceId: string): ResourceBudget | undefined {
    const budget = this.budgets.get(this.key(agentId, resourceId));
    if (budget) this.maybeResetPeriod(budget);
    return budget;
  }
  
  getUtilization(agentId: string, resourceId: string): number {
    const budget = this.getBudget(agentId, resourceId);
    if (!budget || budget.allocated === 0) return 0;
    return budget.used / budget.allocated;
  }
}

// ── Preemption Manager ─────────────────────────────────────────────────

interface PreemptionDecision {
  preempt: boolean;
  victimId?: string;
  reason: string;
  compensationUnits: number;
}

class PreemptionManager {
  private preemptionLog: {
    victimId: string;
    preemptorId: string;
    resourceId: string;
    quantity: number;
    timestamp: number;
  }[] = [];
  
  constructor(
    private config: {
      minPriorityGap?: number; // minimum priority difference to preempt
      compensationRate?: number; // units of compensation per preempted unit
      maxPreemptionsPerAgent?: number; // don't preempt same agent too often
      cooldownMs?: number;
    } = {}
  ) {}
  
  evaluate(
    requestor: AgentDemand,
    currentAllocations: Allocation[],
    resource: ResourceDescriptor
  ): PreemptionDecision {
    if (!resource.preemptible) {
      return { preempt: false, reason: 'Resource is not preemptible', compensationUnits: 0 };
    }
    
    const minGap = this.config.minPriorityGap ?? 2;
    const cooldownMs = this.config.cooldownMs ?? 60000;
    
    // Find lowest-priority preemptible allocation
    const candidates = currentAllocations
      .filter(a => a.preemptible && a.resourceId === resource.id)
      .filter(a => requestor.priority - (this.getPriority(a) ?? 0) >= minGap)
      .filter(a => {
        // Check cooldown
        const recentPreemptions = this.preemptionLog.filter(
          l => l.victimId === a.agentId && Date.now() - l.timestamp < cooldownMs
        );
        const maxPreemptions = this.config.maxPreemptionsPerAgent ?? 3;
        return recentPreemptions.length < maxPreemptions;
      })
      .sort((a, b) => (this.getPriority(a) ?? 0) - (this.getPriority(b) ?? 0));
    
    if (candidates.length === 0) {
      return { preempt: false, reason: 'No preemptible candidates found', compensationUnits: 0 };
    }
    
    const victim = candidates[0];
    const compensationRate = this.config.compensationRate ?? 1.5;
    
    return {
      preempt: true,
      victimId: victim.agentId,
      reason: `Priority gap ${requestor.priority} vs ${this.getPriority(victim)}`,
      compensationUnits: victim.quantity * compensationRate,
    };
  }
  
  recordPreemption(victimId: string, preemptorId: string, resourceId: string, quantity: number): void {
    this.preemptionLog.push({
      victimId,
      preemptorId,
      resourceId,
      quantity,
      timestamp: Date.now(),
    });
    
    if (this.preemptionLog.length > 500) {
      this.preemptionLog = this.preemptionLog.slice(-250);
    }
  }
  
  private getPriority(allocation: Allocation): number {
    // Derive priority from allocation metadata — lower priority = more preemptible
    return allocation.preemptible ? 3 : 7;
  }
  
  getPreemptionCount(agentId: string, windowMs: number = 3600000): number {
    const cutoff = Date.now() - windowMs;
    return this.preemptionLog.filter(
      l => l.victimId === agentId && l.timestamp > cutoff
    ).length;
  }
}

// ── Resource Contention Arbiter (Unified Orchestrator) ─────────────────

interface ArbiterConfig {
  resolutionStrategy: 'auction' | 'bargaining' | 'priority' | 'hybrid';
  starvationCheckIntervalMs: number;
  contentionPredictionHorizonMs: number;
  preemptionEnabled: boolean;
  budgetEnforcementEnabled: boolean;
  maxAllocationDurationMs: number;
  ewmaAlpha: number;
  giniThreshold: number;
  starvationThreshold: number;
}

const ARBITER_PRESETS = {
  'fair-share': {
    resolutionStrategy: 'bargaining' as const,
    starvationCheckIntervalMs: 30000,
    contentionPredictionHorizonMs: 300000,
    preemptionEnabled: false,
    budgetEnforcementEnabled: true,
    maxAllocationDurationMs: 600000,
    ewmaAlpha: 0.2,
    giniThreshold: 0.35,
    starvationThreshold: 0.25,
  },
  'priority-driven': {
    resolutionStrategy: 'priority' as const,
    starvationCheckIntervalMs: 60000,
    contentionPredictionHorizonMs: 120000,
    preemptionEnabled: true,
    budgetEnforcementEnabled: true,
    maxAllocationDurationMs: 300000,
    ewmaAlpha: 0.3,
    giniThreshold: 0.5,
    starvationThreshold: 0.2,
  },
  'market-based': {
    resolutionStrategy: 'auction' as const,
    starvationCheckIntervalMs: 60000,
    contentionPredictionHorizonMs: 600000,
    preemptionEnabled: false,
    budgetEnforcementEnabled: false,
    maxAllocationDurationMs: 900000,
    ewmaAlpha: 0.25,
    giniThreshold: 0.6,
    starvationThreshold: 0.15,
  },
};

class ResourceContentionArbiter {
  private resources = new Map<string, ResourceDescriptor>();
  private allocations = new Map<string, Allocation[]>(); // resourceId -> allocations
  private demandTracker: ResourceDemandTracker;
  private auctionEngine: AuctionEngine;
  private bargainer: CooperativeBargainer;
  private starvationDetector: StarvationDetector;
  private contentionPredictor: ContentionPredictor;
  private waitDie: WaitDieProtocol;
  private budgetPlanner: ResourceBudgetPlanner;
  private preemptionManager: PreemptionManager;
  private events: ContentionEvent[] = [];
  private config: ArbiterConfig;
  
  constructor(config: Partial<ArbiterConfig> = {}, preset?: keyof typeof ARBITER_PRESETS) {
    const base = preset ? ARBITER_PRESETS[preset] : ARBITER_PRESETS['fair-share'];
    this.config = { ...base, ...config };
    
    this.demandTracker = new ResourceDemandTracker({ ewmaAlpha: this.config.ewmaAlpha });
    this.auctionEngine = new AuctionEngine();
    this.bargainer = new CooperativeBargainer();
    this.starvationDetector = new StarvationDetector({
      giniThreshold: this.config.giniThreshold,
      starvationThreshold: this.config.starvationThreshold,
    });
    this.contentionPredictor = new ContentionPredictor();
    this.waitDie = new WaitDieProtocol();
    this.budgetPlanner = new ResourceBudgetPlanner();
    this.preemptionManager = new PreemptionManager();
  }
  
  registerResource(resource: ResourceDescriptor): void {
    this.resources.set(resource.id, resource);
    if (!this.allocations.has(resource.id)) {
      this.allocations.set(resource.id, []);
    }
  }
  
  requestAllocation(demand: AgentDemand): {
    granted: boolean;
    allocation?: Allocation;
    waitDieDecision?: WaitDieDecision;
    events: ContentionEvent[];
  } {
    const resource = this.resources.get(demand.resourceId);
    if (!resource) return { granted: false, events: [] };
    
    const sessionEvents: ContentionEvent[] = [];
    
    // Track demand
    this.demandTracker.recordDemand(
      demand.agentId, demand.resourceId, demand.quantity, false
    );
    
    // Budget check
    if (this.config.budgetEnforcementEnabled) {
      const budgetResult = this.budgetPlanner.tryConsume(
        demand.agentId, demand.resourceId, demand.quantity
      );
      if (!budgetResult.allowed) {
        sessionEvents.push({
          type: 'budget-exhausted',
          resourceId: demand.resourceId,
          agents: [demand.agentId],
          details: { remaining: budgetResult.remaining, burstRemaining: budgetResult.burstRemaining },
          timestamp: Date.now(),
        });
        return { granted: false, events: sessionEvents };
      }
    }
    
    // Check available capacity
    const currentAllocs = this.allocations.get(demand.resourceId) ?? [];
    this.expireAllocations(demand.resourceId);
    const usedCapacity = currentAllocs.reduce((s, a) => s + a.quantity, 0);
    const available = resource.capacity - usedCapacity;
    
    if (available >= demand.quantity) {
      // Sufficient capacity — grant directly
      const allocation = this.grantAllocation(demand, resource);
      return { granted: true, allocation, events: sessionEvents };
    }
    
    // Contention detected
    sessionEvents.push({
      type: 'contention-detected',
      resourceId: demand.resourceId,
      agents: [demand.agentId, ...currentAllocs.map(a => a.agentId)],
      details: { requested: demand.quantity, available, capacity: resource.capacity },
      timestamp: Date.now(),
    });
    
    // Resolve based on strategy
    switch (this.config.resolutionStrategy) {
      case 'auction':
        return this.resolveViaAuction(demand, resource, currentAllocs, sessionEvents);
      case 'bargaining':
        return this.resolveViaBargaining(demand, resource, currentAllocs, sessionEvents);
      case 'priority':
        return this.resolveViaPriority(demand, resource, currentAllocs, sessionEvents);
      case 'hybrid':
        return this.resolveViaHybrid(demand, resource, currentAllocs, sessionEvents);
    }
  }
  
  private resolveViaAuction(
    demand: AgentDemand,
    resource: ResourceDescriptor,
    currentAllocs: Allocation[],
    events: ContentionEvent[]
  ): { granted: boolean; allocation?: Allocation; events: ContentionEvent[] } {
    const auctionId = this.auctionEngine.startAuction(resource.id);
    
    // Submit bid from requestor
    this.auctionEngine.submitBid(auctionId, {
      agentId: demand.agentId,
      resourceId: resource.id,
      bidAmount: demand.utilityPerUnit * demand.quantity,
      quantity: demand.quantity,
      maxPrice: demand.utilityPerUnit * demand.quantity * 2,
      timestamp: Date.now(),
    });
    
    // Submit bids from current holders (they bid their current utility)
    for (const alloc of currentAllocs) {
      this.auctionEngine.submitBid(auctionId, {
        agentId: alloc.agentId,
        resourceId: resource.id,
        bidAmount: alloc.quantity * 0.8, // incumbent discount
        quantity: alloc.quantity,
        maxPrice: alloc.quantity * 1.5,
        timestamp: Date.now(),
      });
    }
    
    const result = this.auctionEngine.resolveAuction(auctionId);
    if (result && result.winnerId === demand.agentId) {
      events.push({
        type: 'auction-completed',
        resourceId: resource.id,
        agents: [demand.agentId],
        details: { priceCharged: result.priceCharged, winningBid: result.winningBid },
        timestamp: Date.now(),
      });
      const allocation = this.grantAllocation(demand, resource);
      return { granted: true, allocation, events };
    }
    
    return { granted: false, events };
  }
  
  private resolveViaBargaining(
    demand: AgentDemand,
    resource: ResourceDescriptor,
    currentAllocs: Allocation[],
    events: ContentionEvent[]
  ): { granted: boolean; allocation?: Allocation; events: ContentionEvent[] } {
    const demands: AgentDemand[] = [demand];
    for (const alloc of currentAllocs) {
      demands.push({
        agentId: alloc.agentId,
        resourceId: resource.id,
        quantity: alloc.quantity,
        priority: alloc.preemptible ? 3 : 7,
        flexibility: 0.3,
        utilityPerUnit: 1,
      });
    }
    
    const solution = this.bargainer.solve(resource.id, resource.capacity, demands);
    
    events.push({
      type: 'bargain-reached',
      resourceId: resource.id,
      agents: [...solution.allocations.keys()],
      details: {
        nashProduct: solution.nashProduct,
        paretoOptimal: solution.paretoOptimal,
        allocations: Object.fromEntries(solution.allocations),
      },
      timestamp: Date.now(),
    });
    
    const grantedAmount = solution.allocations.get(demand.agentId) ?? 0;
    if (grantedAmount > 0) {
      // Adjust existing allocations
      for (const alloc of currentAllocs) {
        const newAmount = solution.allocations.get(alloc.agentId);
        if (newAmount !== undefined) {
          alloc.quantity = newAmount;
        }
      }
      
      const allocation = this.grantAllocation(
        { ...demand, quantity: grantedAmount },
        resource
      );
      return { granted: true, allocation, events };
    }
    
    return { granted: false, events };
  }
  
  private resolveViaPriority(
    demand: AgentDemand,
    resource: ResourceDescriptor,
    currentAllocs: Allocation[],
    events: ContentionEvent[]
  ): { granted: boolean; allocation?: Allocation; waitDieDecision?: WaitDieDecision; events: ContentionEvent[] } {
    // Try preemption
    if (this.config.preemptionEnabled) {
      const preemptDecision = this.preemptionManager.evaluate(demand, currentAllocs, resource);
      if (preemptDecision.preempt && preemptDecision.victimId) {
        // Preempt victim
        const victimAllocs = currentAllocs.filter(a => a.agentId === preemptDecision.victimId);
        for (const va of victimAllocs) {
          this.revokeAllocation(va.agentId, resource.id);
        }
        
        this.preemptionManager.recordPreemption(
          preemptDecision.victimId, demand.agentId, resource.id, demand.quantity
        );
        
        events.push({
          type: 'preemption',
          resourceId: resource.id,
          agents: [demand.agentId, preemptDecision.victimId],
          details: {
            reason: preemptDecision.reason,
            compensation: preemptDecision.compensationUnits,
          },
          timestamp: Date.now(),
        });
        
        const allocation = this.grantAllocation(demand, resource);
        return { granted: true, allocation, events };
      }
    }
    
    // Fall back to Wait-Die
    if (!resource.divisible) {
      this.waitDie.registerAgent(demand.agentId);
      const decision = this.waitDie.requestResource(demand.agentId, resource.id);
      
      if (decision.action === 'die') {
        events.push({
          type: 'deadlock-prevented',
          resourceId: resource.id,
          agents: [demand.agentId],
          details: { decision: 'die', reason: decision.reason },
          timestamp: Date.now(),
        });
      }
      
      return { granted: decision.action === 'granted', waitDieDecision: decision, events };
    }
    
    return { granted: false, events };
  }
  
  private resolveViaHybrid(
    demand: AgentDemand,
    resource: ResourceDescriptor,
    currentAllocs: Allocation[],
    events: ContentionEvent[]
  ): { granted: boolean; allocation?: Allocation; events: ContentionEvent[] } {
    // High priority: use priority + preemption
    if (demand.priority >= 8) {
      return this.resolveViaPriority(demand, resource, currentAllocs, events);
    }
    
    // Divisible resource: use bargaining
    if (resource.divisible) {
      return this.resolveViaBargaining(demand, resource, currentAllocs, events);
    }
    
    // Indivisible: use auction
    return this.resolveViaAuction(demand, resource, currentAllocs, events);
  }
  
  private grantAllocation(demand: AgentDemand, resource: ResourceDescriptor): Allocation {
    const allocation: Allocation = {
      agentId: demand.agentId,
      resourceId: resource.id,
      quantity: demand.quantity,
      grantedAt: Date.now(),
      expiresAt: Date.now() + this.config.maxAllocationDurationMs,
      preemptible: demand.priority < 5,
    };
    
    const allocs = this.allocations.get(resource.id) ?? [];
    allocs.push(allocation);
    this.allocations.set(resource.id, allocs);
    
    this.demandTracker.recordDemand(demand.agentId, resource.id, demand.quantity, true);
    this.starvationDetector.recordAllocation(demand.agentId, resource.id, demand.quantity);
    
    return allocation;
  }
  
  releaseAllocation(agentId: string, resourceId: string): boolean {
    const allocs = this.allocations.get(resourceId);
    if (!allocs) return false;
    
    const idx = allocs.findIndex(a => a.agentId === agentId);
    if (idx === -1) return false;
    
    allocs.splice(idx, 1);
    this.waitDie.releaseResource(agentId, resourceId);
    return true;
  }
  
  private revokeAllocation(agentId: string, resourceId: string): void {
    const allocs = this.allocations.get(resourceId);
    if (!allocs) return;
    
    const idx = allocs.findIndex(a => a.agentId === agentId);
    if (idx !== -1) allocs.splice(idx, 1);
  }
  
  private expireAllocations(resourceId: string): void {
    const allocs = this.allocations.get(resourceId);
    if (!allocs) return;
    
    const now = Date.now();
    const active = allocs.filter(a => !a.expiresAt || a.expiresAt > now);
    this.allocations.set(resourceId, active);
  }
  
  checkStarvation(resourceId: string): StarvationReport {
    const report = this.starvationDetector.analyze(resourceId, this.demandTracker);
    
    if (report.correctionNeeded) {
      this.events.push({
        type: 'starvation-detected',
        resourceId,
        agents: report.starvedAgents.map(a => a.agentId),
        details: {
          gini: report.giniCoefficient,
          severity: report.severity,
          starvedCount: report.starvedAgents.length,
        },
        timestamp: Date.now(),
      });
    }
    
    return report;
  }
  
  predictContention(resourceId: string): ContentionForecast {
    const resource = this.resources.get(resourceId);
    if (!resource) {
      return {
        resourceId,
        currentDemand: 0,
        predictedDemand: 0,
        capacity: 0,
        contentionRatio: 0,
        timeToContention: null,
        confidence: 0,
        trending: 'stable',
      };
    }
    
    const forecast = this.contentionPredictor.predict(
      resourceId,
      resource.capacity,
      this.demandTracker,
      this.config.contentionPredictionHorizonMs
    );
    
    if (forecast.trending === 'rising' && forecast.contentionRatio > 0.8) {
      this.events.push({
        type: 'demand-spike',
        resourceId,
        agents: [],
        details: {
          currentDemand: forecast.currentDemand,
          predictedDemand: forecast.predictedDemand,
          timeToContention: forecast.timeToContention,
        },
        timestamp: Date.now(),
      });
    }
    
    return forecast;
  }
  
  setBudget(agentId: string, resourceId: string, amount: number, burst?: number): void {
    this.budgetPlanner.setBudget(agentId, resourceId, amount, burst);
  }
  
  getResourceStatus(resourceId: string): {
    resource?: ResourceDescriptor;
    allocations: Allocation[];
    usedCapacity: number;
    availableCapacity: number;
    demandProfiles: DemandProfile[];
    starvation: StarvationReport;
    forecast: ContentionForecast;
  } {
    this.expireAllocations(resourceId);
    const resource = this.resources.get(resourceId);
    const allocs = this.allocations.get(resourceId) ?? [];
    const used = allocs.reduce((s, a) => s + a.quantity, 0);
    
    return {
      resource,
      allocations: [...allocs],
      usedCapacity: used,
      availableCapacity: (resource?.capacity ?? 0) - used,
      demandProfiles: this.demandTracker.getResourceDemand(resourceId),
      starvation: this.checkStarvation(resourceId),
      forecast: this.predictContention(resourceId),
    };
  }
  
  getEvents(since?: number): ContentionEvent[] {
    if (since) return this.events.filter(e => e.timestamp > since);
    return [...this.events];
  }
  
  tick(): {
    starvationReports: StarvationReport[];
    forecasts: ContentionForecast[];
    expirations: number;
  } {
    const starvationReports: StarvationReport[] = [];
    const forecasts: ContentionForecast[] = [];
    let expirations = 0;
    
    for (const [resourceId] of this.resources) {
      // Expire old allocations
      const before = (this.allocations.get(resourceId) ?? []).length;
      this.expireAllocations(resourceId);
      const after = (this.allocations.get(resourceId) ?? []).length;
      expirations += before - after;
      
      // Check starvation
      const report = this.checkStarvation(resourceId);
      starvationReports.push(report);
      
      // Predict contention
      const forecast = this.predictContention(resourceId);
      forecasts.push(forecast);
    }
    
    // Trim event log
    if (this.events.length > 1000) {
      this.events = this.events.slice(-500);
    }
    
    return { starvationReports, forecasts, expirations };
  }
}

// ── Exports ────────────────────────────────────────────────────────────

export {
  ResourceContentionArbiter,
  ResourceDemandTracker,
  AuctionEngine,
  CooperativeBargainer,
  StarvationDetector,
  ContentionPredictor,
  WaitDieProtocol,
  ResourceBudgetPlanner,
  PreemptionManager,
  ARBITER_PRESETS,
};

export type {
  ResourceDescriptor,
  AgentDemand,
  Allocation,
  AuctionBid,
  AuctionResult,
  BargainingSolution,
  ContentionEvent,
  ContentionForecast,
  StarvationReport,
  WaitDieDecision,
  ResourceBudget,
  PreemptionDecision,
  DemandProfile,
  ArbiterConfig,
};
