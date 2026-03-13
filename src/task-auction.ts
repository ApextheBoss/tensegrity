/**
 * Agent Task Auction Scheduler
 * 
 * Combinatorial task-to-agent assignment via sealed-bid auctions with
 * VCG pricing, bundle bidding, and anti-collusion mechanisms.
 * 
 * Components:
 * - TaskAuctioneer: orchestrates auction rounds with configurable timing
 * - BundleBidManager: supports combinatorial bids across task groups
 * - VCGPaymentEngine: incentive-compatible externality-based pricing
 * - BidScreener: anti-collusion detection (market allocation, bid suppression, shill)
 * - CapacityValidator: ensures bidders can actually fulfill won tasks
 * - AuctionResultCache: memoizes allocation results for repeated queries
 * - WinnerDetermination: branch-and-bound optimal allocation solver
 * - RevenueTracker: auction revenue analytics with trend detection
 */

// ─── Utilities ───────────────────────────────────────────────────────────────

function fnv1aHash(str: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = (h * 0x01000193) >>> 0;
  }
  return h;
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

function updateWelford(s: WelfordStats, x: number): void {
  s.count++;
  const delta = x - s.mean;
  s.mean += delta / s.count;
  const delta2 = x - s.mean;
  s.m2 += delta * delta2;
}

function welfordVariance(s: WelfordStats): number {
  return s.count < 2 ? 0 : s.m2 / (s.count - 1);
}

// ─── Types ───────────────────────────────────────────────────────────────────

type AuctionPhase = 'collecting' | 'screening' | 'solving' | 'pricing' | 'announcing' | 'completed' | 'cancelled';
type BidStatus = 'submitted' | 'screened' | 'accepted' | 'rejected' | 'won' | 'lost';
type CollusionPattern = 'market_allocation' | 'bid_suppression' | 'shill_bidding' | 'complementary_bidding' | 'bid_rotation';
type EventType =
  | 'auction_opened' | 'auction_closed' | 'bid_submitted' | 'bid_screened'
  | 'allocation_computed' | 'payment_computed' | 'winner_announced'
  | 'collusion_detected' | 'capacity_violation' | 'auction_cancelled'
  | 'revenue_recorded' | 'cache_hit';

interface AuctionEvent {
  type: EventType;
  timestamp: number;
  auctionId: string;
  data: Record<string, unknown>;
}

interface TaskLot {
  taskId: string;
  description: string;
  requiredCapabilities: string[];
  estimatedDuration: number; // ms
  reservePrice: number;
  bundleGroup?: string; // tasks in same group can be bid together
  priority: number;
  deadline?: number;
}

interface AgentBidder {
  agentId: string;
  capabilities: string[];
  maxConcurrency: number;
  currentLoad: number;
  reliabilityScore: number; // 0-1
  historicalLatency: number; // ms average
}

interface Bid {
  bidId: string;
  auctionId: string;
  bidderId: string;
  taskIds: string[]; // single task or bundle
  amount: number; // willingness to pay (or accept, depending on auction direction)
  qualityScore: number; // 0-1 self-reported quality commitment
  estimatedCompletion: number; // ms
  status: BidStatus;
  submittedAt: number;
  screeningResults?: ScreeningResult;
}

interface ScreeningResult {
  passed: boolean;
  capacityValid: boolean;
  collusionFlags: CollusionFlag[];
  adjustedScore: number; // quality * reliability * capacity factor
}

interface CollusionFlag {
  pattern: CollusionPattern;
  severity: number; // 0-1
  involvedBidders: string[];
  evidence: string;
}

interface Allocation {
  auctionId: string;
  assignments: Assignment[];
  totalValue: number;
  socialWelfare: number;
  computedAt: number;
}

interface Assignment {
  bidderId: string;
  taskIds: string[];
  bidAmount: number;
  vcgPayment: number;
  surplus: number; // bidAmount - vcgPayment (bidder surplus)
}

interface AuctionConfig {
  maxBidsPerAgent: number;
  maxBundleSize: number;
  biddingWindowMs: number;
  screeningEnabled: boolean;
  vcgEnabled: boolean;
  reservePriceEnforced: boolean;
  collusionThreshold: number; // 0-1
  capacityMargin: number; // fraction of capacity to reserve
  maxSolverBacktracks: number;
  cacheResultsTTL: number; // ms
}

// ─── Task Auctioneer ─────────────────────────────────────────────────────────

interface AuctionRound {
  auctionId: string;
  phase: AuctionPhase;
  lots: Map<string, TaskLot>;
  bidders: Map<string, AgentBidder>;
  bids: Map<string, Bid>;
  allocation?: Allocation;
  openedAt: number;
  closedAt?: number;
  config: AuctionConfig;
}

export class TaskAuctioneer {
  private auctions: Map<string, AuctionRound> = new Map();
  private auctionHistory: AuctionRound[] = [];
  private maxHistory = 100;

  createAuction(lots: TaskLot[], config: AuctionConfig): AuctionRound {
    const auctionId = `auction_${Date.now()}_${fnv1aHash(lots.map(l => l.taskId).join(','))}`;
    const lotMap = new Map<string, TaskLot>();
    for (const lot of lots) lotMap.set(lot.taskId, lot);

    const round: AuctionRound = {
      auctionId,
      phase: 'collecting',
      lots: lotMap,
      bidders: new Map(),
      bids: new Map(),
      openedAt: Date.now(),
      config,
    };
    this.auctions.set(auctionId, round);
    return round;
  }

  registerBidder(auctionId: string, bidder: AgentBidder): boolean {
    const auction = this.auctions.get(auctionId);
    if (!auction || auction.phase !== 'collecting') return false;
    auction.bidders.set(bidder.agentId, bidder);
    return true;
  }

  submitBid(auctionId: string, bid: Omit<Bid, 'bidId' | 'auctionId' | 'status' | 'submittedAt'>): Bid | null {
    const auction = this.auctions.get(auctionId);
    if (!auction || auction.phase !== 'collecting') return null;

    const bidder = auction.bidders.get(bid.bidderId);
    if (!bidder) return null;

    // Check bid limits
    const agentBids = Array.from(auction.bids.values()).filter(b => b.bidderId === bid.bidderId);
    if (agentBids.length >= auction.config.maxBidsPerAgent) return null;

    // Check bundle size
    if (bid.taskIds.length > auction.config.maxBundleSize) return null;

    // Validate all tasks exist
    if (!bid.taskIds.every(id => auction.lots.has(id))) return null;

    const fullBid: Bid = {
      ...bid,
      bidId: `bid_${fnv1aHash(`${bid.bidderId}_${bid.taskIds.join(',')}_${Date.now()}`)}`,
      auctionId,
      status: 'submitted',
      submittedAt: Date.now(),
    };

    auction.bids.set(fullBid.bidId, fullBid);
    return fullBid;
  }

  closeBidding(auctionId: string): boolean {
    const auction = this.auctions.get(auctionId);
    if (!auction || auction.phase !== 'collecting') return false;
    auction.phase = 'screening';
    auction.closedAt = Date.now();
    return true;
  }

  advancePhase(auctionId: string, phase: AuctionPhase): void {
    const auction = this.auctions.get(auctionId);
    if (auction) auction.phase = phase;
  }

  completeAuction(auctionId: string, allocation: Allocation): void {
    const auction = this.auctions.get(auctionId);
    if (!auction) return;
    auction.allocation = allocation;
    auction.phase = 'completed';
    this.auctionHistory.push(auction);
    if (this.auctionHistory.length > this.maxHistory) this.auctionHistory.shift();
  }

  cancelAuction(auctionId: string): void {
    const auction = this.auctions.get(auctionId);
    if (auction) auction.phase = 'cancelled';
  }

  getAuction(auctionId: string): AuctionRound | undefined {
    return this.auctions.get(auctionId);
  }

  getHistory(): AuctionRound[] {
    return [...this.auctionHistory];
  }

  isExpired(auctionId: string): boolean {
    const auction = this.auctions.get(auctionId);
    if (!auction) return true;
    return auction.phase === 'collecting' && 
      Date.now() - auction.openedAt > auction.config.biddingWindowMs;
  }
}

// ─── Capacity Validator ──────────────────────────────────────────────────────

class CapacityValidator {
  validate(bidder: AgentBidder, taskIds: string[], lots: Map<string, TaskLot>, margin: number): {
    valid: boolean;
    availableSlots: number;
    requiredSlots: number;
    missingCapabilities: string[];
  } {
    const availableSlots = Math.floor((bidder.maxConcurrency - bidder.currentLoad) * (1 - margin));
    const requiredSlots = taskIds.length;

    const missingCapabilities: string[] = [];
    for (const taskId of taskIds) {
      const lot = lots.get(taskId);
      if (!lot) continue;
      for (const cap of lot.requiredCapabilities) {
        if (!bidder.capabilities.includes(cap) && !missingCapabilities.includes(cap)) {
          missingCapabilities.push(cap);
        }
      }
    }

    return {
      valid: availableSlots >= requiredSlots && missingCapabilities.length === 0,
      availableSlots,
      requiredSlots,
      missingCapabilities,
    };
  }

  validateBundleCoherence(taskIds: string[], lots: Map<string, TaskLot>): boolean {
    if (taskIds.length <= 1) return true;
    // Bundle tasks should share a bundle group or have no group
    const groups = new Set<string>();
    for (const id of taskIds) {
      const lot = lots.get(id);
      if (lot?.bundleGroup) groups.add(lot.bundleGroup);
    }
    return groups.size <= 1;
  }
}

// ─── Bid Screener ────────────────────────────────────────────────────────────

class BidScreener {
  private bidHistory: Map<string, Bid[]> = new Map(); // bidderId -> past bids
  private maxHistoryPerBidder = 50;

  screenBid(
    bid: Bid,
    allBids: Bid[],
    bidder: AgentBidder,
    lots: Map<string, TaskLot>,
    config: AuctionConfig,
    capacityValidator: CapacityValidator
  ): ScreeningResult {
    const collusionFlags: CollusionFlag[] = [];

    // 1. Capacity validation
    const capResult = capacityValidator.validate(bid.bidderId === bidder.agentId ? bidder : bidder, bid.taskIds, lots, config.capacityMargin);
    
    // 2. Collusion detection
    if (config.screeningEnabled) {
      this.detectMarketAllocation(bid, allBids, collusionFlags, config.collusionThreshold);
      this.detectBidSuppression(bid, allBids, collusionFlags, config.collusionThreshold);
      this.detectShillBidding(bid, allBids, lots, collusionFlags, config.collusionThreshold);
      this.detectBidRotation(bid, allBids, collusionFlags, config.collusionThreshold);
    }

    // 3. Reserve price check
    let reserveOk = true;
    if (config.reservePriceEnforced) {
      for (const taskId of bid.taskIds) {
        const lot = lots.get(taskId);
        if (lot && bid.amount / bid.taskIds.length < lot.reservePrice) {
          reserveOk = false;
          break;
        }
      }
    }

    const maxSeverity = collusionFlags.length > 0 
      ? Math.max(...collusionFlags.map(f => f.severity))
      : 0;
    
    const adjustedScore = bid.qualityScore * bidder.reliabilityScore * 
      (capResult.valid ? 1 : 0.5) * (1 - maxSeverity * 0.5);

    const passed = capResult.valid && reserveOk && maxSeverity < config.collusionThreshold;

    // Record bid history
    if (!this.bidHistory.has(bid.bidderId)) this.bidHistory.set(bid.bidderId, []);
    const history = this.bidHistory.get(bid.bidderId)!;
    history.push(bid);
    if (history.length > this.maxHistoryPerBidder) history.shift();

    return { passed, capacityValid: capResult.valid, collusionFlags, adjustedScore };
  }

  private detectMarketAllocation(bid: Bid, allBids: Bid[], flags: CollusionFlag[], threshold: number): void {
    // Market allocation: bidders consistently avoid each other's "territory"
    const otherBidders = new Set(allBids.filter(b => b.bidderId !== bid.bidderId).map(b => b.bidderId));
    
    for (const otherId of otherBidders) {
      const otherBids = allBids.filter(b => b.bidderId === otherId);
      const myTasks = new Set(bid.taskIds);
      const otherTasks = new Set(otherBids.flatMap(b => b.taskIds));
      
      // Jaccard similarity
      const intersection = new Set([...myTasks].filter(t => otherTasks.has(t)));
      const union = new Set([...myTasks, ...otherTasks]);
      const jaccard = union.size > 0 ? intersection.size / union.size : 0;

      // Very low overlap across many tasks suggests allocation
      if (jaccard < 0.1 && union.size >= 4) {
        const severity = (1 - jaccard) * 0.5;
        if (severity >= threshold * 0.5) {
          flags.push({
            pattern: 'market_allocation',
            severity,
            involvedBidders: [bid.bidderId, otherId],
            evidence: `Jaccard overlap ${jaccard.toFixed(3)} across ${union.size} tasks`,
          });
        }
      }
    }
  }

  private detectBidSuppression(bid: Bid, allBids: Bid[], flags: CollusionFlag[], threshold: number): void {
    // Bid suppression: other bidders place abnormally low bids on same tasks
    const competingBids = allBids.filter(b => 
      b.bidderId !== bid.bidderId && 
      b.taskIds.some(t => bid.taskIds.includes(t))
    );

    if (competingBids.length < 2) return;

    const amounts = competingBids.map(b => b.amount / b.taskIds.length);
    const myPerTask = bid.amount / bid.taskIds.length;
    const avgCompeting = amounts.reduce((a, b) => a + b, 0) / amounts.length;

    // If competing bids are consistently much lower than ours
    if (avgCompeting < myPerTask * 0.3 && amounts.every(a => a < myPerTask * 0.5)) {
      const severity = 1 - (avgCompeting / myPerTask);
      if (severity >= threshold * 0.5) {
        flags.push({
          pattern: 'bid_suppression',
          severity: Math.min(severity, 1),
          involvedBidders: competingBids.map(b => b.bidderId),
          evidence: `Avg competing bid ${avgCompeting.toFixed(2)} vs ${myPerTask.toFixed(2)} per task`,
        });
      }
    }
  }

  private detectShillBidding(bid: Bid, allBids: Bid[], lots: Map<string, TaskLot>, flags: CollusionFlag[], threshold: number): void {
    // Shill: bids just below reserve price to make winner look better
    for (const taskId of bid.taskIds) {
      const lot = lots.get(taskId);
      if (!lot) continue;

      const competingOnTask = allBids.filter(b => 
        b.bidderId !== bid.bidderId && b.taskIds.includes(taskId)
      );

      const justBelowReserve = competingOnTask.filter(b => {
        const perTask = b.amount / b.taskIds.length;
        return perTask >= lot.reservePrice * 0.9 && perTask <= lot.reservePrice * 1.05;
      });

      if (justBelowReserve.length >= 2) {
        flags.push({
          pattern: 'shill_bidding',
          severity: 0.6,
          involvedBidders: justBelowReserve.map(b => b.bidderId),
          evidence: `${justBelowReserve.length} bids clustered near reserve for task ${taskId}`,
        });
      }
    }
  }

  private detectBidRotation(bid: Bid, allBids: Bid[], flags: CollusionFlag[], threshold: number): void {
    // Bid rotation: check historical patterns for suspicious turn-taking
    const bidderIds = new Set(allBids.map(b => b.bidderId));
    if (bidderIds.size < 3) return;

    // Check if highest bidder rotates in a pattern
    const taskHighBids = new Map<string, string>();
    for (const b of allBids) {
      for (const taskId of b.taskIds) {
        const existing = taskHighBids.get(taskId);
        const existingBid = existing ? allBids.find(ab => ab.bidderId === existing && ab.taskIds.includes(taskId)) : null;
        if (!existing || !existingBid || b.amount / b.taskIds.length > existingBid.amount / existingBid.taskIds.length) {
          taskHighBids.set(taskId, b.bidderId);
        }
      }
    }

    // If each bidder wins exactly their "share", suspicious
    const winCounts = new Map<string, number>();
    for (const winner of taskHighBids.values()) {
      winCounts.set(winner, (winCounts.get(winner) || 0) + 1);
    }

    if (winCounts.size >= 3) {
      const counts = Array.from(winCounts.values());
      const mean = counts.reduce((a, b) => a + b, 0) / counts.length;
      const variance = counts.reduce((a, b) => a + (b - mean) ** 2, 0) / counts.length;
      const cv = mean > 0 ? Math.sqrt(variance) / mean : 0;

      // Very even distribution is suspicious
      if (cv < 0.15 && counts.length >= 3) {
        flags.push({
          pattern: 'bid_rotation',
          severity: 0.5 * (1 - cv),
          involvedBidders: Array.from(winCounts.keys()),
          evidence: `Win distribution CV=${cv.toFixed(3)} across ${counts.length} bidders (suspiciously even)`,
        });
      }
    }
  }
}

// ─── Winner Determination (Branch-and-Bound) ─────────────────────────────────

class WinnerDetermination {
  private maxBacktracks: number;

  constructor(maxBacktracks: number) {
    this.maxBacktracks = maxBacktracks;
  }

  solve(bids: Bid[], lots: Map<string, TaskLot>): { assignments: Map<string, Bid>; totalValue: number } {
    const eligibleBids = bids.filter(b => b.status === 'screened' && b.screeningResults?.passed);
    const taskIds = Array.from(lots.keys());
    
    let bestAssignment = new Map<string, Bid>(); // taskId -> bid
    let bestValue = 0;
    let backtracks = 0;

    const assigned = new Map<string, Bid>();
    const usedBidders = new Map<string, number>(); // bidderId -> tasks assigned count

    const search = (taskIdx: number, currentValue: number): void => {
      if (backtracks >= this.maxBacktracks) return;

      if (taskIdx >= taskIds.length) {
        if (currentValue > bestValue) {
          bestValue = currentValue;
          bestAssignment = new Map(assigned);
        }
        return;
      }

      const taskId = taskIds[taskIdx];

      // Upper bound: current value + sum of best remaining individual bids
      let upperBound = currentValue;
      for (let i = taskIdx; i < taskIds.length; i++) {
        const tid = taskIds[i];
        if (assigned.has(tid)) continue;
        let bestBidValue = 0;
        for (const bid of eligibleBids) {
          if (bid.taskIds.includes(tid)) {
            const perTask = (bid.amount * (bid.screeningResults?.adjustedScore || 1)) / bid.taskIds.length;
            bestBidValue = Math.max(bestBidValue, perTask);
          }
        }
        upperBound += bestBidValue;
      }

      if (upperBound <= bestValue) {
        backtracks++;
        return; // Prune
      }

      // Try assigning this task to each eligible bid
      for (const bid of eligibleBids) {
        if (!bid.taskIds.includes(taskId)) continue;

        // Check if all tasks in this bid's bundle are available
        const bundleTasks = bid.taskIds;
        const allAvailable = bundleTasks.every(t => !assigned.has(t) || assigned.get(t) === bid);
        if (!allAvailable) continue;

        // Check bidder concurrency
        const currentBidderLoad = usedBidders.get(bid.bidderId) || 0;
        if (currentBidderLoad + bundleTasks.filter(t => !assigned.has(t)).length > 10) continue; // soft cap

        // Assign entire bundle
        const newlyAssigned: string[] = [];
        for (const t of bundleTasks) {
          if (!assigned.has(t)) {
            assigned.set(t, bid);
            newlyAssigned.push(t);
          }
        }
        const addedLoad = newlyAssigned.length;
        usedBidders.set(bid.bidderId, currentBidderLoad + addedLoad);

        const bidValue = (bid.amount * (bid.screeningResults?.adjustedScore || 1));
        const addedValue = assigned.get(taskId) === bid ? bidValue / bundleTasks.length : 0;

        search(taskIdx + 1, currentValue + addedValue);

        // Undo
        for (const t of newlyAssigned) assigned.delete(t);
        usedBidders.set(bid.bidderId, currentBidderLoad);
        backtracks++;
      }

      // Try leaving task unassigned
      search(taskIdx + 1, currentValue);
    };

    search(0, 0);
    return { assignments: bestAssignment, totalValue: bestValue };
  }
}

// ─── VCG Payment Engine ──────────────────────────────────────────────────────

class VCGPaymentEngine {
  private solver: WinnerDetermination;

  constructor(solver: WinnerDetermination) {
    this.solver = solver;
  }

  computePayments(
    winners: Map<string, Bid>,
    allBids: Bid[],
    lots: Map<string, TaskLot>
  ): Map<string, number> {
    const payments = new Map<string, number>();

    // Get unique winning bidders
    const winnerBidders = new Set<string>();
    for (const bid of winners.values()) winnerBidders.add(bid.bidderId);

    const totalSocialWelfare = this.computeSocialWelfare(winners);

    for (const winnerId of winnerBidders) {
      // Solve without this winner
      const bidsWithout = allBids.filter(b => b.bidderId !== winnerId);
      const resultWithout = this.solver.solve(bidsWithout, lots);
      const welfareWithout = this.computeSocialWelfare(resultWithout.assignments);

      // Winner's contribution to social welfare
      const winnerBids = Array.from(winners.values()).filter(b => b.bidderId === winnerId);
      const winnerValue = winnerBids.reduce((sum, b) => sum + b.amount * (b.screeningResults?.adjustedScore || 1), 0);
      const othersWelfare = totalSocialWelfare - winnerValue;

      // VCG payment = externality imposed = welfare_others_without_me - welfare_others_with_me
      const vcgPayment = welfareWithout - othersWelfare;
      payments.set(winnerId, Math.max(0, vcgPayment));
    }

    return payments;
  }

  private computeSocialWelfare(assignments: Map<string, Bid>): number {
    const countedBids = new Set<string>();
    let total = 0;
    for (const bid of assignments.values()) {
      if (!countedBids.has(bid.bidId)) {
        total += bid.amount * (bid.screeningResults?.adjustedScore || 1);
        countedBids.add(bid.bidId);
      }
    }
    return total;
  }
}

// ─── Auction Result Cache ────────────────────────────────────────────────────

interface CachedResult {
  allocation: Allocation;
  cachedAt: number;
  fingerprint: number;
}

class AuctionResultCache {
  private cache: Map<string, CachedResult> = new Map();
  private maxEntries: number;
  private ttl: number;

  constructor(maxEntries: number, ttl: number) {
    this.maxEntries = maxEntries;
    this.ttl = ttl;
  }

  private computeFingerprint(bids: Bid[]): number {
    const sorted = [...bids].sort((a, b) => a.bidId.localeCompare(b.bidId));
    return fnv1aHash(sorted.map(b => `${b.bidId}:${b.amount}:${b.status}`).join('|'));
  }

  get(auctionId: string, bids: Bid[]): Allocation | null {
    const entry = this.cache.get(auctionId);
    if (!entry) return null;
    if (Date.now() - entry.cachedAt > this.ttl) {
      this.cache.delete(auctionId);
      return null;
    }
    const fp = this.computeFingerprint(bids);
    if (fp !== entry.fingerprint) return null;
    return entry.allocation;
  }

  set(auctionId: string, bids: Bid[], allocation: Allocation): void {
    if (this.cache.size >= this.maxEntries) {
      // Evict oldest
      let oldestKey = '';
      let oldestTime = Infinity;
      for (const [key, val] of this.cache) {
        if (val.cachedAt < oldestTime) {
          oldestTime = val.cachedAt;
          oldestKey = key;
        }
      }
      if (oldestKey) this.cache.delete(oldestKey);
    }
    this.cache.set(auctionId, {
      allocation,
      cachedAt: Date.now(),
      fingerprint: this.computeFingerprint(bids),
    });
  }

  invalidate(auctionId: string): void {
    this.cache.delete(auctionId);
  }

  prune(): number {
    const now = Date.now();
    let pruned = 0;
    for (const [key, val] of this.cache) {
      if (now - val.cachedAt > this.ttl) {
        this.cache.delete(key);
        pruned++;
      }
    }
    return pruned;
  }
}

// ─── Revenue Tracker ─────────────────────────────────────────────────────────

class RevenueTracker {
  private revenueHistory: { timestamp: number; revenue: number; tasks: number; bidders: number }[] = [];
  private revenueEWMA: EWMATracker = createEWMA(0.3);
  private participationEWMA: EWMATracker = createEWMA(0.3);
  private efficiencyStats: WelfordStats = createWelford();
  private maxHistory = 200;

  record(revenue: number, tasks: number, bidders: number, allocatedTasks: number): void {
    this.revenueHistory.push({ timestamp: Date.now(), revenue, tasks, bidders });
    if (this.revenueHistory.length > this.maxHistory) this.revenueHistory.shift();

    updateEWMA(this.revenueEWMA, revenue);
    updateEWMA(this.participationEWMA, bidders);
    
    const efficiency = tasks > 0 ? allocatedTasks / tasks : 0;
    updateWelford(this.efficiencyStats, efficiency);
  }

  getTrend(): { direction: 'increasing' | 'stable' | 'decreasing'; avgRevenue: number; avgParticipation: number } {
    if (this.revenueHistory.length < 3) {
      return { direction: 'stable', avgRevenue: this.revenueEWMA.value, avgParticipation: this.participationEWMA.value };
    }

    const recent = this.revenueHistory.slice(-10);
    const n = recent.length;
    let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
    for (let i = 0; i < n; i++) {
      sumX += i;
      sumY += recent[i].revenue;
      sumXY += i * recent[i].revenue;
      sumX2 += i * i;
    }
    const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
    const avgY = sumY / n;
    const normalizedSlope = avgY > 0 ? slope / avgY : 0;

    const direction = normalizedSlope > 0.05 ? 'increasing' : normalizedSlope < -0.05 ? 'decreasing' : 'stable';

    return { direction, avgRevenue: this.revenueEWMA.value, avgParticipation: this.participationEWMA.value };
  }

  getAllocationEfficiency(): { mean: number; stddev: number } {
    return {
      mean: this.efficiencyStats.mean,
      stddev: Math.sqrt(welfordVariance(this.efficiencyStats)),
    };
  }
}

// ─── Agent Task Auction Scheduler (Unified Orchestrator) ─────────────────────

interface AuctionSchedulerConfig {
  defaultAuctionConfig: AuctionConfig;
  autoCloseOnExpiry: boolean;
  minBiddersToClose: number;
  revenueTrackingEnabled: boolean;
}

const PRESETS: Record<string, AuctionSchedulerConfig> = {
  'open-marketplace': {
    defaultAuctionConfig: {
      maxBidsPerAgent: 10,
      maxBundleSize: 5,
      biddingWindowMs: 30000,
      screeningEnabled: true,
      vcgEnabled: true,
      reservePriceEnforced: true,
      collusionThreshold: 0.6,
      capacityMargin: 0.1,
      maxSolverBacktracks: 10000,
      cacheResultsTTL: 60000,
    },
    autoCloseOnExpiry: true,
    minBiddersToClose: 2,
    revenueTrackingEnabled: true,
  },
  'trusted-network': {
    defaultAuctionConfig: {
      maxBidsPerAgent: 20,
      maxBundleSize: 10,
      biddingWindowMs: 15000,
      screeningEnabled: false,
      vcgEnabled: false,
      reservePriceEnforced: false,
      collusionThreshold: 0.9,
      capacityMargin: 0.05,
      maxSolverBacktracks: 5000,
      cacheResultsTTL: 30000,
    },
    autoCloseOnExpiry: true,
    minBiddersToClose: 1,
    revenueTrackingEnabled: false,
  },
  'high-security': {
    defaultAuctionConfig: {
      maxBidsPerAgent: 5,
      maxBundleSize: 3,
      biddingWindowMs: 60000,
      screeningEnabled: true,
      vcgEnabled: true,
      reservePriceEnforced: true,
      collusionThreshold: 0.3,
      capacityMargin: 0.2,
      maxSolverBacktracks: 20000,
      cacheResultsTTL: 120000,
    },
    autoCloseOnExpiry: false,
    minBiddersToClose: 3,
    revenueTrackingEnabled: true,
  },
};

export class AgentTaskAuctionScheduler {
  private config: AuctionSchedulerConfig;
  private auctioneer: TaskAuctioneer;
  private screener: BidScreener;
  private capacityValidator: CapacityValidator;
  private solver: WinnerDetermination;
  private vcgEngine: VCGPaymentEngine;
  private resultCache: AuctionResultCache;
  private revenueTracker: RevenueTracker;
  private events: AuctionEvent[] = [];
  private maxEvents = 500;

  constructor(config: AuctionSchedulerConfig) {
    this.config = config;
    this.auctioneer = new TaskAuctioneer();
    this.screener = new BidScreener();
    this.capacityValidator = new CapacityValidator();
    this.solver = new WinnerDetermination(config.defaultAuctionConfig.maxSolverBacktracks);
    this.vcgEngine = new VCGPaymentEngine(this.solver);
    this.resultCache = new AuctionResultCache(50, config.defaultAuctionConfig.cacheResultsTTL);
    this.revenueTracker = new RevenueTracker();
  }

  static fromPreset(preset: string): AgentTaskAuctionScheduler {
    const config = PRESETS[preset];
    if (!config) throw new Error(`Unknown preset: ${preset}`);
    return new AgentTaskAuctionScheduler(config);
  }

  // ── Auction Lifecycle ──────────────────────────────────────────────────────

  openAuction(lots: TaskLot[], configOverrides?: Partial<AuctionConfig>): AuctionRound {
    const config = { ...this.config.defaultAuctionConfig, ...configOverrides };
    const auction = this.auctioneer.createAuction(lots, config);
    this.emit('auction_opened', auction.auctionId, { lotCount: lots.length, taskIds: lots.map(l => l.taskId) });
    return auction;
  }

  registerBidder(auctionId: string, bidder: AgentBidder): boolean {
    return this.auctioneer.registerBidder(auctionId, bidder);
  }

  placeBid(auctionId: string, bidderId: string, taskIds: string[], amount: number, qualityScore: number, estimatedCompletion: number): Bid | null {
    const bid = this.auctioneer.submitBid(auctionId, { bidderId, taskIds, amount, qualityScore, estimatedCompletion });
    if (bid) {
      this.emit('bid_submitted', auctionId, { bidId: bid.bidId, bidderId, taskIds, amount });
    }
    return bid;
  }

  runAuction(auctionId: string): Allocation | null {
    const auction = this.auctioneer.getAuction(auctionId);
    if (!auction) return null;

    // Close bidding
    this.auctioneer.closeBidding(auctionId);
    this.emit('auction_closed', auctionId, { bidCount: auction.bids.size, bidderCount: auction.bidders.size });

    // Check minimum bidders
    if (auction.bidders.size < this.config.minBiddersToClose) {
      this.auctioneer.cancelAuction(auctionId);
      this.emit('auction_cancelled', auctionId, { reason: 'insufficient_bidders' });
      return null;
    }

    // Check cache
    const bids = Array.from(auction.bids.values());
    const cached = this.resultCache.get(auctionId, bids);
    if (cached) {
      this.emit('cache_hit', auctionId, {});
      return cached;
    }

    // Phase: Screening
    this.auctioneer.advancePhase(auctionId, 'screening');
    for (const bid of bids) {
      const bidder = auction.bidders.get(bid.bidderId);
      if (!bidder) {
        bid.status = 'rejected';
        continue;
      }
      bid.screeningResults = this.screener.screenBid(bid, bids, bidder, auction.lots, auction.config, this.capacityValidator);
      bid.status = bid.screeningResults.passed ? 'screened' : 'rejected';
      this.emit('bid_screened', auctionId, {
        bidId: bid.bidId,
        passed: bid.screeningResults.passed,
        collusionFlags: bid.screeningResults.collusionFlags.length,
      });

      if (bid.screeningResults.collusionFlags.length > 0) {
        for (const flag of bid.screeningResults.collusionFlags) {
          this.emit('collusion_detected', auctionId, { pattern: flag.pattern, severity: flag.severity, bidders: flag.involvedBidders });
        }
      }

      if (!bid.screeningResults.capacityValid) {
        this.emit('capacity_violation', auctionId, { bidId: bid.bidId, bidderId: bid.bidderId });
      }
    }

    // Phase: Solving
    this.auctioneer.advancePhase(auctionId, 'solving');
    const { assignments, totalValue } = this.solver.solve(bids, auction.lots);
    this.emit('allocation_computed', auctionId, { assignedTasks: assignments.size, totalValue });

    // Phase: Pricing
    this.auctioneer.advancePhase(auctionId, 'pricing');
    let payments = new Map<string, number>();
    if (auction.config.vcgEnabled) {
      payments = this.vcgEngine.computePayments(assignments, bids, auction.lots);
      this.emit('payment_computed', auctionId, { paymentCount: payments.size });
    }

    // Build allocation
    const assignmentList: Assignment[] = [];
    const bidderTasks = new Map<string, { taskIds: string[]; bid: Bid }>();
    for (const [taskId, bid] of assignments) {
      if (!bidderTasks.has(bid.bidderId)) {
        bidderTasks.set(bid.bidderId, { taskIds: [], bid });
      }
      bidderTasks.get(bid.bidderId)!.taskIds.push(taskId);
    }

    for (const [bidderId, info] of bidderTasks) {
      const vcgPayment = payments.get(bidderId) || info.bid.amount;
      assignmentList.push({
        bidderId,
        taskIds: info.taskIds,
        bidAmount: info.bid.amount,
        vcgPayment,
        surplus: info.bid.amount - vcgPayment,
      });
    }

    const allocation: Allocation = {
      auctionId,
      assignments: assignmentList,
      totalValue,
      socialWelfare: totalValue,
      computedAt: Date.now(),
    };

    // Update bid statuses
    for (const bid of bids) {
      if (bid.status === 'screened') {
        bid.status = assignments.has(bid.taskIds[0]) && assignments.get(bid.taskIds[0]) === bid ? 'won' : 'lost';
      }
    }

    // Phase: Announcing
    this.auctioneer.advancePhase(auctionId, 'announcing');
    this.emit('winner_announced', auctionId, {
      winners: assignmentList.map(a => ({ bidderId: a.bidderId, tasks: a.taskIds.length, payment: a.vcgPayment })),
    });

    // Complete
    this.auctioneer.completeAuction(auctionId, allocation);
    this.resultCache.set(auctionId, bids, allocation);

    // Revenue tracking
    if (this.config.revenueTrackingEnabled) {
      const revenue = assignmentList.reduce((sum, a) => sum + a.vcgPayment, 0);
      this.revenueTracker.record(revenue, auction.lots.size, auction.bidders.size, assignments.size);
      this.emit('revenue_recorded', auctionId, { revenue });
    }

    return allocation;
  }

  // ── Tick ────────────────────────────────────────────────────────────────────

  tick(): { expiredAuctions: string[]; cachePruned: number } {
    const expiredAuctions: string[] = [];

    // Auto-close expired auctions
    if (this.config.autoCloseOnExpiry) {
      for (const auction of this.getActiveAuctions()) {
        if (this.auctioneer.isExpired(auction.auctionId)) {
          const result = this.runAuction(auction.auctionId);
          if (!result) {
            this.auctioneer.cancelAuction(auction.auctionId);
          }
          expiredAuctions.push(auction.auctionId);
        }
      }
    }

    // Prune cache
    const cachePruned = this.resultCache.prune();

    return { expiredAuctions, cachePruned };
  }

  // ── Queries ─────────────────────────────────────────────────────────────────

  getActiveAuctions(): AuctionRound[] {
    const result: AuctionRound[] = [];
    const auctions = this.auctioneer as any;
    if (auctions.auctions) {
      for (const auction of (auctions.auctions as Map<string, AuctionRound>).values()) {
        if (auction.phase === 'collecting') result.push(auction);
      }
    }
    return result;
  }

  getRevenueTrend(): ReturnType<RevenueTracker['getTrend']> {
    return this.revenueTracker.getTrend();
  }

  getAllocationEfficiency(): ReturnType<RevenueTracker['getAllocationEfficiency']> {
    return this.revenueTracker.getAllocationEfficiency();
  }

  getAuctionHistory(): AuctionRound[] {
    return this.auctioneer.getHistory();
  }

  getDashboard(): {
    activeAuctions: number;
    completedAuctions: number;
    revenueTrend: ReturnType<RevenueTracker['getTrend']>;
    efficiency: ReturnType<RevenueTracker['getAllocationEfficiency']>;
    recentEvents: AuctionEvent[];
  } {
    return {
      activeAuctions: this.getActiveAuctions().length,
      completedAuctions: this.getAuctionHistory().length,
      revenueTrend: this.getRevenueTrend(),
      efficiency: this.getAllocationEfficiency(),
      recentEvents: this.events.slice(-20),
    };
  }

  // ── Events ──────────────────────────────────────────────────────────────────

  private emit(type: EventType, auctionId: string, data: Record<string, unknown>): void {
    this.events.push({ type, timestamp: Date.now(), auctionId, data });
    if (this.events.length > this.maxEvents) this.events.shift();
  }

  getEvents(since?: number): AuctionEvent[] {
    if (since) return this.events.filter(e => e.timestamp >= since);
    return [...this.events];
  }
}

export {
  AgentTaskAuctionScheduler,
  TaskAuctioneer,
  BidScreener,
  CapacityValidator,
  WinnerDetermination,
  VCGPaymentEngine,
  AuctionResultCache,
  RevenueTracker,
  PRESETS,
};
