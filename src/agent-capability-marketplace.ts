import { fnv1aHash } from './shared-utils';
/**
 * Agent Capability Marketplace
 * 
 * Decentralized marketplace for agent capabilities: listing, discovery,
 * pricing, escrow, and fulfillment of agent-to-agent service contracts.
 * 
 * Components:
 * - ListingManager: publish/update/delist capability offerings with pricing
 * - PricingEngine: dynamic pricing (fixed/auction/demand-curve/subscription)
 * - EscrowController: hold-release payment with milestone verification
 * - MatchingEngine: buyer-seller matching with multi-factor scoring
 * - ReputationGate: minimum rep thresholds for listing/buying
 * - DisputeArbitrator: claim-evidence-ruling pipeline with appeal
 * - UsageMeterer: track consumption for pay-per-use capabilities
 * - MarketplaceOrchestrator: unified lifecycle management
 */

// ── Helpers ──────────────────────────────────────────────────────────────

function generateId(prefix: string): string {
  const ts = Date.now().toString(36);
  const rnd = fnv1aHash(`${prefix}-${ts}-${Math.random()}`).toString(36);
  return `${prefix}_${ts}_${rnd}`;
}

// ── Types ────────────────────────────────────────────────────────────────

interface CapabilityListing {
  id: string;
  sellerId: string;
  capability: string;
  version: string;
  description: string;
  tags: string[];
  pricingModel: PricingModel;
  status: 'draft' | 'active' | 'paused' | 'delisted';
  minReputation: number;
  maxConcurrent: number;
  currentConcurrent: number;
  sla: SLATerms;
  createdAt: number;
  updatedAt: number;
  totalFulfilled: number;
  totalRevenue: number;
  averageRating: number;
  ratingCount: number;
}

type PricingModel =
  | { type: 'fixed'; pricePerCall: number }
  | { type: 'auction'; startPrice: number; minPrice: number; decrementPerSec: number }
  | { type: 'demand-curve'; basePrice: number; elasticity: number; currentDemand: number }
  | { type: 'subscription'; pricePerPeriod: number; periodMs: number; callsIncluded: number; overage: number }
  | { type: 'tiered'; tiers: Array<{ upTo: number; pricePerCall: number }> };

interface SLATerms {
  maxLatencyMs: number;
  availability: number; // 0-1
  maxErrorRate: number; // 0-1
  penaltyPerViolation: number;
}

interface ServiceContract {
  id: string;
  listingId: string;
  buyerId: string;
  sellerId: string;
  status: 'pending' | 'active' | 'completed' | 'disputed' | 'cancelled';
  pricing: PricingModel;
  escrowAmount: number;
  paidAmount: number;
  milestones: Milestone[];
  startedAt: number;
  completedAt?: number;
  slaViolations: number;
}

interface Milestone {
  id: string;
  description: string;
  amount: number;
  status: 'pending' | 'in-progress' | 'submitted' | 'accepted' | 'rejected';
  submittedAt?: number;
  evidence?: string;
}

interface EscrowAccount {
  contractId: string;
  buyerId: string;
  sellerId: string;
  totalDeposited: number;
  totalReleased: number;
  totalRefunded: number;
  holds: Array<{ milestoneId: string; amount: number; heldAt: number }>;
  status: 'active' | 'releasing' | 'refunding' | 'closed';
}

interface UsageRecord {
  contractId: string;
  callId: string;
  timestamp: number;
  latencyMs: number;
  success: boolean;
  inputSize: number;
  outputSize: number;
  cost: number;
}

interface Dispute {
  id: string;
  contractId: string;
  filedBy: string;
  againstAgent: string;
  reason: string;
  evidence: string[];
  status: 'filed' | 'under-review' | 'ruling' | 'resolved' | 'appealed';
  ruling?: { inFavorOf: string; refundAmount: number; penaltyAmount: number };
  arbitrators: string[];
  filedAt: number;
  resolvedAt?: number;
}

interface MarketEvent {
  type: string;
  timestamp: number;
  data: Record<string, unknown>;
}

// ── Listing Manager ──────────────────────────────────────────────────────

class ListingManager {
  private listings = new Map<string, CapabilityListing>();
  private byCapability = new Map<string, Set<string>>(); // capability -> listing IDs
  private bySeller = new Map<string, Set<string>>(); // seller -> listing IDs

  publish(params: {
    sellerId: string;
    capability: string;
    version: string;
    description: string;
    tags: string[];
    pricingModel: PricingModel;
    maxConcurrent: number;
    sla: SLATerms;
    minReputation?: number;
  }): CapabilityListing {
    const listing: CapabilityListing = {
      id: generateId('lst'),
      sellerId: params.sellerId,
      capability: params.capability,
      version: params.version,
      description: params.description,
      tags: params.tags,
      pricingModel: params.pricingModel,
      status: 'active',
      minReputation: params.minReputation ?? 0,
      maxConcurrent: params.maxConcurrent,
      currentConcurrent: 0,
      sla: params.sla,
      createdAt: Date.now(),
      updatedAt: Date.now(),
      totalFulfilled: 0,
      totalRevenue: 0,
      averageRating: 0,
      ratingCount: 0,
    };
    this.listings.set(listing.id, listing);
    if (!this.byCapability.has(params.capability)) {
      this.byCapability.set(params.capability, new Set());
    }
    this.byCapability.get(params.capability)!.add(listing.id);
    if (!this.bySeller.has(params.sellerId)) {
      this.bySeller.set(params.sellerId, new Set());
    }
    this.bySeller.get(params.sellerId)!.add(listing.id);
    return listing;
  }

  update(listingId: string, updates: Partial<Pick<CapabilityListing, 'description' | 'pricingModel' | 'maxConcurrent' | 'sla' | 'tags'>>): CapabilityListing | null {
    const listing = this.listings.get(listingId);
    if (!listing) return null;
    Object.assign(listing, updates, { updatedAt: Date.now() });
    return listing;
  }

  pause(listingId: string): boolean {
    const listing = this.listings.get(listingId);
    if (!listing || listing.status !== 'active') return false;
    listing.status = 'paused';
    listing.updatedAt = Date.now();
    return true;
  }

  resume(listingId: string): boolean {
    const listing = this.listings.get(listingId);
    if (!listing || listing.status !== 'paused') return false;
    listing.status = 'active';
    listing.updatedAt = Date.now();
    return true;
  }

  delist(listingId: string): boolean {
    const listing = this.listings.get(listingId);
    if (!listing || listing.status === 'delisted') return false;
    listing.status = 'delisted';
    listing.updatedAt = Date.now();
    this.byCapability.get(listing.capability)?.delete(listingId);
    return true;
  }

  search(query: { capability?: string; tags?: string[]; maxPrice?: number; minRating?: number; sellerId?: string }): CapabilityListing[] {
    let candidates: CapabilityListing[];
    if (query.capability && this.byCapability.has(query.capability)) {
      candidates = [...this.byCapability.get(query.capability)!]
        .map(id => this.listings.get(id)!)
        .filter(l => l.status === 'active');
    } else if (query.sellerId && this.bySeller.has(query.sellerId)) {
      candidates = [...this.bySeller.get(query.sellerId)!]
        .map(id => this.listings.get(id)!)
        .filter(l => l.status === 'active');
    } else {
      candidates = [...this.listings.values()].filter(l => l.status === 'active');
    }

    if (query.tags && query.tags.length > 0) {
      const tagSet = new Set(query.tags);
      candidates = candidates.filter(l => l.tags.some(t => tagSet.has(t)));
    }
    if (query.minRating !== undefined) {
      candidates = candidates.filter(l => l.averageRating >= query.minRating!);
    }
    if (query.maxPrice !== undefined) {
      candidates = candidates.filter(l => {
        const p = l.pricingModel;
        if (p.type === 'fixed') return p.pricePerCall <= query.maxPrice!;
        if (p.type === 'demand-curve') return p.basePrice <= query.maxPrice!;
        return true;
      });
    }
    return candidates;
  }

  get(id: string): CapabilityListing | undefined { return this.listings.get(id); }

  recordFulfillment(listingId: string, revenue: number, rating: number): void {
    const listing = this.listings.get(listingId);
    if (!listing) return;
    listing.totalFulfilled++;
    listing.totalRevenue += revenue;
    // Welford-style incremental mean for rating
    listing.ratingCount++;
    listing.averageRating += (rating - listing.averageRating) / listing.ratingCount;
  }
}

// ── Pricing Engine ───────────────────────────────────────────────────────

class PricingEngine {
  /** Calculate current price for a listing based on its pricing model */
  calculatePrice(model: PricingModel, quantity: number = 1): number {
    switch (model.type) {
      case 'fixed':
        return model.pricePerCall * quantity;

      case 'auction': {
        const elapsed = (Date.now() - 0) / 1000; // relative to auction start
        const current = Math.max(model.minPrice, model.startPrice - model.decrementPerSec * elapsed);
        return current * quantity;
      }

      case 'demand-curve': {
        // Price = basePrice * (1 + elasticity * demand)
        // Higher demand → higher price (positive elasticity)
        const multiplier = 1 + model.elasticity * model.currentDemand;
        return model.basePrice * Math.max(0.1, multiplier) * quantity;
      }

      case 'subscription': {
        if (quantity <= model.callsIncluded) return model.pricePerPeriod;
        const overage = quantity - model.callsIncluded;
        return model.pricePerPeriod + overage * model.overage;
      }

      case 'tiered': {
        let total = 0;
        let remaining = quantity;
        let prevBound = 0;
        for (const tier of model.tiers) {
          const tierQty = Math.min(remaining, tier.upTo - prevBound);
          if (tierQty <= 0) break;
          total += tierQty * tier.pricePerCall;
          remaining -= tierQty;
          prevBound = tier.upTo;
        }
        // Remaining beyond last tier uses last tier price
        if (remaining > 0 && model.tiers.length > 0) {
          total += remaining * model.tiers[model.tiers.length - 1].pricePerCall;
        }
        return total;
      }
    }
  }

  /** Update demand curve based on recent usage */
  updateDemand(model: PricingModel, recentCallsPerMinute: number): PricingModel {
    if (model.type !== 'demand-curve') return model;
    // EWMA smoothing on demand
    const alpha = 0.3;
    const newDemand = alpha * recentCallsPerMinute + (1 - alpha) * model.currentDemand;
    return { ...model, currentDemand: newDemand };
  }

  /** Suggest price based on market comparables */
  suggestPrice(listings: CapabilityListing[]): { median: number; p25: number; p75: number } {
    const prices = listings
      .map(l => {
        if (l.pricingModel.type === 'fixed') return l.pricingModel.pricePerCall;
        if (l.pricingModel.type === 'demand-curve') return l.pricingModel.basePrice;
        return null;
      })
      .filter((p): p is number => p !== null)
      .sort((a, b) => a - b);

    if (prices.length === 0) return { median: 0, p25: 0, p75: 0 };
    const median = prices[Math.floor(prices.length / 2)];
    const p25 = prices[Math.floor(prices.length * 0.25)];
    const p75 = prices[Math.floor(prices.length * 0.75)];
    return { median, p25, p75 };
  }
}

// ── Escrow Controller ────────────────────────────────────────────────────

class EscrowController {
  private accounts = new Map<string, EscrowAccount>();
  private events: MarketEvent[] = [];

  createEscrow(contractId: string, buyerId: string, sellerId: string, amount: number): EscrowAccount {
    const account: EscrowAccount = {
      contractId,
      buyerId,
      sellerId,
      totalDeposited: amount,
      totalReleased: 0,
      totalRefunded: 0,
      holds: [],
      status: 'active',
    };
    this.accounts.set(contractId, account);
    this.events.push({ type: 'escrow-created', timestamp: Date.now(), data: { contractId, amount } });
    return account;
  }

  holdForMilestone(contractId: string, milestoneId: string, amount: number): boolean {
    const account = this.accounts.get(contractId);
    if (!account || account.status !== 'active') return false;
    const available = account.totalDeposited - account.totalReleased - account.totalRefunded -
      account.holds.reduce((s, h) => s + h.amount, 0);
    if (amount > available) return false;
    account.holds.push({ milestoneId, amount, heldAt: Date.now() });
    return true;
  }

  releaseMilestone(contractId: string, milestoneId: string): number {
    const account = this.accounts.get(contractId);
    if (!account) return 0;
    const holdIdx = account.holds.findIndex(h => h.milestoneId === milestoneId);
    if (holdIdx === -1) return 0;
    const amount = account.holds[holdIdx].amount;
    account.holds.splice(holdIdx, 1);
    account.totalReleased += amount;
    this.events.push({ type: 'escrow-released', timestamp: Date.now(), data: { contractId, milestoneId, amount } });
    return amount;
  }

  refund(contractId: string, amount: number): boolean {
    const account = this.accounts.get(contractId);
    if (!account) return false;
    const available = account.totalDeposited - account.totalReleased - account.totalRefunded;
    if (amount > available) return false;
    account.totalRefunded += amount;
    this.events.push({ type: 'escrow-refunded', timestamp: Date.now(), data: { contractId, amount } });
    return true;
  }

  close(contractId: string): boolean {
    const account = this.accounts.get(contractId);
    if (!account) return false;
    // Release any remaining holds
    for (const hold of account.holds) {
      account.totalReleased += hold.amount;
    }
    account.holds = [];
    account.status = 'closed';
    return true;
  }

  getBalance(contractId: string): { deposited: number; released: number; refunded: number; held: number; available: number } | null {
    const account = this.accounts.get(contractId);
    if (!account) return null;
    const held = account.holds.reduce((s, h) => s + h.amount, 0);
    return {
      deposited: account.totalDeposited,
      released: account.totalReleased,
      refunded: account.totalRefunded,
      held,
      available: account.totalDeposited - account.totalReleased - account.totalRefunded - held,
    };
  }
}

// ── Matching Engine ──────────────────────────────────────────────────────

interface MatchScore {
  listingId: string;
  sellerId: string;
  score: number;
  breakdown: {
    priceScore: number;
    reputationScore: number;
    slaScore: number;
    availabilityScore: number;
    freshnessScore: number;
  };
}

class MatchingEngine {
  private weights = {
    price: 0.25,
    reputation: 0.30,
    sla: 0.20,
    availability: 0.15,
    freshness: 0.10,
  };

  match(
    query: { capability: string; maxPrice: number; minLatencyMs: number; minAvailability: number },
    listings: CapabilityListing[],
    sellerReputations: Map<string, number>,
  ): MatchScore[] {
    const now = Date.now();
    const scores: MatchScore[] = [];

    for (const listing of listings) {
      if (listing.status !== 'active') continue;
      if (listing.currentConcurrent >= listing.maxConcurrent) continue;

      // Price score: lower is better, normalized to [0, 1]
      const estimatedPrice = this.estimatePrice(listing.pricingModel);
      const priceScore = estimatedPrice <= query.maxPrice
        ? 1 - (estimatedPrice / query.maxPrice) * 0.8
        : 0;

      // Reputation score
      const rep = sellerReputations.get(listing.sellerId) ?? 0.5;
      const reputationScore = Math.min(1, rep);

      // SLA score: how well does the listing meet requirements
      const latencyScore = listing.sla.maxLatencyMs <= query.minLatencyMs ? 1 :
        Math.max(0, 1 - (listing.sla.maxLatencyMs - query.minLatencyMs) / query.minLatencyMs);
      const availScore = listing.sla.availability >= query.minAvailability ? 1 :
        listing.sla.availability / query.minAvailability;
      const slaScore = 0.6 * latencyScore + 0.4 * availScore;

      // Availability score: how much capacity remains
      const capacityRatio = 1 - (listing.currentConcurrent / listing.maxConcurrent);
      const availabilityScore = capacityRatio;

      // Freshness: prefer recently updated listings
      const ageMs = now - listing.updatedAt;
      const freshnessScore = Math.exp(-ageMs / (7 * 24 * 3600 * 1000)); // 7-day half-life

      const totalScore =
        this.weights.price * priceScore +
        this.weights.reputation * reputationScore +
        this.weights.sla * slaScore +
        this.weights.availability * availabilityScore +
        this.weights.freshness * freshnessScore;

      scores.push({
        listingId: listing.id,
        sellerId: listing.sellerId,
        score: totalScore,
        breakdown: { priceScore, reputationScore, slaScore, availabilityScore, freshnessScore },
      });
    }

    // Sort descending by score, FNV-1a tie-break
    scores.sort((a, b) => {
      const diff = b.score - a.score;
      if (Math.abs(diff) > 0.001) return diff;
      return fnv1aHash(a.listingId) - fnv1aHash(b.listingId);
    });

    return scores;
  }

  private estimatePrice(model: PricingModel): number {
    switch (model.type) {
      case 'fixed': return model.pricePerCall;
      case 'demand-curve': return model.basePrice * (1 + model.elasticity * model.currentDemand);
      case 'subscription': return model.pricePerPeriod / Math.max(1, model.callsIncluded);
      case 'auction': return (model.startPrice + model.minPrice) / 2;
      case 'tiered': return model.tiers.length > 0 ? model.tiers[0].pricePerCall : 0;
    }
  }
}

// ── Reputation Gate ──────────────────────────────────────────────────────

class ReputationGate {
  private agentScores = new Map<string, { score: number; history: number[]; lastUpdated: number }>();
  private minListScore = 0.3;
  private minBuyScore = 0.1;
  private decayRate = 0.001; // per hour

  updateScore(agentId: string, newScore: number): void {
    const existing = this.agentScores.get(agentId);
    if (existing) {
      existing.history.push(existing.score);
      if (existing.history.length > 50) existing.history.shift();
      existing.score = newScore;
      existing.lastUpdated = Date.now();
    } else {
      this.agentScores.set(agentId, { score: newScore, history: [], lastUpdated: Date.now() });
    }
  }

  getScore(agentId: string): number {
    const entry = this.agentScores.get(agentId);
    if (!entry) return 0;
    // Apply temporal decay
    const hoursElapsed = (Date.now() - entry.lastUpdated) / 3600000;
    return entry.score * Math.exp(-this.decayRate * hoursElapsed);
  }

  canList(agentId: string): { allowed: boolean; reason?: string } {
    const score = this.getScore(agentId);
    if (score < this.minListScore) {
      return { allowed: false, reason: `Score ${score.toFixed(3)} below listing threshold ${this.minListScore}` };
    }
    return { allowed: true };
  }

  canBuy(agentId: string, listing: CapabilityListing): { allowed: boolean; reason?: string } {
    const score = this.getScore(agentId);
    if (score < this.minBuyScore) {
      return { allowed: false, reason: `Score ${score.toFixed(3)} below buying threshold ${this.minBuyScore}` };
    }
    if (score < listing.minReputation) {
      return { allowed: false, reason: `Score ${score.toFixed(3)} below listing requirement ${listing.minReputation}` };
    }
    return { allowed: true };
  }

  /** Trend: positive means improving, negative means declining */
  getTrend(agentId: string): number {
    const entry = this.agentScores.get(agentId);
    if (!entry || entry.history.length < 3) return 0;
    const recent = entry.history.slice(-5);
    // Simple linear regression slope
    const n = recent.length;
    let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
    for (let i = 0; i < n; i++) {
      sumX += i; sumY += recent[i];
      sumXY += i * recent[i]; sumX2 += i * i;
    }
    return (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
  }
}

// ── Dispute Arbitrator ───────────────────────────────────────────────────

class DisputeArbitrator {
  private disputes = new Map<string, Dispute>();
  private arbitratorPool: string[] = [];
  private minArbitrators = 3;

  setArbitratorPool(agents: string[]): void {
    this.arbitratorPool = agents;
  }

  fileDispute(contractId: string, filedBy: string, againstAgent: string, reason: string, evidence: string[]): Dispute {
    // Select arbitrators via FNV-1a deterministic but excluding parties
    const eligible = this.arbitratorPool.filter(a => a !== filedBy && a !== againstAgent);
    const selected = eligible
      .map(a => ({ agent: a, hash: fnv1aHash(`${contractId}-${a}`) }))
      .sort((a, b) => a.hash - b.hash)
      .slice(0, Math.min(this.minArbitrators, eligible.length))
      .map(a => a.agent);

    const dispute: Dispute = {
      id: generateId('dsp'),
      contractId,
      filedBy,
      againstAgent,
      reason,
      evidence,
      status: 'filed',
      arbitrators: selected,
      filedAt: Date.now(),
    };
    this.disputes.set(dispute.id, dispute);
    return dispute;
  }

  submitEvidence(disputeId: string, evidence: string): boolean {
    const dispute = this.disputes.get(disputeId);
    if (!dispute || dispute.status === 'resolved') return false;
    dispute.evidence.push(evidence);
    dispute.status = 'under-review';
    return true;
  }

  submitRuling(disputeId: string, inFavorOf: string, refundAmount: number, penaltyAmount: number): boolean {
    const dispute = this.disputes.get(disputeId);
    if (!dispute || dispute.status === 'resolved') return false;
    dispute.ruling = { inFavorOf, refundAmount, penaltyAmount };
    dispute.status = 'resolved';
    dispute.resolvedAt = Date.now();
    return true;
  }

  appeal(disputeId: string): boolean {
    const dispute = this.disputes.get(disputeId);
    if (!dispute || dispute.status !== 'resolved') return false;
    // Re-open with fresh arbitrators
    const eligible = this.arbitratorPool.filter(
      a => a !== dispute.filedBy && a !== dispute.againstAgent && !dispute.arbitrators.includes(a)
    );
    if (eligible.length < this.minArbitrators) return false;
    dispute.status = 'appealed';
    dispute.arbitrators = eligible
      .map(a => ({ agent: a, hash: fnv1aHash(`appeal-${disputeId}-${a}`) }))
      .sort((a, b) => a.hash - b.hash)
      .slice(0, this.minArbitrators)
      .map(a => a.agent);
    dispute.ruling = undefined;
    return true;
  }

  getDispute(id: string): Dispute | undefined { return this.disputes.get(id); }
  getByContract(contractId: string): Dispute[] {
    return [...this.disputes.values()].filter(d => d.contractId === contractId);
  }
}

// ── Usage Metering ───────────────────────────────────────────────────────

class UsageMeterer {
  private records = new Map<string, UsageRecord[]>(); // contractId -> records
  private windows = new Map<string, { calls: number; windowStart: number }>(); // contractId -> rate window

  record(contractId: string, latencyMs: number, success: boolean, inputSize: number, outputSize: number, cost: number): UsageRecord {
    const rec: UsageRecord = {
      contractId,
      callId: generateId('call'),
      timestamp: Date.now(),
      latencyMs,
      success,
      inputSize,
      outputSize,
      cost,
    };
    if (!this.records.has(contractId)) this.records.set(contractId, []);
    this.records.get(contractId)!.push(rec);

    // Update rate window (1-minute sliding)
    const win = this.windows.get(contractId);
    const now = Date.now();
    if (!win || now - win.windowStart > 60000) {
      this.windows.set(contractId, { calls: 1, windowStart: now });
    } else {
      win.calls++;
    }

    return rec;
  }

  getStats(contractId: string): {
    totalCalls: number;
    successRate: number;
    avgLatencyMs: number;
    p95LatencyMs: number;
    totalCost: number;
    callsPerMinute: number;
  } | null {
    const recs = this.records.get(contractId);
    if (!recs || recs.length === 0) return null;

    const successes = recs.filter(r => r.success).length;
    const latencies = recs.map(r => r.latencyMs).sort((a, b) => a - b);
    const totalCost = recs.reduce((s, r) => s + r.cost, 0);
    const win = this.windows.get(contractId);

    return {
      totalCalls: recs.length,
      successRate: successes / recs.length,
      avgLatencyMs: latencies.reduce((s, l) => s + l, 0) / latencies.length,
      p95LatencyMs: latencies[Math.floor(latencies.length * 0.95)] ?? 0,
      totalCost,
      callsPerMinute: win?.calls ?? 0,
    };
  }

  /** Check SLA compliance against contract terms */
  checkSLACompliance(contractId: string, sla: SLATerms): {
    compliant: boolean;
    violations: string[];
    violationCount: number;
  } {
    const stats = this.getStats(contractId);
    if (!stats) return { compliant: true, violations: [], violationCount: 0 };

    const violations: string[] = [];
    if (stats.p95LatencyMs > sla.maxLatencyMs) {
      violations.push(`Latency p95 ${stats.p95LatencyMs}ms exceeds ${sla.maxLatencyMs}ms`);
    }
    if (1 - stats.successRate > sla.maxErrorRate) {
      violations.push(`Error rate ${((1 - stats.successRate) * 100).toFixed(1)}% exceeds ${(sla.maxErrorRate * 100).toFixed(1)}%`);
    }
    if (stats.successRate < sla.availability) {
      violations.push(`Availability ${(stats.successRate * 100).toFixed(1)}% below ${(sla.availability * 100).toFixed(1)}%`);
    }

    return { compliant: violations.length === 0, violations, violationCount: violations.length };
  }

  /** Prune old records beyond retention */
  prune(contractId: string, maxRecords: number = 10000): number {
    const recs = this.records.get(contractId);
    if (!recs || recs.length <= maxRecords) return 0;
    const pruned = recs.length - maxRecords;
    this.records.set(contractId, recs.slice(-maxRecords));
    return pruned;
  }
}

// ── Marketplace Orchestrator ─────────────────────────────────────────────

interface MarketplaceConfig {
  maxListingsPerSeller: number;
  escrowRequired: boolean;
  autoReleaseMilestones: boolean;
  slaCheckIntervalMs: number;
  disputeWindowMs: number;
}

const PRESETS = {
  'open-marketplace': {
    maxListingsPerSeller: 50,
    escrowRequired: true,
    autoReleaseMilestones: false,
    slaCheckIntervalMs: 60000,
    disputeWindowMs: 7 * 24 * 3600 * 1000,
  } as MarketplaceConfig,
  'trusted-network': {
    maxListingsPerSeller: 100,
    escrowRequired: false,
    autoReleaseMilestones: true,
    slaCheckIntervalMs: 300000,
    disputeWindowMs: 3 * 24 * 3600 * 1000,
  } as MarketplaceConfig,
  'high-security': {
    maxListingsPerSeller: 10,
    escrowRequired: true,
    autoReleaseMilestones: false,
    slaCheckIntervalMs: 30000,
    disputeWindowMs: 14 * 24 * 3600 * 1000,
  } as MarketplaceConfig,
};

class MarketplaceOrchestrator {
  private listingManager = new ListingManager();
  private pricingEngine = new PricingEngine();
  private escrowController = new EscrowController();
  private matchingEngine = new MatchingEngine();
  private reputationGate = new ReputationGate();
  private disputeArbitrator = new DisputeArbitrator();
  private usageMeterer = new UsageMeterer();
  private contracts = new Map<string, ServiceContract>();
  private events: MarketEvent[] = [];
  private config: MarketplaceConfig;

  constructor(config: MarketplaceConfig) {
    this.config = config;
  }

  static fromPreset(preset: keyof typeof PRESETS): MarketplaceOrchestrator {
    return new MarketplaceOrchestrator(PRESETS[preset]);
  }

  /** Publish a new capability listing */
  publishListing(params: Parameters<ListingManager['publish']>[0]): { success: boolean; listing?: CapabilityListing; error?: string } {
    const repCheck = this.reputationGate.canList(params.sellerId);
    if (!repCheck.allowed) return { success: false, error: repCheck.reason };

    const listing = this.listingManager.publish(params);
    this.events.push({ type: 'listing-published', timestamp: Date.now(), data: { listingId: listing.id, sellerId: params.sellerId } });
    return { success: true, listing };
  }

  /** Search and match capabilities */
  findCapabilities(
    query: { capability: string; maxPrice: number; minLatencyMs: number; minAvailability: number },
    buyerId: string,
  ): MatchScore[] {
    const listings = this.listingManager.search({ capability: query.capability });
    const reputations = new Map<string, number>();
    for (const l of listings) {
      reputations.set(l.sellerId, this.reputationGate.getScore(l.sellerId));
    }
    return this.matchingEngine.match(query, listings, reputations);
  }

  /** Create a service contract with escrow */
  createContract(
    listingId: string,
    buyerId: string,
    milestones: Array<{ description: string; amount: number }>,
  ): { success: boolean; contract?: ServiceContract; error?: string } {
    const listing = this.listingManager.get(listingId);
    if (!listing) return { success: false, error: 'Listing not found' };
    if (listing.status !== 'active') return { success: false, error: 'Listing not active' };

    const buyCheck = this.reputationGate.canBuy(buyerId, listing);
    if (!buyCheck.allowed) return { success: false, error: buyCheck.reason };

    if (listing.currentConcurrent >= listing.maxConcurrent) {
      return { success: false, error: 'Seller at max concurrent capacity' };
    }

    const totalAmount = milestones.reduce((s, m) => s + m.amount, 0);
    const contractId = generateId('ctr');

    const contract: ServiceContract = {
      id: contractId,
      listingId,
      buyerId,
      sellerId: listing.sellerId,
      status: 'active',
      pricing: listing.pricingModel,
      escrowAmount: totalAmount,
      paidAmount: 0,
      milestones: milestones.map(m => ({
        id: generateId('mst'),
        description: m.description,
        amount: m.amount,
        status: 'pending' as const,
      })),
      startedAt: Date.now(),
      slaViolations: 0,
    };

    if (this.config.escrowRequired) {
      this.escrowController.createEscrow(contractId, buyerId, listing.sellerId, totalAmount);
      for (const m of contract.milestones) {
        this.escrowController.holdForMilestone(contractId, m.id, m.amount);
      }
    }

    listing.currentConcurrent++;
    this.contracts.set(contractId, contract);
    this.events.push({ type: 'contract-created', timestamp: Date.now(), data: { contractId, listingId, buyerId } });
    return { success: true, contract };
  }

  /** Submit milestone for review */
  submitMilestone(contractId: string, milestoneId: string, evidence: string): boolean {
    const contract = this.contracts.get(contractId);
    if (!contract || contract.status !== 'active') return false;
    const milestone = contract.milestones.find(m => m.id === milestoneId);
    if (!milestone || milestone.status !== 'pending' && milestone.status !== 'in-progress') return false;
    milestone.status = 'submitted';
    milestone.submittedAt = Date.now();
    milestone.evidence = evidence;

    if (this.config.autoReleaseMilestones) {
      this.acceptMilestone(contractId, milestoneId);
    }
    return true;
  }

  /** Accept milestone and release escrow */
  acceptMilestone(contractId: string, milestoneId: string): boolean {
    const contract = this.contracts.get(contractId);
    if (!contract) return false;
    const milestone = contract.milestones.find(m => m.id === milestoneId);
    if (!milestone || milestone.status !== 'submitted') return false;

    milestone.status = 'accepted';
    if (this.config.escrowRequired) {
      const released = this.escrowController.releaseMilestone(contractId, milestoneId);
      contract.paidAmount += released;
    } else {
      contract.paidAmount += milestone.amount;
    }

    // Check if all milestones complete
    if (contract.milestones.every(m => m.status === 'accepted')) {
      this.completeContract(contractId);
    }

    this.events.push({ type: 'milestone-accepted', timestamp: Date.now(), data: { contractId, milestoneId } });
    return true;
  }

  /** Record API call usage for metering */
  recordUsage(contractId: string, latencyMs: number, success: boolean, inputSize: number, outputSize: number): void {
    const contract = this.contracts.get(contractId);
    if (!contract || contract.status !== 'active') return;
    const listing = this.listingManager.get(contract.listingId);
    if (!listing) return;

    const cost = this.pricingEngine.calculatePrice(contract.pricing);
    this.usageMeterer.record(contractId, latencyMs, success, inputSize, outputSize, cost);

    // SLA check
    const compliance = this.usageMeterer.checkSLACompliance(contractId, listing.sla);
    if (!compliance.compliant) {
      contract.slaViolations += compliance.violationCount;
      this.events.push({ type: 'sla-violation', timestamp: Date.now(), data: { contractId, violations: compliance.violations } });
    }
  }

  /** File a dispute */
  fileDispute(contractId: string, filedBy: string, reason: string, evidence: string[]): Dispute | null {
    const contract = this.contracts.get(contractId);
    if (!contract) return null;
    const against = filedBy === contract.buyerId ? contract.sellerId : contract.buyerId;
    contract.status = 'disputed';
    return this.disputeArbitrator.fileDispute(contractId, filedBy, against, reason, evidence);
  }

  /** Get marketplace health metrics */
  getMarketHealth(): {
    totalListings: number;
    activeContracts: number;
    disputeRate: number;
    averageFulfillmentRate: number;
    totalVolume: number;
  } {
    const contracts = [...this.contracts.values()];
    const completed = contracts.filter(c => c.status === 'completed');
    const disputed = contracts.filter(c => c.status === 'disputed');
    const totalVolume = contracts.reduce((s, c) => s + c.paidAmount, 0);

    return {
      totalListings: this.listingManager.search({}).length,
      activeContracts: contracts.filter(c => c.status === 'active').length,
      disputeRate: contracts.length > 0 ? disputed.length / contracts.length : 0,
      averageFulfillmentRate: contracts.length > 0 ? completed.length / contracts.length : 0,
      totalVolume,
    };
  }

  private completeContract(contractId: string): void {
    const contract = this.contracts.get(contractId);
    if (!contract) return;
    contract.status = 'completed';
    contract.completedAt = Date.now();
    const listing = this.listingManager.get(contract.listingId);
    if (listing) {
      listing.currentConcurrent = Math.max(0, listing.currentConcurrent - 1);
      this.listingManager.recordFulfillment(contract.listingId, contract.paidAmount, 1.0);
    }
    if (this.config.escrowRequired) {
      this.escrowController.close(contractId);
    }
    this.events.push({ type: 'contract-completed', timestamp: Date.now(), data: { contractId } });
  }

  getEvents(since?: number): MarketEvent[] {
    if (!since) return this.events;
    return this.events.filter(e => e.timestamp >= since);
  }
}

// ── Exports ──────────────────────────────────────────────────────────────

export {
  ListingManager,
  PricingEngine,
  EscrowController,
  MatchingEngine,
  ReputationGate,
  DisputeArbitrator,
  UsageMeterer,
  MarketplaceOrchestrator,
  PRESETS,
};
