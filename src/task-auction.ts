/**
 * Task Auction System
 * Implements sealed-bid auctions for agent task allocation.
 * Uses cryptographic commitment schemes to prevent bid manipulation.
 */

import { createHash, randomBytes } from 'crypto';

type AuctionPhase = 'open' | 'sealed' | 'revealed' | 'awarded' | 'expired';

interface TaskAuction {
  id: string;
  title: string;
  description: string;
  domain: string;
  requiredMinReputation: number;
  reward: number;
  createdBy: string;
  createdAt: number;
  sealDeadline: number;      // no more bids after this
  revealDeadline: number;    // all bids must be revealed by this
  phase: AuctionPhase;
  bids: Map<string, SealedBid>;
  revealedBids: RevealedBid[];
  winner: string | null;
}

interface SealedBid {
  agentAddress: string;
  commitmentHash: string;    // hash(bid amount + nonce)
  submittedAt: number;
  reputationAtBid: number;
}

interface RevealedBid {
  agentAddress: string;
  amount: number;
  nonce: string;
  qualityPledge: number;     // 0-100 quality commitment
  estimatedHours: number;
  reputationAtBid: number;
}

interface AuctionResult {
  auctionId: string;
  winner: string;
  winningBid: number;
  winningScore: number;
  totalBids: number;
  revealedBids: number;
}

const REPUTATION_WEIGHT = 0.4;
const PRICE_WEIGHT = 0.35;
const QUALITY_WEIGHT = 0.25;

export class TaskAuctionSystem {
  private auctions: Map<string, TaskAuction> = new Map();

  /**
   * Create a new task auction.
   */
  createAuction(params: {
    title: string;
    description: string;
    domain: string;
    requiredMinReputation: number;
    reward: number;
    createdBy: string;
    biddingWindowMs: number;
    revealWindowMs: number;
  }): TaskAuction {
    const now = Date.now();
    const auction: TaskAuction = {
      id: randomBytes(16).toString('hex'),
      title: params.title,
      description: params.description,
      domain: params.domain,
      requiredMinReputation: params.requiredMinReputation,
      reward: params.reward,
      createdBy: params.createdBy,
      createdAt: now,
      sealDeadline: now + params.biddingWindowMs,
      revealDeadline: now + params.biddingWindowMs + params.revealWindowMs,
      phase: 'open',
      bids: new Map(),
      revealedBids: [],
      winner: null,
    };

    this.auctions.set(auction.id, auction);
    return auction;
  }

  /**
   * Submit a sealed bid (commitment only, amount hidden).
   * Agents commit hash(amount + nonce) without revealing the actual bid.
   */
  submitSealedBid(
    auctionId: string,
    agentAddress: string,
    commitmentHash: string,
    reputation: number
  ): boolean {
    const auction = this.auctions.get(auctionId);
    if (!auction) return false;
    if (auction.phase !== 'open') return false;
    if (Date.now() > auction.sealDeadline) return false;
    if (reputation < auction.requiredMinReputation) return false;
    if (auction.bids.has(agentAddress)) return false; // one bid per agent

    auction.bids.set(agentAddress, {
      agentAddress,
      commitmentHash,
      submittedAt: Date.now(),
      reputationAtBid: reputation,
    });

    return true;
  }

  /**
   * Generate a commitment hash for a bid amount.
   * Agent keeps the nonce secret until reveal phase.
   */
  static generateCommitment(amount: number, nonce: string): string {
    return createHash('sha256')
      .update(`${amount}:${nonce}`)
      .digest('hex');
  }

  /**
   * Reveal a previously sealed bid.
   * Verifies the commitment hash matches the revealed amount + nonce.
   */
  revealBid(
    auctionId: string,
    agentAddress: string,
    amount: number,
    nonce: string,
    qualityPledge: number,
    estimatedHours: number
  ): boolean {
    const auction = this.auctions.get(auctionId);
    if (!auction) return false;

    // Transition to sealed phase if past deadline
    if (Date.now() > auction.sealDeadline && auction.phase === 'open') {
      auction.phase = 'sealed';
    }

    if (auction.phase !== 'sealed') return false;
    if (Date.now() > auction.revealDeadline) return false;

    const sealedBid = auction.bids.get(agentAddress);
    if (!sealedBid) return false;

    // Verify commitment
    const expectedHash = TaskAuctionSystem.generateCommitment(amount, nonce);
    if (expectedHash !== sealedBid.commitmentHash) return false;

    // Validate bid amount
    if (amount <= 0 || amount > auction.reward) return false;
    if (qualityPledge < 0 || qualityPledge > 100) return false;

    auction.revealedBids.push({
      agentAddress,
      amount,
      nonce,
      qualityPledge,
      estimatedHours,
      reputationAtBid: sealedBid.reputationAtBid,
    });

    return true;
  }

  /**
   * Finalize the auction and determine the winner.
   * Scores bids using weighted combination of price, reputation, and quality pledge.
   * Lower price is better (agent is offering to do it for less).
   */
  finalizeAuction(auctionId: string): AuctionResult | null {
    const auction = this.auctions.get(auctionId);
    if (!auction) return null;

    if (auction.revealedBids.length === 0) {
      auction.phase = 'expired';
      return null;
    }

    // Normalize scores
    const maxRep = Math.max(...auction.revealedBids.map(b => b.reputationAtBid));
    const maxAmount = Math.max(...auction.revealedBids.map(b => b.amount));
    const minAmount = Math.min(...auction.revealedBids.map(b => b.amount));

    const scored = auction.revealedBids.map(bid => {
      // Price score: lower is better (inverted)
      const priceRange = maxAmount - minAmount || 1;
      const priceScore = 1 - (bid.amount - minAmount) / priceRange;

      // Reputation score: higher is better
      const repScore = maxRep > 0 ? bid.reputationAtBid / maxRep : 0;

      // Quality score: normalized pledge
      const qualityScore = bid.qualityPledge / 100;

      const totalScore =
        PRICE_WEIGHT * priceScore +
        REPUTATION_WEIGHT * repScore +
        QUALITY_WEIGHT * qualityScore;

      return { bid, score: totalScore };
    });

    // Sort by score descending
    scored.sort((a, b) => b.score - a.score);
    const winner = scored[0];

    auction.winner = winner.bid.agentAddress;
    auction.phase = 'awarded';

    return {
      auctionId: auction.id,
      winner: winner.bid.agentAddress,
      winningBid: winner.bid.amount,
      winningScore: winner.score,
      totalBids: auction.bids.size,
      revealedBids: auction.revealedBids.length,
    };
  }

  /**
   * Get all active auctions an agent is eligible for.
   */
  getEligibleAuctions(agentReputation: number): TaskAuction[] {
    const now = Date.now();
    return [...this.auctions.values()].filter(
      a => a.phase === 'open' &&
           now < a.sealDeadline &&
           agentReputation >= a.requiredMinReputation
    );
  }

  /**
   * Get auction details.
   */
  getAuction(auctionId: string): TaskAuction | undefined {
    return this.auctions.get(auctionId);
  }
}
