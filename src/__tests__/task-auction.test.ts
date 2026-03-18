import { describe, it, expect, beforeEach } from 'vitest';
import { TaskAuctioneer, AgentTaskAuctionScheduler, PRESETS } from '../task-auction';

const defaultConfig = PRESETS['open-marketplace'].defaultAuctionConfig;

function makeLot(id: string, overrides = {}) {
  return {
    taskId: id,
    description: `Task ${id}`,
    requiredCapabilities: ['code'],
    estimatedDuration: 5000,
    reservePrice: 10,
    priority: 1,
    ...overrides,
  };
}

function makeBidder(id: string, overrides = {}) {
  return {
    agentId: id,
    capabilities: ['code'],
    maxConcurrency: 5,
    currentLoad: 0,
    reliabilityScore: 0.9,
    historicalLatency: 100,
    ...overrides,
  };
}

describe('TaskAuctioneer', () => {
  let auctioneer: TaskAuctioneer;

  beforeEach(() => {
    auctioneer = new TaskAuctioneer();
  });

  it('creates an auction', () => {
    const auction = auctioneer.createAuction([makeLot('t1')], defaultConfig);
    expect(auction.auctionId).toBeTruthy();
    expect(auction.phase).toBe('collecting');
    expect(auction.lots.size).toBe(1);
  });

  it('registers bidders', () => {
    const auction = auctioneer.createAuction([makeLot('t1')], defaultConfig);
    const ok = auctioneer.registerBidder(auction.auctionId, makeBidder('a1'));
    expect(ok).toBe(true);
    expect(auction.bidders.size).toBe(1);
  });

  it('rejects bidder registration after bidding closes', () => {
    const auction = auctioneer.createAuction([makeLot('t1')], defaultConfig);
    auctioneer.closeBidding(auction.auctionId);
    const ok = auctioneer.registerBidder(auction.auctionId, makeBidder('a1'));
    expect(ok).toBe(false);
  });

  it('submits bids', () => {
    const auction = auctioneer.createAuction([makeLot('t1')], defaultConfig);
    auctioneer.registerBidder(auction.auctionId, makeBidder('a1'));
    const bid = auctioneer.submitBid(auction.auctionId, {
      bidderId: 'a1',
      taskIds: ['t1'],
      amount: 50,
      qualityScore: 0.9,
      estimatedCompletion: 3000,
    });
    expect(bid).not.toBeNull();
    expect(bid!.status).toBe('submitted');
  });

  it('rejects bids from unregistered bidders', () => {
    const auction = auctioneer.createAuction([makeLot('t1')], defaultConfig);
    const bid = auctioneer.submitBid(auction.auctionId, {
      bidderId: 'unknown',
      taskIds: ['t1'],
      amount: 50,
      qualityScore: 0.9,
      estimatedCompletion: 3000,
    });
    expect(bid).toBeNull();
  });

  it('rejects bids for nonexistent tasks', () => {
    const auction = auctioneer.createAuction([makeLot('t1')], defaultConfig);
    auctioneer.registerBidder(auction.auctionId, makeBidder('a1'));
    const bid = auctioneer.submitBid(auction.auctionId, {
      bidderId: 'a1',
      taskIds: ['nonexistent'],
      amount: 50,
      qualityScore: 0.9,
      estimatedCompletion: 3000,
    });
    expect(bid).toBeNull();
  });

  it('enforces max bids per agent', () => {
    const config = { ...defaultConfig, maxBidsPerAgent: 1 };
    const auction = auctioneer.createAuction([makeLot('t1'), makeLot('t2')], config);
    auctioneer.registerBidder(auction.auctionId, makeBidder('a1'));
    
    const bid1 = auctioneer.submitBid(auction.auctionId, {
      bidderId: 'a1', taskIds: ['t1'], amount: 50, qualityScore: 0.9, estimatedCompletion: 3000,
    });
    const bid2 = auctioneer.submitBid(auction.auctionId, {
      bidderId: 'a1', taskIds: ['t2'], amount: 50, qualityScore: 0.9, estimatedCompletion: 3000,
    });
    expect(bid1).not.toBeNull();
    expect(bid2).toBeNull();
  });

  it('detects expired auctions', async () => {
    const config = { ...defaultConfig, biddingWindowMs: 1 };
    const auction = auctioneer.createAuction([makeLot('t1')], config);
    await new Promise(r => setTimeout(r, 5));
    expect(auctioneer.isExpired(auction.auctionId)).toBe(true);
  });

  it('cancels auctions', () => {
    const auction = auctioneer.createAuction([makeLot('t1')], defaultConfig);
    auctioneer.cancelAuction(auction.auctionId);
    expect(auction.phase).toBe('cancelled');
  });
});

describe('AgentTaskAuctionScheduler', () => {
  it('creates from preset', () => {
    const scheduler = AgentTaskAuctionScheduler.fromPreset('open-marketplace');
    expect(scheduler).toBeTruthy();
  });

  it('throws on unknown preset', () => {
    expect(() => AgentTaskAuctionScheduler.fromPreset('nonexistent')).toThrow();
  });

  it('runs a full auction lifecycle', () => {
    const scheduler = AgentTaskAuctionScheduler.fromPreset('trusted-network');
    
    const auction = scheduler.openAuction([makeLot('t1'), makeLot('t2')]);
    scheduler.registerBidder(auction.auctionId, makeBidder('a1'));
    scheduler.registerBidder(auction.auctionId, makeBidder('a2'));
    
    scheduler.placeBid(auction.auctionId, 'a1', ['t1'], 50, 0.9, 3000);
    scheduler.placeBid(auction.auctionId, 'a2', ['t2'], 40, 0.8, 4000);
    
    const allocation = scheduler.runAuction(auction.auctionId);
    expect(allocation).not.toBeNull();
    expect(allocation!.assignments.length).toBeGreaterThan(0);
    expect(allocation!.totalValue).toBeGreaterThan(0);
  });

  it('cancels auction with insufficient bidders', () => {
    const scheduler = AgentTaskAuctionScheduler.fromPreset('open-marketplace');
    // open-marketplace requires minBiddersToClose: 2
    const auction = scheduler.openAuction([makeLot('t1')]);
    scheduler.registerBidder(auction.auctionId, makeBidder('a1'));
    scheduler.placeBid(auction.auctionId, 'a1', ['t1'], 50, 0.9, 3000);
    
    const allocation = scheduler.runAuction(auction.auctionId);
    expect(allocation).toBeNull();
  });

  it('provides dashboard', () => {
    const scheduler = AgentTaskAuctionScheduler.fromPreset('open-marketplace');
    const dash = scheduler.getDashboard();
    expect(dash).toHaveProperty('activeAuctions');
    expect(dash).toHaveProperty('completedAuctions');
    expect(dash).toHaveProperty('revenueTrend');
    expect(dash).toHaveProperty('efficiency');
    expect(dash).toHaveProperty('recentEvents');
  });

  it('tracks events', () => {
    const scheduler = AgentTaskAuctionScheduler.fromPreset('trusted-network');
    const auction = scheduler.openAuction([makeLot('t1')]);
    const events = scheduler.getEvents();
    expect(events.length).toBeGreaterThan(0);
    expect(events[0].type).toBe('auction_opened');
  });

  it('handles VCG pricing in open-marketplace', () => {
    const scheduler = AgentTaskAuctionScheduler.fromPreset('open-marketplace');
    const auction = scheduler.openAuction([makeLot('t1')]);
    
    scheduler.registerBidder(auction.auctionId, makeBidder('a1'));
    scheduler.registerBidder(auction.auctionId, makeBidder('a2'));
    scheduler.registerBidder(auction.auctionId, makeBidder('a3'));
    
    scheduler.placeBid(auction.auctionId, 'a1', ['t1'], 100, 0.9, 3000);
    scheduler.placeBid(auction.auctionId, 'a2', ['t1'], 80, 0.85, 3000);
    scheduler.placeBid(auction.auctionId, 'a3', ['t1'], 60, 0.8, 3000);
    
    const allocation = scheduler.runAuction(auction.auctionId);
    expect(allocation).not.toBeNull();
    
    // Winner should pay less than their bid (VCG property)
    if (allocation && allocation.assignments.length > 0) {
      const winner = allocation.assignments[0];
      expect(winner.vcgPayment).toBeLessThanOrEqual(winner.bidAmount);
    }
  });

  it('tick auto-closes expired auctions', async () => {
    const scheduler = AgentTaskAuctionScheduler.fromPreset('trusted-network');
    scheduler.openAuction([makeLot('t1')], { biddingWindowMs: 1 });
    await new Promise(r => setTimeout(r, 5));
    
    const result = scheduler.tick();
    expect(result.expiredAuctions.length).toBe(1);
  });
});
