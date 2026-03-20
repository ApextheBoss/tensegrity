/**
 * Example 2: Multi-Agent Task Routing with Reputation + Auctions
 *
 * Combines reputation-weighted routing with task auctions to assign
 * work to the best available agents. Shows two approaches:
 *
 * 1. Simple routing — pick the best agent by reputation score
 * 2. Auction-based — agents bid competitively for task bundles
 *
 * Run: npx tsx examples/02-task-routing.ts
 */

import { routeTask, routeTaskWithFallback, TaskAuctioneer } from 'tensegrity';
import type { Agent, Task } from 'tensegrity';

// --- Define a pool of agents with domain reputations ---

const agents: Agent[] = [
  {
    address: 'agent-alpha',
    name: 'Alpha (Summarizer)',
    reputationByDomain: { summarization: 850, translation: 200, coding: 100 },
    availability: true,
    lastActiveMs: Date.now() - 1000 * 60 * 5, // 5 min ago
  },
  {
    address: 'agent-beta',
    name: 'Beta (Translator)',
    reputationByDomain: { summarization: 300, translation: 900, coding: 400 },
    availability: true,
    lastActiveMs: Date.now() - 1000 * 60 * 60, // 1 hour ago
  },
  {
    address: 'agent-gamma',
    name: 'Gamma (Coder)',
    reputationByDomain: { summarization: 100, translation: 100, coding: 950 },
    availability: true,
    lastActiveMs: Date.now() - 1000 * 60 * 2, // 2 min ago
  },
  {
    address: 'agent-delta',
    name: 'Delta (Generalist)',
    reputationByDomain: { summarization: 500, translation: 500, coding: 500 },
    availability: false, // Currently offline
    lastActiveMs: Date.now() - 1000 * 60 * 60 * 24, // 1 day ago
  },
];

// ─── Part 1: Simple Reputation Routing ───────────────────────────────────────

function demoReputationRouting() {
  console.log('=== Part 1: Reputation-Weighted Routing ===\n');

  const tasks: Task[] = [
    { id: 'task-1', domain: 'summarization', complexity: 'tier2', requiredMinReputation: 200, deadline: Date.now() + 60000 },
    { id: 'task-2', domain: 'translation', complexity: 'tier1', requiredMinReputation: 500, deadline: Date.now() + 60000 },
    { id: 'task-3', domain: 'coding', complexity: 'tier3', requiredMinReputation: 800, deadline: Date.now() + 60000 },
  ];

  for (const task of tasks) {
    const result = routeTask(agents, task);
    if (result) {
      console.log(`  📋 ${task.domain} task → ${result.agent.name} (score: ${result.score.toFixed(3)})`);
      console.log(`     Reason: ${result.reason}\n`);
    } else {
      console.log(`  📋 ${task.domain} task → No suitable agent found\n`);
    }
  }

  // Fallback routing: relaxes reputation requirements if no direct match
  console.log('  Fallback routing (relaxed reputation):');
  const hardTask: Task = { id: 'task-hard', domain: 'coding', complexity: 'tier3', requiredMinReputation: 2000, deadline: Date.now() + 60000 };
  const directResult = routeTask(agents, hardTask);
  const fallbackResult = routeTaskWithFallback(agents, hardTask);
  console.log(`    Direct: ${directResult ? directResult.agent.name : 'No match'}`);
  console.log(`    Fallback: ${fallbackResult ? `${fallbackResult.agent.name} — ${fallbackResult.reason}` : 'No match'}`);
  console.log();
}

// ─── Part 2: Task Auction ────────────────────────────────────────────────────

function demoTaskAuction() {
  console.log('=== Part 2: Task Auction ===\n');

  const auctioneer = new TaskAuctioneer();

  // Create an auction with 3 task lots
  const auction = auctioneer.createAuction(
    [
      { taskId: 'summarize-report', description: 'Summarize Q4 report', requiredCapabilities: ['nlp'], estimatedDuration: 5000, reservePrice: 10, priority: 2 },
      { taskId: 'translate-docs', description: 'Translate docs to French', requiredCapabilities: ['nlp', 'french'], estimatedDuration: 8000, reservePrice: 15, priority: 1 },
      { taskId: 'code-review', description: 'Review PR #42', requiredCapabilities: ['coding'], estimatedDuration: 10000, reservePrice: 20, priority: 3 },
    ],
    {
      maxBidsPerAgent: 3,
      maxBundleSize: 2,
      biddingWindowMs: 30000,
      screeningEnabled: false,
      vcgEnabled: false,
      reservePriceEnforced: true,
      collusionThreshold: 0.7,
      capacityMargin: 0.2,
      maxSolverBacktracks: 1000,
      cacheResultsTTL: 60000,
    }
  );

  console.log(`  🔨 Created auction: ${auction.auctionId}\n`);

  // Register bidders
  const bidders = [
    { agentId: 'agent-alpha', capabilities: ['nlp'], maxConcurrency: 2, currentLoad: 0.3, reliabilityScore: 0.95, historicalLatency: 200 },
    { agentId: 'agent-beta', capabilities: ['nlp', 'french'], maxConcurrency: 3, currentLoad: 0.1, reliabilityScore: 0.88, historicalLatency: 350 },
    { agentId: 'agent-gamma', capabilities: ['coding', 'nlp'], maxConcurrency: 1, currentLoad: 0, reliabilityScore: 0.92, historicalLatency: 500 },
  ];

  for (const b of bidders) {
    auctioneer.registerBidder(auction.auctionId, b);
    console.log(`  👤 Registered bidder: ${b.agentId}`);
  }
  console.log();

  // Submit bids
  const bids = [
    { bidderId: 'agent-alpha', taskIds: ['summarize-report'], amount: 12, qualityScore: 0.9, estimatedCompletion: 4000 },
    { bidderId: 'agent-beta', taskIds: ['translate-docs'], amount: 18, qualityScore: 0.95, estimatedCompletion: 7000 },
    { bidderId: 'agent-beta', taskIds: ['summarize-report', 'translate-docs'], amount: 25, qualityScore: 0.9, estimatedCompletion: 12000 },
    { bidderId: 'agent-gamma', taskIds: ['code-review'], amount: 22, qualityScore: 0.85, estimatedCompletion: 9000 },
  ];

  for (const bid of bids) {
    const submitted = auctioneer.submitBid(auction.auctionId, bid);
    if (submitted) {
      console.log(`  💰 Bid: ${bid.bidderId} → [${bid.taskIds.join(', ')}] for $${bid.amount}`);
    }
  }

  // Close bidding
  auctioneer.closeBidding(auction.auctionId);
  console.log(`\n  🔒 Bidding closed. ${auction.bids.size} bids received.`);

  // In a real system, you'd run winner determination and VCG pricing here.
  // The auctioneer provides the scaffolding; allocation logic is pluggable.
  console.log('  📊 Ready for winner determination + VCG pricing.\n');
}

// ─── Run ─────────────────────────────────────────────────────────────────────

demoReputationRouting();
demoTaskAuction();
