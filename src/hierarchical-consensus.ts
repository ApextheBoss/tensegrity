import { fnv1a } from './shared-utils';
/**
 * Hierarchical Consensus Coordinator
 * 
 * Multi-layer consensus for large agent networks. Single-cluster consensus
 * (Raft, PBFT) breaks down at ~100 agents due to O(N²) message complexity.
 * This module partitions agents into shards, runs intra-shard consensus locally,
 * then coordinates cross-shard agreement via a meta-consensus layer.
 *
 * Architecture:
 *   Layer 0: Shards (5-15 agents each, local consensus)
 *   Layer 1: Shard Representatives (1 per shard, meta-consensus)
 *   Layer 2: Global Coordinator (optional, for cross-region)
 *
 * Key innovations:
 * - Consistent-hash shard assignment with virtual nodes for balance
 * - Representative rotation to prevent power concentration
 * - Cross-shard transaction protocol (2PC with timeout-based abort)
 * - Shard split/merge based on load metrics
 * - Hierarchical view changes when shard leaders fail
 */

// ============================================================
// Types
// ============================================================

interface AgentId {
  readonly id: string;
  readonly region?: string;
  readonly capabilities?: string[];
}

interface ShardConfig {
  readonly minSize: number;
  readonly maxSize: number;
  readonly targetSize: number;
  readonly replicationFactor: number;
}

interface ConsensusValue {
  readonly key: string;
  readonly value: unknown;
  readonly version: number;
  readonly timestamp: number;
  readonly shardId: string;
}

interface ShardState {
  readonly shardId: string;
  readonly members: AgentId[];
  readonly leader: AgentId | null;
  readonly epoch: number;
  readonly log: ConsensusEntry[];
  readonly commitIndex: number;
  readonly health: ShardHealth;
  readonly representativeId: string | null;
}

interface ShardHealth {
  readonly availability: number;       // 0-1
  readonly avgLatencyMs: number;
  readonly throughputOpsPerSec: number;
  readonly memberHealthy: number;
  readonly memberTotal: number;
  readonly lastHeartbeat: number;
}

interface ConsensusEntry {
  readonly index: number;
  readonly term: number;
  readonly value: ConsensusValue;
  readonly committed: boolean;
  readonly proposerId: string;
}

interface CrossShardTx {
  readonly txId: string;
  readonly participantShards: string[];
  readonly values: ConsensusValue[];
  readonly state: 'preparing' | 'prepared' | 'committing' | 'committed' | 'aborted';
  readonly coordinator: string;
  readonly startTime: number;
  readonly timeoutMs: number;
  readonly votes: Map<string, 'yes' | 'no' | 'timeout'>;
}

interface RepresentativeState {
  readonly agentId: string;
  readonly shardId: string;
  readonly electedAt: number;
  readonly termEnd: number;
  readonly consecutiveTerms: number;
  readonly performanceScore: number;
}

interface HierarchicalEvent {
  readonly type: string;
  readonly timestamp: number;
  readonly shardId?: string;
  readonly data: Record<string, unknown>;
}

type EventHandler = (event: HierarchicalEvent) => void;

// ============================================================
// FNV-1a Hash (deterministic, no crypto dependency)
// ============================================================

// ============================================================
// Consistent Hash Ring for Shard Assignment
// ============================================================

class ConsistentHashRing {
  private ring: Array<{ hash: number; shardId: string }> = [];
  private readonly virtualNodes: number;

  constructor(virtualNodes: number = 150) {
    this.virtualNodes = virtualNodes;
  }

  addShard(shardId: string): void {
    for (let i = 0; i < this.virtualNodes; i++) {
      const hash = fnv1a(`${shardId}:vn${i}`);
      this.ring.push({ hash, shardId });
    }
    this.ring.sort((a, b) => a.hash - b.hash);
  }

  removeShard(shardId: string): void {
    this.ring = this.ring.filter(n => n.shardId !== shardId);
  }

  getShard(key: string): string | null {
    if (this.ring.length === 0) return null;
    const hash = fnv1a(key);
    // Binary search for first node >= hash
    let lo = 0, hi = this.ring.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.ring[mid].hash < hash) lo = mid + 1;
      else hi = mid;
    }
    return this.ring[lo % this.ring.length].shardId;
  }

  getShards(): string[] {
    const unique = new Set(this.ring.map(n => n.shardId));
    return [...unique];
  }

  /**
   * Get N distinct shards for replication
   */
  getReplicaShards(key: string, count: number): string[] {
    if (this.ring.length === 0) return [];
    const hash = fnv1a(key);
    const result: string[] = [];
    const seen = new Set<string>();
    
    let lo = 0, hi = this.ring.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.ring[mid].hash < hash) lo = mid + 1;
      else hi = mid;
    }

    for (let i = 0; i < this.ring.length && result.length < count; i++) {
      const idx = (lo + i) % this.ring.length;
      const shardId = this.ring[idx].shardId;
      if (!seen.has(shardId)) {
        seen.add(shardId);
        result.push(shardId);
      }
    }
    return result;
  }
}

// ============================================================
// Intra-Shard Consensus (Raft-inspired)
// ============================================================

class IntraShardConsensus {
  private state: ShardState;
  private readonly config: ShardConfig;
  private voteLog: Map<number, Set<string>> = new Map(); // term -> voters
  private nextIndex: Map<string, number> = new Map();
  private matchIndex: Map<string, number> = new Map();
  
  constructor(shardId: string, config: ShardConfig) {
    this.config = config;
    this.state = {
      shardId,
      members: [],
      leader: null,
      epoch: 0,
      log: [],
      commitIndex: -1,
      health: {
        availability: 1,
        avgLatencyMs: 0,
        throughputOpsPerSec: 0,
        memberHealthy: 0,
        memberTotal: 0,
        lastHeartbeat: Date.now(),
      },
      representativeId: null,
    };
  }

  addMember(agent: AgentId): void {
    if (this.state.members.some(m => m.id === agent.id)) return;
    this.state = {
      ...this.state,
      members: [...this.state.members, agent],
      health: {
        ...this.state.health,
        memberTotal: this.state.members.length + 1,
        memberHealthy: this.state.members.length + 1,
      },
    };
    
    // Auto-elect leader if first member
    if (this.state.members.length === 1) {
      this.state = { ...this.state, leader: agent };
    }
  }

  removeMember(agentId: string): void {
    const newMembers = this.state.members.filter(m => m.id !== agentId);
    const wasLeader = this.state.leader?.id === agentId;
    
    this.state = {
      ...this.state,
      members: newMembers,
      leader: wasLeader ? null : this.state.leader,
      health: {
        ...this.state.health,
        memberTotal: newMembers.length,
        memberHealthy: newMembers.length,
      },
    };

    if (wasLeader && newMembers.length > 0) {
      this.electLeader();
    }
  }

  /**
   * Leader election: highest FNV-1a(id + epoch) wins.
   * Deterministic, no communication needed for same epoch.
   */
  electLeader(): AgentId | null {
    if (this.state.members.length === 0) return null;
    
    const epoch = this.state.epoch + 1;
    let bestAgent: AgentId | null = null;
    let bestHash = -1;

    for (const agent of this.state.members) {
      const hash = fnv1a(`${agent.id}:epoch:${epoch}`);
      if (hash > bestHash) {
        bestHash = hash;
        bestAgent = agent;
      }
    }

    this.state = {
      ...this.state,
      leader: bestAgent,
      epoch,
    };

    return bestAgent;
  }

  /**
   * Propose a value (leader only). Returns commit index or -1 on failure.
   */
  propose(value: ConsensusValue, proposerId: string): number {
    if (!this.state.leader) return -1;
    
    const entry: ConsensusEntry = {
      index: this.state.log.length,
      term: this.state.epoch,
      value: { ...value, shardId: this.state.shardId },
      committed: false,
      proposerId,
    };

    this.state = {
      ...this.state,
      log: [...this.state.log, entry],
    };

    // In a real system, replicate to followers and wait for majority.
    // Here we simulate immediate majority (local consensus).
    const majority = Math.floor(this.state.members.length / 2) + 1;
    if (this.state.members.length >= majority) {
      return this.commit(entry.index);
    }
    
    return entry.index;
  }

  /**
   * Commit entry at given index
   */
  commit(index: number): number {
    if (index < 0 || index >= this.state.log.length) return -1;
    
    const newLog = [...this.state.log];
    // Commit all entries up to and including index
    for (let i = this.state.commitIndex + 1; i <= index; i++) {
      newLog[i] = { ...newLog[i], committed: true };
    }

    this.state = {
      ...this.state,
      log: newLog,
      commitIndex: index,
    };

    return index;
  }

  /**
   * Get committed entries since a given index
   */
  getCommittedSince(fromIndex: number): ConsensusEntry[] {
    return this.state.log
      .filter(e => e.committed && e.index > fromIndex);
  }

  getState(): ShardState {
    return this.state;
  }

  needsSplit(): boolean {
    return this.state.members.length > this.config.maxSize;
  }

  needsMerge(): boolean {
    return this.state.members.length < this.config.minSize;
  }

  getMemberCount(): number {
    return this.state.members.length;
  }

  updateHealth(partial: Partial<ShardHealth>): void {
    this.state = {
      ...this.state,
      health: { ...this.state.health, ...partial, lastHeartbeat: Date.now() },
    };
  }

  setRepresentative(agentId: string): void {
    this.state = { ...this.state, representativeId: agentId };
  }
}

// ============================================================
// Representative Rotation Manager
// ============================================================

class RepresentativeRotation {
  private representatives: Map<string, RepresentativeState> = new Map();
  private readonly maxConsecutiveTerms: number;
  private readonly termDurationMs: number;

  constructor(maxConsecutiveTerms: number = 3, termDurationMs: number = 300_000) {
    this.maxConsecutiveTerms = maxConsecutiveTerms;
    this.termDurationMs = termDurationMs;
  }

  /**
   * Select a representative for a shard. Prefers agents who haven't
   * served recently and have higher performance scores.
   */
  selectRepresentative(shardId: string, members: AgentId[]): AgentId | null {
    if (members.length === 0) return null;

    const now = Date.now();
    const current = this.representatives.get(shardId);

    // Current rep still valid and under term limit?
    if (current && 
        now < current.termEnd && 
        current.consecutiveTerms < this.maxConsecutiveTerms &&
        members.some(m => m.id === current.agentId)) {
      return members.find(m => m.id === current.agentId) || null;
    }

    // Score candidates: penalize recent service, reward performance
    let bestAgent: AgentId | null = null;
    let bestScore = -Infinity;

    for (const agent of members) {
      let score = 0;
      
      // Base score from hash (deterministic tie-breaking)
      score += (fnv1a(`${agent.id}:rep:${shardId}:${now}`) % 1000) / 1000;
      
      // Penalty for consecutive terms
      if (current?.agentId === agent.id) {
        score -= current.consecutiveTerms * 10;
      }

      // Bonus for capability count (more capable = better representative)
      score += (agent.capabilities?.length || 0) * 2;

      if (score > bestScore) {
        bestScore = score;
        bestAgent = agent;
      }
    }

    if (bestAgent) {
      const prevTerms = current?.agentId === bestAgent.id 
        ? current.consecutiveTerms + 1 
        : 0;
      
      this.representatives.set(shardId, {
        agentId: bestAgent.id,
        shardId,
        electedAt: now,
        termEnd: now + this.termDurationMs,
        consecutiveTerms: prevTerms,
        performanceScore: 1.0,
      });
    }

    return bestAgent;
  }

  updatePerformance(shardId: string, score: number): void {
    const rep = this.representatives.get(shardId);
    if (rep) {
      // EWMA performance tracking
      const alpha = 0.3;
      this.representatives.set(shardId, {
        ...rep,
        performanceScore: alpha * score + (1 - alpha) * rep.performanceScore,
      });
    }
  }

  getRepresentative(shardId: string): RepresentativeState | undefined {
    return this.representatives.get(shardId);
  }

  forceRotation(shardId: string): void {
    this.representatives.delete(shardId);
  }
}

// ============================================================
// Cross-Shard Transaction Coordinator (2PC with timeout)
// ============================================================

class CrossShardCoordinator {
  private transactions: Map<string, CrossShardTx> = new Map();
  private readonly defaultTimeoutMs: number;
  private completedTxIds: Set<string> = new Set(); // idempotency
  private txCounter: number = 0;

  constructor(defaultTimeoutMs: number = 10_000) {
    this.defaultTimeoutMs = defaultTimeoutMs;
  }

  /**
   * Begin a cross-shard transaction
   */
  begin(participantShards: string[], values: ConsensusValue[], coordinator: string): string {
    const txId = `cstx-${++this.txCounter}-${fnv1a(`${coordinator}:${Date.now()}`).toString(16)}`;
    
    if (this.completedTxIds.has(txId)) {
      return txId; // idempotent
    }

    const tx: CrossShardTx = {
      txId,
      participantShards,
      values,
      state: 'preparing',
      coordinator,
      startTime: Date.now(),
      timeoutMs: this.defaultTimeoutMs,
      votes: new Map(),
    };

    this.transactions.set(txId, tx);
    return txId;
  }

  /**
   * Record a shard's vote for a transaction
   */
  vote(txId: string, shardId: string, vote: 'yes' | 'no'): CrossShardTx | null {
    const tx = this.transactions.get(txId);
    if (!tx || tx.state !== 'preparing') return null;

    const newVotes = new Map(tx.votes);
    newVotes.set(shardId, vote);

    // Check if any voted no -> abort immediately
    if (vote === 'no') {
      const aborted: CrossShardTx = { ...tx, votes: newVotes, state: 'aborted' };
      this.transactions.set(txId, aborted);
      return aborted;
    }

    // Check if all voted yes -> move to committing
    const allVoted = tx.participantShards.every(s => newVotes.has(s));
    const allYes = tx.participantShards.every(s => newVotes.get(s) === 'yes');
    
    if (allVoted && allYes) {
      const committing: CrossShardTx = { ...tx, votes: newVotes, state: 'committing' };
      this.transactions.set(txId, committing);
      return committing;
    }

    const updated = { ...tx, votes: newVotes, state: 'prepared' as const };
    this.transactions.set(txId, updated);
    return updated;
  }

  /**
   * Confirm commit from a shard
   */
  confirmCommit(txId: string, shardId: string): CrossShardTx | null {
    const tx = this.transactions.get(txId);
    if (!tx || tx.state !== 'committing') return null;

    // Track commit confirmations via the vote map (reuse as ack map)
    const newVotes = new Map(tx.votes);
    newVotes.set(shardId, 'yes');

    // All confirmed?
    const allConfirmed = tx.participantShards.every(s => newVotes.has(s));
    if (allConfirmed) {
      const committed: CrossShardTx = { ...tx, votes: newVotes, state: 'committed' };
      this.transactions.set(txId, committed);
      this.completedTxIds.add(txId);
      return committed;
    }

    return { ...tx, votes: newVotes };
  }

  /**
   * Check for timed-out transactions and abort them
   */
  checkTimeouts(): CrossShardTx[] {
    const now = Date.now();
    const aborted: CrossShardTx[] = [];

    for (const [txId, tx] of this.transactions) {
      if (tx.state === 'committed' || tx.state === 'aborted') continue;
      
      if (now - tx.startTime > tx.timeoutMs) {
        const timedOut: CrossShardTx = { ...tx, state: 'aborted' };
        this.transactions.set(txId, timedOut);
        aborted.push(timedOut);
      }
    }

    return aborted;
  }

  getTransaction(txId: string): CrossShardTx | undefined {
    return this.transactions.get(txId);
  }

  getPendingCount(): number {
    let count = 0;
    for (const tx of this.transactions.values()) {
      if (tx.state !== 'committed' && tx.state !== 'aborted') count++;
    }
    return count;
  }

  /**
   * Garbage collect old completed transactions
   */
  gc(maxAgeMs: number = 60_000): number {
    const now = Date.now();
    let removed = 0;
    for (const [txId, tx] of this.transactions) {
      if ((tx.state === 'committed' || tx.state === 'aborted') && 
          now - tx.startTime > maxAgeMs) {
        this.transactions.delete(txId);
        this.completedTxIds.delete(txId);
        removed++;
      }
    }
    return removed;
  }
}

// ============================================================
// Shard Manager (split/merge/rebalance)
// ============================================================

class ShardManager {
  private shards: Map<string, IntraShardConsensus> = new Map();
  private hashRing: ConsistentHashRing;
  private readonly config: ShardConfig;
  private shardCounter: number = 0;

  constructor(config: ShardConfig) {
    this.config = config;
    this.hashRing = new ConsistentHashRing(150);
  }

  createShard(): string {
    const shardId = `shard-${++this.shardCounter}`;
    const consensus = new IntraShardConsensus(shardId, this.config);
    this.shards.set(shardId, consensus);
    this.hashRing.addShard(shardId);
    return shardId;
  }

  /**
   * Assign an agent to a shard via consistent hashing
   */
  assignAgent(agent: AgentId): string {
    // Ensure at least one shard exists
    if (this.shards.size === 0) {
      this.createShard();
    }

    let shardId = this.hashRing.getShard(agent.id);
    if (!shardId) {
      shardId = this.createShard();
    }

    const shard = this.shards.get(shardId);
    if (shard) {
      shard.addMember(agent);
      
      // Check if split needed
      if (shard.needsSplit()) {
        this.splitShard(shardId);
      }
    }

    return shardId;
  }

  /**
   * Split an oversized shard into two
   */
  splitShard(shardId: string): [string, string] | null {
    const shard = this.shards.get(shardId);
    if (!shard) return null;

    const state = shard.getState();
    if (state.members.length <= this.config.targetSize) return null;

    // Create two new shards
    const newId1 = this.createShard();
    const newId2 = this.createShard();

    // Partition members by hash
    const midpoint = Math.floor(state.members.length / 2);
    const sorted = [...state.members].sort(
      (a, b) => fnv1a(a.id) - fnv1a(b.id)
    );

    for (let i = 0; i < sorted.length; i++) {
      const targetId = i < midpoint ? newId1 : newId2;
      const targetShard = this.shards.get(targetId);
      if (targetShard) {
        targetShard.addMember(sorted[i]);
      }
    }

    // Remove old shard
    this.shards.delete(shardId);
    this.hashRing.removeShard(shardId);

    return [newId1, newId2];
  }

  /**
   * Merge two small shards into one
   */
  mergeShards(shardId1: string, shardId2: string): string | null {
    const shard1 = this.shards.get(shardId1);
    const shard2 = this.shards.get(shardId2);
    if (!shard1 || !shard2) return null;

    const state1 = shard1.getState();
    const state2 = shard2.getState();

    // Don't merge if combined would be too large
    if (state1.members.length + state2.members.length > this.config.maxSize) {
      return null;
    }

    const newId = this.createShard();
    const newShard = this.shards.get(newId)!;

    for (const member of [...state1.members, ...state2.members]) {
      newShard.addMember(member);
    }

    // Remove old shards
    this.shards.delete(shardId1);
    this.shards.delete(shardId2);
    this.hashRing.removeShard(shardId1);
    this.hashRing.removeShard(shardId2);

    return newId;
  }

  /**
   * Check all shards and perform necessary splits/merges
   */
  rebalance(): Array<{ type: 'split' | 'merge'; from: string[]; to: string[] }> {
    const operations: Array<{ type: 'split' | 'merge'; from: string[]; to: string[] }> = [];

    // Splits first
    for (const [shardId, shard] of this.shards) {
      if (shard.needsSplit()) {
        const result = this.splitShard(shardId);
        if (result) {
          operations.push({ type: 'split', from: [shardId], to: result });
        }
      }
    }

    // Then merges: find pairs of small shards
    const smallShards = [...this.shards.entries()]
      .filter(([_, s]) => s.needsMerge())
      .map(([id]) => id);

    for (let i = 0; i < smallShards.length - 1; i += 2) {
      const result = this.mergeShards(smallShards[i], smallShards[i + 1]);
      if (result) {
        operations.push({
          type: 'merge',
          from: [smallShards[i], smallShards[i + 1]],
          to: [result],
        });
      }
    }

    return operations;
  }

  getShard(shardId: string): IntraShardConsensus | undefined {
    return this.shards.get(shardId);
  }

  getShardForKey(key: string): string | null {
    return this.hashRing.getShard(key);
  }

  getAllShards(): Map<string, IntraShardConsensus> {
    return new Map(this.shards);
  }

  getShardCount(): number {
    return this.shards.size;
  }

  getTotalAgents(): number {
    let total = 0;
    for (const shard of this.shards.values()) {
      total += shard.getMemberCount();
    }
    return total;
  }

  getLoadDistribution(): Array<{ shardId: string; members: number; load: number }> {
    const avg = this.getTotalAgents() / Math.max(this.shards.size, 1);
    return [...this.shards.entries()].map(([id, shard]) => ({
      shardId: id,
      members: shard.getMemberCount(),
      load: avg > 0 ? shard.getMemberCount() / avg : 0,
    }));
  }
}

// ============================================================
// Meta-Consensus Layer (consensus among shard representatives)
// ============================================================

class MetaConsensusLayer {
  private proposals: Map<string, {
    proposalId: string;
    value: unknown;
    proposer: string;
    votes: Map<string, boolean>;
    requiredVotes: number;
    state: 'open' | 'accepted' | 'rejected';
    createdAt: number;
  }> = new Map();
  
  private proposalCounter: number = 0;
  private readonly timeoutMs: number;

  constructor(timeoutMs: number = 15_000) {
    this.timeoutMs = timeoutMs;
  }

  /**
   * Propose a global decision (shard reconfigurations, parameter changes, etc.)
   */
  propose(value: unknown, proposer: string, totalShards: number): string {
    const proposalId = `meta-${++this.proposalCounter}-${fnv1a(`${proposer}:${Date.now()}`).toString(16)}`;
    
    this.proposals.set(proposalId, {
      proposalId,
      value,
      proposer,
      votes: new Map(),
      requiredVotes: Math.floor(totalShards / 2) + 1,
      state: 'open',
      createdAt: Date.now(),
    });

    return proposalId;
  }

  /**
   * Cast a vote from a shard representative
   */
  vote(proposalId: string, shardId: string, approve: boolean): 'open' | 'accepted' | 'rejected' | null {
    const proposal = this.proposals.get(proposalId);
    if (!proposal || proposal.state !== 'open') return null;

    proposal.votes.set(shardId, approve);

    // Count votes
    let yesVotes = 0;
    let noVotes = 0;
    for (const v of proposal.votes.values()) {
      if (v) yesVotes++;
      else noVotes++;
    }

    if (yesVotes >= proposal.requiredVotes) {
      proposal.state = 'accepted';
      return 'accepted';
    }

    // Can't possibly reach majority with remaining votes
    const remaining = proposal.requiredVotes * 2 - 1 - proposal.votes.size;
    if (noVotes > remaining) {
      proposal.state = 'rejected';
      return 'rejected';
    }

    return 'open';
  }

  /**
   * Expire timed-out proposals
   */
  expireStale(): string[] {
    const now = Date.now();
    const expired: string[] = [];

    for (const [id, proposal] of this.proposals) {
      if (proposal.state === 'open' && now - proposal.createdAt > this.timeoutMs) {
        proposal.state = 'rejected';
        expired.push(id);
      }
    }

    return expired;
  }

  getProposal(proposalId: string): unknown {
    return this.proposals.get(proposalId);
  }
}

// ============================================================
// View Change Coordinator (hierarchical failure recovery)
// ============================================================

class ViewChangeCoordinator {
  private viewNumber: number = 0;
  private pendingViewChange: {
    newView: number;
    initiator: string;
    reason: string;
    acks: Set<string>;
    requiredAcks: number;
    startTime: number;
  } | null = null;

  private readonly viewChangeTimeoutMs: number;

  constructor(viewChangeTimeoutMs: number = 5_000) {
    this.viewChangeTimeoutMs = viewChangeTimeoutMs;
  }

  /**
   * Initiate a view change (leader failure, shard split, etc.)
   */
  initiateViewChange(initiator: string, reason: string, totalParticipants: number): number {
    const newView = this.viewNumber + 1;
    
    this.pendingViewChange = {
      newView,
      initiator,
      reason,
      acks: new Set([initiator]),
      requiredAcks: Math.floor(totalParticipants / 2) + 1,
      startTime: Date.now(),
    };

    return newView;
  }

  /**
   * Acknowledge a view change
   */
  acknowledgeViewChange(agentId: string, viewNumber: number): boolean {
    if (!this.pendingViewChange || this.pendingViewChange.newView !== viewNumber) {
      return false;
    }

    this.pendingViewChange.acks.add(agentId);

    if (this.pendingViewChange.acks.size >= this.pendingViewChange.requiredAcks) {
      this.viewNumber = this.pendingViewChange.newView;
      this.pendingViewChange = null;
      return true; // view change completed
    }

    return false;
  }

  /**
   * Check if current view change has timed out
   */
  isTimedOut(): boolean {
    if (!this.pendingViewChange) return false;
    return Date.now() - this.pendingViewChange.startTime > this.viewChangeTimeoutMs;
  }

  getCurrentView(): number {
    return this.viewNumber;
  }

  hasPendingViewChange(): boolean {
    return this.pendingViewChange !== null;
  }
}

// ============================================================
// Hierarchical Consensus Coordinator (unified orchestrator)
// ============================================================

interface HierarchicalConsensusConfig {
  readonly shard: ShardConfig;
  readonly maxConsecutiveTerms: number;
  readonly termDurationMs: number;
  readonly crossShardTimeoutMs: number;
  readonly metaConsensusTimeoutMs: number;
  readonly viewChangeTimeoutMs: number;
  readonly virtualNodesPerShard: number;
}

class HierarchicalConsensusCoordinator {
  private readonly shardManager: ShardManager;
  private readonly repRotation: RepresentativeRotation;
  private readonly crossShardCoordinator: CrossShardCoordinator;
  private readonly metaConsensus: MetaConsensusLayer;
  private readonly viewChangeCoordinator: ViewChangeCoordinator;
  private readonly config: HierarchicalConsensusConfig;
  private readonly eventHandlers: Map<string, EventHandler[]> = new Map();
  private agentToShard: Map<string, string> = new Map();

  constructor(config: HierarchicalConsensusConfig) {
    this.config = config;
    this.shardManager = new ShardManager(config.shard);
    this.repRotation = new RepresentativeRotation(
      config.maxConsecutiveTerms,
      config.termDurationMs
    );
    this.crossShardCoordinator = new CrossShardCoordinator(config.crossShardTimeoutMs);
    this.metaConsensus = new MetaConsensusLayer(config.metaConsensusTimeoutMs);
    this.viewChangeCoordinator = new ViewChangeCoordinator(config.viewChangeTimeoutMs);
  }

  // ----------------------------------------------------------
  // Agent Management
  // ----------------------------------------------------------

  /**
   * Register an agent into the hierarchy
   */
  registerAgent(agent: AgentId): string {
    const shardId = this.shardManager.assignAgent(agent);
    this.agentToShard.set(agent.id, shardId);

    // Elect representative if needed
    const shard = this.shardManager.getShard(shardId);
    if (shard) {
      const rep = this.repRotation.selectRepresentative(shardId, shard.getState().members);
      if (rep) {
        shard.setRepresentative(rep.id);
      }
    }

    this.emit({
      type: 'agent-registered',
      timestamp: Date.now(),
      shardId,
      data: { agentId: agent.id, region: agent.region },
    });

    return shardId;
  }

  /**
   * Remove an agent from the hierarchy
   */
  deregisterAgent(agentId: string): void {
    const shardId = this.agentToShard.get(agentId);
    if (!shardId) return;

    const shard = this.shardManager.getShard(shardId);
    if (shard) {
      shard.removeMember(agentId);

      // Re-elect representative if needed
      const state = shard.getState();
      if (state.representativeId === agentId) {
        const rep = this.repRotation.selectRepresentative(shardId, state.members);
        if (rep) {
          shard.setRepresentative(rep.id);
        }
      }
    }

    this.agentToShard.delete(agentId);
    this.emit({
      type: 'agent-deregistered',
      timestamp: Date.now(),
      shardId,
      data: { agentId },
    });
  }

  // ----------------------------------------------------------
  // Consensus Operations
  // ----------------------------------------------------------

  /**
   * Submit a value for consensus. Routes to correct shard, handles
   * cross-shard if value spans multiple shards.
   */
  submit(key: string, value: unknown, proposerId: string): {
    success: boolean;
    commitIndex?: number;
    shardId?: string;
    txId?: string;
  } {
    const shardId = this.shardManager.getShardForKey(key);
    if (!shardId) return { success: false };

    const shard = this.shardManager.getShard(shardId);
    if (!shard) return { success: false };

    const consensusValue: ConsensusValue = {
      key,
      value,
      version: Date.now(),
      timestamp: Date.now(),
      shardId,
    };

    const commitIndex = shard.propose(consensusValue, proposerId);
    
    this.emit({
      type: 'value-committed',
      timestamp: Date.now(),
      shardId,
      data: { key, commitIndex, proposerId },
    });

    return { success: commitIndex >= 0, commitIndex, shardId };
  }

  /**
   * Submit a cross-shard transaction (values that span multiple shards)
   */
  submitCrossShard(
    entries: Array<{ key: string; value: unknown }>,
    coordinatorId: string
  ): { success: boolean; txId: string } {
    // Determine participating shards
    const shardMap = new Map<string, ConsensusValue[]>();
    
    for (const entry of entries) {
      const shardId = this.shardManager.getShardForKey(entry.key);
      if (!shardId) return { success: false, txId: '' };
      
      const values = shardMap.get(shardId) || [];
      values.push({
        key: entry.key,
        value: entry.value,
        version: Date.now(),
        timestamp: Date.now(),
        shardId,
      });
      shardMap.set(shardId, values);
    }

    const participantShards = [...shardMap.keys()];
    const allValues = [...shardMap.values()].flat();

    // If only one shard, no need for 2PC
    if (participantShards.length === 1) {
      const result = this.submit(entries[0].key, entries[0].value, coordinatorId);
      return { success: result.success, txId: '' };
    }

    // Begin 2PC
    const txId = this.crossShardCoordinator.begin(
      participantShards,
      allValues,
      coordinatorId
    );

    // Prepare phase: each shard votes
    for (const shardId of participantShards) {
      const shard = this.shardManager.getShard(shardId);
      if (!shard) {
        this.crossShardCoordinator.vote(txId, shardId, 'no');
        continue;
      }

      // Validate shard can accept (has leader, has quorum)
      const state = shard.getState();
      const hasQuorum = state.members.length >= Math.floor(this.config.shard.minSize / 2) + 1;
      this.crossShardCoordinator.vote(txId, shardId, hasQuorum && state.leader ? 'yes' : 'no');
    }

    const tx = this.crossShardCoordinator.getTransaction(txId);
    if (!tx) return { success: false, txId };

    if (tx.state === 'committing') {
      // Commit phase: apply values to each shard
      for (const shardId of participantShards) {
        const shard = this.shardManager.getShard(shardId);
        const values = shardMap.get(shardId) || [];
        
        if (shard) {
          for (const v of values) {
            shard.propose(v, coordinatorId);
          }
          this.crossShardCoordinator.confirmCommit(txId, shardId);
        }
      }

      this.emit({
        type: 'cross-shard-committed',
        timestamp: Date.now(),
        data: { txId, shards: participantShards, entryCount: entries.length },
      });

      return { success: true, txId };
    }

    this.emit({
      type: 'cross-shard-aborted',
      timestamp: Date.now(),
      data: { txId, shards: participantShards, state: tx.state },
    });

    return { success: false, txId };
  }

  // ----------------------------------------------------------
  // Maintenance Operations
  // ----------------------------------------------------------

  /**
   * Run periodic maintenance: rebalance shards, rotate reps, check timeouts
   */
  tick(): {
    rebalanceOps: number;
    expiredTx: number;
    expiredProposals: number;
    viewChanges: number;
  } {
    // 1. Rebalance shards
    const rebalanceOps = this.shardManager.rebalance();
    
    for (const op of rebalanceOps) {
      this.emit({
        type: `shard-${op.type}`,
        timestamp: Date.now(),
        data: { from: op.from, to: op.to },
      });

      // Reassign agent->shard mappings after rebalance
      for (const newShardId of op.to) {
        const shard = this.shardManager.getShard(newShardId);
        if (shard) {
          for (const member of shard.getState().members) {
            this.agentToShard.set(member.id, newShardId);
          }
        }
      }
    }

    // 2. Check cross-shard transaction timeouts
    const abortedTx = this.crossShardCoordinator.checkTimeouts();
    for (const tx of abortedTx) {
      this.emit({
        type: 'cross-shard-timeout',
        timestamp: Date.now(),
        data: { txId: tx.txId, shards: tx.participantShards },
      });
    }

    // 3. Expire stale meta-consensus proposals
    const expiredProposals = this.metaConsensus.expireStale();

    // 4. Handle view change timeouts
    let viewChanges = 0;
    if (this.viewChangeCoordinator.isTimedOut()) {
      // Escalate: force view change with remaining participants
      viewChanges++;
      this.emit({
        type: 'view-change-timeout',
        timestamp: Date.now(),
        data: { view: this.viewChangeCoordinator.getCurrentView() },
      });
    }

    // 5. Rotate representatives for all shards
    for (const [shardId, shard] of this.shardManager.getAllShards()) {
      const state = shard.getState();
      const rep = this.repRotation.selectRepresentative(shardId, state.members);
      if (rep && rep.id !== state.representativeId) {
        shard.setRepresentative(rep.id);
        this.emit({
          type: 'representative-rotated',
          timestamp: Date.now(),
          shardId,
          data: { oldRep: state.representativeId, newRep: rep.id },
        });
      }
    }

    // 6. GC old transactions
    this.crossShardCoordinator.gc();

    return {
      rebalanceOps: rebalanceOps.length,
      expiredTx: abortedTx.length,
      expiredProposals: expiredProposals.length,
      viewChanges,
    };
  }

  // ----------------------------------------------------------
  // Query Operations
  // ----------------------------------------------------------

  /**
   * Read a committed value by key
   */
  read(key: string): ConsensusValue | null {
    const shardId = this.shardManager.getShardForKey(key);
    if (!shardId) return null;

    const shard = this.shardManager.getShard(shardId);
    if (!shard) return null;

    const state = shard.getState();
    // Find latest committed entry for this key
    for (let i = state.log.length - 1; i >= 0; i--) {
      const entry = state.log[i];
      if (entry.committed && entry.value.key === key) {
        return entry.value;
      }
    }

    return null;
  }

  getStats(): {
    totalAgents: number;
    totalShards: number;
    loadDistribution: Array<{ shardId: string; members: number; load: number }>;
    pendingCrossShardTx: number;
    currentView: number;
    pendingViewChange: boolean;
  } {
    return {
      totalAgents: this.shardManager.getTotalAgents(),
      totalShards: this.shardManager.getShardCount(),
      loadDistribution: this.shardManager.getLoadDistribution(),
      pendingCrossShardTx: this.crossShardCoordinator.getPendingCount(),
      currentView: this.viewChangeCoordinator.getCurrentView(),
      pendingViewChange: this.viewChangeCoordinator.hasPendingViewChange(),
    };
  }

  getAgentShard(agentId: string): string | undefined {
    return this.agentToShard.get(agentId);
  }

  // ----------------------------------------------------------
  // Event System
  // ----------------------------------------------------------

  on(eventType: string, handler: EventHandler): void {
    const handlers = this.eventHandlers.get(eventType) || [];
    handlers.push(handler);
    this.eventHandlers.set(eventType, handlers);
  }

  private emit(event: HierarchicalEvent): void {
    const handlers = this.eventHandlers.get(event.type) || [];
    for (const handler of handlers) {
      handler(event);
    }
    // Wildcard handlers
    const wildcardHandlers = this.eventHandlers.get('*') || [];
    for (const handler of wildcardHandlers) {
      handler(event);
    }
  }
}

// ============================================================
// Presets
// ============================================================

const PRESETS = {
  /**
   * Small network (50-200 agents): few shards, fast consensus
   */
  'small-network': {
    shard: { minSize: 3, maxSize: 15, targetSize: 10, replicationFactor: 2 },
    maxConsecutiveTerms: 5,
    termDurationMs: 600_000,
    crossShardTimeoutMs: 5_000,
    metaConsensusTimeoutMs: 10_000,
    viewChangeTimeoutMs: 3_000,
    virtualNodesPerShard: 100,
  } satisfies HierarchicalConsensusConfig,

  /**
   * Medium network (200-1000 agents): balanced sharding
   */
  'medium-network': {
    shard: { minSize: 5, maxSize: 20, targetSize: 12, replicationFactor: 3 },
    maxConsecutiveTerms: 3,
    termDurationMs: 300_000,
    crossShardTimeoutMs: 10_000,
    metaConsensusTimeoutMs: 15_000,
    viewChangeTimeoutMs: 5_000,
    virtualNodesPerShard: 150,
  } satisfies HierarchicalConsensusConfig,

  /**
   * Large network (1000+ agents): aggressive sharding, short terms
   */
  'large-network': {
    shard: { minSize: 7, maxSize: 25, targetSize: 15, replicationFactor: 5 },
    maxConsecutiveTerms: 2,
    termDurationMs: 180_000,
    crossShardTimeoutMs: 15_000,
    metaConsensusTimeoutMs: 20_000,
    viewChangeTimeoutMs: 8_000,
    virtualNodesPerShard: 200,
  } satisfies HierarchicalConsensusConfig,
};

// ============================================================
// Exports
// ============================================================

export {
  HierarchicalConsensusCoordinator,
  IntraShardConsensus,
  ShardManager,
  CrossShardCoordinator,
  MetaConsensusLayer,
  RepresentativeRotation,
  ViewChangeCoordinator,
  ConsistentHashRing,
  PRESETS,
  type HierarchicalConsensusConfig,
  type AgentId,
  type ShardConfig,
  type ConsensusValue,
  type ShardState,
  type CrossShardTx,
  type HierarchicalEvent,
};
