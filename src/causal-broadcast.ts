/**
 * Causal Broadcast Protocol for Agent Networks
 * 
 * Reliable causal ordering of broadcast messages across distributed agents
 * with vector clock enforcement, message buffering, and partition tolerance.
 * 
 * Components:
 * - VectorClockManager: Per-agent logical clock maintenance with merge semantics
 * - CausalDeliveryBuffer: Hold-back queue ensuring causal delivery order
 * - ReliableBroadcastLayer: Best-effort → reliable broadcast with retransmission
 * - MessageStabilityDetector: Identify globally-stable messages safe for GC
 * - PartitionAwareBroadcaster: Adapt broadcast strategy during network partitions
 * - GossipRepairProtocol: Anti-entropy repair for missed messages
 * - DeliveryGuaranteeTracker: Per-agent delivery confirmation and progress monitoring
 * - CausalBroadcastProtocol: Unified orchestrator
 */

// ─── Vector Clock Manager ───────────────────────────────────────────────────

interface VectorClock {
  readonly entries: ReadonlyMap<string, number>;
}

interface ClockComparison {
  readonly relation: 'before' | 'after' | 'concurrent' | 'equal';
  readonly divergentDimensions: number;
}

class VectorClockManager {
  private readonly clocks: Map<string, Map<string, number>> = new Map();

  initialize(agentId: string): VectorClock {
    if (!this.clocks.has(agentId)) {
      this.clocks.set(agentId, new Map());
    }
    return this.getClock(agentId);
  }

  increment(agentId: string): VectorClock {
    const clock = this.getOrCreateClock(agentId);
    clock.set(agentId, (clock.get(agentId) ?? 0) + 1);
    return { entries: new Map(clock) };
  }

  merge(agentId: string, remoteClock: VectorClock): VectorClock {
    const local = this.getOrCreateClock(agentId);
    for (const [dim, val] of remoteClock.entries) {
      const current = local.get(dim) ?? 0;
      if (val > current) {
        local.set(dim, val);
      }
    }
    // Increment own dimension after merge
    local.set(agentId, (local.get(agentId) ?? 0) + 1);
    return { entries: new Map(local) };
  }

  compare(a: VectorClock, b: VectorClock): ClockComparison {
    const allDims = new Set([...a.entries.keys(), ...b.entries.keys()]);
    let aBeforeB = false;
    let bBeforeA = false;
    let divergent = 0;

    for (const dim of allDims) {
      const va = a.entries.get(dim) ?? 0;
      const vb = b.entries.get(dim) ?? 0;
      if (va < vb) { aBeforeB = true; divergent++; }
      if (va > vb) { bBeforeA = true; divergent++; }
    }

    if (!aBeforeB && !bBeforeA) return { relation: 'equal', divergentDimensions: 0 };
    if (aBeforeB && !bBeforeA) return { relation: 'before', divergentDimensions: divergent };
    if (!aBeforeB && bBeforeA) return { relation: 'after', divergentDimensions: divergent };
    return { relation: 'concurrent', divergentDimensions: divergent };
  }

  /**
   * Check if clock A causally precedes or equals clock B.
   * Used for delivery condition: message deliverable when sender's
   * prior clock ≤ receiver's current clock.
   */
  happensBefore(a: VectorClock, b: VectorClock): boolean {
    const cmp = this.compare(a, b);
    return cmp.relation === 'before' || cmp.relation === 'equal';
  }

  getClock(agentId: string): VectorClock {
    const clock = this.clocks.get(agentId);
    return { entries: new Map(clock ?? new Map()) };
  }

  /**
   * Compute component-wise minimum across all known clocks.
   * Messages with all dimensions ≤ this are globally stable.
   */
  computeGlobalMinimum(): VectorClock {
    if (this.clocks.size === 0) return { entries: new Map() };

    const allDims = new Set<string>();
    for (const clock of this.clocks.values()) {
      for (const dim of clock.keys()) allDims.add(dim);
    }

    const min = new Map<string, number>();
    for (const dim of allDims) {
      let minVal = Infinity;
      for (const clock of this.clocks.values()) {
        const v = clock.get(dim) ?? 0;
        if (v < minVal) minVal = v;
      }
      min.set(dim, minVal === Infinity ? 0 : minVal);
    }

    return { entries: min };
  }

  private getOrCreateClock(agentId: string): Map<string, number> {
    let clock = this.clocks.get(agentId);
    if (!clock) {
      clock = new Map();
      this.clocks.set(agentId, clock);
    }
    return clock;
  }
}

// ─── Causal Message Types ───────────────────────────────────────────────────

interface CausalMessage {
  readonly id: string;
  readonly senderId: string;
  readonly clock: VectorClock;
  readonly senderSeq: number; // Sender's own clock value at send time
  readonly payload: unknown;
  readonly topic: string;
  readonly timestamp: number;
  readonly ttl: number; // Max hops for rebroadcast
  readonly hops: number;
}

interface DeliveryRecord {
  readonly messageId: string;
  readonly deliveredAt: number;
  readonly deliveredTo: string;
  readonly causalDelay: number; // Time spent buffered waiting for causal deps
}

// ─── Causal Delivery Buffer ─────────────────────────────────────────────────

interface BufferStats {
  readonly buffered: number;
  readonly delivered: number;
  readonly expired: number;
  readonly maxBufferDepth: number;
  readonly avgCausalDelay: number;
}

class CausalDeliveryBuffer {
  private readonly pending: Map<string, Map<string, CausalMessage>> = new Map(); // agentId → msgId → msg
  private readonly delivered: Map<string, Set<string>> = new Map(); // agentId → delivered msgIds
  private readonly receiveTime: Map<string, number> = new Map(); // msgId → first receive timestamp
  private readonly config: {
    maxBufferSize: number;
    messageExpiry: number; // ms
    maxCausalDelay: number; // ms — force-deliver after this
  };

  // Stats
  private totalDelivered = 0;
  private totalExpired = 0;
  private maxDepth = 0;
  private totalCausalDelay = 0;

  constructor(config: Partial<CausalDeliveryBuffer['config']> = {}) {
    this.config = {
      maxBufferSize: 10000,
      messageExpiry: 60000,
      maxCausalDelay: 30000,
      ...config,
    };
  }

  /**
   * Buffer a message for an agent. Returns immediately deliverable messages.
   */
  enqueue(
    agentId: string,
    message: CausalMessage,
    clockManager: VectorClockManager
  ): CausalMessage[] {
    // Already delivered?
    const deliveredSet = this.getDeliveredSet(agentId);
    if (deliveredSet.has(message.id)) return [];

    // Record receive time
    if (!this.receiveTime.has(message.id)) {
      this.receiveTime.set(message.id, Date.now());
    }

    // Add to pending
    const pendingMap = this.getPendingMap(agentId);
    pendingMap.set(message.id, message);

    // Update max depth
    if (pendingMap.size > this.maxDepth) this.maxDepth = pendingMap.size;

    // Try to deliver as many as possible
    return this.tryDeliver(agentId, clockManager);
  }

  /**
   * Attempt to deliver buffered messages whose causal dependencies are met.
   * A message from sender S with clock C is deliverable at agent A when:
   * - C[S] == A_clock[S] + 1 (next expected from sender)
   * - For all other dimensions D: C[D] <= A_clock[D] (all causal deps seen)
   */
  tryDeliver(agentId: string, clockManager: VectorClockManager): CausalMessage[] {
    const pendingMap = this.getPendingMap(agentId);
    const deliveredSet = this.getDeliveredSet(agentId);
    const deliverable: CausalMessage[] = [];
    const now = Date.now();

    let progress = true;
    while (progress) {
      progress = false;
      const agentClock = clockManager.getClock(agentId);

      for (const [msgId, msg] of pendingMap) {
        // Check expiry
        const receiveAt = this.receiveTime.get(msgId) ?? now;
        if (now - receiveAt > this.config.messageExpiry) {
          pendingMap.delete(msgId);
          this.totalExpired++;
          continue;
        }

        // Force-deliver after max causal delay
        const forceDeliver = (now - receiveAt) > this.config.maxCausalDelay;

        if (forceDeliver || this.isCausallyReady(msg, agentClock)) {
          // Deliver: merge clock and record
          clockManager.merge(agentId, msg.clock);
          deliveredSet.add(msgId);
          pendingMap.delete(msgId);

          const causalDelay = now - receiveAt;
          this.totalCausalDelay += causalDelay;
          this.totalDelivered++;

          deliverable.push(msg);
          progress = true;
        }
      }
    }

    return deliverable;
  }

  /**
   * Check if message is causally ready for delivery.
   * Sender's clock must be "next expected" from that sender,
   * and all other causal dependencies must be satisfied.
   */
  private isCausallyReady(msg: CausalMessage, receiverClock: VectorClock): boolean {
    const senderId = msg.senderId;

    for (const [dim, val] of msg.clock.entries) {
      const receiverVal = receiverClock.entries.get(dim) ?? 0;
      if (dim === senderId) {
        // Sender dimension: must be exactly next expected
        if (val !== receiverVal + 1) return false;
      } else {
        // Other dimensions: must have already seen at least this much
        if (val > receiverVal) return false;
      }
    }
    return true;
  }

  /**
   * Expire old messages and prune delivery records.
   */
  gc(now: number): number {
    let pruned = 0;

    for (const [, pendingMap] of this.pending) {
      for (const [msgId, msg] of pendingMap) {
        const receiveAt = this.receiveTime.get(msgId) ?? now;
        if (now - receiveAt > this.config.messageExpiry) {
          pendingMap.delete(msgId);
          this.receiveTime.delete(msgId);
          this.totalExpired++;
          pruned++;
        }
      }
    }

    // Prune old receive timestamps for delivered messages
    for (const [msgId, ts] of this.receiveTime) {
      if (now - ts > this.config.messageExpiry * 2) {
        this.receiveTime.delete(msgId);
        pruned++;
      }
    }

    return pruned;
  }

  getStats(): BufferStats {
    let buffered = 0;
    for (const m of this.pending.values()) buffered += m.size;
    return {
      buffered,
      delivered: this.totalDelivered,
      expired: this.totalExpired,
      maxBufferDepth: this.maxDepth,
      avgCausalDelay: this.totalDelivered > 0 ? this.totalCausalDelay / this.totalDelivered : 0,
    };
  }

  private getPendingMap(agentId: string): Map<string, CausalMessage> {
    let m = this.pending.get(agentId);
    if (!m) { m = new Map(); this.pending.set(agentId, m); }
    return m;
  }

  private getDeliveredSet(agentId: string): Set<string> {
    let s = this.delivered.get(agentId);
    if (!s) { s = new Set(); this.delivered.set(agentId, s); }
    return s;
  }
}

// ─── Reliable Broadcast Layer ───────────────────────────────────────────────

interface RetransmissionEntry {
  readonly message: CausalMessage;
  readonly targets: Set<string>;
  readonly acked: Set<string>;
  attempts: number;
  lastAttempt: number;
  nextRetry: number;
}

interface BroadcastConfig {
  readonly maxRetransmissions: number;
  readonly baseRetryInterval: number; // ms
  readonly maxRetryInterval: number; // ms
  readonly retryJitterFactor: number;
  readonly batchSize: number; // Max messages per retransmission batch
}

class ReliableBroadcastLayer {
  private readonly pending: Map<string, RetransmissionEntry> = new Map();
  private readonly seenMessages: Map<string, number> = new Map(); // msgId → timestamp
  private readonly config: BroadcastConfig;

  // Stats
  private totalBroadcasts = 0;
  private totalRetransmissions = 0;
  private totalDropped = 0;
  private totalAcked = 0;

  constructor(config: Partial<BroadcastConfig> = {}) {
    this.config = {
      maxRetransmissions: 5,
      baseRetryInterval: 1000,
      maxRetryInterval: 30000,
      retryJitterFactor: 0.2,
      batchSize: 50,
      ...config,
    };
  }

  /**
   * Register a broadcast message for reliable delivery to all targets.
   */
  broadcast(message: CausalMessage, targets: Set<string>): void {
    if (targets.size === 0) return;

    this.pending.set(message.id, {
      message,
      targets: new Set(targets),
      acked: new Set(),
      attempts: 0,
      lastAttempt: Date.now(),
      nextRetry: Date.now() + this.config.baseRetryInterval,
    });
    this.seenMessages.set(message.id, Date.now());
    this.totalBroadcasts++;
  }

  /**
   * Record acknowledgement from a target agent.
   */
  acknowledge(messageId: string, agentId: string): void {
    const entry = this.pending.get(messageId);
    if (!entry) return;

    entry.acked.add(agentId);
    this.totalAcked++;

    // All targets acked → remove
    if (entry.acked.size >= entry.targets.size) {
      this.pending.delete(messageId);
    }
  }

  /**
   * Check if a message has been seen (for deduplication).
   */
  hasSeen(messageId: string): boolean {
    return this.seenMessages.has(messageId);
  }

  markSeen(messageId: string): void {
    this.seenMessages.set(messageId, Date.now());
  }

  /**
   * Get messages due for retransmission.
   * Returns list of (message, unacked targets) pairs.
   */
  getRetransmissions(now: number): Array<{ message: CausalMessage; targets: string[] }> {
    const result: Array<{ message: CausalMessage; targets: string[] }> = [];

    for (const [msgId, entry] of this.pending) {
      if (now < entry.nextRetry) continue;

      if (entry.attempts >= this.config.maxRetransmissions) {
        this.pending.delete(msgId);
        this.totalDropped++;
        continue;
      }

      const unacked = [...entry.targets].filter(t => !entry.acked.has(t));
      if (unacked.length === 0) {
        this.pending.delete(msgId);
        continue;
      }

      entry.attempts++;
      entry.lastAttempt = now;

      // Exponential backoff with jitter
      const baseDelay = Math.min(
        this.config.baseRetryInterval * Math.pow(2, entry.attempts - 1),
        this.config.maxRetryInterval
      );
      const jitter = baseDelay * this.config.retryJitterFactor * (Math.random() * 2 - 1);
      entry.nextRetry = now + baseDelay + jitter;

      result.push({ message: entry.message, targets: unacked });
      this.totalRetransmissions++;

      if (result.length >= this.config.batchSize) break;
    }

    return result;
  }

  /**
   * Prune old seen-message records.
   */
  pruneSeenMessages(maxAge: number): number {
    const now = Date.now();
    let pruned = 0;
    for (const [msgId, ts] of this.seenMessages) {
      if (now - ts > maxAge) {
        this.seenMessages.delete(msgId);
        pruned++;
      }
    }
    return pruned;
  }

  getStats(): { broadcasts: number; retransmissions: number; dropped: number; acked: number; pending: number } {
    return {
      broadcasts: this.totalBroadcasts,
      retransmissions: this.totalRetransmissions,
      dropped: this.totalDropped,
      acked: this.totalAcked,
      pending: this.pending.size,
    };
  }
}

// ─── Message Stability Detector ─────────────────────────────────────────────

interface StabilityReport {
  readonly stableMessages: string[];
  readonly stableFrontier: VectorClock;
  readonly unstableCount: number;
}

class MessageStabilityDetector {
  private readonly messageClocks: Map<string, VectorClock> = new Map();
  private readonly stableMessages: Set<string> = new Set();

  /**
   * Register a message's vector clock for stability tracking.
   */
  trackMessage(messageId: string, clock: VectorClock): void {
    if (!this.stableMessages.has(messageId)) {
      this.messageClocks.set(messageId, clock);
    }
  }

  /**
   * Compute stable frontier and identify newly stable messages.
   * A message is stable when its clock ≤ globalMin (all agents have seen
   * everything up to this point, so the message can never be reordered).
   */
  detectStable(clockManager: VectorClockManager): StabilityReport {
    const globalMin = clockManager.computeGlobalMinimum();
    const newlyStable: string[] = [];

    for (const [msgId, msgClock] of this.messageClocks) {
      if (this.isStable(msgClock, globalMin)) {
        newlyStable.push(msgId);
        this.stableMessages.add(msgId);
        this.messageClocks.delete(msgId);
      }
    }

    return {
      stableMessages: newlyStable,
      stableFrontier: globalMin,
      unstableCount: this.messageClocks.size,
    };
  }

  private isStable(msgClock: VectorClock, globalMin: VectorClock): boolean {
    for (const [dim, val] of msgClock.entries) {
      if (val > (globalMin.entries.get(dim) ?? 0)) return false;
    }
    return true;
  }

  isMessageStable(messageId: string): boolean {
    return this.stableMessages.has(messageId);
  }

  /**
   * Prune old stability records to prevent unbounded growth.
   */
  pruneStableRecords(maxSize: number): number {
    if (this.stableMessages.size <= maxSize) return 0;
    const excess = this.stableMessages.size - maxSize;
    const iter = this.stableMessages.values();
    for (let i = 0; i < excess; i++) {
      const val = iter.next();
      if (!val.done) this.stableMessages.delete(val.value);
    }
    return excess;
  }
}

// ─── Partition-Aware Broadcaster ────────────────────────────────────────────

type BroadcastMode = 'normal' | 'degraded' | 'partitioned';

interface PartitionState {
  readonly mode: BroadcastMode;
  readonly reachableAgents: Set<string>;
  readonly unreachableAgents: Set<string>;
  readonly partitionDetectedAt: number | null;
  readonly lastModeChange: number;
}

interface PartitionConfig {
  readonly unreachableThreshold: number; // ms without ack → unreachable
  readonly partitionThreshold: number; // Fraction of unreachable agents → partitioned mode
  readonly recoveryDelay: number; // ms to wait before returning to normal from degraded
  readonly gossipFanoutNormal: number;
  readonly gossipFanoutDegraded: number;
}

class PartitionAwareBroadcaster {
  private mode: BroadcastMode = 'normal';
  private readonly lastAck: Map<string, number> = new Map();
  private readonly knownAgents: Set<string> = new Set();
  private partitionDetectedAt: number | null = null;
  private lastModeChange = 0;
  private readonly config: PartitionConfig;

  constructor(config: Partial<PartitionConfig> = {}) {
    this.config = {
      unreachableThreshold: 10000,
      partitionThreshold: 0.3,
      recoveryDelay: 15000,
      gossipFanoutNormal: 3,
      gossipFanoutDegraded: 5,
      ...config,
    };
  }

  registerAgent(agentId: string): void {
    this.knownAgents.add(agentId);
    this.lastAck.set(agentId, Date.now());
  }

  removeAgent(agentId: string): void {
    this.knownAgents.delete(agentId);
    this.lastAck.delete(agentId);
  }

  recordAck(agentId: string): void {
    this.lastAck.set(agentId, Date.now());
  }

  /**
   * Evaluate partition state and adjust broadcast mode.
   */
  evaluate(now: number): PartitionState {
    const reachable = new Set<string>();
    const unreachable = new Set<string>();

    for (const agentId of this.knownAgents) {
      const lastSeen = this.lastAck.get(agentId) ?? 0;
      if (now - lastSeen <= this.config.unreachableThreshold) {
        reachable.add(agentId);
      } else {
        unreachable.add(agentId);
      }
    }

    const unreachableFraction = this.knownAgents.size > 0
      ? unreachable.size / this.knownAgents.size
      : 0;

    const prevMode = this.mode;

    if (unreachableFraction >= this.config.partitionThreshold) {
      if (this.mode !== 'partitioned') {
        this.mode = 'partitioned';
        this.partitionDetectedAt = now;
        this.lastModeChange = now;
      }
    } else if (unreachable.size > 0) {
      if (this.mode === 'partitioned' && (now - this.lastModeChange) >= this.config.recoveryDelay) {
        this.mode = 'degraded';
        this.lastModeChange = now;
      } else if (this.mode === 'normal') {
        this.mode = 'degraded';
        this.lastModeChange = now;
      }
    } else {
      if (this.mode !== 'normal' && (now - this.lastModeChange) >= this.config.recoveryDelay) {
        this.mode = 'normal';
        this.partitionDetectedAt = null;
        this.lastModeChange = now;
      }
    }

    return {
      mode: this.mode,
      reachableAgents: reachable,
      unreachableAgents: unreachable,
      partitionDetectedAt: this.partitionDetectedAt,
      lastModeChange: this.lastModeChange,
    };
  }

  /**
   * Get gossip fanout based on current mode.
   * Higher fanout in degraded/partitioned mode for faster convergence.
   */
  getFanout(): number {
    switch (this.mode) {
      case 'normal': return this.config.gossipFanoutNormal;
      case 'degraded': return this.config.gossipFanoutDegraded;
      case 'partitioned': return Math.max(this.config.gossipFanoutDegraded, Math.ceil(this.knownAgents.size * 0.5));
    }
  }

  /**
   * Select gossip targets with preference for unreachable agents
   * to help repair partitions faster.
   */
  selectTargets(selfId: string, now: number): string[] {
    const state = this.evaluate(now);
    const fanout = this.getFanout();
    const targets: string[] = [];

    // In degraded/partitioned mode, prioritize unreachable agents
    if (this.mode !== 'normal') {
      const unreachableArr = [...state.unreachableAgents].filter(a => a !== selfId);
      for (let i = unreachableArr.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [unreachableArr[i], unreachableArr[j]] = [unreachableArr[j], unreachableArr[i]];
      }
      targets.push(...unreachableArr.slice(0, Math.ceil(fanout / 2)));
    }

    // Fill remaining with random reachable agents
    const reachableArr = [...state.reachableAgents].filter(a => a !== selfId && !targets.includes(a));
    for (let i = reachableArr.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [reachableArr[i], reachableArr[j]] = [reachableArr[j], reachableArr[i]];
    }
    targets.push(...reachableArr.slice(0, fanout - targets.length));

    return targets;
  }

  getMode(): BroadcastMode { return this.mode; }
}

// ─── Gossip Repair Protocol ─────────────────────────────────────────────────

interface DigestEntry {
  readonly agentId: string;
  readonly maxSeq: number;
  readonly messageCount: number;
}

interface RepairRequest {
  readonly requesterId: string;
  readonly gaps: Array<{ senderId: string; fromSeq: number; toSeq: number }>;
}

interface RepairResponse {
  readonly messages: CausalMessage[];
  readonly responderId: string;
}

class GossipRepairProtocol {
  private readonly messageLog: Map<string, CausalMessage[]> = new Map(); // senderId → messages sorted by senderSeq
  private readonly maxSeqSeen: Map<string, number> = new Map(); // senderId → max senderSeq seen
  private readonly config: {
    maxLogSize: number;
    digestInterval: number; // ms between digest exchanges
    maxRepairBatch: number;
  };

  private lastDigestExchange = 0;

  constructor(config: Partial<GossipRepairProtocol['config']> = {}) {
    this.config = {
      maxLogSize: 5000,
      digestInterval: 5000,
      maxRepairBatch: 100,
      ...config,
    };
  }

  /**
   * Record a message in the log for future repair requests.
   */
  recordMessage(msg: CausalMessage): void {
    let log = this.messageLog.get(msg.senderId);
    if (!log) {
      log = [];
      this.messageLog.set(msg.senderId, log);
    }

    // Insert in order by senderSeq
    const insertIdx = this.binarySearchInsert(log, msg.senderSeq);
    if (insertIdx < log.length && log[insertIdx].senderSeq === msg.senderSeq) {
      return; // Duplicate
    }
    log.splice(insertIdx, 0, msg);

    // Track max seq
    const currentMax = this.maxSeqSeen.get(msg.senderId) ?? 0;
    if (msg.senderSeq > currentMax) {
      this.maxSeqSeen.set(msg.senderId, msg.senderSeq);
    }

    // Enforce log size
    if (log.length > this.config.maxLogSize) {
      log.splice(0, log.length - this.config.maxLogSize);
    }
  }

  /**
   * Generate a digest of our message log state for comparison with peers.
   */
  generateDigest(): DigestEntry[] {
    const digest: DigestEntry[] = [];
    for (const [senderId, log] of this.messageLog) {
      digest.push({
        agentId: senderId,
        maxSeq: this.maxSeqSeen.get(senderId) ?? 0,
        messageCount: log.length,
      });
    }
    return digest;
  }

  /**
   * Compare a remote digest with ours to find gaps.
   */
  findGaps(remoteDigest: DigestEntry[]): RepairRequest['gaps'] {
    const gaps: RepairRequest['gaps'] = [];

    for (const entry of remoteDigest) {
      const ourMax = this.maxSeqSeen.get(entry.agentId) ?? 0;
      if (entry.maxSeq > ourMax) {
        gaps.push({
          senderId: entry.agentId,
          fromSeq: ourMax + 1,
          toSeq: entry.maxSeq,
        });
      }
    }

    // Also check for senders we don't know about
    const remoteSenders = new Set(remoteDigest.map(e => e.agentId));
    // We might have gaps for senders we DO know about but remote has more
    // Already handled above

    return gaps;
  }

  /**
   * Fulfill a repair request with messages from our log.
   */
  fulfillRepair(request: RepairRequest, selfId: string): RepairResponse {
    const messages: CausalMessage[] = [];

    for (const gap of request.gaps) {
      const log = this.messageLog.get(gap.senderId);
      if (!log) continue;

      for (const msg of log) {
        if (msg.senderSeq >= gap.fromSeq && msg.senderSeq <= gap.toSeq) {
          messages.push(msg);
          if (messages.length >= this.config.maxRepairBatch) break;
        }
      }
      if (messages.length >= this.config.maxRepairBatch) break;
    }

    return { messages, responderId: selfId };
  }

  shouldExchangeDigest(now: number): boolean {
    if (now - this.lastDigestExchange >= this.config.digestInterval) {
      this.lastDigestExchange = now;
      return true;
    }
    return false;
  }

  private binarySearchInsert(log: CausalMessage[], seq: number): number {
    let lo = 0, hi = log.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (log[mid].senderSeq < seq) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  /**
   * Prune old log entries.
   */
  pruneLog(stableFrontier: VectorClock): number {
    let pruned = 0;
    for (const [senderId, log] of this.messageLog) {
      const stableSeq = stableFrontier.entries.get(senderId) ?? 0;
      const newLog = log.filter(m => m.senderSeq > stableSeq);
      pruned += log.length - newLog.length;
      if (newLog.length === 0) {
        this.messageLog.delete(senderId);
      } else {
        this.messageLog.set(senderId, newLog);
      }
    }
    return pruned;
  }
}

// ─── Delivery Guarantee Tracker ─────────────────────────────────────────────

interface AgentDeliveryStats {
  readonly agentId: string;
  readonly delivered: number;
  readonly pending: number;
  readonly lost: number;
  readonly deliveryRate: number; // 0-1
  readonly avgLatency: number;
}

class DeliveryGuaranteeTracker {
  private readonly perAgent: Map<string, {
    delivered: number;
    pending: Set<string>;
    lost: number;
    latencySum: number;
    latencyCount: number;
    sentTimes: Map<string, number>;
  }> = new Map();

  /**
   * Record a message sent to an agent.
   */
  recordSent(agentId: string, messageId: string): void {
    const stats = this.getOrCreate(agentId);
    stats.pending.add(messageId);
    stats.sentTimes.set(messageId, Date.now());
  }

  /**
   * Record delivery confirmation.
   */
  recordDelivered(agentId: string, messageId: string): void {
    const stats = this.getOrCreate(agentId);
    if (stats.pending.delete(messageId)) {
      stats.delivered++;
      const sentAt = stats.sentTimes.get(messageId);
      if (sentAt !== undefined) {
        stats.latencySum += Date.now() - sentAt;
        stats.latencyCount++;
        stats.sentTimes.delete(messageId);
      }
    }
  }

  /**
   * Mark a message as lost (exceeded retransmission limit).
   */
  recordLost(agentId: string, messageId: string): void {
    const stats = this.getOrCreate(agentId);
    if (stats.pending.delete(messageId)) {
      stats.lost++;
      stats.sentTimes.delete(messageId);
    }
  }

  getAgentStats(agentId: string): AgentDeliveryStats {
    const stats = this.getOrCreate(agentId);
    const total = stats.delivered + stats.lost;
    return {
      agentId,
      delivered: stats.delivered,
      pending: stats.pending.size,
      lost: stats.lost,
      deliveryRate: total > 0 ? stats.delivered / total : 1,
      avgLatency: stats.latencyCount > 0 ? stats.latencySum / stats.latencyCount : 0,
    };
  }

  getAllStats(): AgentDeliveryStats[] {
    return [...this.perAgent.keys()].map(id => this.getAgentStats(id));
  }

  private getOrCreate(agentId: string) {
    let s = this.perAgent.get(agentId);
    if (!s) {
      s = { delivered: 0, pending: new Set(), lost: 0, latencySum: 0, latencyCount: 0, sentTimes: new Map() };
      this.perAgent.set(agentId, s);
    }
    return s;
  }
}

// ─── FNV-1a Hash ────────────────────────────────────────────────────────────

function fnv1a(input: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = (hash * 0x01000193) >>> 0;
  }
  return hash;
}

// ─── Causal Broadcast Protocol (Unified Orchestrator) ───────────────────────

type CausalBroadcastEventType =
  | 'message_broadcast'
  | 'message_delivered'
  | 'message_buffered'
  | 'message_expired'
  | 'message_stable'
  | 'partition_detected'
  | 'partition_healed'
  | 'repair_requested'
  | 'repair_fulfilled'
  | 'retransmission'
  | 'agent_joined'
  | 'agent_left';

interface CausalBroadcastEvent {
  readonly type: CausalBroadcastEventType;
  readonly timestamp: number;
  readonly data: Record<string, unknown>;
}

interface CausalBroadcastConfig {
  // Buffer
  readonly maxBufferSize: number;
  readonly messageExpiry: number;
  readonly maxCausalDelay: number;
  // Broadcast
  readonly maxRetransmissions: number;
  readonly baseRetryInterval: number;
  // Partition
  readonly unreachableThreshold: number;
  readonly partitionThreshold: number;
  readonly gossipFanoutNormal: number;
  readonly gossipFanoutDegraded: number;
  // Repair
  readonly digestInterval: number;
  readonly maxRepairBatch: number;
  readonly maxLogSize: number;
  // GC
  readonly gcInterval: number;
  readonly maxStableRecords: number;
}

interface BroadcastResult {
  readonly messageId: string;
  readonly clock: VectorClock;
  readonly targets: string[];
  readonly mode: BroadcastMode;
}

interface ReceiveResult {
  readonly delivered: CausalMessage[];
  readonly buffered: number;
}

interface ProtocolDashboard {
  readonly agents: number;
  readonly mode: BroadcastMode;
  readonly bufferStats: BufferStats;
  readonly broadcastStats: ReturnType<ReliableBroadcastLayer['getStats']>;
  readonly deliveryStats: AgentDeliveryStats[];
  readonly stabilityReport: StabilityReport;
  readonly eventCount: number;
}

class CausalBroadcastProtocol {
  private readonly clockManager: VectorClockManager;
  private readonly buffer: CausalDeliveryBuffer;
  private readonly broadcast: ReliableBroadcastLayer;
  private readonly stability: MessageStabilityDetector;
  private readonly partition: PartitionAwareBroadcaster;
  private readonly repair: GossipRepairProtocol;
  private readonly delivery: DeliveryGuaranteeTracker;
  private readonly config: CausalBroadcastConfig;

  private readonly agents: Set<string> = new Set();
  private readonly events: CausalBroadcastEvent[] = [];
  private readonly maxEvents = 5000;
  private messageCounter = 0;
  private lastGc = 0;

  constructor(config: Partial<CausalBroadcastConfig> = {}) {
    this.config = {
      maxBufferSize: 10000,
      messageExpiry: 60000,
      maxCausalDelay: 30000,
      maxRetransmissions: 5,
      baseRetryInterval: 1000,
      unreachableThreshold: 10000,
      partitionThreshold: 0.3,
      gossipFanoutNormal: 3,
      gossipFanoutDegraded: 5,
      digestInterval: 5000,
      maxRepairBatch: 100,
      maxLogSize: 5000,
      gcInterval: 30000,
      maxStableRecords: 10000,
      ...config,
    };

    this.clockManager = new VectorClockManager();
    this.buffer = new CausalDeliveryBuffer({
      maxBufferSize: this.config.maxBufferSize,
      messageExpiry: this.config.messageExpiry,
      maxCausalDelay: this.config.maxCausalDelay,
    });
    this.broadcast = new ReliableBroadcastLayer({
      maxRetransmissions: this.config.maxRetransmissions,
      baseRetryInterval: this.config.baseRetryInterval,
    });
    this.stability = new MessageStabilityDetector();
    this.partition = new PartitionAwareBroadcaster({
      unreachableThreshold: this.config.unreachableThreshold,
      partitionThreshold: this.config.partitionThreshold,
      gossipFanoutNormal: this.config.gossipFanoutNormal,
      gossipFanoutDegraded: this.config.gossipFanoutDegraded,
    });
    this.repair = new GossipRepairProtocol({
      maxLogSize: this.config.maxLogSize,
      digestInterval: this.config.digestInterval,
      maxRepairBatch: this.config.maxRepairBatch,
    });
    this.delivery = new DeliveryGuaranteeTracker();
  }

  // ─── Agent Management ───────────────────────────────────────────────

  addAgent(agentId: string): void {
    this.agents.add(agentId);
    this.clockManager.initialize(agentId);
    this.partition.registerAgent(agentId);
    this.emit('agent_joined', { agentId });
  }

  removeAgent(agentId: string): void {
    this.agents.delete(agentId);
    this.partition.removeAgent(agentId);
    this.emit('agent_left', { agentId });
  }

  // ─── Broadcast ──────────────────────────────────────────────────────

  /**
   * Broadcast a message from sender to all other agents with causal ordering.
   * The message's vector clock is set by incrementing the sender's clock.
   */
  broadcastMessage(
    senderId: string,
    topic: string,
    payload: unknown,
    ttl: number = 10
  ): BroadcastResult {
    const now = Date.now();
    const clock = this.clockManager.increment(senderId);
    const senderSeq = clock.entries.get(senderId) ?? 1;

    const messageId = `${senderId}:${senderSeq}:${fnv1a(`${senderId}${senderSeq}${now}`).toString(16)}`;
    this.messageCounter++;

    const message: CausalMessage = {
      id: messageId,
      senderId,
      clock,
      senderSeq,
      payload,
      topic,
      timestamp: now,
      ttl,
      hops: 0,
    };

    // Determine targets based on partition awareness
    const targets = this.partition.selectTargets(senderId, now);
    const allOtherAgents = [...this.agents].filter(a => a !== senderId);
    const targetSet = new Set(allOtherAgents);

    // Register for reliable broadcast to all
    this.broadcast.broadcast(message, targetSet);
    this.broadcast.markSeen(messageId);

    // Track for stability
    this.stability.trackMessage(messageId, clock);

    // Record in gossip repair log
    this.repair.recordMessage(message);

    // Track delivery to each target
    for (const target of targetSet) {
      this.delivery.recordSent(target, messageId);
    }

    this.emit('message_broadcast', { messageId, senderId, topic, targets: targets.length, mode: this.partition.getMode() });

    return {
      messageId,
      clock,
      targets,
      mode: this.partition.getMode(),
    };
  }

  // ─── Receive ────────────────────────────────────────────────────────

  /**
   * Receive a message at a specific agent. The message is buffered until
   * causal dependencies are met, then delivered.
   */
  receiveMessage(agentId: string, message: CausalMessage): ReceiveResult {
    const now = Date.now();

    // Deduplication
    if (this.broadcast.hasSeen(message.id)) {
      // Already processed — just ack
      this.broadcast.acknowledge(message.id, agentId);
      this.partition.recordAck(message.senderId);
      return { delivered: [], buffered: 0 };
    }
    this.broadcast.markSeen(message.id);

    // Record in repair log
    this.repair.recordMessage(message);

    // Buffer for causal delivery
    const delivered = this.buffer.enqueue(agentId, message, this.clockManager);

    // Record delivery confirmations
    for (const msg of delivered) {
      this.broadcast.acknowledge(msg.id, agentId);
      this.delivery.recordDelivered(agentId, msg.id);
      this.partition.recordAck(msg.senderId);
      this.emit('message_delivered', { messageId: msg.id, agentId, topic: msg.topic });
    }

    const bufferStats = this.buffer.getStats();
    if (bufferStats.buffered > 0 && delivered.length === 0) {
      this.emit('message_buffered', { messageId: message.id, agentId, buffered: bufferStats.buffered });
    }

    // Gossip: rebroadcast to fanout targets (epidemic relay)
    if (message.hops < message.ttl) {
      const relayTargets = this.partition.selectTargets(agentId, now);
      for (const target of relayTargets) {
        if (target !== message.senderId && target !== agentId) {
          // Would relay here in real network
        }
      }
    }

    return {
      delivered,
      buffered: bufferStats.buffered,
    };
  }

  // ─── Acknowledge ────────────────────────────────────────────────────

  acknowledgeMessage(agentId: string, messageId: string): void {
    this.broadcast.acknowledge(messageId, agentId);
    this.delivery.recordDelivered(agentId, messageId);
    this.partition.recordAck(agentId);
  }

  // ─── Repair ─────────────────────────────────────────────────────────

  /**
   * Generate a digest for gossip-based repair.
   */
  generateDigest(): DigestEntry[] {
    return this.repair.generateDigest();
  }

  /**
   * Process a remote digest and identify gaps we need repaired.
   */
  requestRepair(remoteDigest: DigestEntry[], requesterId: string): RepairRequest | null {
    const gaps = this.repair.findGaps(remoteDigest);
    if (gaps.length === 0) return null;

    this.emit('repair_requested', { requesterId, gapCount: gaps.length });
    return { requesterId, gaps };
  }

  /**
   * Fulfill a repair request from our log.
   */
  fulfillRepair(request: RepairRequest, selfId: string): RepairResponse {
    const response = this.repair.fulfillRepair(request, selfId);
    this.emit('repair_fulfilled', { responderId: selfId, messageCount: response.messages.length });
    return response;
  }

  /**
   * Apply repair messages — feed them through normal receive path.
   */
  applyRepair(agentId: string, response: RepairResponse): ReceiveResult {
    let totalDelivered: CausalMessage[] = [];
    let maxBuffered = 0;

    for (const msg of response.messages) {
      const result = this.receiveMessage(agentId, msg);
      totalDelivered = totalDelivered.concat(result.delivered);
      if (result.buffered > maxBuffered) maxBuffered = result.buffered;
    }

    return { delivered: totalDelivered, buffered: maxBuffered };
  }

  // ─── Tick ───────────────────────────────────────────────────────────

  /**
   * Periodic maintenance tick:
   * 1. Partition evaluation
   * 2. Retransmission processing
   * 3. Stability detection
   * 4. Gossip repair (digest exchange)
   * 5. Buffer retry delivery
   * 6. Garbage collection
   */
  tick(now: number = Date.now()): {
    partitionState: PartitionState;
    retransmissions: number;
    newlyStable: string[];
    repairNeeded: boolean;
    redelivered: number;
    gcPruned: number;
  } {
    // 1. Evaluate partition state
    const partitionState = this.partition.evaluate(now);
    if (partitionState.mode === 'partitioned' && partitionState.partitionDetectedAt === now) {
      this.emit('partition_detected', {
        unreachable: partitionState.unreachableAgents.size,
        reachable: partitionState.reachableAgents.size,
      });
    }
    if (partitionState.mode === 'normal' && partitionState.lastModeChange === now) {
      this.emit('partition_healed', {});
    }

    // 2. Process retransmissions
    const retransmissions = this.broadcast.getRetransmissions(now);
    for (const { message, targets } of retransmissions) {
      this.emit('retransmission', { messageId: message.id, targets: targets.length });
    }

    // 3. Stability detection
    const stabilityReport = this.stability.detectStable(this.clockManager);
    for (const msgId of stabilityReport.stableMessages) {
      this.emit('message_stable', { messageId: msgId });
    }

    // 4. Gossip repair check
    const repairNeeded = this.repair.shouldExchangeDigest(now);

    // 5. Retry buffer delivery for all agents
    let redelivered = 0;
    for (const agentId of this.agents) {
      const delivered = this.buffer.tryDeliver(agentId, this.clockManager);
      for (const msg of delivered) {
        this.broadcast.acknowledge(msg.id, agentId);
        this.delivery.recordDelivered(agentId, msg.id);
        this.emit('message_delivered', { messageId: msg.id, agentId, topic: msg.topic, fromRetry: true });
      }
      redelivered += delivered.length;
    }

    // 6. GC
    let gcPruned = 0;
    if (now - this.lastGc >= this.config.gcInterval) {
      this.lastGc = now;
      gcPruned += this.buffer.gc(now);
      gcPruned += this.broadcast.pruneSeenMessages(this.config.messageExpiry * 2);
      gcPruned += this.stability.pruneStableRecords(this.config.maxStableRecords);

      // Prune repair log using stable frontier
      if (stabilityReport.stableMessages.length > 0) {
        gcPruned += this.repair.pruneLog(stabilityReport.stableFrontier);
      }

      // Prune events
      if (this.events.length > this.maxEvents) {
        this.events.splice(0, this.events.length - this.maxEvents);
      }
    }

    return {
      partitionState,
      retransmissions: retransmissions.length,
      newlyStable: stabilityReport.stableMessages,
      repairNeeded,
      redelivered,
      gcPruned,
    };
  }

  // ─── Queries ────────────────────────────────────────────────────────

  getAgentClock(agentId: string): VectorClock {
    return this.clockManager.getClock(agentId);
  }

  isMessageStable(messageId: string): boolean {
    return this.stability.isMessageStable(messageId);
  }

  getDashboard(): ProtocolDashboard {
    return {
      agents: this.agents.size,
      mode: this.partition.getMode(),
      bufferStats: this.buffer.getStats(),
      broadcastStats: this.broadcast.getStats(),
      deliveryStats: this.delivery.getAllStats(),
      stabilityReport: this.stability.detectStable(this.clockManager),
      eventCount: this.events.length,
    };
  }

  getRecentEvents(limit: number = 50): CausalBroadcastEvent[] {
    return this.events.slice(-limit);
  }

  private emit(type: CausalBroadcastEventType, data: Record<string, unknown>): void {
    this.events.push({ type, timestamp: Date.now(), data });
  }
}

// ─── Presets ────────────────────────────────────────────────────────────────

const PRESETS = {
  /**
   * Small local cluster — low latency, fast retransmissions.
   */
  'local-cluster': {
    maxBufferSize: 1000,
    messageExpiry: 15000,
    maxCausalDelay: 5000,
    maxRetransmissions: 3,
    baseRetryInterval: 500,
    unreachableThreshold: 5000,
    partitionThreshold: 0.5,
    gossipFanoutNormal: 2,
    gossipFanoutDegraded: 3,
    digestInterval: 3000,
    maxRepairBatch: 50,
    maxLogSize: 2000,
    gcInterval: 10000,
    maxStableRecords: 5000,
  } satisfies Partial<CausalBroadcastConfig>,

  /**
   * Wide-area network — higher latency tolerance, more aggressive repair.
   */
  'wide-area': {
    maxBufferSize: 20000,
    messageExpiry: 120000,
    maxCausalDelay: 60000,
    maxRetransmissions: 8,
    baseRetryInterval: 2000,
    unreachableThreshold: 30000,
    partitionThreshold: 0.2,
    gossipFanoutNormal: 4,
    gossipFanoutDegraded: 8,
    digestInterval: 10000,
    maxRepairBatch: 200,
    maxLogSize: 10000,
    gcInterval: 60000,
    maxStableRecords: 20000,
  } satisfies Partial<CausalBroadcastConfig>,

  /**
   * Agent swarm — many agents, high throughput, partition-tolerant.
   */
  'agent-swarm': {
    maxBufferSize: 50000,
    messageExpiry: 90000,
    maxCausalDelay: 30000,
    maxRetransmissions: 5,
    baseRetryInterval: 1000,
    unreachableThreshold: 15000,
    partitionThreshold: 0.3,
    gossipFanoutNormal: 3,
    gossipFanoutDegraded: 6,
    digestInterval: 5000,
    maxRepairBatch: 100,
    maxLogSize: 5000,
    gcInterval: 30000,
    maxStableRecords: 10000,
  } satisfies Partial<CausalBroadcastConfig>,
} as const;

export {
  VectorClockManager,
  CausalDeliveryBuffer,
  ReliableBroadcastLayer,
  MessageStabilityDetector,
  PartitionAwareBroadcaster,
  GossipRepairProtocol,
  DeliveryGuaranteeTracker,
  CausalBroadcastProtocol,
  PRESETS,
};

export type {
  VectorClock,
  ClockComparison,
  CausalMessage,
  DeliveryRecord,
  BufferStats,
  BroadcastConfig,
  RetransmissionEntry,
  StabilityReport,
  BroadcastMode,
  PartitionState,
  PartitionConfig,
  DigestEntry,
  RepairRequest,
  RepairResponse,
  AgentDeliveryStats,
  CausalBroadcastEventType,
  CausalBroadcastEvent,
  CausalBroadcastConfig,
  BroadcastResult,
  ReceiveResult,
  ProtocolDashboard,
};
