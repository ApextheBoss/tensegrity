/**
 * gossip-protocol-engine.ts — Epidemic dissemination for agent networks
 *
 * Implements multiple gossip strategies for efficient information propagation
 * across large, dynamic agent topologies. Covers membership, rumor spreading,
 * anti-entropy repair, and protocol composition.
 *
 * Key algorithms:
 * - SWIM-inspired failure detection (Suspicion + Piggyback)
 * - Plumtree hybrid gossip (eager push + lazy pull repair)
 * - Anti-entropy with Merkle tree digest comparison
 * - Bimodal multicast for high-reliability dissemination
 * - Infection-style rumor mongering with adaptive fanout
 */

// ============================================================
// Types & Interfaces
// ============================================================

export interface AgentEndpoint {
  id: string;
  address: string;
  metadata: Record<string, unknown>;
  generation: number; // monotonic incarnation counter
  heartbeat: number;  // lamport-style sequence
}

export type GossipEventType =
  | 'member-join' | 'member-leave' | 'member-suspect' | 'member-confirm-dead'
  | 'rumor-received' | 'rumor-propagated' | 'rumor-expired'
  | 'digest-sync-start' | 'digest-sync-complete' | 'digest-conflict'
  | 'fanout-adjusted' | 'partition-detected' | 'protocol-tick';

export interface GossipEvent {
  type: GossipEventType;
  source: string;
  target?: string;
  timestamp: number;
  payload?: unknown;
}

export type EventHandler = (event: GossipEvent) => void;

export interface MemberState {
  endpoint: AgentEndpoint;
  status: 'alive' | 'suspect' | 'dead';
  statusChangeAt: number;
  suspicionTimeout?: number;
  lastAckSeq: number;
  indirectProbeCount: number;
}

export interface Rumor {
  id: string;
  origin: string;
  payload: unknown;
  hops: number;
  maxHops: number;
  createdAt: number;
  ttlMs: number;
  infectionCount: number; // how many agents received this
}

export interface DigestEntry {
  key: string;
  version: number;
  hash: number; // FNV-1a of value
}

export interface GossipConfig {
  selfId: string;
  // Membership / SWIM
  protocolPeriodMs: number;      // tick interval
  suspicionMultiplier: number;   // multiplier for suspicion timeout = mult * log(N) * period
  indirectProbes: number;        // k-indirect probes on failed direct
  // Rumor spreading
  fanout: number;                // number of peers per gossip round
  adaptiveFanout: boolean;       // adjust fanout based on network size
  rumorTtlMs: number;            // max age before rumor expires
  maxHops: number;               // max propagation depth
  // Anti-entropy
  antiEntropyIntervalMs: number; // how often to run full-state sync
  digestBuckets: number;         // Merkle tree width
  // Bimodal
  bimodalEnabled: boolean;
  bimodalBeta: number;           // probability of gossip in phase 2
  // General
  maxPiggybackPerMsg: number;    // membership updates piggybacked
  maxRumorsPerMsg: number;       // rumors piggybacked per message
}

// ============================================================
// FNV-1a Hash (32-bit, zero-dep)
// ============================================================

function fnv1a(input: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = (hash * 0x01000193) >>> 0;
  }
  return hash;
}

// ============================================================
// SWIM Membership Protocol
// ============================================================

export class SwimMembership {
  private members = new Map<string, MemberState>();
  private readonly config: GossipConfig;
  private pingSeq = 0;
  private pendingAcks = new Map<number, { target: string; deadline: number; indirect: boolean }>();
  private piggybackQueue: Array<{ id: string; status: 'alive' | 'suspect' | 'dead'; generation: number }> = [];
  private emit: EventHandler;

  constructor(config: GossipConfig, emit: EventHandler) {
    this.config = config;
    this.emit = emit;
  }

  /** Add or update a member */
  addMember(endpoint: AgentEndpoint): void {
    const existing = this.members.get(endpoint.id);
    if (existing) {
      // Only update if generation is newer or (same gen, higher heartbeat)
      if (
        endpoint.generation > existing.endpoint.generation ||
        (endpoint.generation === existing.endpoint.generation &&
          endpoint.heartbeat > existing.endpoint.heartbeat)
      ) {
        existing.endpoint = { ...endpoint };
        if (existing.status === 'suspect' || existing.status === 'dead') {
          existing.status = 'alive';
          existing.statusChangeAt = Date.now();
          this.enqueuePiggyback(endpoint.id, 'alive', endpoint.generation);
          this.emit({ type: 'member-join', source: this.config.selfId, target: endpoint.id, timestamp: Date.now() });
        }
      }
      return;
    }

    this.members.set(endpoint.id, {
      endpoint: { ...endpoint },
      status: 'alive',
      statusChangeAt: Date.now(),
      lastAckSeq: 0,
      indirectProbeCount: 0,
    });
    this.enqueuePiggyback(endpoint.id, 'alive', endpoint.generation);
    this.emit({ type: 'member-join', source: this.config.selfId, target: endpoint.id, timestamp: Date.now() });
  }

  /** Run one protocol period — select probe target, check timeouts */
  tick(now: number): { directProbe?: string; indirectProbes?: string[]; deadMembers: string[] } {
    const result: { directProbe?: string; indirectProbes?: string[]; deadMembers: string[] } = { deadMembers: [] };

    // Check suspicion timeouts
    for (const [id, state] of this.members) {
      if (state.status === 'suspect' && state.suspicionTimeout) {
        if (now - state.statusChangeAt > state.suspicionTimeout) {
          state.status = 'dead';
          state.statusChangeAt = now;
          this.enqueuePiggyback(id, 'dead', state.endpoint.generation);
          result.deadMembers.push(id);
          this.emit({ type: 'member-confirm-dead', source: this.config.selfId, target: id, timestamp: now });
        }
      }
    }

    // Check pending ack deadlines
    for (const [seq, pending] of this.pendingAcks) {
      if (now > pending.deadline) {
        this.pendingAcks.delete(seq);
        if (!pending.indirect) {
          // Direct probe failed — try indirect
          const state = this.members.get(pending.target);
          if (state && state.status === 'alive') {
            const probers = this.selectRandomMembers(this.config.indirectProbes, [pending.target]);
            if (probers.length > 0) {
              result.indirectProbes = probers;
              // Mark as suspect after indirect probes
              this.startSuspicion(pending.target, now);
            } else {
              this.startSuspicion(pending.target, now);
            }
          }
        }
      }
    }

    // Select random alive member to probe
    const aliveMembers = [...this.members.entries()]
      .filter(([id, s]) => id !== this.config.selfId && s.status === 'alive')
      .map(([id]) => id);

    if (aliveMembers.length > 0) {
      const idx = Math.floor(Math.random() * aliveMembers.length);
      result.directProbe = aliveMembers[idx];
      const seq = ++this.pingSeq;
      this.pendingAcks.set(seq, {
        target: result.directProbe,
        deadline: now + this.config.protocolPeriodMs,
        indirect: false,
      });
    }

    this.emit({ type: 'protocol-tick', source: this.config.selfId, timestamp: now });
    return result;
  }

  /** Handle ack for a probe */
  handleAck(fromId: string, seq: number): void {
    this.pendingAcks.delete(seq);
    const state = this.members.get(fromId);
    if (state && state.status === 'suspect') {
      state.status = 'alive';
      state.statusChangeAt = Date.now();
      state.endpoint.generation++; // refutation
      this.enqueuePiggyback(fromId, 'alive', state.endpoint.generation);
    }
  }

  /** Get piggybacked membership updates */
  drainPiggyback(max?: number): Array<{ id: string; status: string; generation: number }> {
    const limit = max ?? this.config.maxPiggybackPerMsg;
    return this.piggybackQueue.splice(0, limit);
  }

  /** Suspicion timeout = mult * log(N+1) * period */
  private suspicionTimeoutMs(): number {
    const n = Math.max(1, this.members.size);
    return this.config.suspicionMultiplier * Math.log(n + 1) * this.config.protocolPeriodMs;
  }

  private startSuspicion(targetId: string, now: number): void {
    const state = this.members.get(targetId);
    if (!state || state.status !== 'alive') return;
    state.status = 'suspect';
    state.statusChangeAt = now;
    state.suspicionTimeout = this.suspicionTimeoutMs();
    this.enqueuePiggyback(targetId, 'suspect', state.endpoint.generation);
    this.emit({ type: 'member-suspect', source: this.config.selfId, target: targetId, timestamp: now });
  }

  private enqueuePiggyback(id: string, status: 'alive' | 'suspect' | 'dead', generation: number): void {
    // Newer info overwrites older in queue
    const idx = this.piggybackQueue.findIndex(p => p.id === id);
    if (idx >= 0) this.piggybackQueue.splice(idx, 1);
    this.piggybackQueue.push({ id, status, generation });
  }

  selectRandomMembers(count: number, exclude: string[] = []): string[] {
    const candidates = [...this.members.entries()]
      .filter(([id, s]) => id !== this.config.selfId && s.status === 'alive' && !exclude.includes(id))
      .map(([id]) => id);
    // Fisher-Yates partial shuffle
    const result: string[] = [];
    for (let i = 0; i < Math.min(count, candidates.length); i++) {
      const j = i + Math.floor(Math.random() * (candidates.length - i));
      [candidates[i], candidates[j]] = [candidates[j], candidates[i]];
      result.push(candidates[i]);
    }
    return result;
  }

  getAliveCount(): number {
    return [...this.members.values()].filter(m => m.status === 'alive').length;
  }

  getAllMembers(): Map<string, MemberState> {
    return new Map(this.members);
  }

  removeDead(): string[] {
    const removed: string[] = [];
    for (const [id, state] of this.members) {
      if (state.status === 'dead') {
        this.members.delete(id);
        removed.push(id);
        this.emit({ type: 'member-leave', source: this.config.selfId, target: id, timestamp: Date.now() });
      }
    }
    return removed;
  }
}

// ============================================================
// Plumtree Hybrid Gossip (Eager Push + Lazy Pull)
// ============================================================

export class PlumtreeGossip {
  private eagerPeers = new Set<string>();
  private lazyPeers = new Set<string>();
  private lazyQueue = new Map<string, { rumorId: string; origin: string; receivedAt: number }[]>();
  private seen = new Set<string>(); // rumor IDs already delivered
  private missingTimers = new Map<string, number>(); // rumorId -> deadline for lazy pull
  private readonly repairTimeoutMs: number;
  private emit: EventHandler;
  private selfId: string;

  constructor(selfId: string, repairTimeoutMs: number, emit: EventHandler) {
    this.selfId = selfId;
    this.repairTimeoutMs = repairTimeoutMs;
    this.emit = emit;
  }

  /** Add a peer, initially eager */
  addPeer(peerId: string): void {
    this.eagerPeers.add(peerId);
  }

  removePeer(peerId: string): void {
    this.eagerPeers.delete(peerId);
    this.lazyPeers.delete(peerId);
    this.lazyQueue.delete(peerId);
  }

  /**
   * Handle an incoming rumor via eager push.
   * Returns { deliver, forwardTo } — deliver = true if new rumor, forwardTo = eager peers to forward to.
   */
  handleEagerPush(fromPeer: string, rumorId: string, origin: string): { deliver: boolean; forwardTo: string[] } {
    if (this.seen.has(rumorId)) {
      // Duplicate — move sender from eager to lazy (prune)
      this.eagerPeers.delete(fromPeer);
      this.lazyPeers.add(fromPeer);
      // Cancel any missing timer
      this.missingTimers.delete(rumorId);
      return { deliver: false, forwardTo: [] };
    }

    this.seen.add(rumorId);
    this.missingTimers.delete(rumorId);

    // Forward to all eager peers except sender
    const forwardTo = [...this.eagerPeers].filter(p => p !== fromPeer);

    // Queue lazy notifications for lazy peers
    for (const lp of this.lazyPeers) {
      if (lp === fromPeer) continue;
      if (!this.lazyQueue.has(lp)) this.lazyQueue.set(lp, []);
      this.lazyQueue.get(lp)!.push({ rumorId, origin, receivedAt: Date.now() });
    }

    this.emit({ type: 'rumor-received', source: fromPeer, target: this.selfId, timestamp: Date.now(), payload: { rumorId } });
    return { deliver: true, forwardTo };
  }

  /**
   * Handle IHAVE (lazy notification). If we haven't seen it, set a timer to GRAFT.
   * Returns { needGraft: boolean, graftTarget?: string }
   */
  handleIHave(fromPeer: string, rumorId: string, now: number): { needGraft: boolean } {
    if (this.seen.has(rumorId)) return { needGraft: false };
    if (this.missingTimers.has(rumorId)) return { needGraft: false };

    // Set timer — if we don't receive via eager push before deadline, graft
    this.missingTimers.set(rumorId, now + this.repairTimeoutMs);
    return { needGraft: false }; // will graft on timeout
  }

  /**
   * Check for missing rumors that need grafting.
   * Returns peers to graft (promote back to eager) and request rumorIds from.
   */
  checkRepairTimers(now: number): Array<{ peerId: string; rumorId: string }> {
    const grafts: Array<{ peerId: string; rumorId: string }> = [];
    for (const [rumorId, deadline] of this.missingTimers) {
      if (now >= deadline && !this.seen.has(rumorId)) {
        // Find a lazy peer that sent IHAVE for this rumor
        for (const [peerId, queue] of this.lazyQueue) {
          const entry = queue.find(e => e.rumorId === rumorId);
          if (entry) {
            // GRAFT — move peer back to eager
            this.lazyPeers.delete(peerId);
            this.eagerPeers.add(peerId);
            grafts.push({ peerId, rumorId });
            break;
          }
        }
        this.missingTimers.delete(rumorId);
      }
    }
    return grafts;
  }

  /** Drain lazy IHAVE notifications for a peer */
  drainLazyQueue(peerId: string, max: number): Array<{ rumorId: string; origin: string }> {
    const queue = this.lazyQueue.get(peerId);
    if (!queue) return [];
    return queue.splice(0, max).map(e => ({ rumorId: e.rumorId, origin: e.origin }));
  }

  getEagerPeers(): string[] { return [...this.eagerPeers]; }
  getLazyPeers(): string[] { return [...this.lazyPeers]; }
  getSeenCount(): number { return this.seen.size; }
}

// ============================================================
// Merkle-based Anti-Entropy Sync
// ============================================================

export class MerkleAntiEntropy {
  private state = new Map<string, { value: unknown; version: number; hash: number }>();
  private bucketCount: number;
  private emit: EventHandler;
  private selfId: string;

  constructor(selfId: string, bucketCount: number, emit: EventHandler) {
    this.selfId = selfId;
    this.bucketCount = bucketCount;
    this.emit = emit;
  }

  /** Set a key-value in local state */
  set(key: string, value: unknown, version: number): void {
    this.state.set(key, { value, version, hash: fnv1a(JSON.stringify(value)) });
  }

  get(key: string): { value: unknown; version: number } | undefined {
    const entry = this.state.get(key);
    return entry ? { value: entry.value, version: entry.version } : undefined;
  }

  /**
   * Compute digest — bucket keys by hash, compute per-bucket aggregate hash.
   * This is the "top level" of our Merkle tree (1 level for simplicity).
   */
  computeDigest(): Map<number, number> {
    const buckets = new Map<number, number>();
    for (let i = 0; i < this.bucketCount; i++) buckets.set(i, 0);

    for (const [key, entry] of this.state) {
      const bucket = fnv1a(key) % this.bucketCount;
      const current = buckets.get(bucket) ?? 0;
      // XOR the version-tagged hash into the bucket
      buckets.set(bucket, (current ^ (entry.hash * (entry.version + 1))) >>> 0);
    }
    return buckets;
  }

  /**
   * Compare local digest with remote digest.
   * Returns bucket indices that differ — these need key-level exchange.
   */
  diffDigests(remoteDigest: Map<number, number>): number[] {
    const localDigest = this.computeDigest();
    const diffBuckets: number[] = [];
    for (let i = 0; i < this.bucketCount; i++) {
      if ((localDigest.get(i) ?? 0) !== (remoteDigest.get(i) ?? 0)) {
        diffBuckets.push(i);
      }
    }
    return diffBuckets;
  }

  /** Get all entries in a given bucket for key-level comparison */
  getEntriesInBucket(bucket: number): DigestEntry[] {
    const entries: DigestEntry[] = [];
    for (const [key, entry] of this.state) {
      if (fnv1a(key) % this.bucketCount === bucket) {
        entries.push({ key, version: entry.version, hash: entry.hash });
      }
    }
    return entries;
  }

  /**
   * Merge remote entries — accept if remote version is higher.
   * Returns keys that were updated.
   */
  mergeEntries(remoteEntries: Array<{ key: string; value: unknown; version: number }>): string[] {
    const updated: string[] = [];
    for (const remote of remoteEntries) {
      const local = this.state.get(remote.key);
      if (!local || remote.version > local.version) {
        this.set(remote.key, remote.value, remote.version);
        updated.push(remote.key);
      }
    }
    if (updated.length > 0) {
      this.emit({
        type: 'digest-sync-complete',
        source: this.selfId,
        timestamp: Date.now(),
        payload: { updatedKeys: updated.length },
      });
    }
    return updated;
  }

  getStateSize(): number { return this.state.size; }
}

// ============================================================
// Bimodal Multicast
// ============================================================

/**
 * Two-phase gossip:
 * Phase 1: Unreliable broadcast (best-effort UDP-style)
 * Phase 2: Gossip-based repair — each round, agents exchange buffer digests
 *          and probabilistically solicit missing messages.
 *
 * Achieves very high reliability with low overhead for large clusters.
 */
export class BimodalMulticast {
  private buffer = new Map<string, { payload: unknown; receivedAt: number }>(); // msgId -> payload
  private bufferMaxAge: number;
  private beta: number; // probability of selecting a random peer for solicitation
  private emit: EventHandler;
  private selfId: string;

  constructor(selfId: string, beta: number, bufferMaxAgeMs: number, emit: EventHandler) {
    this.selfId = selfId;
    this.beta = beta;
    this.bufferMaxAge = bufferMaxAgeMs;
    this.emit = emit;
  }

  /** Phase 1: Receive a broadcast message */
  receivePhase1(msgId: string, payload: unknown): boolean {
    if (this.buffer.has(msgId)) return false; // already have it
    this.buffer.set(msgId, { payload, receivedAt: Date.now() });
    return true; // new message
  }

  /** Phase 2: Generate our digest (set of msgIds we have) */
  getDigest(): string[] {
    return [...this.buffer.keys()];
  }

  /**
   * Phase 2: Compare peer's digest with ours.
   * Returns msgIds we have that the peer is missing.
   */
  findMissingForPeer(peerDigest: string[]): string[] {
    const peerSet = new Set(peerDigest);
    return [...this.buffer.keys()].filter(id => !peerSet.has(id));
  }

  /**
   * Phase 2: Determine if we should solicit from a peer this round.
   * Bernoulli trial with probability beta.
   */
  shouldSolicit(): boolean {
    return Math.random() < this.beta;
  }

  /** Get messages we're missing that the peer has */
  findMissingFromPeer(peerDigest: string[]): string[] {
    return peerDigest.filter(id => !this.buffer.has(id));
  }

  /** Accept solicited messages */
  acceptSolicited(messages: Array<{ id: string; payload: unknown }>): string[] {
    const newIds: string[] = [];
    for (const msg of messages) {
      if (!this.buffer.has(msg.id)) {
        this.buffer.set(msg.id, { payload: msg.payload, receivedAt: Date.now() });
        newIds.push(msg.id);
      }
    }
    return newIds;
  }

  /** Garbage collect old messages */
  gc(now: number): number {
    let removed = 0;
    for (const [id, entry] of this.buffer) {
      if (now - entry.receivedAt > this.bufferMaxAge) {
        this.buffer.delete(id);
        removed++;
      }
    }
    return removed;
  }

  getBufferSize(): number { return this.buffer.size; }
}

// ============================================================
// Adaptive Fanout Controller
// ============================================================

/**
 * Adjusts gossip fanout based on network size and observed delivery rates.
 *
 * Theory: For epidemic gossip to infect all N nodes with high probability,
 * fanout >= ln(N) + c (where c is a small constant for the desired
 * probability). We track delivery success and adjust dynamically.
 */
export class AdaptiveFanout {
  private baselineFanout: number;
  private currentFanout: number;
  private networkSize: number;
  private deliveryWindow: boolean[] = []; // rolling window of delivery successes
  private windowSize = 100;
  private targetDeliveryRate = 0.99;
  private minFanout = 2;
  private maxFanoutMultiplier = 4;
  private emit: EventHandler;
  private selfId: string;

  constructor(selfId: string, baselineFanout: number, emit: EventHandler) {
    this.selfId = selfId;
    this.baselineFanout = baselineFanout;
    this.currentFanout = baselineFanout;
    this.networkSize = 10; // initial estimate
    this.emit = emit;
  }

  /** Update network size estimate */
  updateNetworkSize(size: number): void {
    this.networkSize = Math.max(2, size);
    this.recalculate();
  }

  /** Record whether a rumor was successfully delivered */
  recordDelivery(success: boolean): void {
    this.deliveryWindow.push(success);
    if (this.deliveryWindow.length > this.windowSize) {
      this.deliveryWindow.shift();
    }
    this.recalculate();
  }

  /** Get current fanout value */
  getFanout(): number {
    return this.currentFanout;
  }

  private recalculate(): void {
    // Theoretical minimum: ln(N) + 1
    const theoreticalMin = Math.ceil(Math.log(this.networkSize) + 1);

    // Observed delivery rate
    if (this.deliveryWindow.length >= 20) {
      const successCount = this.deliveryWindow.filter(Boolean).length;
      const rate = successCount / this.deliveryWindow.length;

      if (rate < this.targetDeliveryRate) {
        // Increase fanout — not delivering enough
        this.currentFanout = Math.min(
          this.currentFanout + 1,
          this.baselineFanout * this.maxFanoutMultiplier
        );
      } else if (rate > 0.999 && this.currentFanout > theoreticalMin) {
        // Over-delivering — can reduce
        this.currentFanout = Math.max(this.currentFanout - 1, theoreticalMin);
      }
    } else {
      // Not enough data — use theoretical value
      this.currentFanout = Math.max(this.baselineFanout, theoreticalMin);
    }

    this.currentFanout = Math.max(this.currentFanout, this.minFanout);
    this.emit({
      type: 'fanout-adjusted',
      source: this.selfId,
      timestamp: Date.now(),
      payload: { fanout: this.currentFanout, networkSize: this.networkSize },
    });
  }
}

// ============================================================
// Rumor Manager — Infection-style dissemination
// ============================================================

export class RumorManager {
  private rumors = new Map<string, Rumor>();
  private maxRumors: number;
  private emit: EventHandler;
  private selfId: string;

  constructor(selfId: string, maxRumors: number, emit: EventHandler) {
    this.selfId = selfId;
    this.maxRumors = maxRumors;
    this.emit = emit;
  }

  /** Create a new rumor to spread */
  createRumor(id: string, payload: unknown, ttlMs: number, maxHops: number): Rumor {
    const rumor: Rumor = {
      id,
      origin: this.selfId,
      payload,
      hops: 0,
      maxHops,
      createdAt: Date.now(),
      ttlMs,
      infectionCount: 1, // self
    };
    this.rumors.set(id, rumor);
    this.evictOldest();
    return rumor;
  }

  /** Receive a rumor from another agent */
  receiveRumor(rumor: Rumor): { isNew: boolean; shouldForward: boolean } {
    if (this.rumors.has(rumor.id)) {
      // Already seen — increment infection count for tracking
      const existing = this.rumors.get(rumor.id)!;
      existing.infectionCount++;
      return { isNew: false, shouldForward: false };
    }

    const now = Date.now();
    if (now - rumor.createdAt > rumor.ttlMs) {
      this.emit({ type: 'rumor-expired', source: this.selfId, timestamp: now, payload: { rumorId: rumor.id } });
      return { isNew: false, shouldForward: false };
    }

    if (rumor.hops >= rumor.maxHops) {
      return { isNew: false, shouldForward: false };
    }

    const received: Rumor = {
      ...rumor,
      hops: rumor.hops + 1,
      infectionCount: rumor.infectionCount + 1,
    };
    this.rumors.set(received.id, received);
    this.evictOldest();

    return { isNew: true, shouldForward: received.hops < received.maxHops };
  }

  /** Get rumors to piggyback on outgoing messages */
  getRumorsToSpread(max: number): Rumor[] {
    const now = Date.now();
    const active = [...this.rumors.values()]
      .filter(r => now - r.createdAt < r.ttlMs && r.hops < r.maxHops)
      .sort((a, b) => a.infectionCount - b.infectionCount); // least-spread first
    return active.slice(0, max);
  }

  /** Expire old rumors */
  gc(now: number): number {
    let removed = 0;
    for (const [id, rumor] of this.rumors) {
      if (now - rumor.createdAt > rumor.ttlMs) {
        this.rumors.delete(id);
        removed++;
      }
    }
    return removed;
  }

  private evictOldest(): void {
    if (this.rumors.size <= this.maxRumors) return;
    // Evict oldest by creation time
    let oldestId: string | null = null;
    let oldestTime = Infinity;
    for (const [id, rumor] of this.rumors) {
      if (rumor.createdAt < oldestTime) {
        oldestTime = rumor.createdAt;
        oldestId = id;
      }
    }
    if (oldestId) this.rumors.delete(oldestId);
  }

  getActiveCount(): number {
    const now = Date.now();
    return [...this.rumors.values()].filter(r => now - r.createdAt < r.ttlMs).length;
  }
}

// ============================================================
// Partition Detector
// ============================================================

/**
 * Detects network partitions by monitoring member reachability patterns.
 * If a cluster of agents all become suspect/dead simultaneously,
 * it's likely a partition rather than individual failures.
 */
export class PartitionDetector {
  private failureEvents: Array<{ agentId: string; timestamp: number }> = [];
  private windowMs: number;
  private thresholdRatio: number; // fraction of members failing simultaneously = partition
  private emit: EventHandler;
  private selfId: string;

  constructor(selfId: string, windowMs: number, thresholdRatio: number, emit: EventHandler) {
    this.selfId = selfId;
    this.windowMs = windowMs;
    this.thresholdRatio = thresholdRatio;
    this.emit = emit;
  }

  /** Record a suspected failure */
  recordFailure(agentId: string, now: number): void {
    this.failureEvents.push({ agentId, timestamp: now });
    // GC old events
    this.failureEvents = this.failureEvents.filter(e => now - e.timestamp < this.windowMs);
  }

  /** Check if current failure pattern looks like a partition */
  checkPartition(totalAlive: number, now: number): { isPartition: boolean; failedCount: number; ratio: number } {
    const recentFailures = new Set(
      this.failureEvents
        .filter(e => now - e.timestamp < this.windowMs)
        .map(e => e.agentId)
    );
    const failedCount = recentFailures.size;
    const ratio = totalAlive > 0 ? failedCount / totalAlive : 0;
    const isPartition = ratio >= this.thresholdRatio && failedCount >= 2;

    if (isPartition) {
      this.emit({
        type: 'partition-detected',
        source: this.selfId,
        timestamp: now,
        payload: { failedCount, ratio, agents: [...recentFailures] },
      });
    }

    return { isPartition, failedCount, ratio };
  }
}

// ============================================================
// Unified Gossip Engine — Composes all subsystems
// ============================================================

export class GossipEngine {
  readonly config: GossipConfig;
  readonly membership: SwimMembership;
  readonly plumtree: PlumtreeGossip;
  readonly antiEntropy: MerkleAntiEntropy;
  readonly bimodal: BimodalMulticast;
  readonly fanout: AdaptiveFanout;
  readonly rumors: RumorManager;
  readonly partitionDetector: PartitionDetector;

  private events: GossipEvent[] = [];
  private handlers: EventHandler[] = [];
  private tickCount = 0;

  constructor(config: GossipConfig) {
    this.config = config;
    const emit: EventHandler = (e) => {
      this.events.push(e);
      for (const h of this.handlers) h(e);
    };

    this.membership = new SwimMembership(config, emit);
    this.plumtree = new PlumtreeGossip(config.selfId, config.protocolPeriodMs * 2, emit);
    this.antiEntropy = new MerkleAntiEntropy(config.selfId, config.digestBuckets, emit);
    this.bimodal = new BimodalMulticast(config.selfId, config.bimodalBeta, config.rumorTtlMs, emit);
    this.fanout = new AdaptiveFanout(config.selfId, config.fanout, emit);
    this.rumors = new RumorManager(config.selfId, 1000, emit);
    this.partitionDetector = new PartitionDetector(config.selfId, config.protocolPeriodMs * 10, 0.3, emit);
  }

  onEvent(handler: EventHandler): void {
    this.handlers.push(handler);
  }

  /**
   * Main tick — runs one gossip round.
   * Returns actions the caller should execute (send messages to peers).
   */
  tick(now: number): GossipTickResult {
    this.tickCount++;
    const result: GossipTickResult = {
      probes: [],
      rumorPushes: [],
      lazyNotifications: [],
      repairRequests: [],
      antiEntropyTarget: null,
    };

    // 1. SWIM membership tick
    const swimResult = this.membership.tick(now);
    if (swimResult.directProbe) {
      result.probes.push({ target: swimResult.directProbe, type: 'direct' });
    }
    if (swimResult.indirectProbes) {
      for (const p of swimResult.indirectProbes) {
        result.probes.push({ target: p, type: 'indirect' });
      }
    }
    for (const dead of swimResult.deadMembers) {
      this.plumtree.removePeer(dead);
      this.partitionDetector.recordFailure(dead, now);
    }

    // 2. Partition detection
    this.partitionDetector.checkPartition(this.membership.getAliveCount(), now);

    // 3. Update adaptive fanout
    this.fanout.updateNetworkSize(this.membership.getAliveCount());
    const currentFanout = this.fanout.getFanout();

    // 4. Plumtree repair check
    const grafts = this.plumtree.checkRepairTimers(now);
    for (const g of grafts) {
      result.repairRequests.push({ target: g.peerId, rumorId: g.rumorId });
    }

    // 5. Drain lazy notifications
    for (const lp of this.plumtree.getLazyPeers()) {
      const notifications = this.plumtree.drainLazyQueue(lp, this.config.maxRumorsPerMsg);
      if (notifications.length > 0) {
        result.lazyNotifications.push({ target: lp, rumors: notifications });
      }
    }

    // 6. Spread active rumors to random peers
    const activeRumors = this.rumors.getRumorsToSpread(this.config.maxRumorsPerMsg);
    if (activeRumors.length > 0) {
      const targets = this.membership.selectRandomMembers(currentFanout);
      for (const target of targets) {
        result.rumorPushes.push({ target, rumors: activeRumors });
      }
    }

    // 7. Anti-entropy sync (periodic)
    if (this.tickCount % Math.ceil(this.config.antiEntropyIntervalMs / this.config.protocolPeriodMs) === 0) {
      const syncTargets = this.membership.selectRandomMembers(1);
      if (syncTargets.length > 0) {
        result.antiEntropyTarget = syncTargets[0];
      }
    }

    // 8. Bimodal GC
    if (this.config.bimodalEnabled) {
      this.bimodal.gc(now);
    }

    // 9. Rumor GC
    this.rumors.gc(now);

    // 10. Remove confirmed-dead members periodically
    if (this.tickCount % 10 === 0) {
      this.membership.removeDead();
    }

    return result;
  }

  /** Drain and return all accumulated events */
  drainEvents(): GossipEvent[] {
    return this.events.splice(0);
  }

  /** Get engine statistics */
  getStats(): GossipStats {
    return {
      aliveMembers: this.membership.getAliveCount(),
      eagerPeers: this.plumtree.getEagerPeers().length,
      lazyPeers: this.plumtree.getLazyPeers().length,
      seenRumors: this.plumtree.getSeenCount(),
      activeRumors: this.rumors.getActiveCount(),
      currentFanout: this.fanout.getFanout(),
      antiEntropyKeys: this.antiEntropy.getStateSize(),
      bimodalBuffer: this.bimodal.getBufferSize(),
      tickCount: this.tickCount,
    };
  }
}

export interface GossipTickResult {
  probes: Array<{ target: string; type: 'direct' | 'indirect' }>;
  rumorPushes: Array<{ target: string; rumors: Rumor[] }>;
  lazyNotifications: Array<{ target: string; rumors: Array<{ rumorId: string; origin: string }> }>;
  repairRequests: Array<{ target: string; rumorId: string }>;
  antiEntropyTarget: string | null;
}

export interface GossipStats {
  aliveMembers: number;
  eagerPeers: number;
  lazyPeers: number;
  seenRumors: number;
  activeRumors: number;
  currentFanout: number;
  antiEntropyKeys: number;
  bimodalBuffer: number;
  tickCount: number;
}

// ============================================================
// Presets
// ============================================================

export const PRESETS = {
  /** Small cluster (5-20 agents), fast convergence */
  'small-cluster': {
    protocolPeriodMs: 500,
    suspicionMultiplier: 3,
    indirectProbes: 2,
    fanout: 3,
    adaptiveFanout: false,
    rumorTtlMs: 10_000,
    maxHops: 5,
    antiEntropyIntervalMs: 5_000,
    digestBuckets: 16,
    bimodalEnabled: false,
    bimodalBeta: 0,
    maxPiggybackPerMsg: 4,
    maxRumorsPerMsg: 4,
  },

  /** Medium network (20-200 agents), balanced */
  'medium-network': {
    protocolPeriodMs: 1_000,
    suspicionMultiplier: 4,
    indirectProbes: 3,
    fanout: 4,
    adaptiveFanout: true,
    rumorTtlMs: 30_000,
    maxHops: 10,
    antiEntropyIntervalMs: 15_000,
    digestBuckets: 64,
    bimodalEnabled: true,
    bimodalBeta: 0.3,
    maxPiggybackPerMsg: 6,
    maxRumorsPerMsg: 6,
  },

  /** Large federation (200+ agents), high reliability */
  'large-federation': {
    protocolPeriodMs: 2_000,
    suspicionMultiplier: 5,
    indirectProbes: 4,
    fanout: 6,
    adaptiveFanout: true,
    rumorTtlMs: 60_000,
    maxHops: 20,
    antiEntropyIntervalMs: 30_000,
    digestBuckets: 256,
    bimodalEnabled: true,
    bimodalBeta: 0.5,
    maxPiggybackPerMsg: 8,
    maxRumorsPerMsg: 8,
  },
} as const;

/**
 * Factory — create a GossipEngine from a preset.
 */
export function createGossipEngine(
  selfId: string,
  preset: keyof typeof PRESETS = 'medium-network',
  overrides: Partial<GossipConfig> = {}
): GossipEngine {
  const config: GossipConfig = {
    selfId,
    ...PRESETS[preset],
    ...overrides,
  };
  return new GossipEngine(config);
}
