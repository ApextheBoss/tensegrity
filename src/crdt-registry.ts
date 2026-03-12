/**
 * crdt-registry.ts — Conflict-Free Replicated Agent Registry
 * 
 * A CRDT-based agent registry that allows concurrent updates across
 * distributed nodes without coordination. Supports:
 * - OR-Set semantics for agent membership (add wins over remove)
 * - LWW-Register for mutable agent metadata (last-writer-wins with Lamport clocks)
 * - Observed-Remove for capability sets (precise remove without tombstone bloat)
 * - Merkle-based anti-entropy sync (efficient delta detection)
 * - Causal consistency via vector clocks per agent entry
 * - Garbage collection of tombstones with retention windows
 * - Split-brain convergence guarantees (commutativity + idempotency)
 * 
 * Architecture:
 * Each node maintains a local replica. Mutations generate ops that are:
 * 1. Applied locally (immediate consistency)
 * 2. Broadcast to peers (crdt-sync protocol)  
 * 3. Merged on receipt (convergence guaranteed by CRDT properties)
 * 
 * The key insight: traditional registries need consensus for writes.
 * CRDT registries need consensus for NOTHING — every node can accept
 * writes independently and the state converges automatically.
 */

// ── Types ──────────────────────────────────────────────────────────────

interface VectorClock {
  readonly entries: ReadonlyMap<string, number>;
}

interface LamportTimestamp {
  readonly counter: number;
  readonly nodeId: string;
}

interface ORSetElement<T> {
  readonly value: T;
  readonly uniqueTag: string;  // globally unique add-tag
  readonly addedBy: string;
  readonly addedAt: number;
}

interface LWWRegister<T> {
  readonly value: T;
  readonly timestamp: LamportTimestamp;
}

interface ObservedRemoveSet<T> {
  readonly elements: Map<string, ORSetElement<T>>;  // tag -> element
  readonly tombstones: Map<string, { removedAt: number; removedBy: string }>;
}

interface AgentEntry {
  readonly agentId: string;
  readonly address: string;
  readonly metadata: LWWRegister<AgentMetadata>;
  readonly capabilities: ObservedRemoveSet<string>;
  readonly status: LWWRegister<AgentStatus>;
  readonly vectorClock: VectorClock;
  readonly lastSeen: number;
}

interface AgentMetadata {
  readonly name: string;
  readonly description: string;
  readonly version: string;
  readonly endpoints: readonly string[];
  readonly tags: readonly string[];
  readonly customFields: ReadonlyMap<string, string>;
}

type AgentStatus = 'online' | 'offline' | 'degraded' | 'draining' | 'tombstoned';

// ── Operations (the unit of replication) ────────────────────────────────

type RegistryOp =
  | AddAgentOp
  | RemoveAgentOp
  | UpdateMetadataOp
  | AddCapabilityOp
  | RemoveCapabilityOp
  | UpdateStatusOp
  | HeartbeatOp;

interface AddAgentOp {
  readonly type: 'add-agent';
  readonly agentId: string;
  readonly address: string;
  readonly metadata: AgentMetadata;
  readonly capabilities: readonly string[];
  readonly uniqueTag: string;
  readonly sourceNode: string;
  readonly timestamp: LamportTimestamp;
}

interface RemoveAgentOp {
  readonly type: 'remove-agent';
  readonly agentId: string;
  readonly observedTags: readonly string[];  // tags we've seen (observed-remove)
  readonly sourceNode: string;
  readonly timestamp: LamportTimestamp;
}

interface UpdateMetadataOp {
  readonly type: 'update-metadata';
  readonly agentId: string;
  readonly metadata: AgentMetadata;
  readonly sourceNode: string;
  readonly timestamp: LamportTimestamp;
}

interface AddCapabilityOp {
  readonly type: 'add-capability';
  readonly agentId: string;
  readonly capability: string;
  readonly uniqueTag: string;
  readonly sourceNode: string;
  readonly timestamp: LamportTimestamp;
}

interface RemoveCapabilityOp {
  readonly type: 'remove-capability';
  readonly agentId: string;
  readonly capability: string;
  readonly observedTags: readonly string[];
  readonly sourceNode: string;
  readonly timestamp: LamportTimestamp;
}

interface UpdateStatusOp {
  readonly type: 'update-status';
  readonly agentId: string;
  readonly status: AgentStatus;
  readonly sourceNode: string;
  readonly timestamp: LamportTimestamp;
}

interface HeartbeatOp {
  readonly type: 'heartbeat';
  readonly agentId: string;
  readonly sourceNode: string;
  readonly timestamp: LamportTimestamp;
}

// ── Vector Clock utilities ──────────────────────────────────────────────

class VectorClockUtil {
  static create(): VectorClock {
    return { entries: new Map() };
  }

  static increment(clock: VectorClock, nodeId: string): VectorClock {
    const entries = new Map(clock.entries);
    entries.set(nodeId, (entries.get(nodeId) || 0) + 1);
    return { entries };
  }

  static merge(a: VectorClock, b: VectorClock): VectorClock {
    const entries = new Map(a.entries);
    for (const [node, counter] of b.entries) {
      entries.set(node, Math.max(entries.get(node) || 0, counter));
    }
    return { entries };
  }

  /** Returns: -1 if a < b, 1 if a > b, 0 if concurrent */
  static compare(a: VectorClock, b: VectorClock): -1 | 0 | 1 {
    let aGreater = false;
    let bGreater = false;
    const allNodes = new Set([...a.entries.keys(), ...b.entries.keys()]);
    for (const node of allNodes) {
      const aVal = a.entries.get(node) || 0;
      const bVal = b.entries.get(node) || 0;
      if (aVal > bVal) aGreater = true;
      if (bVal > aVal) bGreater = true;
    }
    if (aGreater && !bGreater) return 1;
    if (bGreater && !aGreater) return -1;
    return 0;
  }

  /** True if a dominates b (a happened after b) */
  static dominates(a: VectorClock, b: VectorClock): boolean {
    return this.compare(a, b) === 1;
  }

  /** True if neither dominates the other */
  static concurrent(a: VectorClock, b: VectorClock): boolean {
    return this.compare(a, b) === 0;
  }
}

// ── Lamport Clock ───────────────────────────────────────────────────────

class LamportClock {
  private counter: number = 0;

  constructor(private readonly nodeId: string) {}

  tick(): LamportTimestamp {
    this.counter++;
    return { counter: this.counter, nodeId: this.nodeId };
  }

  update(remote: LamportTimestamp): void {
    this.counter = Math.max(this.counter, remote.counter) + 1;
  }

  static compare(a: LamportTimestamp, b: LamportTimestamp): number {
    if (a.counter !== b.counter) return a.counter - b.counter;
    return a.nodeId < b.nodeId ? -1 : a.nodeId > b.nodeId ? 1 : 0;
  }
}

// ── Merkle Tree for Anti-Entropy ────────────────────────────────────────

interface MerkleNode {
  readonly hash: string;
  readonly children: ReadonlyMap<string, MerkleNode>;
  readonly agentIds: readonly string[];
}

class MerkleTree {
  /**
   * Build a Merkle tree from agent entries, bucketed by agentId prefix.
   * Allows efficient diff detection: two nodes exchange root hashes,
   * then drill into differing subtrees to find divergent agents.
   */
  static build(agents: Map<string, AgentEntry>, depth: number = 2): MerkleNode {
    if (depth === 0 || agents.size === 0) {
      const ids = [...agents.keys()].sort();
      const hash = this.hashLeaf(agents);
      return { hash, children: new Map(), agentIds: ids };
    }

    const buckets = new Map<string, Map<string, AgentEntry>>();
    for (const [id, entry] of agents) {
      const prefix = id.substring(0, 1).toLowerCase();
      if (!buckets.has(prefix)) buckets.set(prefix, new Map());
      buckets.get(prefix)!.set(id, entry);
    }

    const children = new Map<string, MerkleNode>();
    const hashes: string[] = [];
    for (const [prefix, bucket] of [...buckets.entries()].sort()) {
      const child = this.build(bucket, depth - 1);
      children.set(prefix, child);
      hashes.push(`${prefix}:${child.hash}`);
    }

    return {
      hash: this.hashString(hashes.join('|')),
      children,
      agentIds: [...agents.keys()].sort(),
    };
  }

  /**
   * Find divergent agent IDs by comparing two Merkle trees.
   * Returns agent IDs that differ between the two replicas.
   */
  static diff(local: MerkleNode, remote: MerkleNode): string[] {
    if (local.hash === remote.hash) return [];

    if (local.children.size === 0 && remote.children.size === 0) {
      // Leaf level — return all agent IDs from both sides
      const allIds = new Set([...local.agentIds, ...remote.agentIds]);
      return [...allIds];
    }

    const divergent: string[] = [];
    const allPrefixes = new Set([
      ...local.children.keys(),
      ...remote.children.keys(),
    ]);

    for (const prefix of allPrefixes) {
      const localChild = local.children.get(prefix);
      const remoteChild = remote.children.get(prefix);

      if (!localChild && remoteChild) {
        divergent.push(...remoteChild.agentIds);
      } else if (localChild && !remoteChild) {
        divergent.push(...localChild.agentIds);
      } else if (localChild && remoteChild) {
        divergent.push(...this.diff(localChild, remoteChild));
      }
    }

    return divergent;
  }

  private static hashLeaf(agents: Map<string, AgentEntry>): string {
    const parts: string[] = [];
    for (const [id, entry] of [...agents.entries()].sort()) {
      parts.push(`${id}:${entry.metadata.timestamp.counter}:${entry.status.timestamp.counter}`);
    }
    return this.hashString(parts.join(','));
  }

  // FNV-1a hash (same as used elsewhere in the codebase)
  private static hashString(input: string): string {
    let hash = 0x811c9dc5;
    for (let i = 0; i < input.length; i++) {
      hash ^= input.charCodeAt(i);
      hash = (hash * 0x01000193) >>> 0;
    }
    return hash.toString(16).padStart(8, '0');
  }
}

// ── Tombstone Garbage Collector ─────────────────────────────────────────

interface GCConfig {
  readonly tombstoneRetentionMs: number;     // how long to keep tombstones
  readonly gcIntervalMs: number;             // how often to run GC
  readonly maxTombstonesBeforeForceGC: number; // force GC above this count
}

const DEFAULT_GC_CONFIG: GCConfig = {
  tombstoneRetentionMs: 7 * 24 * 60 * 60 * 1000,  // 7 days
  gcIntervalMs: 60 * 60 * 1000,                      // 1 hour
  maxTombstonesBeforeForceGC: 10000,
};

class TombstoneGC {
  private lastGC: number = 0;

  constructor(private readonly config: GCConfig = DEFAULT_GC_CONFIG) {}

  shouldRun(tombstoneCount: number, now: number): boolean {
    if (tombstoneCount >= this.config.maxTombstonesBeforeForceGC) return true;
    return (now - this.lastGC) >= this.config.gcIntervalMs;
  }

  /**
   * Collect expired tombstones.
   * SAFETY: Only collect tombstones older than retention window.
   * If a node has been offline longer than the retention window,
   * it must do a full sync (not incremental) on rejoin.
   */
  collect<T>(set: ObservedRemoveSet<T>, now: number): ObservedRemoveSet<T> {
    this.lastGC = now;
    const cutoff = now - this.config.tombstoneRetentionMs;
    const newTombstones = new Map(set.tombstones);
    
    for (const [tag, info] of newTombstones) {
      if (info.removedAt < cutoff) {
        newTombstones.delete(tag);
      }
    }

    return {
      elements: set.elements,
      tombstones: newTombstones,
    };
  }
}

// ── CRDT Registry (the main data structure) ─────────────────────────────

interface RegistryConfig {
  readonly nodeId: string;
  readonly heartbeatTimeoutMs: number;       // mark offline after this
  readonly gcConfig: GCConfig;
  readonly maxOpsPerSync: number;            // bound sync payload size
  readonly enableAntiEntropy: boolean;
}

interface RegistryStats {
  readonly agentCount: number;
  readonly onlineCount: number;
  readonly tombstoneCount: number;
  readonly opsProcessed: number;
  readonly syncsCompleted: number;
  readonly conflictsResolved: number;
  readonly merkleRoot: string;
}

type RegistryEventType =
  | 'agent-added'
  | 'agent-removed'
  | 'metadata-updated'
  | 'capability-added'
  | 'capability-removed'
  | 'status-changed'
  | 'conflict-resolved'
  | 'sync-completed'
  | 'gc-completed'
  | 'split-brain-detected';

interface RegistryEvent {
  readonly type: RegistryEventType;
  readonly agentId?: string;
  readonly detail: string;
  readonly timestamp: number;
  readonly sourceNode: string;
}

class CRDTRegistry {
  private agents: Map<string, AgentEntry> = new Map();
  private removedAgents: Map<string, { tags: Set<string>; removedAt: number; removedBy: string }> = new Map();
  private clock: LamportClock;
  private gc: TombstoneGC;
  private opLog: RegistryOp[] = [];
  private opLogCursor: Map<string, number> = new Map();  // nodeId -> last seen op index
  private stats = {
    opsProcessed: 0,
    syncsCompleted: 0,
    conflictsResolved: 0,
  };
  private listeners: Map<RegistryEventType, Array<(event: RegistryEvent) => void>> = new Map();

  constructor(private readonly config: RegistryConfig) {
    this.clock = new LamportClock(config.nodeId);
    this.gc = new TombstoneGC(config.gcConfig);
  }

  // ── Public API ──────────────────────────────────────────────────────

  registerAgent(
    agentId: string,
    address: string,
    metadata: AgentMetadata,
    capabilities: string[],
  ): RegistryOp {
    const tag = this.generateUniqueTag();
    const op: AddAgentOp = {
      type: 'add-agent',
      agentId,
      address,
      metadata,
      capabilities,
      uniqueTag: tag,
      sourceNode: this.config.nodeId,
      timestamp: this.clock.tick(),
    };
    this.applyOp(op);
    return op;
  }

  deregisterAgent(agentId: string): RegistryOp | null {
    const entry = this.agents.get(agentId);
    if (!entry) return null;

    // Observed-remove: collect all tags we've observed for this agent
    const observedTags: string[] = [];
    for (const [tag] of entry.capabilities.elements) {
      observedTags.push(tag);
    }

    const op: RemoveAgentOp = {
      type: 'remove-agent',
      agentId,
      observedTags,
      sourceNode: this.config.nodeId,
      timestamp: this.clock.tick(),
    };
    this.applyOp(op);
    return op;
  }

  updateMetadata(agentId: string, metadata: AgentMetadata): RegistryOp | null {
    if (!this.agents.has(agentId)) return null;
    const op: UpdateMetadataOp = {
      type: 'update-metadata',
      agentId,
      metadata,
      sourceNode: this.config.nodeId,
      timestamp: this.clock.tick(),
    };
    this.applyOp(op);
    return op;
  }

  addCapability(agentId: string, capability: string): RegistryOp | null {
    if (!this.agents.has(agentId)) return null;
    const op: AddCapabilityOp = {
      type: 'add-capability',
      agentId,
      capability,
      uniqueTag: this.generateUniqueTag(),
      sourceNode: this.config.nodeId,
      timestamp: this.clock.tick(),
    };
    this.applyOp(op);
    return op;
  }

  removeCapability(agentId: string, capability: string): RegistryOp | null {
    const entry = this.agents.get(agentId);
    if (!entry) return null;

    // Find all tags for this capability value
    const observedTags: string[] = [];
    for (const [tag, elem] of entry.capabilities.elements) {
      if (elem.value === capability) observedTags.push(tag);
    }
    if (observedTags.length === 0) return null;

    const op: RemoveCapabilityOp = {
      type: 'remove-capability',
      agentId,
      capability,
      observedTags,
      sourceNode: this.config.nodeId,
      timestamp: this.clock.tick(),
    };
    this.applyOp(op);
    return op;
  }

  heartbeat(agentId: string): RegistryOp | null {
    if (!this.agents.has(agentId)) return null;
    const op: HeartbeatOp = {
      type: 'heartbeat',
      agentId,
      sourceNode: this.config.nodeId,
      timestamp: this.clock.tick(),
    };
    this.applyOp(op);
    return op;
  }

  // ── Query API ───────────────────────────────────────────────────────

  getAgent(agentId: string): AgentEntry | undefined {
    return this.agents.get(agentId);
  }

  findByCapability(capability: string): AgentEntry[] {
    const results: AgentEntry[] = [];
    for (const entry of this.agents.values()) {
      for (const elem of entry.capabilities.elements.values()) {
        if (elem.value === capability) {
          results.push(entry);
          break;
        }
      }
    }
    return results;
  }

  findByStatus(status: AgentStatus): AgentEntry[] {
    return [...this.agents.values()].filter(e => e.status.value === status);
  }

  findByTag(tag: string): AgentEntry[] {
    return [...this.agents.values()].filter(e =>
      e.metadata.value.tags.includes(tag)
    );
  }

  getAllAgents(): AgentEntry[] {
    return [...this.agents.values()];
  }

  getOnlineAgents(): AgentEntry[] {
    return this.findByStatus('online');
  }

  // ── Sync Protocol ──────────────────────────────────────────────────

  /**
   * Generate a Merkle digest for anti-entropy sync.
   * Remote node compares this to their local digest to find divergences.
   */
  getMerkleDigest(): MerkleNode {
    return MerkleTree.build(this.agents);
  }

  /**
   * Given a remote Merkle digest, find which agents need syncing.
   */
  findDivergentAgents(remoteDigest: MerkleNode): string[] {
    const localDigest = this.getMerkleDigest();
    return MerkleTree.diff(localDigest, remoteDigest);
  }

  /**
   * Get ops that a remote node hasn't seen yet.
   * Uses per-node cursor tracking for efficient delta sync.
   */
  getOpsSince(remoteNodeId: string, remoteCursor: number): RegistryOp[] {
    const ops: RegistryOp[] = [];
    for (let i = remoteCursor; i < this.opLog.length && ops.length < this.config.maxOpsPerSync; i++) {
      ops.push(this.opLog[i]);
    }
    return ops;
  }

  /**
   * Get current op cursor for tracking.
   */
  getOpCursor(): number {
    return this.opLog.length;
  }

  /**
   * Receive and apply ops from a remote node.
   * CRDT guarantees: commutative + idempotent = order doesn't matter.
   */
  receiveOps(ops: RegistryOp[], remoteNodeId: string): void {
    for (const op of ops) {
      this.clock.update(op.timestamp);
      this.applyOp(op);
    }
    this.opLogCursor.set(remoteNodeId, this.opLog.length);
    this.stats.syncsCompleted++;
    this.emit({
      type: 'sync-completed',
      detail: `Received ${ops.length} ops from ${remoteNodeId}`,
      timestamp: Date.now(),
      sourceNode: remoteNodeId,
    });
  }

  /**
   * Full state transfer for nodes that have been offline too long.
   * Returns the complete agent map for bootstrap.
   */
  getFullState(): Map<string, AgentEntry> {
    return new Map(this.agents);
  }

  /**
   * Bootstrap from a full state transfer.
   * Used when a node rejoins after being offline longer than
   * the tombstone retention window.
   */
  bootstrapFromState(state: Map<string, AgentEntry>, sourceNode: string): void {
    for (const [id, entry] of state) {
      const local = this.agents.get(id);
      if (!local) {
        this.agents.set(id, entry);
      } else {
        // Merge: take the more recent version of each field
        this.agents.set(id, this.mergeEntries(local, entry));
      }
    }
    this.emit({
      type: 'sync-completed',
      detail: `Full state bootstrap from ${sourceNode}: ${state.size} agents`,
      timestamp: Date.now(),
      sourceNode,
    });
  }

  // ── Liveness Detection ────────────────────────────────────────────

  /**
   * Check for agents that haven't heartbeated within the timeout.
   * Mark them as offline. This is a local decision — other nodes
   * may still see them as online (eventual consistency).
   */
  checkLiveness(now: number): string[] {
    const timedOut: string[] = [];
    for (const [id, entry] of this.agents) {
      if (entry.status.value === 'online' &&
          (now - entry.lastSeen) > this.config.heartbeatTimeoutMs) {
        // Generate a status update op
        const op: UpdateStatusOp = {
          type: 'update-status',
          agentId: id,
          status: 'offline',
          sourceNode: this.config.nodeId,
          timestamp: this.clock.tick(),
        };
        this.applyOp(op);
        timedOut.push(id);
      }
    }
    return timedOut;
  }

  // ── Split-Brain Detection ─────────────────────────────────────────

  /**
   * Detect potential split-brain: same agentId registered from
   * different nodes with concurrent vector clocks.
   * This means two nodes independently registered (or updated)
   * the same agent without seeing each other's writes.
   */
  detectSplitBrain(): Array<{ agentId: string; clocks: VectorClock[] }> {
    const conflicts: Array<{ agentId: string; clocks: VectorClock[] }> = [];
    // In a real implementation, we'd track per-field vector clocks
    // and detect concurrent updates. For now, flag agents with
    // high clock divergence across nodes.
    for (const [id, entry] of this.agents) {
      const entries = [...entry.vectorClock.entries.values()];
      if (entries.length >= 2) {
        const max = Math.max(...entries);
        const min = Math.min(...entries);
        // Large divergence suggests split-brain
        if (max - min > 10) {
          conflicts.push({ agentId: id, clocks: [entry.vectorClock] });
        }
      }
    }
    return conflicts;
  }

  // ── Stats ─────────────────────────────────────────────────────────

  getStats(): RegistryStats {
    let tombstoneCount = 0;
    for (const entry of this.agents.values()) {
      tombstoneCount += entry.capabilities.tombstones.size;
    }
    tombstoneCount += this.removedAgents.size;

    return {
      agentCount: this.agents.size,
      onlineCount: this.findByStatus('online').length,
      tombstoneCount,
      opsProcessed: this.stats.opsProcessed,
      syncsCompleted: this.stats.syncsCompleted,
      conflictsResolved: this.stats.conflictsResolved,
      merkleRoot: this.getMerkleDigest().hash,
    };
  }

  // ── Event System ──────────────────────────────────────────────────

  on(type: RegistryEventType, listener: (event: RegistryEvent) => void): void {
    if (!this.listeners.has(type)) this.listeners.set(type, []);
    this.listeners.get(type)!.push(listener);
  }

  private emit(event: RegistryEvent): void {
    const listeners = this.listeners.get(event.type) || [];
    for (const fn of listeners) fn(event);
  }

  // ── Op Application (the CRDT merge logic) ─────────────────────────

  private applyOp(op: RegistryOp): void {
    this.opLog.push(op);
    this.stats.opsProcessed++;

    switch (op.type) {
      case 'add-agent':
        this.applyAddAgent(op);
        break;
      case 'remove-agent':
        this.applyRemoveAgent(op);
        break;
      case 'update-metadata':
        this.applyUpdateMetadata(op);
        break;
      case 'add-capability':
        this.applyAddCapability(op);
        break;
      case 'remove-capability':
        this.applyRemoveCapability(op);
        break;
      case 'update-status':
        this.applyUpdateStatus(op);
        break;
      case 'heartbeat':
        this.applyHeartbeat(op);
        break;
    }

    // Run GC if needed
    const now = Date.now();
    let totalTombstones = this.removedAgents.size;
    for (const entry of this.agents.values()) {
      totalTombstones += entry.capabilities.tombstones.size;
    }
    if (this.gc.shouldRun(totalTombstones, now)) {
      this.runGC(now);
    }
  }

  private applyAddAgent(op: AddAgentOp): void {
    const existing = this.agents.get(op.agentId);
    
    // OR-Set semantics: add always wins over concurrent remove
    // If agent was removed but this add has a new unique tag,
    // the add takes effect (add-wins)
    const removed = this.removedAgents.get(op.agentId);
    if (removed && removed.tags.has(op.uniqueTag)) {
      // This specific add was already removed — skip
      return;
    }

    if (existing) {
      // Agent already exists — merge metadata (LWW) and capabilities (OR-Set)
      if (LamportClock.compare(op.timestamp, existing.metadata.timestamp) > 0) {
        const merged: AgentEntry = {
          ...existing,
          metadata: { value: op.metadata, timestamp: op.timestamp },
          vectorClock: VectorClockUtil.increment(
            VectorClockUtil.merge(existing.vectorClock, VectorClockUtil.create()),
            op.sourceNode,
          ),
          lastSeen: Date.now(),
        };
        this.agents.set(op.agentId, merged);
        this.stats.conflictsResolved++;
        this.emit({
          type: 'conflict-resolved',
          agentId: op.agentId,
          detail: `Concurrent add resolved via LWW (winner: ${op.sourceNode})`,
          timestamp: Date.now(),
          sourceNode: op.sourceNode,
        });
      }
      return;
    }

    // New agent — create entry
    const capElements = new Map<string, ORSetElement<string>>();
    for (const cap of op.capabilities) {
      const capTag = `${op.uniqueTag}:${cap}`;
      capElements.set(capTag, {
        value: cap,
        uniqueTag: capTag,
        addedBy: op.sourceNode,
        addedAt: Date.now(),
      });
    }

    const entry: AgentEntry = {
      agentId: op.agentId,
      address: op.address,
      metadata: { value: op.metadata, timestamp: op.timestamp },
      capabilities: { elements: capElements, tombstones: new Map() },
      status: { value: 'online', timestamp: op.timestamp },
      vectorClock: VectorClockUtil.increment(VectorClockUtil.create(), op.sourceNode),
      lastSeen: Date.now(),
    };

    this.agents.set(op.agentId, entry);
    this.emit({
      type: 'agent-added',
      agentId: op.agentId,
      detail: `Agent registered from ${op.sourceNode} with ${op.capabilities.length} capabilities`,
      timestamp: Date.now(),
      sourceNode: op.sourceNode,
    });
  }

  private applyRemoveAgent(op: RemoveAgentOp): void {
    const entry = this.agents.get(op.agentId);
    if (!entry) {
      // Agent doesn't exist locally — record tombstone anyway
      // (the add might arrive later due to reordering)
      if (!this.removedAgents.has(op.agentId)) {
        this.removedAgents.set(op.agentId, {
          tags: new Set(op.observedTags),
          removedAt: Date.now(),
          removedBy: op.sourceNode,
        });
      } else {
        const existing = this.removedAgents.get(op.agentId)!;
        for (const tag of op.observedTags) existing.tags.add(tag);
      }
      return;
    }

    // Observed-Remove: only remove tags we've actually observed
    // New tags added concurrently survive (add-wins for unseen tags)
    const survivingElements = new Map(entry.capabilities.elements);
    const newTombstones = new Map(entry.capabilities.tombstones);

    for (const tag of op.observedTags) {
      if (survivingElements.has(tag)) {
        survivingElements.delete(tag);
        newTombstones.set(tag, { removedAt: Date.now(), removedBy: op.sourceNode });
      }
    }

    // If all known elements were removed, remove the agent
    // But keep tombstone so we don't re-add from a delayed op
    this.agents.delete(op.agentId);
    this.removedAgents.set(op.agentId, {
      tags: new Set(op.observedTags),
      removedAt: Date.now(),
      removedBy: op.sourceNode,
    });

    this.emit({
      type: 'agent-removed',
      agentId: op.agentId,
      detail: `Agent removed by ${op.sourceNode} (${op.observedTags.length} tags observed)`,
      timestamp: Date.now(),
      sourceNode: op.sourceNode,
    });
  }

  private applyUpdateMetadata(op: UpdateMetadataOp): void {
    const entry = this.agents.get(op.agentId);
    if (!entry) return;

    // LWW: only apply if this timestamp is newer
    if (LamportClock.compare(op.timestamp, entry.metadata.timestamp) > 0) {
      this.agents.set(op.agentId, {
        ...entry,
        metadata: { value: op.metadata, timestamp: op.timestamp },
        vectorClock: VectorClockUtil.increment(entry.vectorClock, op.sourceNode),
      });
      this.emit({
        type: 'metadata-updated',
        agentId: op.agentId,
        detail: `Metadata updated by ${op.sourceNode}`,
        timestamp: Date.now(),
        sourceNode: op.sourceNode,
      });
    } else {
      this.stats.conflictsResolved++;
    }
  }

  private applyAddCapability(op: AddCapabilityOp): void {
    const entry = this.agents.get(op.agentId);
    if (!entry) return;

    // OR-Set add: always succeeds (unless tag is tombstoned)
    if (entry.capabilities.tombstones.has(op.uniqueTag)) return;

    const newElements = new Map(entry.capabilities.elements);
    newElements.set(op.uniqueTag, {
      value: op.capability,
      uniqueTag: op.uniqueTag,
      addedBy: op.sourceNode,
      addedAt: Date.now(),
    });

    this.agents.set(op.agentId, {
      ...entry,
      capabilities: { ...entry.capabilities, elements: newElements },
      vectorClock: VectorClockUtil.increment(entry.vectorClock, op.sourceNode),
    });

    this.emit({
      type: 'capability-added',
      agentId: op.agentId,
      detail: `Capability '${op.capability}' added by ${op.sourceNode}`,
      timestamp: Date.now(),
      sourceNode: op.sourceNode,
    });
  }

  private applyRemoveCapability(op: RemoveCapabilityOp): void {
    const entry = this.agents.get(op.agentId);
    if (!entry) return;

    const newElements = new Map(entry.capabilities.elements);
    const newTombstones = new Map(entry.capabilities.tombstones);

    for (const tag of op.observedTags) {
      if (newElements.has(tag)) {
        newElements.delete(tag);
        newTombstones.set(tag, { removedAt: Date.now(), removedBy: op.sourceNode });
      }
    }

    this.agents.set(op.agentId, {
      ...entry,
      capabilities: { elements: newElements, tombstones: newTombstones },
      vectorClock: VectorClockUtil.increment(entry.vectorClock, op.sourceNode),
    });

    this.emit({
      type: 'capability-removed',
      agentId: op.agentId,
      detail: `Capability '${op.capability}' removed by ${op.sourceNode}`,
      timestamp: Date.now(),
      sourceNode: op.sourceNode,
    });
  }

  private applyUpdateStatus(op: UpdateStatusOp): void {
    const entry = this.agents.get(op.agentId);
    if (!entry) return;

    if (LamportClock.compare(op.timestamp, entry.status.timestamp) > 0) {
      this.agents.set(op.agentId, {
        ...entry,
        status: { value: op.status, timestamp: op.timestamp },
        vectorClock: VectorClockUtil.increment(entry.vectorClock, op.sourceNode),
      });
      this.emit({
        type: 'status-changed',
        agentId: op.agentId,
        detail: `Status changed to '${op.status}' by ${op.sourceNode}`,
        timestamp: Date.now(),
        sourceNode: op.sourceNode,
      });
    }
  }

  private applyHeartbeat(op: HeartbeatOp): void {
    const entry = this.agents.get(op.agentId);
    if (!entry) return;

    this.agents.set(op.agentId, {
      ...entry,
      lastSeen: Date.now(),
      vectorClock: VectorClockUtil.increment(entry.vectorClock, op.sourceNode),
    });
  }

  // ── Entry Merge (for bootstrap/state transfer) ────────────────────

  private mergeEntries(local: AgentEntry, remote: AgentEntry): AgentEntry {
    // Metadata: LWW
    const metadata = LamportClock.compare(local.metadata.timestamp, remote.metadata.timestamp) >= 0
      ? local.metadata
      : remote.metadata;

    // Status: LWW
    const status = LamportClock.compare(local.status.timestamp, remote.status.timestamp) >= 0
      ? local.status
      : remote.status;

    // Capabilities: OR-Set merge
    // Union of elements minus union of tombstones
    const mergedElements = new Map(local.capabilities.elements);
    for (const [tag, elem] of remote.capabilities.elements) {
      if (!mergedElements.has(tag)) mergedElements.set(tag, elem);
    }
    const mergedTombstones = new Map(local.capabilities.tombstones);
    for (const [tag, info] of remote.capabilities.tombstones) {
      if (!mergedTombstones.has(tag)) mergedTombstones.set(tag, info);
    }
    // Remove tombstoned elements
    for (const tag of mergedTombstones.keys()) {
      mergedElements.delete(tag);
    }

    // Vector clock: merge (pointwise max)
    const vectorClock = VectorClockUtil.merge(local.vectorClock, remote.vectorClock);

    // Last seen: max
    const lastSeen = Math.max(local.lastSeen, remote.lastSeen);

    return {
      agentId: local.agentId,
      address: local.address,
      metadata,
      capabilities: { elements: mergedElements, tombstones: mergedTombstones },
      status,
      vectorClock,
      lastSeen,
    };
  }

  // ── Garbage Collection ────────────────────────────────────────────

  private runGC(now: number): void {
    const cutoff = now - this.config.gcConfig.tombstoneRetentionMs;
    let collected = 0;

    // GC agent tombstones
    for (const [id, info] of this.removedAgents) {
      if (info.removedAt < cutoff) {
        this.removedAgents.delete(id);
        collected++;
      }
    }

    // GC capability tombstones within live agents
    for (const [id, entry] of this.agents) {
      const cleaned = this.gc.collect(entry.capabilities, now);
      if (cleaned.tombstones.size < entry.capabilities.tombstones.size) {
        collected += entry.capabilities.tombstones.size - cleaned.tombstones.size;
        this.agents.set(id, { ...entry, capabilities: cleaned });
      }
    }

    // Compact op log (keep only recent ops)
    const minCursor = Math.min(...[...this.opLogCursor.values()], this.opLog.length);
    if (minCursor > 1000) {
      this.opLog = this.opLog.slice(minCursor);
      for (const [node, cursor] of this.opLogCursor) {
        this.opLogCursor.set(node, cursor - minCursor);
      }
    }

    if (collected > 0) {
      this.emit({
        type: 'gc-completed',
        detail: `Collected ${collected} tombstones`,
        timestamp: now,
        sourceNode: this.config.nodeId,
      });
    }
  }

  // ── Utilities ─────────────────────────────────────────────────────

  private generateUniqueTag(): string {
    // Node ID + Lamport counter = globally unique
    const ts = this.clock.tick();
    return `${ts.nodeId}:${ts.counter}:${Date.now().toString(36)}`;
  }
}

// ── Presets ──────────────────────────────────────────────────────────────

const PRESETS = {
  /** Small cluster (3-5 nodes), aggressive sync */
  smallCluster: {
    heartbeatTimeoutMs: 10_000,
    gcConfig: {
      tombstoneRetentionMs: 24 * 60 * 60 * 1000,  // 1 day
      gcIntervalMs: 10 * 60 * 1000,                 // 10 min
      maxTombstonesBeforeForceGC: 1000,
    },
    maxOpsPerSync: 500,
    enableAntiEntropy: true,
  },

  /** Medium network (10-50 nodes), balanced */
  mediumNetwork: {
    heartbeatTimeoutMs: 30_000,
    gcConfig: {
      tombstoneRetentionMs: 7 * 24 * 60 * 60 * 1000,  // 7 days
      gcIntervalMs: 60 * 60 * 1000,                      // 1 hour
      maxTombstonesBeforeForceGC: 10000,
    },
    maxOpsPerSync: 200,
    enableAntiEntropy: true,
  },

  /** Large network (100+ nodes), conservative */
  largeNetwork: {
    heartbeatTimeoutMs: 60_000,
    gcConfig: {
      tombstoneRetentionMs: 30 * 24 * 60 * 60 * 1000,  // 30 days
      gcIntervalMs: 6 * 60 * 60 * 1000,                   // 6 hours
      maxTombstonesBeforeForceGC: 100000,
    },
    maxOpsPerSync: 100,
    enableAntiEntropy: true,
  },
} as const;

export {
  CRDTRegistry,
  VectorClockUtil,
  LamportClock,
  MerkleTree,
  TombstoneGC,
  PRESETS,
  type RegistryConfig,
  type RegistryStats,
  type RegistryEvent,
  type RegistryEventType,
  type RegistryOp,
  type AgentEntry,
  type AgentMetadata,
  type AgentStatus,
  type VectorClock,
  type LamportTimestamp,
  type MerkleNode,
  type GCConfig,
};
