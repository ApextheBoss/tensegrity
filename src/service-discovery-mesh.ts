/**
 * Service Discovery Mesh for Agent Networks
 * 
 * Decentralized service registration, discovery, and health-aware routing
 * without a central registry. Combines multiple discovery mechanisms:
 * 
 * 1. **Local Registry** — Each node maintains its own service table with TTL-based expiry
 * 2. **Gossip Dissemination** — Service advertisements propagate via epidemic gossip
 * 3. **DNS-SD Style Queries** — Structured service type + attribute matching
 * 4. **Health-Aware Routing** — Integrates liveness/readiness probes into discovery results
 * 5. **Locality-Aware Selection** — Prefers topologically-close instances (zone/region/rack)
 * 6. **Watch/Subscribe** — Reactive notifications when service topology changes
 * 7. **Anti-Entropy Sync** — Periodic full-state reconciliation between peers
 * 8. **Split-Brain Detection** — Identifies divergent registry views across partitions
 * 
 * Design Philosophy:
 * - AP over CP: availability and partition tolerance over strict consistency
 * - Crdt-like merge semantics for concurrent registrations
 * - Bounded memory: LRU eviction with priority retention for critical services
 * - Zero external dependencies: pure TypeScript, no database required
 */

// ============================================================================
// Types & Interfaces
// ============================================================================

interface ServiceInstance {
  /** Globally unique instance ID */
  instanceId: string;
  /** Service type identifier (e.g., "agent.compute", "agent.storage") */
  serviceType: string;
  /** Human-readable service name */
  serviceName: string;
  /** Agent address owning this instance */
  agentAddress: string;
  /** Endpoint URL or connection string */
  endpoint: string;
  /** Service version (semver) */
  version: string;
  /** Arbitrary key-value metadata for attribute-based discovery */
  metadata: Record<string, string>;
  /** Locality tags for proximity-aware routing */
  locality: LocalityInfo;
  /** Registration timestamp (ms) */
  registeredAt: number;
  /** Last heartbeat timestamp (ms) */
  lastHeartbeat: number;
  /** TTL in milliseconds — instance expires if no heartbeat within TTL */
  ttlMs: number;
  /** Lamport clock for crdt merge ordering */
  logicalClock: number;
  /** Current health status */
  health: HealthStatus;
  /** Load metric (0-1, higher = more loaded) */
  load: number;
  /** Priority for LRU eviction (higher = retained longer) */
  priority: number;
  /** Whether this instance was explicitly deregistered (tombstone) */
  tombstoned: boolean;
  /** Tombstone timestamp if deregistered */
  tombstonedAt?: number;
}

interface LocalityInfo {
  region?: string;
  zone?: string;
  rack?: string;
  /** Custom locality labels */
  labels: Record<string, string>;
}

interface HealthStatus {
  alive: boolean;
  ready: boolean;
  /** Last successful health check timestamp */
  lastCheckAt: number;
  /** Consecutive failure count */
  consecutiveFailures: number;
  /** Exponentially-weighted average latency (ms) */
  latencyEwma: number;
  /** Health score 0-1 (composite of alive, ready, latency, failures) */
  score: number;
}

interface ServiceQuery {
  /** Required service type (exact match) */
  serviceType: string;
  /** Optional version constraint (semver range string, simplified) */
  versionConstraint?: string;
  /** Attribute filters — all must match */
  attributes?: Record<string, string>;
  /** Minimum health score threshold */
  minHealthScore?: number;
  /** Maximum load threshold */
  maxLoad?: number;
  /** Preferred locality (closer = better score) */
  preferredLocality?: Partial<LocalityInfo>;
  /** Maximum results to return */
  limit?: number;
  /** Exclude specific instance IDs */
  excludeInstances?: Set<string>;
  /** Only return ready instances */
  readyOnly?: boolean;
}

interface DiscoveryResult {
  instance: ServiceInstance;
  /** Composite score combining health, locality, load, version preference */
  score: number;
  /** Score breakdown */
  breakdown: {
    healthScore: number;
    localityScore: number;
    loadScore: number;
    versionScore: number;
    freshnessScore: number;
  };
}

interface WatchSubscription {
  id: string;
  query: ServiceQuery;
  callback: (event: TopologyChangeEvent) => void;
  createdAt: number;
}

interface TopologyChangeEvent {
  type: 'registered' | 'deregistered' | 'health-changed' | 'updated';
  instance: ServiceInstance;
  previousHealth?: HealthStatus;
  timestamp: number;
}

interface GossipDigest {
  /** instanceId -> logicalClock */
  entries: Map<string, number>;
  /** Sender node ID */
  senderId: string;
  /** Digest generation timestamp */
  generatedAt: number;
}

interface AntiEntropyDelta {
  /** Full instance records that the receiver is missing or behind on */
  updates: ServiceInstance[];
  /** Instance IDs that sender has tombstoned */
  tombstones: string[];
}

interface SplitBrainReport {
  detected: boolean;
  /** Partition groups — sets of node IDs that see different registry states */
  partitions: string[][];
  /** Divergent instance IDs across partitions */
  divergentInstances: string[];
  /** Timestamp of detection */
  detectedAt: number;
}

type MeshEvent =
  | { type: 'instance-registered'; instance: ServiceInstance }
  | { type: 'instance-deregistered'; instanceId: string; reason: string }
  | { type: 'instance-expired'; instanceId: string; lastHeartbeat: number }
  | { type: 'health-changed'; instanceId: string; oldScore: number; newScore: number }
  | { type: 'gossip-sent'; peerId: string; entriesCount: number }
  | { type: 'gossip-received'; peerId: string; updatesApplied: number }
  | { type: 'anti-entropy-sync'; peerId: string; deltaSize: number }
  | { type: 'split-brain-detected'; report: SplitBrainReport }
  | { type: 'eviction'; instanceId: string; reason: string }
  | { type: 'watch-triggered'; subscriptionId: string; eventType: string };

// ============================================================================
// FNV-1a Hash (deterministic tie-breaking)
// ============================================================================

function fnv1a(input: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = (hash * 0x01000193) >>> 0;
  }
  return hash;
}

// ============================================================================
// Simple Semver Utilities
// ============================================================================

function parseSemver(v: string): [number, number, number] | null {
  const m = v.match(/^(\d+)\.(\d+)\.(\d+)/);
  if (!m) return null;
  return [parseInt(m[1]), parseInt(m[2]), parseInt(m[3])];
}

function semverSatisfies(version: string, constraint: string): boolean {
  const v = parseSemver(version);
  if (!v) return false;
  
  // Support: ^1.2.3 (compatible), ~1.2.3 (patch-level), >=1.2.3, exact 1.2.3
  if (constraint.startsWith('^')) {
    const c = parseSemver(constraint.slice(1));
    if (!c) return false;
    if (v[0] !== c[0]) return false;
    if (v[0] === 0) {
      if (v[1] !== c[1]) return false;
      return v[2] >= c[2];
    }
    return v[1] > c[1] || (v[1] === c[1] && v[2] >= c[2]);
  }
  if (constraint.startsWith('~')) {
    const c = parseSemver(constraint.slice(1));
    if (!c) return false;
    return v[0] === c[0] && v[1] === c[1] && v[2] >= c[2];
  }
  if (constraint.startsWith('>=')) {
    const c = parseSemver(constraint.slice(2));
    if (!c) return false;
    if (v[0] !== c[0]) return v[0] > c[0];
    if (v[1] !== c[1]) return v[1] > c[1];
    return v[2] >= c[2];
  }
  // Exact match
  const c = parseSemver(constraint);
  if (!c) return false;
  return v[0] === c[0] && v[1] === c[1] && v[2] === c[2];
}

function semverScore(version: string): number {
  const v = parseSemver(version);
  if (!v) return 0;
  return v[0] * 10000 + v[1] * 100 + v[2];
}

// ============================================================================
// Health Checker
// ============================================================================

class HealthChecker {
  private readonly latencyAlpha: number;
  private readonly unhealthyThreshold: number;
  private readonly degradedThreshold: number;

  constructor(config: {
    latencyAlpha?: number;
    unhealthyThreshold?: number;
    degradedThreshold?: number;
  } = {}) {
    this.latencyAlpha = config.latencyAlpha ?? 0.3;
    this.unhealthyThreshold = config.unhealthyThreshold ?? 5;
    this.degradedThreshold = config.degradedThreshold ?? 2;
  }

  /**
   * Simulate a health probe and update the instance's health status.
   * In production, this would make actual HTTP/gRPC health check calls.
   */
  probe(instance: ServiceInstance, now: number): HealthStatus {
    const age = now - instance.lastHeartbeat;
    const expired = age > instance.ttlMs;
    const stale = age > instance.ttlMs * 0.7;

    // Simulate probe result based on heartbeat freshness
    const alive = !expired;
    const ready = alive && !stale;

    const consecutiveFailures = alive
      ? 0
      : (instance.health.consecutiveFailures + 1);

    // Update latency EWMA (simulate latency proportional to staleness)
    const simulatedLatency = alive ? Math.min(age * 0.1, 5000) : 10000;
    const latencyEwma = instance.health.latencyEwma === 0
      ? simulatedLatency
      : this.latencyAlpha * simulatedLatency + (1 - this.latencyAlpha) * instance.health.latencyEwma;

    // Compute composite health score
    let score = 0;
    if (alive) {
      score += 0.4; // alive base
      if (ready) score += 0.2; // ready bonus
      // Latency component (lower = better, normalized against 5000ms ceiling)
      score += 0.2 * Math.max(0, 1 - latencyEwma / 5000);
      // Freshness component
      score += 0.2 * Math.max(0, 1 - age / instance.ttlMs);
    }

    return {
      alive,
      ready,
      lastCheckAt: now,
      consecutiveFailures,
      latencyEwma,
      score: Math.max(0, Math.min(1, score)),
    };
  }
}

// ============================================================================
// Locality Scorer
// ============================================================================

class LocalityScorer {
  private readonly weights: { region: number; zone: number; rack: number; label: number };

  constructor(weights?: { region?: number; zone?: number; rack?: number; label?: number }) {
    this.weights = {
      region: weights?.region ?? 0.5,
      zone: weights?.zone ?? 0.3,
      rack: weights?.rack ?? 0.15,
      label: weights?.label ?? 0.05,
    };
  }

  /**
   * Score locality proximity. 1.0 = perfect match, 0.0 = completely different.
   */
  score(instance: LocalityInfo, preferred: Partial<LocalityInfo>): number {
    let totalWeight = 0;
    let matchWeight = 0;

    if (preferred.region !== undefined) {
      totalWeight += this.weights.region;
      if (instance.region === preferred.region) matchWeight += this.weights.region;
    }
    if (preferred.zone !== undefined) {
      totalWeight += this.weights.zone;
      if (instance.zone === preferred.zone) matchWeight += this.weights.zone;
    }
    if (preferred.rack !== undefined) {
      totalWeight += this.weights.rack;
      if (instance.rack === preferred.rack) matchWeight += this.weights.rack;
    }
    if (preferred.labels) {
      for (const [k, v] of Object.entries(preferred.labels)) {
        totalWeight += this.weights.label;
        if (instance.labels[k] === v) matchWeight += this.weights.label;
      }
    }

    return totalWeight === 0 ? 0.5 : matchWeight / totalWeight;
  }
}

// ============================================================================
// Local Service Registry
// ============================================================================

class LocalRegistry {
  private instances: Map<string, ServiceInstance> = new Map();
  private readonly maxInstances: number;
  private readonly tombstoneRetentionMs: number;
  private logicalClock: number = 0;

  constructor(config: { maxInstances?: number; tombstoneRetentionMs?: number } = {}) {
    this.maxInstances = config.maxInstances ?? 10000;
    this.tombstoneRetentionMs = config.tombstoneRetentionMs ?? 300_000; // 5 min
  }

  get size(): number {
    return this.instances.size;
  }

  get activeCount(): number {
    let count = 0;
    for (const inst of this.instances.values()) {
      if (!inst.tombstoned) count++;
    }
    return count;
  }

  nextClock(): number {
    return ++this.logicalClock;
  }

  advanceClock(remote: number): void {
    this.logicalClock = Math.max(this.logicalClock, remote) + 1;
  }

  register(instance: ServiceInstance): { applied: boolean; evicted?: string } {
    const existing = this.instances.get(instance.instanceId);
    if (existing && existing.logicalClock >= instance.logicalClock) {
      return { applied: false }; // stale update, reject
    }

    let evicted: string | undefined;
    // Evict if at capacity
    if (!existing && this.instances.size >= this.maxInstances) {
      evicted = this.evictOne(instance.priority);
      if (!evicted) return { applied: false }; // can't evict anything lower priority
    }

    instance.logicalClock = instance.logicalClock || this.nextClock();
    this.instances.set(instance.instanceId, { ...instance });
    return { applied: true, evicted };
  }

  deregister(instanceId: string, now: number): ServiceInstance | null {
    const inst = this.instances.get(instanceId);
    if (!inst || inst.tombstoned) return null;

    inst.tombstoned = true;
    inst.tombstonedAt = now;
    inst.logicalClock = this.nextClock();
    inst.health.alive = false;
    inst.health.ready = false;
    inst.health.score = 0;
    return inst;
  }

  heartbeat(instanceId: string, now: number, load?: number): boolean {
    const inst = this.instances.get(instanceId);
    if (!inst || inst.tombstoned) return false;

    inst.lastHeartbeat = now;
    inst.logicalClock = this.nextClock();
    if (load !== undefined) inst.load = Math.max(0, Math.min(1, load));
    return true;
  }

  get(instanceId: string): ServiceInstance | undefined {
    return this.instances.get(instanceId);
  }

  getAll(): ServiceInstance[] {
    return Array.from(this.instances.values());
  }

  getActive(): ServiceInstance[] {
    return this.getAll().filter(i => !i.tombstoned);
  }

  getByType(serviceType: string): ServiceInstance[] {
    const results: ServiceInstance[] = [];
    for (const inst of this.instances.values()) {
      if (inst.serviceType === serviceType && !inst.tombstoned) {
        results.push(inst);
      }
    }
    return results;
  }

  /** Merge a remote instance using last-writer-wins on logical clock */
  merge(remote: ServiceInstance): boolean {
    const local = this.instances.get(remote.instanceId);
    if (local) {
      if (remote.logicalClock <= local.logicalClock) return false;
      this.advanceClock(remote.logicalClock);
    } else {
      this.advanceClock(remote.logicalClock);
      // Check capacity
      if (this.instances.size >= this.maxInstances) {
        const evicted = this.evictOne(remote.priority);
        if (!evicted) return false;
      }
    }
    this.instances.set(remote.instanceId, { ...remote });
    return true;
  }

  /** Build gossip digest — instanceId -> logicalClock */
  buildDigest(): Map<string, number> {
    const digest = new Map<string, number>();
    for (const [id, inst] of this.instances) {
      digest.set(id, inst.logicalClock);
    }
    return digest;
  }

  /** Compute delta: instances where local clock > remote clock */
  computeDelta(remoteDigest: Map<string, number>): AntiEntropyDelta {
    const updates: ServiceInstance[] = [];
    const tombstones: string[] = [];

    for (const [id, inst] of this.instances) {
      const remoteClock = remoteDigest.get(id) ?? -1;
      if (inst.logicalClock > remoteClock) {
        if (inst.tombstoned) {
          tombstones.push(id);
        } else {
          updates.push({ ...inst });
        }
      }
    }

    return { updates, tombstones };
  }

  /** Clean up expired tombstones and expired (no-heartbeat) instances */
  gc(now: number): string[] {
    const removed: string[] = [];
    for (const [id, inst] of this.instances) {
      if (inst.tombstoned && inst.tombstonedAt &&
          now - inst.tombstonedAt > this.tombstoneRetentionMs) {
        this.instances.delete(id);
        removed.push(id);
      }
    }
    return removed;
  }

  /** Find expired (heartbeat timeout) instances */
  findExpired(now: number): ServiceInstance[] {
    const expired: ServiceInstance[] = [];
    for (const inst of this.instances.values()) {
      if (!inst.tombstoned && now - inst.lastHeartbeat > inst.ttlMs) {
        expired.push(inst);
      }
    }
    return expired;
  }

  /** Evict lowest-priority, oldest instance. Returns evicted ID or null */
  private evictOne(incomingPriority: number): string | undefined {
    let victim: ServiceInstance | null = null;
    for (const inst of this.instances.values()) {
      // Only evict tombstoned or lower-priority instances
      if (inst.tombstoned) {
        this.instances.delete(inst.instanceId);
        return inst.instanceId;
      }
      if (inst.priority < incomingPriority) {
        if (!victim || inst.priority < victim.priority ||
            (inst.priority === victim.priority && inst.lastHeartbeat < victim.lastHeartbeat)) {
          victim = inst;
        }
      }
    }
    if (victim) {
      this.instances.delete(victim.instanceId);
      return victim.instanceId;
    }
    return undefined;
  }
}

// ============================================================================
// Query Engine
// ============================================================================

class QueryEngine {
  private readonly localityScorer: LocalityScorer;
  private readonly weights: {
    health: number;
    locality: number;
    load: number;
    version: number;
    freshness: number;
  };

  constructor(config: {
    localityScorer?: LocalityScorer;
    weights?: { health?: number; locality?: number; load?: number; version?: number; freshness?: number };
  } = {}) {
    this.localityScorer = config.localityScorer ?? new LocalityScorer();
    this.weights = {
      health: config.weights?.health ?? 0.35,
      locality: config.weights?.locality ?? 0.20,
      load: config.weights?.load ?? 0.25,
      version: config.weights?.version ?? 0.10,
      freshness: config.weights?.freshness ?? 0.10,
    };
  }

  query(registry: LocalRegistry, query: ServiceQuery, now: number): DiscoveryResult[] {
    const candidates = registry.getByType(query.serviceType);
    const results: DiscoveryResult[] = [];
    
    // Find max version for normalization
    let maxVersion = 0;
    for (const inst of candidates) {
      maxVersion = Math.max(maxVersion, semverScore(inst.version));
    }

    for (const inst of candidates) {
      // Hard filters
      if (query.readyOnly && !inst.health.ready) continue;
      if (query.minHealthScore !== undefined && inst.health.score < query.minHealthScore) continue;
      if (query.maxLoad !== undefined && inst.load > query.maxLoad) continue;
      if (query.excludeInstances?.has(inst.instanceId)) continue;
      if (query.versionConstraint && !semverSatisfies(inst.version, query.versionConstraint)) continue;
      
      // Attribute matching
      if (query.attributes) {
        let attrMatch = true;
        for (const [k, v] of Object.entries(query.attributes)) {
          if (inst.metadata[k] !== v) { attrMatch = false; break; }
        }
        if (!attrMatch) continue;
      }

      // Soft scoring
      const healthScore = inst.health.score;
      
      const localityScore = query.preferredLocality
        ? this.localityScorer.score(inst.locality, query.preferredLocality)
        : 0.5;
      
      const loadScore = 1 - inst.load;
      
      const versionScore = maxVersion > 0 ? semverScore(inst.version) / maxVersion : 0.5;
      
      const age = now - inst.lastHeartbeat;
      const freshnessScore = Math.max(0, 1 - age / inst.ttlMs);

      const compositeScore =
        this.weights.health * healthScore +
        this.weights.locality * localityScore +
        this.weights.load * loadScore +
        this.weights.version * versionScore +
        this.weights.freshness * freshnessScore;

      results.push({
        instance: inst,
        score: compositeScore,
        breakdown: { healthScore, localityScore, loadScore, versionScore, freshnessScore },
      });
    }

    // Sort by score descending, tie-break with FNV-1a hash
    results.sort((a, b) => {
      const diff = b.score - a.score;
      if (Math.abs(diff) > 0.0001) return diff;
      return fnv1a(a.instance.instanceId) - fnv1a(b.instance.instanceId);
    });

    const limit = query.limit ?? results.length;
    return results.slice(0, limit);
  }
}

// ============================================================================
// Watch Manager
// ============================================================================

class WatchManager {
  private subscriptions: Map<string, WatchSubscription> = new Map();
  private nextId: number = 0;

  subscribe(query: ServiceQuery, callback: (event: TopologyChangeEvent) => void): string {
    const id = `watch-${++this.nextId}`;
    this.subscriptions.set(id, {
      id,
      query,
      callback,
      createdAt: Date.now(),
    });
    return id;
  }

  unsubscribe(id: string): boolean {
    return this.subscriptions.delete(id);
  }

  notify(event: TopologyChangeEvent): string[] {
    const triggered: string[] = [];
    for (const sub of this.subscriptions.values()) {
      if (this.matchesQuery(event.instance, sub.query)) {
        try {
          sub.callback(event);
          triggered.push(sub.id);
        } catch {
          // Swallow callback errors
        }
      }
    }
    return triggered;
  }

  private matchesQuery(instance: ServiceInstance, query: ServiceQuery): boolean {
    if (instance.serviceType !== query.serviceType) return false;
    if (query.versionConstraint && !semverSatisfies(instance.version, query.versionConstraint)) return false;
    if (query.attributes) {
      for (const [k, v] of Object.entries(query.attributes)) {
        if (instance.metadata[k] !== v) return false;
      }
    }
    return true;
  }
}

// ============================================================================
// Gossip Disseminator
// ============================================================================

class GossipDisseminator {
  private readonly fanout: number;
  private readonly peers: Set<string> = new Set();
  private roundRobinIndex: number = 0;

  constructor(config: { fanout?: number } = {}) {
    this.fanout = config.fanout ?? 3;
  }

  addPeer(peerId: string): void {
    this.peers.add(peerId);
  }

  removePeer(peerId: string): void {
    this.peers.delete(peerId);
  }

  /** Select peers for this gossip round */
  selectPeers(): string[] {
    const peerList = Array.from(this.peers);
    if (peerList.length <= this.fanout) return peerList;

    // Shuffle with Fisher-Yates then take fanout count
    // Use round-robin offset to ensure coverage over multiple rounds
    const selected: string[] = [];
    const indices = new Set<number>();
    let attempts = 0;
    while (selected.length < this.fanout && attempts < peerList.length * 2) {
      const idx = (this.roundRobinIndex + attempts) % peerList.length;
      if (!indices.has(idx)) {
        indices.add(idx);
        selected.push(peerList[idx]);
      }
      attempts++;
    }
    this.roundRobinIndex = (this.roundRobinIndex + this.fanout) % Math.max(1, peerList.length);
    return selected;
  }

  /** Build digest for gossip exchange */
  buildDigest(registry: LocalRegistry, nodeId: string): GossipDigest {
    return {
      entries: registry.buildDigest(),
      senderId: nodeId,
      generatedAt: Date.now(),
    };
  }

  /** Process incoming digest, return delta to send back */
  processDigest(registry: LocalRegistry, remoteDigest: GossipDigest): AntiEntropyDelta {
    return registry.computeDelta(remoteDigest.entries);
  }

  /** Apply received delta to local registry */
  applyDelta(registry: LocalRegistry, delta: AntiEntropyDelta, now: number): number {
    let applied = 0;
    for (const update of delta.updates) {
      if (registry.merge(update)) applied++;
    }
    for (const tombstoneId of delta.tombstones) {
      const inst = registry.get(tombstoneId);
      if (inst && !inst.tombstoned) {
        registry.deregister(tombstoneId, now);
        applied++;
      }
    }
    return applied;
  }
}

// ============================================================================
// Split-Brain Detector
// ============================================================================

class SplitBrainDetector {
  private peerDigests: Map<string, Map<string, number>> = new Map();
  private readonly divergenceThreshold: number;

  constructor(config: { divergenceThreshold?: number } = {}) {
    this.divergenceThreshold = config.divergenceThreshold ?? 0.15;
  }

  /** Record a peer's digest snapshot for comparison */
  recordPeerDigest(peerId: string, digest: Map<string, number>): void {
    this.peerDigests.set(peerId, new Map(digest));
  }

  /** Detect split-brain by comparing registry views across peers */
  detect(localDigest: Map<string, number>, now: number): SplitBrainReport {
    if (this.peerDigests.size === 0) {
      return { detected: false, partitions: [], divergentInstances: [], detectedAt: now };
    }

    // Build adjacency: peers are "connected" if their digests are similar enough
    const allNodes = ['local', ...this.peerDigests.keys()];
    const allDigests = new Map<string, Map<string, number>>();
    allDigests.set('local', localDigest);
    for (const [pid, d] of this.peerDigests) allDigests.set(pid, d);

    // Compute divergence between each pair
    const groups: string[][] = [];
    const visited = new Set<string>();

    for (const node of allNodes) {
      if (visited.has(node)) continue;
      // BFS to find all nodes in this partition
      const group: string[] = [];
      const queue = [node];
      visited.add(node);
      while (queue.length > 0) {
        const current = queue.shift()!;
        group.push(current);
        for (const other of allNodes) {
          if (visited.has(other)) continue;
          const divergence = this.computeDivergence(
            allDigests.get(current)!,
            allDigests.get(other)!
          );
          if (divergence < this.divergenceThreshold) {
            visited.add(other);
            queue.push(other);
          }
        }
      }
      groups.push(group);
    }

    const detected = groups.length > 1;

    // Find divergent instances
    const divergentInstances: Set<string> = new Set();
    if (detected) {
      const allInstanceIds = new Set<string>();
      for (const d of allDigests.values()) {
        for (const id of d.keys()) allInstanceIds.add(id);
      }
      for (const id of allInstanceIds) {
        const clocks = new Set<number>();
        for (const d of allDigests.values()) {
          clocks.add(d.get(id) ?? -1);
        }
        if (clocks.size > 1) divergentInstances.add(id);
      }
    }

    return {
      detected,
      partitions: groups,
      divergentInstances: Array.from(divergentInstances),
      detectedAt: now,
    };
  }

  private computeDivergence(a: Map<string, number>, b: Map<string, number>): number {
    const allKeys = new Set([...a.keys(), ...b.keys()]);
    if (allKeys.size === 0) return 0;
    let mismatches = 0;
    for (const key of allKeys) {
      if ((a.get(key) ?? -1) !== (b.get(key) ?? -1)) mismatches++;
    }
    return mismatches / allKeys.size;
  }

  clearPeerData(peerId: string): void {
    this.peerDigests.delete(peerId);
  }
}

// ============================================================================
// Service Discovery Mesh (Main Orchestrator)
// ============================================================================

interface MeshConfig {
  nodeId: string;
  /** Max instances in local registry */
  maxInstances?: number;
  /** How often to run health checks (ms) */
  healthCheckIntervalMs?: number;
  /** How often to gossip (ms) */
  gossipIntervalMs?: number;
  /** How often to run anti-entropy sync (ms) */
  antiEntropyIntervalMs?: number;
  /** How often to run GC (ms) */
  gcIntervalMs?: number;
  /** Gossip fanout per round */
  gossipFanout?: number;
  /** Default TTL for registered services (ms) */
  defaultTtlMs?: number;
  /** Tombstone retention period (ms) */
  tombstoneRetentionMs?: number;
  /** Health check config */
  healthConfig?: { latencyAlpha?: number; unhealthyThreshold?: number; degradedThreshold?: number };
  /** Query scoring weights */
  queryWeights?: { health?: number; locality?: number; load?: number; version?: number; freshness?: number };
  /** Locality scoring weights */
  localityWeights?: { region?: number; zone?: number; rack?: number; label?: number };
  /** Split-brain divergence threshold */
  splitBrainThreshold?: number;
}

class ServiceDiscoveryMesh {
  readonly nodeId: string;
  private readonly registry: LocalRegistry;
  private readonly queryEngine: QueryEngine;
  private readonly healthChecker: HealthChecker;
  private readonly watchManager: WatchManager;
  private readonly gossipDisseminator: GossipDisseminator;
  private readonly splitBrainDetector: SplitBrainDetector;
  private readonly config: Required<Pick<MeshConfig,
    'healthCheckIntervalMs' | 'gossipIntervalMs' | 'antiEntropyIntervalMs' |
    'gcIntervalMs' | 'defaultTtlMs'>>;
  private readonly eventLog: MeshEvent[] = [];
  private readonly maxEventLog: number = 1000;
  private tickCount: number = 0;

  constructor(meshConfig: MeshConfig) {
    this.nodeId = meshConfig.nodeId;
    this.config = {
      healthCheckIntervalMs: meshConfig.healthCheckIntervalMs ?? 10_000,
      gossipIntervalMs: meshConfig.gossipIntervalMs ?? 5_000,
      antiEntropyIntervalMs: meshConfig.antiEntropyIntervalMs ?? 30_000,
      gcIntervalMs: meshConfig.gcIntervalMs ?? 60_000,
      defaultTtlMs: meshConfig.defaultTtlMs ?? 30_000,
    };

    this.registry = new LocalRegistry({
      maxInstances: meshConfig.maxInstances,
      tombstoneRetentionMs: meshConfig.tombstoneRetentionMs,
    });
    this.healthChecker = new HealthChecker(meshConfig.healthConfig);
    this.queryEngine = new QueryEngine({
      localityScorer: new LocalityScorer(meshConfig.localityWeights),
      weights: meshConfig.queryWeights,
    });
    this.watchManager = new WatchManager();
    this.gossipDisseminator = new GossipDisseminator({ fanout: meshConfig.gossipFanout });
    this.splitBrainDetector = new SplitBrainDetector({
      divergenceThreshold: meshConfig.splitBrainThreshold,
    });
  }

  // --- Registration API ---

  register(params: {
    instanceId: string;
    serviceType: string;
    serviceName: string;
    agentAddress: string;
    endpoint: string;
    version: string;
    metadata?: Record<string, string>;
    locality?: Partial<LocalityInfo>;
    ttlMs?: number;
    priority?: number;
  }): boolean {
    const now = Date.now();
    const instance: ServiceInstance = {
      instanceId: params.instanceId,
      serviceType: params.serviceType,
      serviceName: params.serviceName,
      agentAddress: params.agentAddress,
      endpoint: params.endpoint,
      version: params.version,
      metadata: params.metadata ?? {},
      locality: {
        region: params.locality?.region,
        zone: params.locality?.zone,
        rack: params.locality?.rack,
        labels: params.locality?.labels ?? {},
      },
      registeredAt: now,
      lastHeartbeat: now,
      ttlMs: params.ttlMs ?? this.config.defaultTtlMs,
      logicalClock: this.registry.nextClock(),
      health: { alive: true, ready: true, lastCheckAt: now, consecutiveFailures: 0, latencyEwma: 0, score: 1.0 },
      load: 0,
      priority: params.priority ?? 5,
      tombstoned: false,
    };

    const result = this.registry.register(instance);
    if (result.applied) {
      this.emit({ type: 'instance-registered', instance });
      if (result.evicted) {
        this.emit({ type: 'eviction', instanceId: result.evicted, reason: 'capacity' });
      }
      this.watchManager.notify({
        type: 'registered',
        instance,
        timestamp: now,
      });
    }
    return result.applied;
  }

  deregister(instanceId: string): boolean {
    const now = Date.now();
    const inst = this.registry.deregister(instanceId, now);
    if (inst) {
      this.emit({ type: 'instance-deregistered', instanceId, reason: 'explicit' });
      this.watchManager.notify({ type: 'deregistered', instance: inst, timestamp: now });
      return true;
    }
    return false;
  }

  heartbeat(instanceId: string, load?: number): boolean {
    return this.registry.heartbeat(instanceId, Date.now(), load);
  }

  // --- Discovery API ---

  discover(query: ServiceQuery): DiscoveryResult[] {
    return this.queryEngine.query(this.registry, query, Date.now());
  }

  /** Discover the single best instance for a service type */
  resolveOne(serviceType: string, preferredLocality?: Partial<LocalityInfo>): DiscoveryResult | null {
    const results = this.discover({
      serviceType,
      readyOnly: true,
      preferredLocality,
      limit: 1,
    });
    return results[0] ?? null;
  }

  /** Get all instances of a service type (unscored) */
  listInstances(serviceType: string): ServiceInstance[] {
    return this.registry.getByType(serviceType);
  }

  // --- Watch API ---

  watch(query: ServiceQuery, callback: (event: TopologyChangeEvent) => void): string {
    return this.watchManager.subscribe(query, callback);
  }

  unwatch(subscriptionId: string): boolean {
    return this.watchManager.unsubscribe(subscriptionId);
  }

  // --- Gossip API ---

  addPeer(peerId: string): void {
    this.gossipDisseminator.addPeer(peerId);
  }

  removePeer(peerId: string): void {
    this.gossipDisseminator.removePeer(peerId);
    this.splitBrainDetector.clearPeerData(peerId);
  }

  /** Initiate a gossip round — returns digest to send to selected peers */
  initiateGossip(): { peers: string[]; digest: GossipDigest } {
    const peers = this.gossipDisseminator.selectPeers();
    const digest = this.gossipDisseminator.buildDigest(this.registry, this.nodeId);
    for (const p of peers) {
      this.emit({ type: 'gossip-sent', peerId: p, entriesCount: digest.entries.size });
    }
    return { peers, digest };
  }

  /** Handle incoming gossip digest — returns delta to send back */
  handleGossipDigest(digest: GossipDigest): AntiEntropyDelta {
    // Record for split-brain detection
    this.splitBrainDetector.recordPeerDigest(digest.senderId, digest.entries);
    return this.gossipDisseminator.processDigest(this.registry, digest);
  }

  /** Apply received delta from a gossip exchange */
  applyGossipDelta(delta: AntiEntropyDelta, peerId: string): number {
    const now = Date.now();
    const applied = this.gossipDisseminator.applyDelta(this.registry, delta, now);
    this.emit({ type: 'gossip-received', peerId, updatesApplied: applied });

    // Notify watchers for each applied update
    for (const update of delta.updates) {
      this.watchManager.notify({ type: 'updated', instance: update, timestamp: now });
    }
    return applied;
  }

  // --- Tick (periodic maintenance) ---

  tick(now?: number): {
    healthChecks: number;
    expired: number;
    gcCleaned: number;
    splitBrain: SplitBrainReport | null;
  } {
    const t = now ?? Date.now();
    this.tickCount++;
    let healthChecks = 0;
    let expired = 0;
    let gcCleaned = 0;
    let splitBrain: SplitBrainReport | null = null;

    // Health checks
    for (const inst of this.registry.getActive()) {
      const oldScore = inst.health.score;
      inst.health = this.healthChecker.probe(inst, t);
      healthChecks++;

      if (Math.abs(inst.health.score - oldScore) > 0.1) {
        this.emit({ type: 'health-changed', instanceId: inst.instanceId, oldScore, newScore: inst.health.score });
        this.watchManager.notify({
          type: 'health-changed',
          instance: inst,
          previousHealth: { ...inst.health, score: oldScore } as HealthStatus,
          timestamp: t,
        });
      }
    }

    // Expire instances that missed heartbeats
    const expiredInstances = this.registry.findExpired(t);
    for (const inst of expiredInstances) {
      this.registry.deregister(inst.instanceId, t);
      this.emit({ type: 'instance-expired', instanceId: inst.instanceId, lastHeartbeat: inst.lastHeartbeat });
      this.watchManager.notify({ type: 'deregistered', instance: inst, timestamp: t });
      expired++;
    }

    // GC tombstones
    const cleaned = this.registry.gc(t);
    gcCleaned = cleaned.length;

    // Split-brain check every 5 ticks
    if (this.tickCount % 5 === 0) {
      const localDigest = this.registry.buildDigest();
      const report = this.splitBrainDetector.detect(localDigest, t);
      if (report.detected) {
        this.emit({ type: 'split-brain-detected', report });
        splitBrain = report;
      }
    }

    return { healthChecks, expired, gcCleaned, splitBrain };
  }

  // --- Stats ---

  stats(): {
    totalInstances: number;
    activeInstances: number;
    serviceTypes: number;
    tickCount: number;
    eventCount: number;
  } {
    const active = this.registry.getActive();
    const types = new Set(active.map(i => i.serviceType));
    return {
      totalInstances: this.registry.size,
      activeInstances: active.length,
      serviceTypes: types.size,
      tickCount: this.tickCount,
      eventCount: this.eventLog.length,
    };
  }

  getEvents(limit?: number): MeshEvent[] {
    const n = limit ?? this.eventLog.length;
    return this.eventLog.slice(-n);
  }

  private emit(event: MeshEvent): void {
    this.eventLog.push(event);
    if (this.eventLog.length > this.maxEventLog) {
      this.eventLog.splice(0, this.eventLog.length - this.maxEventLog);
    }
  }
}

// ============================================================================
// Presets
// ============================================================================

const PRESETS = {
  /** Small cluster (<50 agents), low latency, aggressive health checks */
  'small-cluster': {
    maxInstances: 500,
    healthCheckIntervalMs: 5_000,
    gossipIntervalMs: 3_000,
    antiEntropyIntervalMs: 15_000,
    gcIntervalMs: 30_000,
    defaultTtlMs: 15_000,
    gossipFanout: 2,
    tombstoneRetentionMs: 120_000,
    splitBrainThreshold: 0.1,
  },
  /** Medium network (50-500 agents), balanced */
  'medium-network': {
    maxInstances: 5_000,
    healthCheckIntervalMs: 10_000,
    gossipIntervalMs: 5_000,
    antiEntropyIntervalMs: 30_000,
    gcIntervalMs: 60_000,
    defaultTtlMs: 30_000,
    gossipFanout: 3,
    tombstoneRetentionMs: 300_000,
    splitBrainThreshold: 0.15,
  },
  /** Large federation (500+ agents), conservative, minimize overhead */
  'large-federation': {
    maxInstances: 50_000,
    healthCheckIntervalMs: 30_000,
    gossipIntervalMs: 15_000,
    antiEntropyIntervalMs: 120_000,
    gcIntervalMs: 300_000,
    defaultTtlMs: 90_000,
    gossipFanout: 5,
    tombstoneRetentionMs: 600_000,
    splitBrainThreshold: 0.2,
  },
} as const;

function createMesh(nodeId: string, preset: keyof typeof PRESETS): ServiceDiscoveryMesh {
  return new ServiceDiscoveryMesh({ nodeId, ...PRESETS[preset] });
}

// ============================================================================
// Exports
// ============================================================================

export {
  ServiceDiscoveryMesh,
  LocalRegistry,
  QueryEngine,
  HealthChecker,
  LocalityScorer,
  WatchManager,
  GossipDisseminator,
  SplitBrainDetector,
  createMesh,
  PRESETS,
  // Types
  ServiceInstance,
  ServiceQuery,
  DiscoveryResult,
  TopologyChangeEvent,
  WatchSubscription,
  GossipDigest,
  AntiEntropyDelta,
  SplitBrainReport,
  MeshEvent,
  MeshConfig,
  HealthStatus,
  LocalityInfo,
};
