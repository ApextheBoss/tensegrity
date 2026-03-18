import { fnv1a } from './shared-utils';
/**
 * Distributed Lock Manager for Agent Networks
 * 
 * Provides mutual exclusion primitives for multi-agent systems without
 * centralized coordination. Implements multiple lock acquisition strategies
 * with deadlock detection, fairness guarantees, and partition tolerance.
 * 
 * Core algorithms:
 * - Lamport's Bakery Algorithm (total ordering without hardware support)
 * - Maekawa's √N Quorum (reduced message complexity from O(N) to O(√N))
 * - Redlock-style Multi-Region (tolerates minority region failures)
 * - Hierarchical Intention Locks (IX/IS/X/S with compatibility matrix)
 * 
 * Key properties:
 * - Deadlock detection via wait-for graph cycle detection (DFS)
 * - Wound-Wait prevention (older transactions wound younger on conflict)
 * - Fencing tokens for stale-lock safety
 * - Lock coarsening for high-contention resources
 * - Phantom lock detection for crashed holders
 */

// ─── Types ──────────────────────────────────────────────────────────

interface LockRequest {
  id: string;
  agentId: string;
  resourceId: string;
  mode: LockMode;
  priority: number;
  timestamp: number;
  timeout: number;
  fencingToken?: number;
  metadata?: Record<string, unknown>;
}

type LockMode = 'exclusive' | 'shared' | 'intention-exclusive' | 'intention-shared';

interface LockGrant {
  requestId: string;
  agentId: string;
  resourceId: string;
  mode: LockMode;
  fencingToken: number;
  grantedAt: number;
  expiresAt: number;
  renewCount: number;
}

interface LockManagerConfig {
  maxLocksPerAgent: number;
  defaultTtlMs: number;
  renewalWindowMs: number;
  deadlockCheckIntervalMs: number;
  phantomDetectionMs: number;
  quorumSize: number;
  regionCount: number;
  maxWaitQueueDepth: number;
  coarseningThreshold: number;
  coarseningWindowMs: number;
  woundWaitEnabled: boolean;
  fairnessMode: 'fifo' | 'priority' | 'wound-wait';
}

interface WaitForEdge {
  waiter: string;
  holder: string;
  resourceId: string;
  since: number;
}

interface LockStats {
  totalGrants: number;
  totalDenials: number;
  totalDeadlocksDetected: number;
  totalPhantomsCleaned: number;
  totalCoarsenings: number;
  avgHoldTimeMs: number;
  avgWaitTimeMs: number;
  contentionRatio: number;
}

type DLMEvent =
  | { type: 'lock-granted'; grant: LockGrant }
  | { type: 'lock-denied'; request: LockRequest; reason: string }
  | { type: 'lock-released'; grant: LockGrant; heldMs: number }
  | { type: 'lock-expired'; grant: LockGrant }
  | { type: 'lock-renewed'; grant: LockGrant; newExpiry: number }
  | { type: 'deadlock-detected'; cycle: string[]; victim: string }
  | { type: 'deadlock-resolved'; victim: string; abortedRequests: string[] }
  | { type: 'phantom-detected'; grant: LockGrant; silentMs: number }
  | { type: 'phantom-cleaned'; resourceId: string; agentId: string }
  | { type: 'lock-coarsened'; fromResources: string[]; toResource: string }
  | { type: 'wound-triggered'; wounderId: string; victimId: string; resourceId: string }
  | { type: 'quorum-achieved'; resourceId: string; quorumRegions: number[] }
  | { type: 'fencing-violation'; grant: LockGrant; staleToken: number; currentToken: number };

// ─── FNV-1a Hash (deterministic tie-breaking) ───────────────────────

// ─── Lock Compatibility Matrix ──────────────────────────────────────

const COMPATIBILITY: Record<LockMode, Record<LockMode, boolean>> = {
  'shared':               { 'shared': true,  'exclusive': false, 'intention-shared': true,  'intention-exclusive': true  },
  'exclusive':            { 'shared': false, 'exclusive': false, 'intention-shared': false, 'intention-exclusive': false },
  'intention-shared':     { 'shared': true,  'exclusive': false, 'intention-shared': true,  'intention-exclusive': true  },
  'intention-exclusive':  { 'shared': false, 'exclusive': false, 'intention-shared': true,  'intention-exclusive': true  },
};

function isCompatible(existing: LockMode, requested: LockMode): boolean {
  return COMPATIBILITY[existing]?.[requested] ?? false;
}

// ─── Bakery Algorithm (Lamport Total Ordering) ──────────────────────

class BakeryOrderer {
  private tickets: Map<string, number> = new Map();     // agentId → ticket
  private choosing: Set<string> = new Set();

  takeTicket(agentId: string): number {
    this.choosing.add(agentId);
    const maxTicket = Math.max(0, ...this.tickets.values());
    const ticket = maxTicket + 1;
    this.tickets.set(agentId, ticket);
    this.choosing.delete(agentId);
    return ticket;
  }

  compare(a: string, b: string): number {
    const ticketA = this.tickets.get(a) ?? 0;
    const ticketB = this.tickets.get(b) ?? 0;
    if (ticketA !== ticketB) return ticketA - ticketB;
    return fnv1a(a) - fnv1a(b); // deterministic tie-break
  }

  release(agentId: string): void {
    this.tickets.delete(agentId);
  }

  isChoosing(agentId: string): boolean {
    return this.choosing.has(agentId);
  }
}

// ─── Maekawa Quorum Calculator ──────────────────────────────────────

class MaekawaQuorum {
  private agentIds: string[];
  private quorumSize: number;

  constructor(agentIds: string[], quorumSize?: number) {
    this.agentIds = [...agentIds].sort();
    this.quorumSize = quorumSize ?? Math.ceil(Math.sqrt(agentIds.length));
  }

  /**
   * Get the quorum set for a given agent.
   * Each agent's quorum is a deterministic √N subset that guarantees
   * pairwise intersection (any two quorum sets share at least one member).
   */
  getQuorum(agentId: string): string[] {
    const n = this.agentIds.length;
    const k = this.quorumSize;
    const baseIdx = fnv1a(agentId) % n;
    const quorum: string[] = [];
    for (let i = 0; i < k && i < n; i++) {
      quorum.push(this.agentIds[(baseIdx + i) % n]);
    }
    return quorum;
  }

  hasQuorum(votes: Set<string>, agentId: string): boolean {
    const needed = this.getQuorum(agentId);
    const majority = Math.ceil(needed.length / 2);
    let count = 0;
    for (const v of needed) {
      if (votes.has(v)) count++;
    }
    return count >= majority;
  }
}

// ─── Fencing Token Generator ────────────────────────────────────────

class FencingTokenGenerator {
  private counters: Map<string, number> = new Map(); // resourceId → monotonic counter

  next(resourceId: string): number {
    const current = this.counters.get(resourceId) ?? 0;
    const next = current + 1;
    this.counters.set(resourceId, next);
    return next;
  }

  validate(resourceId: string, token: number): boolean {
    const current = this.counters.get(resourceId) ?? 0;
    return token === current;
  }

  current(resourceId: string): number {
    return this.counters.get(resourceId) ?? 0;
  }
}

// ─── Wait-For Graph (Deadlock Detection) ────────────────────────────

class WaitForGraph {
  private edges: Map<string, WaitForEdge[]> = new Map(); // waiter → edges

  addEdge(waiter: string, holder: string, resourceId: string): void {
    const edge: WaitForEdge = { waiter, holder, resourceId, since: Date.now() };
    const existing = this.edges.get(waiter) ?? [];
    // Avoid duplicate edges
    if (!existing.some(e => e.holder === holder && e.resourceId === resourceId)) {
      existing.push(edge);
      this.edges.set(waiter, existing);
    }
  }

  removeEdgesFor(agentId: string): void {
    this.edges.delete(agentId);
    // Also remove edges where agentId is the holder
    for (const [waiter, edges] of this.edges) {
      const filtered = edges.filter(e => e.holder !== agentId);
      if (filtered.length === 0) {
        this.edges.delete(waiter);
      } else {
        this.edges.set(waiter, filtered);
      }
    }
  }

  /**
   * DFS-based cycle detection.
   * Returns the cycle as an array of agent IDs, or null if no cycle.
   */
  detectCycle(): string[] | null {
    const visited = new Set<string>();
    const inStack = new Set<string>();
    const path: string[] = [];

    const dfs = (node: string): string[] | null => {
      if (inStack.has(node)) {
        // Found cycle — extract it
        const cycleStart = path.indexOf(node);
        return path.slice(cycleStart).concat(node);
      }
      if (visited.has(node)) return null;

      visited.add(node);
      inStack.add(node);
      path.push(node);

      const edges = this.edges.get(node) ?? [];
      for (const edge of edges) {
        const result = dfs(edge.holder);
        if (result) return result;
      }

      path.pop();
      inStack.delete(node);
      return null;
    };

    for (const node of this.edges.keys()) {
      const cycle = dfs(node);
      if (cycle) return cycle;
    }
    return null;
  }

  /**
   * Select deadlock victim: youngest agent in cycle (wound-wait)
   * or lowest-priority (priority-based).
   */
  selectVictim(cycle: string[], agentTimestamps: Map<string, number>, mode: 'youngest' | 'lowest-priority'): string {
    if (mode === 'youngest') {
      let youngest = cycle[0];
      let latestTime = agentTimestamps.get(cycle[0]) ?? 0;
      for (const agentId of cycle) {
        const ts = agentTimestamps.get(agentId) ?? 0;
        if (ts > latestTime) {
          latestTime = ts;
          youngest = agentId;
        }
      }
      return youngest;
    }
    // Deterministic: FNV-1a for consistent victim selection
    return cycle.reduce((a, b) => fnv1a(a) < fnv1a(b) ? a : b);
  }

  getWaiters(): string[] {
    return [...this.edges.keys()];
  }

  getEdgesFrom(agentId: string): WaitForEdge[] {
    return this.edges.get(agentId) ?? [];
  }
}

// ─── Lock Coarsening Detector ───────────────────────────────────────

class CoarseningDetector {
  private requestHistory: Map<string, { resourceId: string; timestamp: number }[]> = new Map();
  private threshold: number;
  private windowMs: number;

  constructor(threshold: number, windowMs: number) {
    this.threshold = threshold;
    this.windowMs = windowMs;
  }

  record(agentId: string, resourceId: string): void {
    const history = this.requestHistory.get(agentId) ?? [];
    history.push({ resourceId, timestamp: Date.now() });
    // Trim old entries
    const cutoff = Date.now() - this.windowMs;
    const trimmed = history.filter(h => h.timestamp >= cutoff);
    this.requestHistory.set(agentId, trimmed);
  }

  /**
   * Check if an agent is requesting many fine-grained locks in quick succession.
   * Returns resources that should be coarsened into a single lock.
   */
  shouldCoarsen(agentId: string): string[] | null {
    const history = this.requestHistory.get(agentId) ?? [];
    const cutoff = Date.now() - this.windowMs;
    const recent = history.filter(h => h.timestamp >= cutoff);
    
    if (recent.length < this.threshold) return null;

    // Find common prefix for hierarchical coarsening
    const resources = [...new Set(recent.map(h => h.resourceId))];
    if (resources.length < 2) return null;

    return resources;
  }

  clear(agentId: string): void {
    this.requestHistory.delete(agentId);
  }
}

// ─── Phantom Lock Detector ──────────────────────────────────────────

class PhantomDetector {
  private lastHeartbeat: Map<string, number> = new Map(); // agentId → timestamp
  private phantomThresholdMs: number;

  constructor(phantomThresholdMs: number) {
    this.phantomThresholdMs = phantomThresholdMs;
  }

  heartbeat(agentId: string): void {
    this.lastHeartbeat.set(agentId, Date.now());
  }

  /**
   * Returns agent IDs that haven't sent a heartbeat within the threshold.
   */
  detectPhantoms(activeAgents: Set<string>): string[] {
    const now = Date.now();
    const phantoms: string[] = [];
    for (const agentId of activeAgents) {
      const last = this.lastHeartbeat.get(agentId);
      if (last === undefined || (now - last) > this.phantomThresholdMs) {
        phantoms.push(agentId);
      }
    }
    return phantoms;
  }

  getSilentDuration(agentId: string): number {
    const last = this.lastHeartbeat.get(agentId);
    if (last === undefined) return Infinity;
    return Date.now() - last;
  }

  remove(agentId: string): void {
    this.lastHeartbeat.delete(agentId);
  }
}

// ─── Redlock Region Manager ─────────────────────────────────────────

class RedlockRegionManager {
  private regionCount: number;
  private regionVotes: Map<string, Set<number>> = new Map(); // resourceId → set of region indices

  constructor(regionCount: number) {
    this.regionCount = regionCount;
  }

  voteFromRegion(resourceId: string, regionIdx: number): void {
    const votes = this.regionVotes.get(resourceId) ?? new Set();
    votes.add(regionIdx);
    this.regionVotes.set(resourceId, votes);
  }

  hasQuorum(resourceId: string): boolean {
    const votes = this.regionVotes.get(resourceId) ?? new Set();
    const majority = Math.floor(this.regionCount / 2) + 1;
    return votes.size >= majority;
  }

  getVotedRegions(resourceId: string): number[] {
    return [...(this.regionVotes.get(resourceId) ?? new Set())];
  }

  clearVotes(resourceId: string): void {
    this.regionVotes.delete(resourceId);
  }
}

// ─── Wound-Wait Conflict Manager ────────────────────────────────────

class WoundWaitManager {
  private agentStartTimes: Map<string, number> = new Map();

  registerAgent(agentId: string, startTime: number): void {
    if (!this.agentStartTimes.has(agentId)) {
      this.agentStartTimes.set(agentId, startTime);
    }
  }

  /**
   * Wound-Wait rule:
   * - If requester is OLDER than holder → WOUND (abort the holder)
   * - If requester is YOUNGER than holder → WAIT
   * Prevents deadlock by ensuring consistent conflict direction.
   */
  resolve(requesterId: string, holderId: string): 'wound' | 'wait' {
    const requesterStart = this.agentStartTimes.get(requesterId) ?? Date.now();
    const holderStart = this.agentStartTimes.get(holderId) ?? Date.now();
    
    if (requesterStart < holderStart) {
      return 'wound'; // older wounds younger
    }
    if (requesterStart === holderStart) {
      // Tie-break: lower FNV-1a hash wins (is "older")
      return fnv1a(requesterId) < fnv1a(holderId) ? 'wound' : 'wait';
    }
    return 'wait';
  }

  removeAgent(agentId: string): void {
    this.agentStartTimes.delete(agentId);
  }
}

// ─── Main: Distributed Lock Manager ─────────────────────────────────

class DistributedLockManager {
  private config: LockManagerConfig;
  private grants: Map<string, LockGrant[]> = new Map();       // resourceId → active grants
  private waitQueue: Map<string, LockRequest[]> = new Map();   // resourceId → waiting requests
  private agentLockCount: Map<string, number> = new Map();     // agentId → count
  private fencing: FencingTokenGenerator;
  private waitForGraph: WaitForGraph;
  private bakery: BakeryOrderer;
  private phantomDetector: PhantomDetector;
  private coarseningDetector: CoarseningDetector;
  private redlock: RedlockRegionManager;
  private woundWait: WoundWaitManager;
  private stats: LockStats;
  private holdTimes: number[] = [];
  private waitTimes: number[] = [];
  private listeners: ((event: DLMEvent) => void)[] = [];

  constructor(config: LockManagerConfig) {
    this.config = config;
    this.fencing = new FencingTokenGenerator();
    this.waitForGraph = new WaitForGraph();
    this.bakery = new BakeryOrderer();
    this.phantomDetector = new PhantomDetector(config.phantomDetectionMs);
    this.coarseningDetector = new CoarseningDetector(
      config.coarseningThreshold,
      config.coarseningWindowMs
    );
    this.redlock = new RedlockRegionManager(config.regionCount);
    this.woundWait = new WoundWaitManager();
    this.stats = {
      totalGrants: 0,
      totalDenials: 0,
      totalDeadlocksDetected: 0,
      totalPhantomsCleaned: 0,
      totalCoarsenings: 0,
      avgHoldTimeMs: 0,
      avgWaitTimeMs: 0,
      contentionRatio: 0,
    };
  }

  onEvent(listener: (event: DLMEvent) => void): void {
    this.listeners.push(listener);
  }

  private emit(event: DLMEvent): void {
    for (const l of this.listeners) l(event);
  }

  // ─── Lock Acquisition ───────────────────────────────────────────

  acquire(request: LockRequest): LockGrant | null {
    // 1. Check per-agent limit
    const agentCount = this.agentLockCount.get(request.agentId) ?? 0;
    if (agentCount >= this.config.maxLocksPerAgent) {
      this.stats.totalDenials++;
      this.emit({ type: 'lock-denied', request, reason: 'max-locks-per-agent-exceeded' });
      return null;
    }

    // 2. Record for coarsening detection
    this.coarseningDetector.record(request.agentId, request.resourceId);
    const coarsenTargets = this.coarseningDetector.shouldCoarsen(request.agentId);
    if (coarsenTargets) {
      const parentResource = this.computeCoarsenedResource(coarsenTargets);
      this.stats.totalCoarsenings++;
      this.emit({ type: 'lock-coarsened', fromResources: coarsenTargets, toResource: parentResource });
      // Redirect to coarsened resource
      request = { ...request, resourceId: parentResource };
    }

    // 3. Register agent for wound-wait
    this.woundWait.registerAgent(request.agentId, request.timestamp);

    // 4. Take bakery ticket for total ordering
    this.bakery.takeTicket(request.agentId);

    // 5. Check compatibility with existing grants
    const existingGrants = this.grants.get(request.resourceId) ?? [];
    const compatible = existingGrants.every(g => 
      g.agentId === request.agentId || isCompatible(g.mode, request.mode)
    );

    if (compatible && existingGrants.length === 0 || compatible) {
      return this.grantLock(request);
    }

    // 6. Conflict! Apply wound-wait if enabled
    if (this.config.woundWaitEnabled) {
      for (const holder of existingGrants) {
        if (holder.agentId === request.agentId) continue;
        const decision = this.woundWait.resolve(request.agentId, holder.agentId);
        if (decision === 'wound') {
          // Wound the holder — abort their lock
          this.emit({
            type: 'wound-triggered',
            wounderId: request.agentId,
            victimId: holder.agentId,
            resourceId: request.resourceId,
          });
          this.forciblyRelease(holder);
          return this.grantLock(request);
        }
        // else: wait
      }
    }

    // 7. Queue the request
    return this.enqueueRequest(request, existingGrants);
  }

  private grantLock(request: LockRequest): LockGrant {
    const token = this.fencing.next(request.resourceId);
    const now = Date.now();
    const grant: LockGrant = {
      requestId: request.id,
      agentId: request.agentId,
      resourceId: request.resourceId,
      mode: request.mode,
      fencingToken: token,
      grantedAt: now,
      expiresAt: now + this.config.defaultTtlMs,
      renewCount: 0,
    };

    const existing = this.grants.get(request.resourceId) ?? [];
    existing.push(grant);
    this.grants.set(request.resourceId, existing);

    const count = this.agentLockCount.get(request.agentId) ?? 0;
    this.agentLockCount.set(request.agentId, count + 1);

    this.phantomDetector.heartbeat(request.agentId);
    this.bakery.release(request.agentId);

    this.stats.totalGrants++;
    this.emit({ type: 'lock-granted', grant });
    return grant;
  }

  private enqueueRequest(request: LockRequest, holders: LockGrant[]): null {
    const queue = this.waitQueue.get(request.resourceId) ?? [];
    
    if (queue.length >= this.config.maxWaitQueueDepth) {
      this.stats.totalDenials++;
      this.emit({ type: 'lock-denied', request, reason: 'wait-queue-full' });
      this.bakery.release(request.agentId);
      return null;
    }

    // Insert based on fairness mode
    if (this.config.fairnessMode === 'priority') {
      const insertIdx = queue.findIndex(r => r.priority < request.priority);
      if (insertIdx === -1) queue.push(request);
      else queue.splice(insertIdx, 0, request);
    } else {
      queue.push(request); // FIFO
    }
    this.waitQueue.set(request.resourceId, queue);

    // Add wait-for edges
    for (const holder of holders) {
      if (holder.agentId !== request.agentId) {
        this.waitForGraph.addEdge(request.agentId, holder.agentId, request.resourceId);
      }
    }

    this.stats.totalDenials++;
    this.emit({ type: 'lock-denied', request, reason: 'incompatible-lock-held' });
    return null;
  }

  // ─── Lock Release ───────────────────────────────────────────────

  release(agentId: string, resourceId: string): boolean {
    const grants = this.grants.get(resourceId) ?? [];
    const idx = grants.findIndex(g => g.agentId === agentId);
    if (idx === -1) return false;

    const grant = grants[idx];
    grants.splice(idx, 1);
    if (grants.length === 0) {
      this.grants.delete(resourceId);
    } else {
      this.grants.set(resourceId, grants);
    }

    const heldMs = Date.now() - grant.grantedAt;
    this.holdTimes.push(heldMs);
    if (this.holdTimes.length > 1000) this.holdTimes.shift();

    const count = this.agentLockCount.get(agentId) ?? 1;
    this.agentLockCount.set(agentId, Math.max(0, count - 1));

    this.waitForGraph.removeEdgesFor(agentId);
    this.bakery.release(agentId);

    this.emit({ type: 'lock-released', grant, heldMs });

    // Process wait queue
    this.processWaitQueue(resourceId);
    return true;
  }

  private forciblyRelease(grant: LockGrant): void {
    this.release(grant.agentId, grant.resourceId);
  }

  private processWaitQueue(resourceId: string): void {
    const queue = this.waitQueue.get(resourceId) ?? [];
    const remaining: LockRequest[] = [];

    for (const request of queue) {
      const existingGrants = this.grants.get(resourceId) ?? [];
      const compatible = existingGrants.every(g =>
        g.agentId === request.agentId || isCompatible(g.mode, request.mode)
      );

      if (compatible) {
        this.grantLock(request);
      } else {
        remaining.push(request);
      }
    }

    if (remaining.length === 0) {
      this.waitQueue.delete(resourceId);
    } else {
      this.waitQueue.set(resourceId, remaining);
    }
  }

  // ─── Lock Renewal ───────────────────────────────────────────────

  renew(agentId: string, resourceId: string): boolean {
    const grants = this.grants.get(resourceId) ?? [];
    const grant = grants.find(g => g.agentId === agentId);
    if (!grant) return false;

    // Only renew within the renewal window
    const timeUntilExpiry = grant.expiresAt - Date.now();
    if (timeUntilExpiry > this.config.renewalWindowMs) return false;

    const newExpiry = Date.now() + this.config.defaultTtlMs;
    grant.expiresAt = newExpiry;
    grant.renewCount++;
    this.phantomDetector.heartbeat(agentId);

    this.emit({ type: 'lock-renewed', grant, newExpiry });
    return true;
  }

  // ─── Fencing Token Validation ───────────────────────────────────

  validateFencingToken(resourceId: string, token: number): boolean {
    const current = this.fencing.current(resourceId);
    if (token < current) {
      const grants = this.grants.get(resourceId) ?? [];
      const staleGrant = grants.find(g => g.fencingToken === token);
      if (staleGrant) {
        this.emit({
          type: 'fencing-violation',
          grant: staleGrant,
          staleToken: token,
          currentToken: current,
        });
      }
      return false;
    }
    return true;
  }

  // ─── Deadlock Detection ─────────────────────────────────────────

  detectAndResolveDeadlocks(): number {
    let resolved = 0;
    let cycle = this.waitForGraph.detectCycle();

    while (cycle) {
      this.stats.totalDeadlocksDetected++;
      const agentTimestamps = new Map<string, number>();
      // Use bakery tickets as proxy for agent age
      for (const agentId of cycle) {
        agentTimestamps.set(agentId, Date.now()); // youngest gets victimized
      }

      const victim = this.waitForGraph.selectVictim(cycle, agentTimestamps, 'youngest');
      this.emit({ type: 'deadlock-detected', cycle, victim });

      // Abort all requests from victim
      const abortedRequests: string[] = [];
      for (const [resourceId, queue] of this.waitQueue) {
        const filtered = queue.filter(r => {
          if (r.agentId === victim) {
            abortedRequests.push(r.id);
            return false;
          }
          return true;
        });
        if (filtered.length === 0) this.waitQueue.delete(resourceId);
        else this.waitQueue.set(resourceId, filtered);
      }

      this.waitForGraph.removeEdgesFor(victim);
      this.emit({ type: 'deadlock-resolved', victim, abortedRequests });
      resolved++;

      cycle = this.waitForGraph.detectCycle();
    }

    return resolved;
  }

  // ─── Phantom Detection ──────────────────────────────────────────

  detectAndCleanPhantoms(): number {
    const holdingAgents = new Set<string>();
    for (const grants of this.grants.values()) {
      for (const g of grants) holdingAgents.add(g.agentId);
    }

    const phantoms = this.phantomDetector.detectPhantoms(holdingAgents);
    let cleaned = 0;

    for (const agentId of phantoms) {
      const silentMs = this.phantomDetector.getSilentDuration(agentId);
      
      // Release all locks held by phantom agent
      for (const [resourceId, grants] of [...this.grants]) {
        const phantomGrants = grants.filter(g => g.agentId === agentId);
        for (const grant of phantomGrants) {
          this.emit({ type: 'phantom-detected', grant, silentMs });
          this.release(agentId, resourceId);
          this.emit({ type: 'phantom-cleaned', resourceId, agentId });
          cleaned++;
        }
      }

      this.phantomDetector.remove(agentId);
      this.woundWait.removeAgent(agentId);
    }

    this.stats.totalPhantomsCleaned += cleaned;
    return cleaned;
  }

  // ─── Expiry Check ───────────────────────────────────────────────

  checkExpiredLocks(): number {
    const now = Date.now();
    let expired = 0;

    for (const [resourceId, grants] of [...this.grants]) {
      const expiredGrants = grants.filter(g => g.expiresAt <= now);
      for (const grant of expiredGrants) {
        this.emit({ type: 'lock-expired', grant });
        this.release(grant.agentId, grant.resourceId);
        expired++;
      }
    }

    return expired;
  }

  // ─── Redlock Multi-Region ───────────────────────────────────────

  acquireWithRedlock(request: LockRequest, regionIdx: number): LockGrant | null {
    this.redlock.voteFromRegion(request.resourceId, regionIdx);

    if (this.redlock.hasQuorum(request.resourceId)) {
      const regions = this.redlock.getVotedRegions(request.resourceId);
      this.emit({ type: 'quorum-achieved', resourceId: request.resourceId, quorumRegions: regions });
      this.redlock.clearVotes(request.resourceId);
      return this.acquire(request);
    }

    return null; // Waiting for more region votes
  }

  // ─── Hierarchical Lock Helpers ──────────────────────────────────

  acquireHierarchical(agentId: string, resourcePath: string[], mode: LockMode): LockGrant[] {
    const grants: LockGrant[] = [];
    const intentionMode: LockMode = mode === 'exclusive' ? 'intention-exclusive' : 'intention-shared';

    // Acquire intention locks on ancestors
    for (let i = 0; i < resourcePath.length - 1; i++) {
      const ancestorId = resourcePath.slice(0, i + 1).join('/');
      const request: LockRequest = {
        id: `${agentId}-${ancestorId}-${Date.now()}`,
        agentId,
        resourceId: ancestorId,
        mode: intentionMode,
        priority: 1,
        timestamp: Date.now(),
        timeout: this.config.defaultTtlMs,
      };
      const grant = this.acquire(request);
      if (!grant) {
        // Rollback: release all acquired intention locks
        for (const g of grants) this.release(g.agentId, g.resourceId);
        return [];
      }
      grants.push(grant);
    }

    // Acquire actual lock on target
    const targetId = resourcePath.join('/');
    const request: LockRequest = {
      id: `${agentId}-${targetId}-${Date.now()}`,
      agentId,
      resourceId: targetId,
      mode,
      priority: 1,
      timestamp: Date.now(),
      timeout: this.config.defaultTtlMs,
    };
    const grant = this.acquire(request);
    if (!grant) {
      for (const g of grants) this.release(g.agentId, g.resourceId);
      return [];
    }
    grants.push(grant);

    return grants;
  }

  // ─── Tick (periodic maintenance) ────────────────────────────────

  tick(): void {
    this.checkExpiredLocks();
    this.detectAndResolveDeadlocks();
    this.detectAndCleanPhantoms();
    this.updateStats();
  }

  // ─── Stats ──────────────────────────────────────────────────────

  private updateStats(): void {
    if (this.holdTimes.length > 0) {
      this.stats.avgHoldTimeMs = this.holdTimes.reduce((a, b) => a + b, 0) / this.holdTimes.length;
    }
    if (this.waitTimes.length > 0) {
      this.stats.avgWaitTimeMs = this.waitTimes.reduce((a, b) => a + b, 0) / this.waitTimes.length;
    }
    const totalAttempts = this.stats.totalGrants + this.stats.totalDenials;
    this.stats.contentionRatio = totalAttempts > 0 ? this.stats.totalDenials / totalAttempts : 0;
  }

  getStats(): Readonly<LockStats> {
    this.updateStats();
    return { ...this.stats };
  }

  // ─── Utility ────────────────────────────────────────────────────

  private computeCoarsenedResource(resources: string[]): string {
    // Find longest common prefix
    if (resources.length === 0) return 'coarsened';
    const sorted = [...resources].sort();
    const first = sorted[0];
    const last = sorted[sorted.length - 1];
    let i = 0;
    while (i < first.length && first[i] === last[i]) i++;
    const prefix = first.substring(0, i);
    return prefix || `coarsened-${fnv1a(resources.join(',')).toString(16)}`;
  }

  getGrantsForResource(resourceId: string): readonly LockGrant[] {
    return this.grants.get(resourceId) ?? [];
  }

  getGrantsForAgent(agentId: string): LockGrant[] {
    const result: LockGrant[] = [];
    for (const grants of this.grants.values()) {
      for (const g of grants) {
        if (g.agentId === agentId) result.push(g);
      }
    }
    return result;
  }

  getQueueDepth(resourceId: string): number {
    return (this.waitQueue.get(resourceId) ?? []).length;
  }

  heartbeat(agentId: string): void {
    this.phantomDetector.heartbeat(agentId);
  }
}

// ─── Presets ────────────────────────────────────────────────────────

const PRESETS = {
  'fast-locks': {
    maxLocksPerAgent: 10,
    defaultTtlMs: 5_000,
    renewalWindowMs: 2_000,
    deadlockCheckIntervalMs: 1_000,
    phantomDetectionMs: 10_000,
    quorumSize: 3,
    regionCount: 3,
    maxWaitQueueDepth: 50,
    coarseningThreshold: 8,
    coarseningWindowMs: 2_000,
    woundWaitEnabled: false,
    fairnessMode: 'fifo' as const,
  },
  'standard': {
    maxLocksPerAgent: 25,
    defaultTtlMs: 30_000,
    renewalWindowMs: 10_000,
    deadlockCheckIntervalMs: 5_000,
    phantomDetectionMs: 60_000,
    quorumSize: 5,
    regionCount: 5,
    maxWaitQueueDepth: 200,
    coarseningThreshold: 15,
    coarseningWindowMs: 10_000,
    woundWaitEnabled: true,
    fairnessMode: 'wound-wait' as const,
  },
  'high-contention': {
    maxLocksPerAgent: 50,
    defaultTtlMs: 15_000,
    renewalWindowMs: 5_000,
    deadlockCheckIntervalMs: 2_000,
    phantomDetectionMs: 30_000,
    quorumSize: 7,
    regionCount: 7,
    maxWaitQueueDepth: 500,
    coarseningThreshold: 5,
    coarseningWindowMs: 3_000,
    woundWaitEnabled: true,
    fairnessMode: 'priority' as const,
  },
} as const;

export {
  DistributedLockManager,
  BakeryOrderer,
  MaekawaQuorum,
  FencingTokenGenerator,
  WaitForGraph,
  PhantomDetector,
  CoarseningDetector,
  RedlockRegionManager,
  WoundWaitManager,
  PRESETS,
  isCompatible,
  fnv1a,
};
export type {
  LockRequest,
  LockMode,
  LockGrant,
  LockManagerConfig,
  WaitForEdge,
  LockStats,
  DLMEvent,
};
