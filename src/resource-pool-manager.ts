/**
 * Resource Pool Manager for Multi-Agent Systems
 * 
 * Manages shared resource pools (connections, compute slots, memory regions, API quotas)
 * across agents with fair allocation, reservation, and reclamation.
 * 
 * Key features:
 * - Hierarchical resource pools with inheritance and limits
 * - Reservation protocol with timeout-based auto-release
 * - Fair-share allocation using max-min fairness
 * - Resource fragmentation detection and defragmentation
 * - Quota enforcement with burst allowances
 * - Priority-based preemption with compensation tracking
 * - Pool health monitoring and auto-scaling triggers
 */

// ─── Types ───────────────────────────────────────────────────────────────────

export interface ResourceDescriptor {
  readonly id: string;
  readonly type: ResourceType;
  readonly capacity: number;
  readonly unit: string;
  readonly tags: ReadonlySet<string>;
  readonly metadata: Record<string, unknown>;
}

export type ResourceType =
  | 'compute'
  | 'memory'
  | 'connection'
  | 'api-quota'
  | 'storage'
  | 'bandwidth'
  | 'custom';

export interface Reservation {
  readonly id: string;
  readonly agentId: string;
  readonly resourceId: string;
  readonly amount: number;
  readonly priority: number;
  readonly createdAt: number;
  readonly expiresAt: number;
  readonly renewable: boolean;
  readonly purpose: string;
}

export interface AllocationResult {
  readonly granted: number;
  readonly reservationId: string | null;
  readonly waitEstimateMs: number;
  readonly alternativeResources: string[];
}

export interface PoolHealth {
  readonly utilization: number;        // 0-1
  readonly fragmentation: number;      // 0-1 (0 = no fragmentation)
  readonly reservationPressure: number; // pending / capacity
  readonly avgWaitMs: number;
  readonly preemptionRate: number;     // preemptions per minute
  readonly healthy: boolean;
}

export interface QuotaPolicy {
  readonly agentId: string;
  readonly resourceType: ResourceType;
  readonly maxAllocation: number;       // hard cap
  readonly guaranteedMinimum: number;   // floor allocation
  readonly burstAllowance: number;      // temporary over-allocation
  readonly burstWindowMs: number;
  readonly priorityWeight: number;      // for fair-share calculation
}

export interface PreemptionRecord {
  readonly preemptedReservation: string;
  readonly preemptingAgent: string;
  readonly reason: string;
  readonly compensationCredits: number;
  readonly timestamp: number;
}

export type PoolEvent =
  | { type: 'resource-added'; resource: ResourceDescriptor }
  | { type: 'resource-removed'; resourceId: string }
  | { type: 'reservation-created'; reservation: Reservation }
  | { type: 'reservation-released'; reservationId: string; agentId: string }
  | { type: 'reservation-expired'; reservationId: string }
  | { type: 'reservation-renewed'; reservationId: string; newExpiry: number }
  | { type: 'preemption'; record: PreemptionRecord }
  | { type: 'quota-exceeded'; agentId: string; resourceType: ResourceType; requested: number; limit: number }
  | { type: 'pool-exhausted'; resourceId: string }
  | { type: 'pool-recovered'; resourceId: string; utilization: number }
  | { type: 'fragmentation-alert'; resourceId: string; fragmentation: number }
  | { type: 'scale-trigger'; resourceType: ResourceType; direction: 'up' | 'down'; currentUtilization: number };

type EventHandler = (event: PoolEvent) => void;

// ─── Resource Pool ───────────────────────────────────────────────────────────

interface PoolState {
  resource: ResourceDescriptor;
  allocated: number;
  reservations: Map<string, Reservation>;
  waitQueue: WaitEntry[];
  peakUtilization: number;
  totalPreemptions: number;
  recentWaitTimesMs: number[];
}

interface WaitEntry {
  agentId: string;
  amount: number;
  priority: number;
  enqueuedAt: number;
  resolve: (result: AllocationResult) => void;
  timeoutHandle: ReturnType<typeof setTimeout> | null;
}

// ─── Fair Share Calculator ───────────────────────────────────────────────────

class MaxMinFairShare {
  /**
   * Compute max-min fair allocation across agents.
   * Each agent has a demand and a weight. Resources are allocated
   * in rounds: smallest weighted-demand agents get saturated first,
   * remaining capacity redistributed proportionally.
   */
  static allocate(
    totalCapacity: number,
    demands: Map<string, { demand: number; weight: number; minimum: number }>
  ): Map<string, number> {
    const result = new Map<string, number>();
    const agents = [...demands.entries()].map(([id, d]) => ({
      id,
      demand: d.demand,
      weight: d.weight,
      minimum: d.minimum,
      allocated: 0,
      satisfied: false,
    }));

    // First pass: guarantee minimums
    let remaining = totalCapacity;
    for (const a of agents) {
      const min = Math.min(a.minimum, a.demand, remaining);
      a.allocated = min;
      remaining -= min;
    }

    // Iterative max-min fairness on the remainder
    let unsatisfied = agents.filter(a => !a.satisfied && a.allocated < a.demand);
    while (remaining > 0.001 && unsatisfied.length > 0) {
      const totalWeight = unsatisfied.reduce((s, a) => s + a.weight, 0);
      if (totalWeight === 0) break;

      const fairSharePerWeight = remaining / totalWeight;
      let consumed = 0;
      let newlySatisfied = false;

      for (const a of unsatisfied) {
        const share = fairSharePerWeight * a.weight;
        const needed = a.demand - a.allocated;
        const grant = Math.min(share, needed);
        a.allocated += grant;
        consumed += grant;
        if (a.allocated >= a.demand - 0.001) {
          a.satisfied = true;
          newlySatisfied = true;
        }
      }

      remaining -= consumed;
      if (!newlySatisfied) break; // all got proportional share, done
      unsatisfied = agents.filter(a => !a.satisfied && a.allocated < a.demand);
    }

    for (const a of agents) {
      result.set(a.id, Math.round(a.allocated * 1000) / 1000);
    }
    return result;
  }
}

// ─── Fragmentation Detector ──────────────────────────────────────────────────

class FragmentationDetector {
  /**
   * Measures fragmentation as the ratio of wasted capacity due to
   * non-contiguous allocation gaps. For discrete resources (connections,
   * compute slots), fragmentation occurs when free capacity exists but
   * can't satisfy requests due to sizing constraints.
   * 
   * Score: 0 = perfectly packed, 1 = fully fragmented
   */
  static measure(
    totalCapacity: number,
    allocated: number,
    reservations: Reservation[],
    largestRequestable: number
  ): number {
    const free = totalCapacity - allocated;
    if (free <= 0 || largestRequestable <= 0) return 0;

    // Sort reservations by size to find gaps
    const sorted = [...reservations].sort((a, b) => a.amount - b.amount);
    
    // External fragmentation: can we satisfy the largest pending request?
    if (free >= largestRequestable) return 0;

    // Internal fragmentation: ratio of unusable free space
    // Small scattered allocations create gaps
    const avgReservationSize = sorted.length > 0
      ? sorted.reduce((s, r) => s + r.amount, 0) / sorted.length
      : 0;

    if (avgReservationSize === 0) return 0;

    // Heuristic: fragmentation is high when free space exists but
    // average reservation size is much smaller than free space
    // (indicating many small holes)
    const utilizationVariance = sorted.length > 1
      ? sorted.reduce((s, r) => s + Math.pow(r.amount - avgReservationSize, 2), 0) / sorted.length
      : 0;
    const cv = avgReservationSize > 0 ? Math.sqrt(utilizationVariance) / avgReservationSize : 0;

    // High CV = diverse reservation sizes = more fragmentation potential
    return Math.min(1, cv * (1 - allocated / totalCapacity));
  }
}

// ─── Resource Pool Manager ───────────────────────────────────────────────────

export class ResourcePoolManager {
  private pools = new Map<string, PoolState>();
  private quotas = new Map<string, QuotaPolicy[]>(); // agentId -> policies
  private agentAllocations = new Map<string, Map<string, number>>(); // agentId -> resourceId -> amount
  private burstTracking = new Map<string, { used: number; windowStart: number }>();
  private preemptionHistory: PreemptionRecord[] = [];
  private eventHandlers: EventHandler[] = [];
  private reservationCounter = 0;

  // Thresholds
  private readonly EXHAUSTION_THRESHOLD = 0.95;
  private readonly RECOVERY_THRESHOLD = 0.80;
  private readonly FRAGMENTATION_ALERT_THRESHOLD = 0.6;
  private readonly SCALE_UP_THRESHOLD = 0.85;
  private readonly SCALE_DOWN_THRESHOLD = 0.30;
  private readonly MAX_WAIT_HISTORY = 100;
  private readonly PREEMPTION_COMPENSATION_RATE = 1.5; // 1.5x the resource value

  // ─── Lifecycle ───────────────────────────────────────────────────────

  addResource(resource: ResourceDescriptor): void {
    if (this.pools.has(resource.id)) {
      throw new Error(`Resource ${resource.id} already exists`);
    }

    this.pools.set(resource.id, {
      resource,
      allocated: 0,
      reservations: new Map(),
      waitQueue: [],
      peakUtilization: 0,
      totalPreemptions: 0,
      recentWaitTimesMs: [],
    });

    this.emit({ type: 'resource-added', resource });
  }

  removeResource(resourceId: string): Reservation[] {
    const pool = this.pools.get(resourceId);
    if (!pool) return [];

    // Return all active reservations for cleanup
    const displaced = [...pool.reservations.values()];

    // Clear wait queue
    for (const entry of pool.waitQueue) {
      if (entry.timeoutHandle) clearTimeout(entry.timeoutHandle);
      entry.resolve({
        granted: 0,
        reservationId: null,
        waitEstimateMs: -1,
        alternativeResources: this.findAlternatives(pool.resource.type, entry.amount),
      });
    }

    // Clean agent allocations
    for (const reservation of displaced) {
      this.releaseAgentAllocation(reservation.agentId, resourceId, reservation.amount);
    }

    this.pools.delete(resourceId);
    this.emit({ type: 'resource-removed', resourceId });
    return displaced;
  }

  // ─── Allocation ──────────────────────────────────────────────────────

  /**
   * Request a resource allocation. Returns immediately if capacity is
   * available, otherwise enqueues with optional timeout.
   */
  allocate(
    agentId: string,
    resourceId: string,
    amount: number,
    options: {
      priority?: number;
      timeoutMs?: number;
      renewable?: boolean;
      ttlMs?: number;
      purpose?: string;
    } = {}
  ): Promise<AllocationResult> {
    const pool = this.pools.get(resourceId);
    if (!pool) {
      return Promise.resolve({
        granted: 0,
        reservationId: null,
        waitEstimateMs: -1,
        alternativeResources: [],
      });
    }

    const priority = options.priority ?? 0;
    const ttlMs = options.ttlMs ?? 300_000; // 5 min default

    // Check quota
    if (!this.checkQuota(agentId, pool.resource.type, amount)) {
      this.emit({
        type: 'quota-exceeded',
        agentId,
        resourceType: pool.resource.type,
        requested: amount,
        limit: this.getEffectiveLimit(agentId, pool.resource.type),
      });
      return Promise.resolve({
        granted: 0,
        reservationId: null,
        waitEstimateMs: -1,
        alternativeResources: this.findAlternatives(pool.resource.type, amount),
      });
    }

    const available = pool.resource.capacity - pool.allocated;

    // Fast path: capacity available
    if (available >= amount) {
      return Promise.resolve(this.createReservation(pool, agentId, amount, priority, ttlMs, options));
    }

    // Try preemption if high priority
    if (priority > 0) {
      const preempted = this.tryPreemption(pool, agentId, amount, priority);
      if (preempted) {
        return Promise.resolve(this.createReservation(pool, agentId, amount, priority, ttlMs, options));
      }
    }

    // Enqueue and wait
    return new Promise<AllocationResult>((resolve) => {
      const entry: WaitEntry = {
        agentId,
        amount,
        priority,
        enqueuedAt: Date.now(),
        resolve,
        timeoutHandle: null,
      };

      if (options.timeoutMs) {
        entry.timeoutHandle = setTimeout(() => {
          const idx = pool.waitQueue.indexOf(entry);
          if (idx !== -1) {
            pool.waitQueue.splice(idx, 1);
            resolve({
              granted: 0,
              reservationId: null,
              waitEstimateMs: -1,
              alternativeResources: this.findAlternatives(pool.resource.type, amount),
            });
          }
        }, options.timeoutMs);
      }

      // Insert sorted by priority (highest first)
      const insertIdx = pool.waitQueue.findIndex(e => e.priority < priority);
      if (insertIdx === -1) {
        pool.waitQueue.push(entry);
      } else {
        pool.waitQueue.splice(insertIdx, 0, entry);
      }
    });
  }

  /**
   * Release a reservation, returning resources to the pool.
   */
  release(reservationId: string): boolean {
    for (const [, pool] of this.pools) {
      const reservation = pool.reservations.get(reservationId);
      if (reservation) {
        pool.reservations.delete(reservationId);
        pool.allocated -= reservation.amount;
        this.releaseAgentAllocation(reservation.agentId, reservation.resourceId, reservation.amount);

        this.emit({ type: 'reservation-released', reservationId, agentId: reservation.agentId });

        // Check recovery
        const utilization = pool.allocated / pool.resource.capacity;
        if (utilization < this.RECOVERY_THRESHOLD) {
          this.emit({ type: 'pool-recovered', resourceId: reservation.resourceId, utilization });
        }

        // Process wait queue
        this.drainWaitQueue(pool);
        return true;
      }
    }
    return false;
  }

  /**
   * Renew an expiring reservation.
   */
  renew(reservationId: string, additionalMs: number): boolean {
    for (const [, pool] of this.pools) {
      const reservation = pool.reservations.get(reservationId);
      if (reservation && reservation.renewable) {
        const renewed: Reservation = {
          ...reservation,
          expiresAt: Date.now() + additionalMs,
        };
        pool.reservations.set(reservationId, renewed);
        this.emit({ type: 'reservation-renewed', reservationId, newExpiry: renewed.expiresAt });
        return true;
      }
    }
    return false;
  }

  // ─── Quota Management ────────────────────────────────────────────────

  setQuota(policy: QuotaPolicy): void {
    const existing = this.quotas.get(policy.agentId) ?? [];
    const idx = existing.findIndex(p => p.resourceType === policy.resourceType);
    if (idx !== -1) {
      existing[idx] = policy;
    } else {
      existing.push(policy);
    }
    this.quotas.set(policy.agentId, existing);
  }

  removeQuota(agentId: string, resourceType?: ResourceType): void {
    if (!resourceType) {
      this.quotas.delete(agentId);
    } else {
      const policies = this.quotas.get(agentId);
      if (policies) {
        const filtered = policies.filter(p => p.resourceType !== resourceType);
        if (filtered.length === 0) {
          this.quotas.delete(agentId);
        } else {
          this.quotas.set(agentId, filtered);
        }
      }
    }
  }

  /**
   * Compute fair-share allocations across all agents for a resource type.
   */
  computeFairShare(resourceType: ResourceType): Map<string, number> {
    const totalCapacity = this.getTotalCapacity(resourceType);
    const demands = new Map<string, { demand: number; weight: number; minimum: number }>();

    for (const [agentId, policies] of this.quotas) {
      const policy = policies.find(p => p.resourceType === resourceType);
      if (policy) {
        demands.set(agentId, {
          demand: policy.maxAllocation,
          weight: policy.priorityWeight,
          minimum: policy.guaranteedMinimum,
        });
      }
    }

    return MaxMinFairShare.allocate(totalCapacity, demands);
  }

  // ─── Health & Monitoring ─────────────────────────────────────────────

  getPoolHealth(resourceId: string): PoolHealth | null {
    const pool = this.pools.get(resourceId);
    if (!pool) return null;

    const utilization = pool.allocated / pool.resource.capacity;
    const reservations = [...pool.reservations.values()];
    const largestWaiting = pool.waitQueue.length > 0
      ? Math.max(...pool.waitQueue.map(e => e.amount))
      : 0;

    const fragmentation = FragmentationDetector.measure(
      pool.resource.capacity,
      pool.allocated,
      reservations,
      largestWaiting
    );

    const avgWaitMs = pool.recentWaitTimesMs.length > 0
      ? pool.recentWaitTimesMs.reduce((s, t) => s + t, 0) / pool.recentWaitTimesMs.length
      : 0;

    const reservationPressure = pool.waitQueue.length > 0
      ? pool.waitQueue.reduce((s, e) => s + e.amount, 0) / pool.resource.capacity
      : 0;

    // Preemption rate: count in last 60s
    const now = Date.now();
    const recentPreemptions = this.preemptionHistory.filter(
      p => now - p.timestamp < 60_000
    ).length;

    return {
      utilization,
      fragmentation,
      reservationPressure,
      avgWaitMs,
      preemptionRate: recentPreemptions,
      healthy: utilization < this.EXHAUSTION_THRESHOLD && fragmentation < this.FRAGMENTATION_ALERT_THRESHOLD,
    };
  }

  /**
   * Scan all pools for expired reservations and reclaim them.
   */
  reclaimExpired(): number {
    const now = Date.now();
    let reclaimed = 0;

    for (const [, pool] of this.pools) {
      const expired: string[] = [];
      for (const [id, res] of pool.reservations) {
        if (res.expiresAt <= now) {
          expired.push(id);
        }
      }

      for (const id of expired) {
        const res = pool.reservations.get(id)!;
        pool.reservations.delete(id);
        pool.allocated -= res.amount;
        this.releaseAgentAllocation(res.agentId, res.resourceId, res.amount);
        this.emit({ type: 'reservation-expired', reservationId: id });
        reclaimed++;
      }

      if (expired.length > 0) {
        this.drainWaitQueue(pool);
      }
    }

    return reclaimed;
  }

  /**
   * Get a snapshot of all pools for monitoring dashboards.
   */
  snapshot(): {
    pools: Array<{
      resource: ResourceDescriptor;
      allocated: number;
      reservationCount: number;
      waitQueueLength: number;
      health: PoolHealth;
    }>;
    totalPreemptions: number;
    agentCount: number;
  } {
    const pools = [...this.pools.values()].map(pool => ({
      resource: pool.resource,
      allocated: pool.allocated,
      reservationCount: pool.reservations.size,
      waitQueueLength: pool.waitQueue.length,
      health: this.getPoolHealth(pool.resource.id)!,
    }));

    return {
      pools,
      totalPreemptions: this.preemptionHistory.length,
      agentCount: this.agentAllocations.size,
    };
  }

  // ─── Auto-Scaling Triggers ───────────────────────────────────────────

  /**
   * Check all pools and emit scale triggers when thresholds are crossed.
   * Call periodically (e.g., every 30s) from a monitoring loop.
   */
  checkScalingTriggers(): void {
    const typeUtilization = new Map<ResourceType, { total: number; used: number }>();

    for (const [, pool] of this.pools) {
      const existing = typeUtilization.get(pool.resource.type) ?? { total: 0, used: 0 };
      existing.total += pool.resource.capacity;
      existing.used += pool.allocated;
      typeUtilization.set(pool.resource.type, existing);
    }

    for (const [type, { total, used }] of typeUtilization) {
      const utilization = total > 0 ? used / total : 0;

      if (utilization > this.SCALE_UP_THRESHOLD) {
        this.emit({ type: 'scale-trigger', resourceType: type, direction: 'up', currentUtilization: utilization });
      } else if (utilization < this.SCALE_DOWN_THRESHOLD) {
        this.emit({ type: 'scale-trigger', resourceType: type, direction: 'down', currentUtilization: utilization });
      }
    }
  }

  // ─── Event System ────────────────────────────────────────────────────

  on(handler: EventHandler): () => void {
    this.eventHandlers.push(handler);
    return () => {
      const idx = this.eventHandlers.indexOf(handler);
      if (idx !== -1) this.eventHandlers.splice(idx, 1);
    };
  }

  // ─── Internals ───────────────────────────────────────────────────────

  private emit(event: PoolEvent): void {
    for (const handler of this.eventHandlers) {
      try { handler(event); } catch {}
    }
  }

  private createReservation(
    pool: PoolState,
    agentId: string,
    amount: number,
    priority: number,
    ttlMs: number,
    options: { renewable?: boolean; purpose?: string }
  ): AllocationResult {
    const now = Date.now();
    const id = `res-${++this.reservationCounter}-${now.toString(36)}`;

    const reservation: Reservation = {
      id,
      agentId,
      resourceId: pool.resource.id,
      amount,
      priority,
      createdAt: now,
      expiresAt: now + ttlMs,
      renewable: options.renewable ?? true,
      purpose: options.purpose ?? '',
    };

    pool.reservations.set(id, reservation);
    pool.allocated += amount;
    this.trackAgentAllocation(agentId, pool.resource.id, amount);

    // Update peak utilization
    const utilization = pool.allocated / pool.resource.capacity;
    pool.peakUtilization = Math.max(pool.peakUtilization, utilization);

    this.emit({ type: 'reservation-created', reservation });

    // Check exhaustion
    if (utilization >= this.EXHAUSTION_THRESHOLD) {
      this.emit({ type: 'pool-exhausted', resourceId: pool.resource.id });
    }

    // Check fragmentation
    const health = this.getPoolHealth(pool.resource.id);
    if (health && health.fragmentation >= this.FRAGMENTATION_ALERT_THRESHOLD) {
      this.emit({ type: 'fragmentation-alert', resourceId: pool.resource.id, fragmentation: health.fragmentation });
    }

    return {
      granted: amount,
      reservationId: id,
      waitEstimateMs: 0,
      alternativeResources: [],
    };
  }

  private tryPreemption(pool: PoolState, requestingAgent: string, needed: number, requestPriority: number): boolean {
    // Find lower-priority reservations that could be preempted
    const candidates = [...pool.reservations.values()]
      .filter(r => r.priority < requestPriority && r.agentId !== requestingAgent)
      .sort((a, b) => a.priority - b.priority); // lowest priority first

    let reclaimable = 0;
    const toPreempt: Reservation[] = [];

    for (const candidate of candidates) {
      toPreempt.push(candidate);
      reclaimable += candidate.amount;
      if (pool.resource.capacity - pool.allocated + reclaimable >= needed) break;
    }

    const available = pool.resource.capacity - pool.allocated + reclaimable;
    if (available < needed) return false;

    // Execute preemption
    for (const victim of toPreempt) {
      pool.reservations.delete(victim.id);
      pool.allocated -= victim.amount;
      this.releaseAgentAllocation(victim.agentId, victim.resourceId, victim.amount);

      const record: PreemptionRecord = {
        preemptedReservation: victim.id,
        preemptingAgent: requestingAgent,
        reason: `Priority preemption: ${requestPriority} > ${victim.priority}`,
        compensationCredits: Math.ceil(victim.amount * this.PREEMPTION_COMPENSATION_RATE),
        timestamp: Date.now(),
      };

      this.preemptionHistory.push(record);
      pool.totalPreemptions++;
      this.emit({ type: 'preemption', record });
    }

    return true;
  }

  private drainWaitQueue(pool: PoolState): void {
    while (pool.waitQueue.length > 0) {
      const next = pool.waitQueue[0];
      const available = pool.resource.capacity - pool.allocated;

      if (available < next.amount) break;

      pool.waitQueue.shift();
      if (next.timeoutHandle) clearTimeout(next.timeoutHandle);

      const waitTime = Date.now() - next.enqueuedAt;
      pool.recentWaitTimesMs.push(waitTime);
      if (pool.recentWaitTimesMs.length > this.MAX_WAIT_HISTORY) {
        pool.recentWaitTimesMs.shift();
      }

      const result = this.createReservation(
        pool,
        next.agentId,
        next.amount,
        next.priority,
        300_000,
        { renewable: true, purpose: '' }
      );

      next.resolve(result);
    }
  }

  private checkQuota(agentId: string, resourceType: ResourceType, amount: number): boolean {
    const policies = this.quotas.get(agentId);
    if (!policies) return true; // no quota = unlimited

    const policy = policies.find(p => p.resourceType === resourceType);
    if (!policy) return true;

    const currentUsage = this.getAgentUsage(agentId, resourceType);
    const burstAvailable = this.getBurstAllowance(agentId, policy);
    const effectiveLimit = policy.maxAllocation + burstAvailable;

    if (currentUsage + amount > effectiveLimit) return false;

    // Track burst consumption if allocation exceeds base max
    const overBase = (currentUsage + amount) - policy.maxAllocation;
    if (overBase > 0) {
      const key = `${agentId}:${policy.resourceType}`;
      const tracking = this.burstTracking.get(key);
      if (tracking) {
        tracking.used += overBase;
      }
    }

    return true;
  }

  private getBurstAllowance(agentId: string, policy: QuotaPolicy): number {
    const key = `${agentId}:${policy.resourceType}`;
    const tracking = this.burstTracking.get(key);
    const now = Date.now();

    if (!tracking || now - tracking.windowStart > policy.burstWindowMs) {
      // New window
      this.burstTracking.set(key, { used: 0, windowStart: now });
      return policy.burstAllowance;
    }

    return Math.max(0, policy.burstAllowance - tracking.used);
  }

  private getEffectiveLimit(agentId: string, resourceType: ResourceType): number {
    const policies = this.quotas.get(agentId);
    if (!policies) return Infinity;
    const policy = policies.find(p => p.resourceType === resourceType);
    return policy ? policy.maxAllocation : Infinity;
  }

  private getAgentUsage(agentId: string, resourceType: ResourceType): number {
    const allocations = this.agentAllocations.get(agentId);
    if (!allocations) return 0;

    let total = 0;
    for (const [resourceId, amount] of allocations) {
      const pool = this.pools.get(resourceId);
      if (pool && pool.resource.type === resourceType) {
        total += amount;
      }
    }
    return total;
  }

  private trackAgentAllocation(agentId: string, resourceId: string, amount: number): void {
    if (!this.agentAllocations.has(agentId)) {
      this.agentAllocations.set(agentId, new Map());
    }
    const allocations = this.agentAllocations.get(agentId)!;
    allocations.set(resourceId, (allocations.get(resourceId) ?? 0) + amount);
  }

  private releaseAgentAllocation(agentId: string, resourceId: string, amount: number): void {
    const allocations = this.agentAllocations.get(agentId);
    if (!allocations) return;
    const current = allocations.get(resourceId) ?? 0;
    const remaining = current - amount;
    if (remaining <= 0) {
      allocations.delete(resourceId);
    } else {
      allocations.set(resourceId, remaining);
    }
    if (allocations.size === 0) {
      this.agentAllocations.delete(agentId);
    }
  }

  private getTotalCapacity(resourceType: ResourceType): number {
    let total = 0;
    for (const [, pool] of this.pools) {
      if (pool.resource.type === resourceType) {
        total += pool.resource.capacity;
      }
    }
    return total;
  }

  private findAlternatives(resourceType: ResourceType, amount: number): string[] {
    const alternatives: string[] = [];
    for (const [, pool] of this.pools) {
      if (pool.resource.type === resourceType) {
        const available = pool.resource.capacity - pool.allocated;
        if (available >= amount) {
          alternatives.push(pool.resource.id);
        }
      }
    }
    return alternatives;
  }

  destroy(): void {
    for (const pool of this.pools.values()) {
      for (const entry of pool.waitQueue) {
        if (entry.timeoutHandle) clearTimeout(entry.timeoutHandle);
      }
      pool.waitQueue.length = 0;
      pool.reservations.clear();
    }
    this.pools.clear();
  }
}

// ─── Pre-built Pool Templates ────────────────────────────────────────────────

export function createConnectionPool(id: string, maxConnections: number): ResourceDescriptor {
  return {
    id,
    type: 'connection',
    capacity: maxConnections,
    unit: 'connections',
    tags: new Set(['network', 'pooled']),
    metadata: {},
  };
}

export function createComputePool(id: string, slots: number): ResourceDescriptor {
  return {
    id,
    type: 'compute',
    capacity: slots,
    unit: 'slots',
    tags: new Set(['compute', 'preemptible']),
    metadata: {},
  };
}

export function createApiQuotaPool(id: string, requestsPerMinute: number): ResourceDescriptor {
  return {
    id,
    type: 'api-quota',
    capacity: requestsPerMinute,
    unit: 'req/min',
    tags: new Set(['rate-limited', 'shared']),
    metadata: { windowMs: 60_000 },
  };
}

export function createMemoryPool(id: string, megabytes: number): ResourceDescriptor {
  return {
    id,
    type: 'memory',
    capacity: megabytes,
    unit: 'MB',
    tags: new Set(['memory', 'elastic']),
    metadata: {},
  };
}
