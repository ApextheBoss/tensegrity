/**
 * Rate-Aware Federation Protocol
 * 
 * Cross-network agent federation with built-in rate awareness:
 * - Per-network rate budget tracking with token bucket + sliding window hybrid
 * - Adaptive request prioritization based on remaining budget
 * - Quota negotiation between federation peers (bilateral + multilateral)
 * - Graceful degradation when approaching limits (shed low-priority first)
 * - Budget forecasting with linear regression over consumption history
 * - Cooperative rate sharing: idle peers donate unused quota
 * - Request coalescing across federation boundary (batch similar requests)
 * - Circuit breaker integration: trip on repeated 429s, not just 5xx
 * - Fairness enforcement: no single peer can starve others
 * - Audit trail: every cross-boundary request logged with budget impact
 */

// ============================================================
// Types
// ============================================================

interface FederationPeer {
  readonly id: string;
  readonly networkId: string;
  readonly endpoint: string;
  capabilities: string[];
  trustLevel: number; // 0-1
  quotaGranted: number; // requests/window we've been granted
  quotaUsed: number; // requests/window we've consumed
  windowStartMs: number;
  windowDurationMs: number;
  lastContactMs: number;
  circuitState: 'closed' | 'open' | 'half-open';
  circuitFailCount: number;
  circuitOpenUntilMs: number;
  consecutiveRateLimits: number;
  latencyEwmaMs: number;
}

interface RateBudget {
  readonly networkId: string;
  capacity: number; // max tokens
  tokens: number; // current tokens
  refillRate: number; // tokens per second
  lastRefillMs: number;
  // Sliding window for burst detection
  windowRequests: number[];
  windowDurationMs: number;
  // Consumption history for forecasting
  history: Array<{ timestampMs: number; consumed: number }>;
  historyMaxSize: number;
}

interface FederatedRequest {
  readonly id: string;
  readonly sourceNetwork: string;
  readonly targetNetwork: string;
  readonly capability: string;
  readonly payload: unknown;
  readonly priority: RequestPriority;
  readonly createdMs: number;
  readonly deadlineMs: number | null;
  readonly coalescingKey: string | null; // requests with same key can be batched
  attempts: number;
  lastAttemptMs: number | null;
  status: 'pending' | 'inflight' | 'completed' | 'failed' | 'shed';
  result: unknown | null;
}

type RequestPriority = 'critical' | 'high' | 'normal' | 'low' | 'background';

const PRIORITY_WEIGHTS: Record<RequestPriority, number> = {
  critical: 100,
  high: 75,
  normal: 50,
  low: 25,
  background: 10,
};

interface QuotaNegotiation {
  readonly id: string;
  readonly initiatorId: string;
  readonly responderId: string;
  requestedQuota: number;
  offeredQuota: number;
  status: 'proposed' | 'counter' | 'accepted' | 'rejected' | 'expired';
  rounds: number;
  expiresMs: number;
}

interface FederationEvent {
  type: FederationEventType;
  timestampMs: number;
  data: Record<string, unknown>;
}

type FederationEventType =
  | 'request_sent'
  | 'request_completed'
  | 'request_failed'
  | 'request_shed'
  | 'request_coalesced'
  | 'budget_low'
  | 'budget_exhausted'
  | 'budget_refilled'
  | 'quota_negotiation_start'
  | 'quota_negotiation_complete'
  | 'quota_donated'
  | 'circuit_opened'
  | 'circuit_closed'
  | 'peer_added'
  | 'peer_removed'
  | 'fairness_violation';

interface FederationConfig {
  readonly localNetworkId: string;
  // Rate limiting
  defaultBudgetCapacity: number;
  defaultRefillRate: number; // tokens/sec
  windowDurationMs: number;
  // Degradation thresholds (fraction of budget remaining)
  shedBackgroundAt: number; // e.g. 0.3 = shed background when 30% budget left
  shedLowAt: number;
  shedNormalAt: number;
  shedHighAt: number; // only critical gets through below this
  // Circuit breaker
  circuitFailThreshold: number;
  circuitOpenDurationMs: number;
  rateLimitCircuitThreshold: number; // consecutive 429s to trip
  // Coalescing
  coalescingWindowMs: number;
  maxCoalesceSize: number;
  // Forecasting
  forecastWindowSize: number; // number of history points
  budgetWarningThreshold: number; // fraction remaining to warn
  // Fairness
  maxPeerBudgetFraction: number; // no peer can use more than this fraction
  // Quota negotiation
  negotiationTtlMs: number;
  maxNegotiationRounds: number;
  // General
  maxRetries: number;
  baseRetryMs: number;
}

// ============================================================
// Token Bucket with Sliding Window
// ============================================================

class HybridRateLimiter {
  private budget: RateBudget;

  constructor(
    networkId: string,
    capacity: number,
    refillRate: number,
    windowDurationMs: number,
  ) {
    this.budget = {
      networkId,
      capacity,
      tokens: capacity,
      refillRate,
      lastRefillMs: Date.now(),
      windowRequests: [],
      windowDurationMs,
      history: [],
      historyMaxSize: 100,
    };
  }

  tryConsume(count: number = 1, nowMs: number = Date.now()): boolean {
    this.refill(nowMs);
    this.pruneWindow(nowMs);

    if (this.budget.tokens < count) return false;

    this.budget.tokens -= count;
    for (let i = 0; i < count; i++) {
      this.budget.windowRequests.push(nowMs);
    }
    return true;
  }

  getRemaining(): number {
    this.refill(Date.now());
    return this.budget.tokens;
  }

  getRemainingFraction(): number {
    return this.getRemaining() / this.budget.capacity;
  }

  getWindowRate(nowMs: number = Date.now()): number {
    this.pruneWindow(nowMs);
    const windowSec = this.budget.windowDurationMs / 1000;
    return this.budget.windowRequests.length / windowSec;
  }

  recordConsumption(count: number, nowMs: number = Date.now()): void {
    this.budget.history.push({ timestampMs: nowMs, consumed: count });
    if (this.budget.history.length > this.budget.historyMaxSize) {
      this.budget.history.shift();
    }
  }

  /**
   * Linear regression forecast: predict tokens remaining at futureMs
   */
  forecastRemaining(futureMs: number, windowSize: number = 20): number {
    const h = this.budget.history;
    if (h.length < 3) return this.getRemaining();

    const recent = h.slice(-windowSize);
    const n = recent.length;
    // Compute consumption rate via least squares
    let sumX = 0, sumY = 0, sumXY = 0, sumXX = 0;
    const baseMs = recent[0].timestampMs;
    for (let i = 0; i < n; i++) {
      const x = (recent[i].timestampMs - baseMs) / 1000; // seconds
      const y = recent[i].consumed;
      sumX += x;
      sumY += y;
      sumXY += x * y;
      sumXX += x * x;
    }
    const denom = n * sumXX - sumX * sumX;
    if (Math.abs(denom) < 1e-9) return this.getRemaining();

    const slope = (n * sumXY - sumX * sumY) / denom; // consumption per second
    const futureSeconds = (futureMs - Date.now()) / 1000;
    const projectedConsumption = slope * futureSeconds;

    return Math.max(0, this.getRemaining() - projectedConsumption);
  }

  adjustCapacity(newCapacity: number): void {
    const ratio = this.budget.tokens / this.budget.capacity;
    this.budget.capacity = newCapacity;
    this.budget.tokens = Math.min(newCapacity, ratio * newCapacity);
  }

  private refill(nowMs: number): void {
    const elapsed = (nowMs - this.budget.lastRefillMs) / 1000;
    if (elapsed <= 0) return;
    this.budget.tokens = Math.min(
      this.budget.capacity,
      this.budget.tokens + elapsed * this.budget.refillRate,
    );
    this.budget.lastRefillMs = nowMs;
  }

  private pruneWindow(nowMs: number): void {
    const cutoff = nowMs - this.budget.windowDurationMs;
    while (
      this.budget.windowRequests.length > 0 &&
      this.budget.windowRequests[0] < cutoff
    ) {
      this.budget.windowRequests.shift();
    }
  }
}

// ============================================================
// Request Coalescer
// ============================================================

class RequestCoalescer {
  private pending: Map<string, FederatedRequest[]> = new Map();
  private timers: Map<string, ReturnType<typeof setTimeout>> = new Map();

  constructor(
    private windowMs: number,
    private maxSize: number,
    private onFlush: (key: string, requests: FederatedRequest[]) => void,
  ) {}

  add(request: FederatedRequest): boolean {
    if (!request.coalescingKey) return false;

    const key = `${request.targetNetwork}:${request.coalescingKey}`;
    let batch = this.pending.get(key);
    if (!batch) {
      batch = [];
      this.pending.set(key, batch);
    }
    batch.push(request);

    if (batch.length >= this.maxSize) {
      this.flush(key);
      return true;
    }

    if (!this.timers.has(key)) {
      this.timers.set(
        key,
        setTimeout(() => this.flush(key), this.windowMs),
      );
    }
    return true;
  }

  private flush(key: string): void {
    const batch = this.pending.get(key);
    if (!batch || batch.length === 0) return;

    this.pending.delete(key);
    const timer = this.timers.get(key);
    if (timer) {
      clearTimeout(timer);
      this.timers.delete(key);
    }
    this.onFlush(key, batch);
  }

  flushAll(): void {
    for (const key of this.pending.keys()) {
      this.flush(key);
    }
  }
}

// ============================================================
// Fairness Enforcer
// ============================================================

class FairnessEnforcer {
  private peerUsage: Map<string, number> = new Map(); // peerId -> requests this window
  private windowStartMs: number;
  private windowDurationMs: number;
  private totalBudget: number;
  private maxFraction: number;

  constructor(totalBudget: number, windowDurationMs: number, maxFraction: number) {
    this.totalBudget = totalBudget;
    this.windowDurationMs = windowDurationMs;
    this.maxFraction = maxFraction;
    this.windowStartMs = Date.now();
  }

  canPeerSend(peerId: string, nowMs: number = Date.now()): boolean {
    this.maybeResetWindow(nowMs);
    const used = this.peerUsage.get(peerId) ?? 0;
    return used < this.totalBudget * this.maxFraction;
  }

  recordPeerUsage(peerId: string, count: number = 1, nowMs: number = Date.now()): void {
    this.maybeResetWindow(nowMs);
    const used = this.peerUsage.get(peerId) ?? 0;
    this.peerUsage.set(peerId, used + count);
  }

  getPeerUsage(peerId: string): number {
    return this.peerUsage.get(peerId) ?? 0;
  }

  updateBudget(newTotal: number): void {
    this.totalBudget = newTotal;
  }

  private maybeResetWindow(nowMs: number): void {
    if (nowMs - this.windowStartMs > this.windowDurationMs) {
      this.peerUsage.clear();
      this.windowStartMs = nowMs;
    }
  }
}

// ============================================================
// Quota Negotiator
// ============================================================

class QuotaNegotiator {
  private negotiations: Map<string, QuotaNegotiation> = new Map();
  private nextId: number = 0;

  constructor(
    private localId: string,
    private maxRounds: number,
    private ttlMs: number,
  ) {}

  propose(responderId: string, requestedQuota: number, offeredQuota: number): QuotaNegotiation {
    const neg: QuotaNegotiation = {
      id: `neg-${this.nextId++}`,
      initiatorId: this.localId,
      responderId,
      requestedQuota,
      offeredQuota,
      status: 'proposed',
      rounds: 1,
      expiresMs: Date.now() + this.ttlMs,
    };
    this.negotiations.set(neg.id, neg);
    return neg;
  }

  counter(negId: string, newRequested: number, newOffered: number): QuotaNegotiation | null {
    const neg = this.negotiations.get(negId);
    if (!neg || neg.status === 'accepted' || neg.status === 'rejected') return null;
    if (neg.rounds >= this.maxRounds) {
      neg.status = 'rejected';
      return neg;
    }
    neg.requestedQuota = newRequested;
    neg.offeredQuota = newOffered;
    neg.status = 'counter';
    neg.rounds++;
    return neg;
  }

  accept(negId: string): QuotaNegotiation | null {
    const neg = this.negotiations.get(negId);
    if (!neg) return null;
    neg.status = 'accepted';
    return neg;
  }

  reject(negId: string): QuotaNegotiation | null {
    const neg = this.negotiations.get(negId);
    if (!neg) return null;
    neg.status = 'rejected';
    return neg;
  }

  pruneExpired(nowMs: number = Date.now()): number {
    let pruned = 0;
    for (const [id, neg] of this.negotiations) {
      if (nowMs > neg.expiresMs && neg.status !== 'accepted') {
        neg.status = 'expired';
        this.negotiations.delete(id);
        pruned++;
      }
    }
    return pruned;
  }

  getActive(): QuotaNegotiation[] {
    return [...this.negotiations.values()].filter(
      n => n.status === 'proposed' || n.status === 'counter',
    );
  }
}

// ============================================================
// Federation Router (main orchestrator)
// ============================================================

class FederationRouter {
  private peers: Map<string, FederationPeer> = new Map();
  private limiters: Map<string, HybridRateLimiter> = new Map();
  private fairness: FairnessEnforcer;
  private negotiator: QuotaNegotiator;
  private coalescer: RequestCoalescer;
  private pendingQueue: FederatedRequest[] = [];
  private events: FederationEvent[] = [];
  private config: FederationConfig;
  private nextRequestId: number = 0;

  constructor(config: FederationConfig) {
    this.config = config;
    this.fairness = new FairnessEnforcer(
      config.defaultBudgetCapacity,
      config.windowDurationMs,
      config.maxPeerBudgetFraction,
    );
    this.negotiator = new QuotaNegotiator(
      config.localNetworkId,
      config.maxNegotiationRounds,
      config.negotiationTtlMs,
    );
    this.coalescer = new RequestCoalescer(
      config.coalescingWindowMs,
      config.maxCoalesceSize,
      (key, requests) => this.handleCoalescedBatch(key, requests),
    );
  }

  // --- Peer Management ---

  addPeer(peer: Omit<FederationPeer, 'circuitState' | 'circuitFailCount' | 'circuitOpenUntilMs' | 'consecutiveRateLimits' | 'latencyEwmaMs'>): void {
    const fullPeer: FederationPeer = {
      ...peer,
      circuitState: 'closed',
      circuitFailCount: 0,
      circuitOpenUntilMs: 0,
      consecutiveRateLimits: 0,
      latencyEwmaMs: 100,
    };
    this.peers.set(peer.id, fullPeer);

    if (!this.limiters.has(peer.networkId)) {
      this.limiters.set(
        peer.networkId,
        new HybridRateLimiter(
          peer.networkId,
          this.config.defaultBudgetCapacity,
          this.config.defaultRefillRate,
          this.config.windowDurationMs,
        ),
      );
    }

    this.emit('peer_added', { peerId: peer.id, networkId: peer.networkId });
  }

  removePeer(peerId: string): boolean {
    const removed = this.peers.delete(peerId);
    if (removed) this.emit('peer_removed', { peerId });
    return removed;
  }

  // --- Request Submission ---

  submit(
    targetNetwork: string,
    capability: string,
    payload: unknown,
    priority: RequestPriority = 'normal',
    deadlineMs: number | null = null,
    coalescingKey: string | null = null,
  ): FederatedRequest {
    const request: FederatedRequest = {
      id: `freq-${this.nextRequestId++}`,
      sourceNetwork: this.config.localNetworkId,
      targetNetwork,
      capability,
      payload,
      priority,
      createdMs: Date.now(),
      deadlineMs,
      coalescingKey,
      attempts: 0,
      lastAttemptMs: null,
      status: 'pending',
      result: null,
    };

    // Check if should be shed based on budget
    if (this.shouldShed(request, targetNetwork)) {
      request.status = 'shed';
      this.emit('request_shed', {
        requestId: request.id,
        priority: request.priority,
        targetNetwork,
        reason: 'budget_degradation',
      });
      return request;
    }

    // Try coalescing
    if (coalescingKey && this.coalescer.add(request)) {
      return request;
    }

    this.pendingQueue.push(request);
    return request;
  }

  // --- Processing Loop ---

  async processNext(): Promise<FederatedRequest | null> {
    // Sort by priority (highest first), then by creation time (oldest first)
    this.pendingQueue.sort((a, b) => {
      const pw = PRIORITY_WEIGHTS[b.priority] - PRIORITY_WEIGHTS[a.priority];
      if (pw !== 0) return pw;
      return a.createdMs - b.createdMs;
    });

    // Remove expired requests
    const nowMs = Date.now();
    this.pendingQueue = this.pendingQueue.filter(r => {
      if (r.deadlineMs && nowMs > r.deadlineMs) {
        r.status = 'failed';
        this.emit('request_failed', { requestId: r.id, reason: 'deadline_exceeded' });
        return false;
      }
      return true;
    });

    if (this.pendingQueue.length === 0) return null;

    const request = this.pendingQueue[0];
    const peer = this.selectPeer(request.targetNetwork, request.capability);
    if (!peer) {
      // No available peer — leave in queue for retry
      return null;
    }

    // Budget check
    const limiter = this.limiters.get(request.targetNetwork);
    if (!limiter || !limiter.tryConsume(1, nowMs)) {
      this.emit('budget_exhausted', { targetNetwork: request.targetNetwork });
      return null;
    }

    // Fairness check
    if (!this.fairness.canPeerSend(peer.id, nowMs)) {
      this.emit('fairness_violation', { peerId: peer.id });
      return null;
    }

    // Execute
    this.pendingQueue.shift();
    request.status = 'inflight';
    request.attempts++;
    request.lastAttemptMs = nowMs;

    this.fairness.recordPeerUsage(peer.id, 1, nowMs);
    limiter.recordConsumption(1, nowMs);

    this.emit('request_sent', {
      requestId: request.id,
      peerId: peer.id,
      attempt: request.attempts,
      budgetRemaining: limiter.getRemainingFraction(),
    });

    // Simulate response (in real impl, this would be network call)
    const startMs = Date.now();
    try {
      // In production: const result = await this.sendToPeer(peer, request);
      request.status = 'completed';
      const latencyMs = Date.now() - startMs;
      peer.latencyEwmaMs = 0.8 * peer.latencyEwmaMs + 0.2 * latencyMs;
      peer.lastContactMs = Date.now();
      peer.consecutiveRateLimits = 0;

      if (peer.circuitState === 'half-open') {
        peer.circuitState = 'closed';
        peer.circuitFailCount = 0;
        this.emit('circuit_closed', { peerId: peer.id });
      }

      this.emit('request_completed', {
        requestId: request.id,
        peerId: peer.id,
        latencyMs,
      });
    } catch (err: unknown) {
      const error = err as { statusCode?: number; message?: string };
      if (error?.statusCode === 429) {
        peer.consecutiveRateLimits++;
        if (peer.consecutiveRateLimits >= this.config.rateLimitCircuitThreshold) {
          this.tripCircuit(peer);
        }
      } else {
        peer.circuitFailCount++;
        if (peer.circuitFailCount >= this.config.circuitFailThreshold) {
          this.tripCircuit(peer);
        }
      }

      if (request.attempts < this.config.maxRetries) {
        request.status = 'pending';
        this.pendingQueue.push(request);
      } else {
        request.status = 'failed';
        this.emit('request_failed', {
          requestId: request.id,
          peerId: peer.id,
          error: error?.message,
        });
      }
    }

    return request;
  }

  // --- Quota Donation ---

  /**
   * Cooperative rate sharing: peers with unused quota can donate to peers
   * approaching their limits. Donation is temporary (current window only).
   */
  donateQuota(fromNetworkId: string, toNetworkId: string, amount: number): boolean {
    const fromLimiter = this.limiters.get(fromNetworkId);
    const toLimiter = this.limiters.get(toNetworkId);
    if (!fromLimiter || !toLimiter) return false;

    const available = fromLimiter.getRemaining();
    if (available < amount * 2) return false; // keep at least half for self

    // Transfer: reduce from's capacity temporarily, increase to's
    fromLimiter.adjustCapacity(fromLimiter.getRemaining() - amount);
    toLimiter.adjustCapacity(toLimiter.getRemaining() + amount);

    this.emit('quota_donated', { from: fromNetworkId, to: toNetworkId, amount });
    return true;
  }

  // --- Budget Forecasting ---

  getForecast(networkId: string, horizonMs: number): {
    currentRemaining: number;
    forecastedRemaining: number;
    exhaustionRisk: 'low' | 'medium' | 'high' | 'critical';
  } {
    const limiter = this.limiters.get(networkId);
    if (!limiter) {
      return { currentRemaining: 0, forecastedRemaining: 0, exhaustionRisk: 'critical' };
    }

    const current = limiter.getRemainingFraction();
    const forecasted = limiter.forecastRemaining(Date.now() + horizonMs) /
      this.config.defaultBudgetCapacity;

    let risk: 'low' | 'medium' | 'high' | 'critical' = 'low';
    if (forecasted < 0.1) risk = 'critical';
    else if (forecasted < 0.25) risk = 'high';
    else if (forecasted < 0.5) risk = 'medium';

    return { currentRemaining: current, forecastedRemaining: forecasted, exhaustionRisk: risk };
  }

  // --- Stats ---

  getStats(): {
    peers: number;
    pendingRequests: number;
    networks: Array<{ id: string; budgetFraction: number; windowRate: number }>;
    eventCount: number;
  } {
    const networks = [...this.limiters.entries()].map(([id, limiter]) => ({
      id,
      budgetFraction: limiter.getRemainingFraction(),
      windowRate: limiter.getWindowRate(),
    }));
    return {
      peers: this.peers.size,
      pendingRequests: this.pendingQueue.length,
      networks,
      eventCount: this.events.length,
    };
  }

  getRecentEvents(count: number = 20): FederationEvent[] {
    return this.events.slice(-count);
  }

  // --- Internals ---

  private shouldShed(request: FederatedRequest, targetNetwork: string): boolean {
    const limiter = this.limiters.get(targetNetwork);
    if (!limiter) return true;

    const remaining = limiter.getRemainingFraction();
    const priority = request.priority;

    if (remaining < this.config.shedHighAt && priority !== 'critical') return true;
    if (remaining < this.config.shedNormalAt && (priority === 'low' || priority === 'background')) return true;
    if (remaining < this.config.shedLowAt && priority === 'background') return true;
    if (remaining < this.config.shedBackgroundAt && priority === 'background') return true;

    return false;
  }

  private selectPeer(networkId: string, capability: string): FederationPeer | null {
    const nowMs = Date.now();
    const candidates = [...this.peers.values()].filter(p => {
      if (p.networkId !== networkId) return false;
      if (!p.capabilities.includes(capability)) return false;
      if (p.circuitState === 'open' && nowMs < p.circuitOpenUntilMs) return false;
      if (p.circuitState === 'open' && nowMs >= p.circuitOpenUntilMs) {
        p.circuitState = 'half-open'; // allow one probe
      }
      return true;
    });

    if (candidates.length === 0) return null;

    // Weighted selection: trust * (1/latency)
    let totalWeight = 0;
    const weights = candidates.map(p => {
      const w = p.trustLevel / Math.max(1, p.latencyEwmaMs);
      totalWeight += w;
      return w;
    });

    let r = Math.random() * totalWeight;
    for (let i = 0; i < candidates.length; i++) {
      r -= weights[i];
      if (r <= 0) return candidates[i];
    }
    return candidates[candidates.length - 1];
  }

  private tripCircuit(peer: FederationPeer): void {
    peer.circuitState = 'open';
    peer.circuitOpenUntilMs = Date.now() + this.config.circuitOpenDurationMs;
    this.emit('circuit_opened', {
      peerId: peer.id,
      failCount: peer.circuitFailCount,
      rateLimitCount: peer.consecutiveRateLimits,
    });
  }

  private handleCoalescedBatch(key: string, requests: FederatedRequest[]): void {
    // Mark all as coalesced, submit as single batch request
    const primary = requests[0];
    for (let i = 1; i < requests.length; i++) {
      requests[i].status = 'completed'; // piggyback on primary
      this.emit('request_coalesced', {
        primaryId: primary.id,
        coalescedId: requests[i].id,
      });
    }
    this.pendingQueue.push(primary);
  }

  private emit(type: FederationEventType, data: Record<string, unknown>): void {
    this.events.push({ type, timestampMs: Date.now(), data });
    // Keep bounded
    if (this.events.length > 1000) {
      this.events = this.events.slice(-500);
    }
  }
}

// ============================================================
// Presets
// ============================================================

const PRESETS = {
  /** Conservative: low limits, aggressive shedding, tight fairness */
  conservative: (): FederationConfig => ({
    localNetworkId: 'local',
    defaultBudgetCapacity: 100,
    defaultRefillRate: 2,
    windowDurationMs: 60_000,
    shedBackgroundAt: 0.5,
    shedLowAt: 0.4,
    shedNormalAt: 0.25,
    shedHighAt: 0.1,
    circuitFailThreshold: 3,
    circuitOpenDurationMs: 30_000,
    rateLimitCircuitThreshold: 2,
    coalescingWindowMs: 500,
    maxCoalesceSize: 10,
    forecastWindowSize: 20,
    budgetWarningThreshold: 0.3,
    maxPeerBudgetFraction: 0.25,
    negotiationTtlMs: 60_000,
    maxNegotiationRounds: 3,
    maxRetries: 2,
    baseRetryMs: 1000,
  }),

  /** Balanced: moderate limits, gradual shedding */
  balanced: (): FederationConfig => ({
    localNetworkId: 'local',
    defaultBudgetCapacity: 500,
    defaultRefillRate: 10,
    windowDurationMs: 60_000,
    shedBackgroundAt: 0.3,
    shedLowAt: 0.2,
    shedNormalAt: 0.1,
    shedHighAt: 0.05,
    circuitFailThreshold: 5,
    circuitOpenDurationMs: 15_000,
    rateLimitCircuitThreshold: 3,
    coalescingWindowMs: 200,
    maxCoalesceSize: 20,
    forecastWindowSize: 30,
    budgetWarningThreshold: 0.25,
    maxPeerBudgetFraction: 0.4,
    negotiationTtlMs: 120_000,
    maxNegotiationRounds: 5,
    maxRetries: 3,
    baseRetryMs: 500,
  }),

  /** Aggressive: high throughput, minimal shedding, loose fairness */
  aggressive: (): FederationConfig => ({
    localNetworkId: 'local',
    defaultBudgetCapacity: 2000,
    defaultRefillRate: 50,
    windowDurationMs: 30_000,
    shedBackgroundAt: 0.15,
    shedLowAt: 0.1,
    shedNormalAt: 0.05,
    shedHighAt: 0.02,
    circuitFailThreshold: 10,
    circuitOpenDurationMs: 5_000,
    rateLimitCircuitThreshold: 5,
    coalescingWindowMs: 100,
    maxCoalesceSize: 50,
    forecastWindowSize: 50,
    budgetWarningThreshold: 0.15,
    maxPeerBudgetFraction: 0.6,
    negotiationTtlMs: 300_000,
    maxNegotiationRounds: 7,
    maxRetries: 5,
    baseRetryMs: 200,
  }),
};

export {
  FederationRouter,
  HybridRateLimiter,
  RequestCoalescer,
  FairnessEnforcer,
  QuotaNegotiator,
  PRESETS,
  type FederationConfig,
  type FederatedRequest,
  type FederationPeer,
  type FederationEvent,
  type RequestPriority,
  type QuotaNegotiation,
};
