import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  FederationRouter,
  HybridRateLimiter,
  RequestCoalescer,
  FairnessEnforcer,
  QuotaNegotiator,
  PRESETS,
  type FederationConfig,
  type FederatedRequest,
} from '../rate-aware-federation.js';

// ============================================================
// HybridRateLimiter
// ============================================================

describe('HybridRateLimiter', () => {
  it('allows consumption within capacity', () => {
    const limiter = new HybridRateLimiter('net-a', 10, 1, 60_000);
    expect(limiter.tryConsume(1, 1000)).toBe(true);
    expect(limiter.getRemaining()).toBeGreaterThanOrEqual(9);
  });

  it('rejects consumption when tokens exhausted', () => {
    const limiter = new HybridRateLimiter('net-a', 3, 0, 60_000);
    expect(limiter.tryConsume(3, 1000)).toBe(true);
    expect(limiter.tryConsume(1, 1000)).toBe(false);
  });

  it('refills tokens over time', () => {
    vi.useFakeTimers({ now: 1000 });
    const limiter = new HybridRateLimiter('net-a', 10, 5, 60_000);
    limiter.tryConsume(10, 1000); // drain all
    // 2 seconds later at refillRate=5/sec → 10 tokens
    expect(limiter.tryConsume(1, 3000)).toBe(true);
    vi.useRealTimers();
  });

  it('tracks window rate', () => {
    const limiter = new HybridRateLimiter('net-a', 100, 10, 10_000);
    limiter.tryConsume(5, 5000);
    // 5 requests in 10s window → 0.5/sec
    expect(limiter.getWindowRate(5000)).toBeCloseTo(0.5, 1);
  });

  it('prunes old window entries', () => {
    const limiter = new HybridRateLimiter('net-a', 100, 10, 1000);
    limiter.tryConsume(3, 1000);
    // After window expires, rate should be 0
    expect(limiter.getWindowRate(3000)).toBe(0);
  });

  it('getRemainingFraction returns ratio', () => {
    const limiter = new HybridRateLimiter('net-a', 10, 0, 60_000);
    limiter.tryConsume(5, 1000);
    // Fraction should be ~0.5
    expect(limiter.getRemainingFraction()).toBeCloseTo(0.5, 1);
  });

  it('records consumption history', () => {
    const limiter = new HybridRateLimiter('net-a', 100, 10, 60_000);
    limiter.recordConsumption(5, 1000);
    limiter.recordConsumption(3, 2000);
    // forecastRemaining needs >= 3 history points
    limiter.recordConsumption(7, 3000);
    const forecast = limiter.forecastRemaining(10000, 20);
    expect(typeof forecast).toBe('number');
  });

  it('forecastRemaining returns current remaining with < 3 history points', () => {
    const limiter = new HybridRateLimiter('net-a', 100, 0, 60_000);
    limiter.recordConsumption(5, 1000);
    const remaining = limiter.getRemaining();
    expect(limiter.forecastRemaining(10000)).toBeCloseTo(remaining, 0);
  });

  it('adjustCapacity scales tokens proportionally', () => {
    const limiter = new HybridRateLimiter('net-a', 100, 0, 60_000);
    limiter.tryConsume(50, 1000); // 50% remaining
    limiter.adjustCapacity(200);
    // Should have ~100 tokens (50% of 200)
    expect(limiter.getRemaining()).toBeCloseTo(100, 0);
  });
});

// ============================================================
// RequestCoalescer
// ============================================================

describe('RequestCoalescer', () => {
  let flushed: Array<{ key: string; requests: FederatedRequest[] }>;
  let coalescer: RequestCoalescer;

  beforeEach(() => {
    vi.useFakeTimers();
    flushed = [];
    coalescer = new RequestCoalescer(100, 3, (key, requests) => {
      flushed.push({ key, requests });
    });
  });

  afterEach(() => {
    coalescer.destroy();
    vi.useRealTimers();
  });

  function makeRequest(overrides: Partial<FederatedRequest> = {}): FederatedRequest {
    return {
      id: `req-${Math.random()}`,
      sourceNetwork: 'local',
      targetNetwork: 'remote',
      capability: 'compute',
      payload: {},
      priority: 'normal',
      createdMs: Date.now(),
      deadlineMs: null,
      coalescingKey: 'batch-key',
      attempts: 0,
      lastAttemptMs: null,
      status: 'pending',
      result: null,
      ...overrides,
    };
  }

  it('returns false for requests without coalescing key', () => {
    expect(coalescer.add(makeRequest({ coalescingKey: null }))).toBe(false);
  });

  it('flushes on max size', () => {
    coalescer.add(makeRequest());
    coalescer.add(makeRequest());
    expect(flushed).toHaveLength(0);
    coalescer.add(makeRequest()); // hits maxSize=3
    expect(flushed).toHaveLength(1);
    expect(flushed[0].requests).toHaveLength(3);
  });

  it('flushes on timer', () => {
    coalescer.add(makeRequest());
    coalescer.add(makeRequest());
    vi.advanceTimersByTime(150); // window is 100ms
    expect(flushed).toHaveLength(1);
    expect(flushed[0].requests).toHaveLength(2);
  });

  it('flushAll drains all pending batches', () => {
    coalescer.add(makeRequest({ targetNetwork: 'a', coalescingKey: 'k1' }));
    coalescer.add(makeRequest({ targetNetwork: 'b', coalescingKey: 'k2' }));
    coalescer.flushAll();
    expect(flushed).toHaveLength(2);
  });

  it('destroy clears timers and pending', () => {
    coalescer.add(makeRequest());
    coalescer.destroy();
    vi.advanceTimersByTime(200);
    expect(flushed).toHaveLength(0);
  });
});

// ============================================================
// FairnessEnforcer
// ============================================================

describe('FairnessEnforcer', () => {
  it('allows peer within budget fraction', () => {
    const enforcer = new FairnessEnforcer(100, 60_000, 0.5);
    expect(enforcer.canPeerSend('peer-1', 1000)).toBe(true);
  });

  it('blocks peer exceeding budget fraction', () => {
    const enforcer = new FairnessEnforcer(100, 60_000, 0.1); // max 10
    for (let i = 0; i < 10; i++) {
      enforcer.recordPeerUsage('peer-1', 1, 1000);
    }
    expect(enforcer.canPeerSend('peer-1', 1000)).toBe(false);
  });

  it('resets on new window', () => {
    vi.useFakeTimers({ now: 500 });
    const enforcer = new FairnessEnforcer(100, 1000, 0.1);
    for (let i = 0; i < 10; i++) {
      enforcer.recordPeerUsage('peer-1', 1, 500);
    }
    expect(enforcer.canPeerSend('peer-1', 500)).toBe(false);
    // After window expires (1500 - 500 = 1000 > windowDuration 1000? No, need >)
    expect(enforcer.canPeerSend('peer-1', 1501)).toBe(true);
    vi.useRealTimers();
  });

  it('tracks per-peer usage independently', () => {
    const enforcer = new FairnessEnforcer(100, 60_000, 0.1);
    enforcer.recordPeerUsage('peer-1', 10, 1000);
    expect(enforcer.canPeerSend('peer-1', 1000)).toBe(false);
    expect(enforcer.canPeerSend('peer-2', 1000)).toBe(true);
  });

  it('getPeerUsage returns 0 for unknown peer', () => {
    const enforcer = new FairnessEnforcer(100, 60_000, 0.5);
    expect(enforcer.getPeerUsage('unknown')).toBe(0);
  });

  it('updateBudget changes the total', () => {
    const enforcer = new FairnessEnforcer(10, 60_000, 0.5); // max 5
    enforcer.recordPeerUsage('peer-1', 5, 1000);
    expect(enforcer.canPeerSend('peer-1', 1000)).toBe(false);
    enforcer.updateBudget(100); // now max 50
    expect(enforcer.canPeerSend('peer-1', 1000)).toBe(true);
  });
});

// ============================================================
// QuotaNegotiator
// ============================================================

describe('QuotaNegotiator', () => {
  let negotiator: QuotaNegotiator;

  beforeEach(() => {
    negotiator = new QuotaNegotiator('local', 3, 60_000);
  });

  it('proposes a negotiation', () => {
    const neg = negotiator.propose('peer-1', 50, 20);
    expect(neg.status).toBe('proposed');
    expect(neg.requestedQuota).toBe(50);
    expect(neg.offeredQuota).toBe(20);
    expect(neg.rounds).toBe(1);
  });

  it('counters a negotiation', () => {
    const neg = negotiator.propose('peer-1', 50, 20);
    const countered = negotiator.counter(neg.id, 40, 30);
    expect(countered!.status).toBe('counter');
    expect(countered!.rounds).toBe(2);
    expect(countered!.requestedQuota).toBe(40);
  });

  it('rejects on max rounds exceeded', () => {
    const neg = negotiator.propose('peer-1', 50, 20);
    negotiator.counter(neg.id, 45, 25); // round 2
    negotiator.counter(neg.id, 40, 30); // round 3 = max
    const result = negotiator.counter(neg.id, 35, 35); // exceeds max
    expect(result!.status).toBe('rejected');
  });

  it('accepts a negotiation', () => {
    const neg = negotiator.propose('peer-1', 50, 20);
    const accepted = negotiator.accept(neg.id);
    expect(accepted!.status).toBe('accepted');
  });

  it('rejects a negotiation', () => {
    const neg = negotiator.propose('peer-1', 50, 20);
    const rejected = negotiator.reject(neg.id);
    expect(rejected!.status).toBe('rejected');
  });

  it('cannot counter an accepted negotiation', () => {
    const neg = negotiator.propose('peer-1', 50, 20);
    negotiator.accept(neg.id);
    expect(negotiator.counter(neg.id, 40, 30)).toBeNull();
  });

  it('returns null for unknown negotiation', () => {
    expect(negotiator.accept('nonexistent')).toBeNull();
    expect(negotiator.reject('nonexistent')).toBeNull();
    expect(negotiator.counter('nonexistent', 10, 10)).toBeNull();
  });

  it('prunes expired negotiations', () => {
    vi.useFakeTimers({ now: 1000 });
    negotiator = new QuotaNegotiator('local', 3, 100);
    negotiator.propose('peer-1', 50, 20);
    expect(negotiator.getActive()).toHaveLength(1);
    vi.advanceTimersByTime(200);
    const pruned = negotiator.pruneExpired();
    expect(pruned).toBe(1);
    expect(negotiator.getActive()).toHaveLength(0);
    vi.useRealTimers();
  });

  it('does not prune accepted negotiations', () => {
    vi.useFakeTimers({ now: 1000 });
    negotiator = new QuotaNegotiator('local', 3, 100);
    const neg = negotiator.propose('peer-1', 50, 20);
    negotiator.accept(neg.id);
    vi.advanceTimersByTime(200);
    expect(negotiator.pruneExpired()).toBe(0);
    vi.useRealTimers();
  });

  it('getActive filters by status', () => {
    const neg1 = negotiator.propose('peer-1', 50, 20);
    negotiator.propose('peer-2', 30, 10);
    negotiator.accept(neg1.id);
    expect(negotiator.getActive()).toHaveLength(1);
  });
});

// ============================================================
// FederationRouter
// ============================================================

describe('FederationRouter', () => {
  let config: FederationConfig;
  let router: FederationRouter;

  function makeConfig(overrides: Partial<FederationConfig> = {}): FederationConfig {
    return {
      ...PRESETS.conservative(),
      ...overrides,
    };
  }

  function addPeer(id: string, networkId: string, capabilities: string[] = ['compute']) {
    router.addPeer({
      id,
      networkId,
      endpoint: `http://${id}`,
      capabilities,
      trustLevel: 0.8,
      quotaGranted: 100,
      quotaUsed: 0,
      windowStartMs: Date.now(),
      windowDurationMs: 60_000,
      lastContactMs: Date.now(),
    });
  }

  beforeEach(() => {
    config = makeConfig();
    router = new FederationRouter(config);
  });

  afterEach(() => {
    router.destroy();
  });

  it('adds and removes peers', () => {
    addPeer('p1', 'net-a');
    let stats = router.getStats();
    expect(stats.peers).toBe(1);

    router.removePeer('p1');
    stats = router.getStats();
    expect(stats.peers).toBe(0);
  });

  it('removePeer returns false for unknown peer', () => {
    expect(router.removePeer('nonexistent')).toBe(false);
  });

  it('emits peer_added and peer_removed events', () => {
    addPeer('p1', 'net-a');
    router.removePeer('p1');
    const events = router.getRecentEvents();
    expect(events.map(e => e.type)).toContain('peer_added');
    expect(events.map(e => e.type)).toContain('peer_removed');
  });

  it('submits a request to pending queue', () => {
    addPeer('p1', 'net-a');
    const req = router.submit('net-a', 'compute', { data: 1 });
    expect(req.status).toBe('pending');
    expect(router.getStats().pendingRequests).toBe(1);
  });

  it('processes a request successfully', async () => {
    addPeer('p1', 'net-a');
    router.submit('net-a', 'compute', { data: 1 });
    const result = await router.processNext();
    expect(result).not.toBeNull();
    expect(result!.status).toBe('completed');
  });

  it('returns null when queue is empty', async () => {
    const result = await router.processNext();
    expect(result).toBeNull();
  });

  it('returns null when no peer available', async () => {
    router.submit('net-a', 'compute', { data: 1 });
    // No peers added
    const result = await router.processNext();
    expect(result).toBeNull();
  });

  it('sheds low-priority requests when budget is low', async () => {
    config = makeConfig({ defaultBudgetCapacity: 10, defaultRefillRate: 0, shedBackgroundAt: 0.5 });
    router = new FederationRouter(config);
    addPeer('p1', 'net-a');

    // Submit and process requests to actually consume budget tokens
    for (let i = 0; i < 6; i++) {
      router.submit('net-a', 'compute', { i }, 'critical');
    }
    for (let i = 0; i < 6; i++) {
      await router.processNext();
    }

    // Background request should be shed (only 4/10 = 40% remaining < shedBackgroundAt 50%)
    const bgReq = router.submit('net-a', 'compute', {}, 'background');
    expect(bgReq.status).toBe('shed');
  });

  it('sheds request when no limiter exists for target network', () => {
    router = new FederationRouter(config);
    // No peer added for 'unknown-net', so no limiter created
    const req = router.submit('unknown-net', 'compute', {});
    expect(req.status).toBe('shed');
  });

  it('processes requests in priority order', async () => {
    addPeer('p1', 'net-a');
    router.submit('net-a', 'compute', {}, 'low');
    router.submit('net-a', 'compute', {}, 'critical');
    router.submit('net-a', 'compute', {}, 'normal');

    const r1 = await router.processNext();
    expect(r1!.priority).toBe('critical');
    const r2 = await router.processNext();
    expect(r2!.priority).toBe('normal');
    const r3 = await router.processNext();
    expect(r3!.priority).toBe('low');
  });

  it('removes expired requests on processNext', async () => {
    vi.useFakeTimers({ now: 1000 });
    router = new FederationRouter(config);
    addPeer('p1', 'net-a');
    router.submit('net-a', 'compute', {}, 'normal', 500); // already expired
    vi.advanceTimersByTime(1000);
    const result = await router.processNext();
    expect(result).toBeNull();
    expect(router.getStats().pendingRequests).toBe(0);
    vi.useRealTimers();
  });

  it('getForecast returns risk assessment', () => {
    addPeer('p1', 'net-a');
    const forecast = router.getForecast('net-a', 60_000);
    expect(forecast.exhaustionRisk).toBe('low');
    expect(forecast.currentRemaining).toBeGreaterThan(0);
  });

  it('getForecast returns critical for unknown network', () => {
    const forecast = router.getForecast('nonexistent', 60_000);
    expect(forecast.exhaustionRisk).toBe('critical');
    expect(forecast.currentRemaining).toBe(0);
  });

  it('donateQuota transfers between networks', () => {
    addPeer('p1', 'net-a');
    addPeer('p2', 'net-b');
    const result = router.donateQuota('net-a', 'net-b', 10);
    expect(result).toBe(true);
    const events = router.getRecentEvents();
    expect(events.some(e => e.type === 'quota_donated')).toBe(true);
  });

  it('donateQuota fails for unknown network', () => {
    expect(router.donateQuota('nonexistent', 'also-nonexistent', 10)).toBe(false);
  });

  it('donateQuota fails when insufficient budget', () => {
    config = makeConfig({ defaultBudgetCapacity: 5, defaultRefillRate: 0 });
    router = new FederationRouter(config);
    addPeer('p1', 'net-a');
    addPeer('p2', 'net-b');
    // Trying to donate 10 from capacity 5 → not enough (need amount*2)
    expect(router.donateQuota('net-a', 'net-b', 10)).toBe(false);
  });

  it('getStats returns network info', () => {
    addPeer('p1', 'net-a');
    addPeer('p2', 'net-b');
    const stats = router.getStats();
    expect(stats.peers).toBe(2);
    expect(stats.networks).toHaveLength(2);
    expect(stats.networks[0]).toHaveProperty('budgetFraction');
    expect(stats.networks[0]).toHaveProperty('windowRate');
  });

  it('coalesces requests with same key', async () => {
    vi.useFakeTimers({ now: 1000 });
    config = makeConfig({ coalescingWindowMs: 50, maxCoalesceSize: 2 });
    router = new FederationRouter(config);
    addPeer('p1', 'net-a');

    const r1 = router.submit('net-a', 'compute', { a: 1 }, 'normal', null, 'batch');
    const r2 = router.submit('net-a', 'compute', { a: 2 }, 'normal', null, 'batch');

    // r2 triggers flush (maxCoalesceSize=2), coalescing marks r2 completed and queues r1
    expect(r2.status).toBe('completed');

    const processed = await router.processNext();
    expect(processed).not.toBeNull();
    
    const events = router.getRecentEvents();
    expect(events.some(e => e.type === 'request_coalesced')).toBe(true);
    vi.useRealTimers();
  });

  it('events are bounded to 1000', () => {
    addPeer('p1', 'net-a');
    // Generate > 1000 events
    for (let i = 0; i < 600; i++) {
      router.submit('net-a', 'compute', { i }, 'critical');
    }
    const events = router.getRecentEvents(2000);
    expect(events.length).toBeLessThanOrEqual(1000);
  });

  it('destroy cleans up everything', () => {
    addPeer('p1', 'net-a');
    router.submit('net-a', 'compute', {});
    router.destroy();
    const stats = router.getStats();
    expect(stats.peers).toBe(0);
    expect(stats.pendingRequests).toBe(0);
    expect(stats.eventCount).toBe(0);
  });
});

// ============================================================
// Presets
// ============================================================

describe('Presets', () => {
  it('conservative preset has expected properties', () => {
    const config = PRESETS.conservative();
    expect(config.defaultBudgetCapacity).toBe(100);
    expect(config.maxPeerBudgetFraction).toBe(0.25);
  });

  it('balanced preset has expected properties', () => {
    const config = PRESETS.balanced();
    expect(config.defaultBudgetCapacity).toBe(500);
  });

  it('aggressive preset has expected properties', () => {
    const config = PRESETS.aggressive();
    expect(config.defaultBudgetCapacity).toBe(2000);
  });

  it('each preset returns independent configs', () => {
    const a = PRESETS.conservative();
    const b = PRESETS.conservative();
    a.defaultBudgetCapacity = 999;
    expect(b.defaultBudgetCapacity).toBe(100);
  });
});
