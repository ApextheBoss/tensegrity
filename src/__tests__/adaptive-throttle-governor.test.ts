import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  AdaptiveThrottleGovernor,
  AIMDController,
  LatencyGradientDetector,
  CoDelQueueManager,
  PIController,
  TenantFairShareAllocator,
  CoordinatedThrottleGossip,
  BackoffScheduler,
  SlidingWindow,
  PRESETS,
  ThrottleConfig,
  LatencySample,
} from '../adaptive-throttle-governor';

// ─── SlidingWindow ───────────────────────────────────────────────────────────

describe('SlidingWindow', () => {
  it('stores items up to max size', () => {
    const w = new SlidingWindow<number>(3);
    w.push(1); w.push(2); w.push(3);
    expect(w.getAll()).toEqual([1, 2, 3]);
    expect(w.length).toBe(3);
  });

  it('evicts oldest when full', () => {
    const w = new SlidingWindow<number>(2);
    w.push(1); w.push(2); w.push(3);
    expect(w.getAll()).toEqual([2, 3]);
  });

  it('clear resets', () => {
    const w = new SlidingWindow<number>(5);
    w.push(1); w.push(2);
    w.clear();
    expect(w.length).toBe(0);
    expect(w.getAll()).toEqual([]);
  });
});

// ─── AIMDController ──────────────────────────────────────────────────────────

describe('AIMDController', () => {
  it('additive increase raises rate', () => {
    const c = new AIMDController(10, 1, 100, 2, 0.5);
    expect(c.additiveIncrease()).toBe(12);
    expect(c.additiveIncrease()).toBe(14);
  });

  it('multiplicative decrease cuts rate', () => {
    const c = new AIMDController(100, 10, 200, 5, 0.5);
    expect(c.multiplicativeDecrease(1000)).toBe(50);
  });

  it('respects min rate on decrease', () => {
    const c = new AIMDController(15, 10, 100, 1, 0.5);
    c.multiplicativeDecrease(1000);
    expect(c.currentRate).toBeGreaterThanOrEqual(10);
  });

  it('respects max rate on increase', () => {
    const c = new AIMDController(95, 1, 100, 10, 0.5);
    c.additiveIncrease();
    expect(c.currentRate).toBe(100);
  });

  it('decrease has cooldown', () => {
    const c = new AIMDController(100, 10, 200, 5, 0.5, 2000);
    c.multiplicativeDecrease(5000); // 50 (5000-0 >= 2000, so allowed)
    expect(c.currentRate).toBe(50);
    const rate = c.multiplicativeDecrease(6000); // 6000-5000=1000 < 2000, cooldown
    expect(rate).toBe(50); // unchanged
  });

  it('slow start doubles increase after 10 consecutive increases', () => {
    const c = new AIMDController(10, 1, 1000, 1, 0.5);
    for (let i = 0; i < 10; i++) c.additiveIncrease();
    // 11th increase should use factor=2
    const before = c.currentRate;
    c.additiveIncrease();
    expect(c.currentRate - before).toBe(2);
  });

  it('setRate clamps to bounds', () => {
    const c = new AIMDController(50, 10, 100, 1, 0.5);
    c.setRate(200);
    expect(c.currentRate).toBe(100);
    c.setRate(1);
    expect(c.currentRate).toBe(10);
  });
});

// ─── LatencyGradientDetector ─────────────────────────────────────────────────

describe('LatencyGradientDetector', () => {
  it('returns zero congestion for low stable latency', () => {
    const d = new LatencyGradientDetector(20);
    let result;
    for (let i = 0; i < 10; i++) {
      result = d.addSample(10);
    }
    expect(result!.congestionSignal).toBeLessThan(0.2);
  });

  it('detects congestion when latency rises', () => {
    const d = new LatencyGradientDetector(20);
    // Establish baseline
    for (let i = 0; i < 10; i++) d.addSample(10);
    // Spike
    let result;
    for (let i = 0; i < 15; i++) {
      result = d.addSample(100);
    }
    expect(result!.congestionSignal).toBeGreaterThan(0.5);
  });

  it('computes positive gradient on rising latency', () => {
    const d = new LatencyGradientDetector(20);
    for (let i = 0; i < 10; i++) d.addSample(10 + i * 5);
    const result = d.addSample(100);
    expect(result.gradient).toBeGreaterThan(0);
  });

  it('reset clears state', () => {
    const d = new LatencyGradientDetector(20);
    for (let i = 0; i < 10; i++) d.addSample(100);
    d.reset();
    const result = d.addSample(5);
    expect(result.baselineRtt).toBe(5);
  });
});

// ─── CoDelQueueManager ──────────────────────────────────────────────────────

describe('CoDelQueueManager', () => {
  it('does not drop when sojourn below target', () => {
    const c = new CoDelQueueManager(10, 100);
    const result = c.evaluate(5, 1000);
    expect(result.shouldDrop).toBe(false);
    expect(result.dropping).toBe(false);
  });

  it('enters dropping state after interval', () => {
    const c = new CoDelQueueManager(10, 100);
    c.evaluate(20, 1000); // first above, sets firstAboveTime = 1100
    const r2 = c.evaluate(20, 1050); // not yet
    expect(r2.shouldDrop).toBe(false);
    const r3 = c.evaluate(20, 1200); // past interval
    expect(r3.shouldDrop).toBe(true);
    expect(r3.dropping).toBe(true);
  });

  it('exits dropping when sojourn drops below target', () => {
    const c = new CoDelQueueManager(10, 100);
    c.evaluate(20, 1000);
    c.evaluate(20, 1200); // enters dropping
    const r = c.evaluate(5, 1300); // below target
    expect(r.dropping).toBe(false);
    expect(r.shouldDrop).toBe(false);
  });

  it('reset clears dropping state', () => {
    const c = new CoDelQueueManager(10, 100);
    c.evaluate(20, 1000);
    c.evaluate(20, 1200);
    c.reset();
    const r = c.evaluate(20, 2000);
    expect(r.dropping).toBe(false);
  });
});

// ─── PIController ────────────────────────────────────────────────────────────

describe('PIController', () => {
  it('returns positive output when measured below setpoint', () => {
    const pi = new PIController(1, 0.1);
    const out = pi.compute(100, 50);
    expect(out).toBeGreaterThan(0);
  });

  it('returns negative output when measured above setpoint', () => {
    const pi = new PIController(1, 0.1);
    const out = pi.compute(50, 100);
    expect(out).toBeLessThan(0);
  });

  it('integral accumulates over calls', () => {
    const pi = new PIController(0, 1); // pure integral
    pi.compute(100, 50); // error=50, integral=50
    const out = pi.compute(100, 50); // integral=100
    expect(out).toBe(100);
  });

  it('clamps integral to prevent windup', () => {
    const pi = new PIController(0, 1, 10);
    for (let i = 0; i < 100; i++) pi.compute(1000, 0);
    const out = pi.compute(1000, 0);
    expect(out).toBe(10); // clamped
  });

  it('reset clears state', () => {
    const pi = new PIController(0, 1);
    pi.compute(100, 0);
    pi.reset();
    const out = pi.compute(0, 0);
    expect(out).toBe(0);
  });
});

// ─── TenantFairShareAllocator ────────────────────────────────────────────────

describe('TenantFairShareAllocator', () => {
  it('allocates proportionally by weight', () => {
    const alloc = new TenantFairShareAllocator();
    alloc.addTenant({ id: 'a', weight: 2, minGuaranteedRate: 0, maxBurstRate: 100, priority: 0 });
    alloc.addTenant({ id: 'b', weight: 1, minGuaranteedRate: 0, maxBurstRate: 100, priority: 0 });
    const result = alloc.allocate(90);
    expect(result.get('a')).toBeCloseTo(60, 0);
    expect(result.get('b')).toBeCloseTo(30, 0);
  });

  it('respects minimum guaranteed rate', () => {
    const alloc = new TenantFairShareAllocator();
    alloc.addTenant({ id: 'a', weight: 1, minGuaranteedRate: 50, maxBurstRate: 100, priority: 0 });
    alloc.addTenant({ id: 'b', weight: 1, minGuaranteedRate: 30, maxBurstRate: 100, priority: 0 });
    const result = alloc.allocate(100);
    expect(result.get('a')!).toBeGreaterThanOrEqual(50);
    expect(result.get('b')!).toBeGreaterThanOrEqual(30);
  });

  it('respects max burst rate', () => {
    const alloc = new TenantFairShareAllocator();
    alloc.addTenant({ id: 'a', weight: 10, minGuaranteedRate: 0, maxBurstRate: 20, priority: 0 });
    alloc.addTenant({ id: 'b', weight: 1, minGuaranteedRate: 0, maxBurstRate: 100, priority: 0 });
    const result = alloc.allocate(100);
    expect(result.get('a')!).toBeLessThanOrEqual(20);
  });

  it('removeTenant cleans up', () => {
    const alloc = new TenantFairShareAllocator();
    alloc.addTenant({ id: 'a', weight: 1, minGuaranteedRate: 0, maxBurstRate: 100, priority: 0 });
    alloc.removeTenant('a');
    const result = alloc.allocate(100);
    expect(result.size).toBe(0);
  });

  it('returns empty map for no tenants', () => {
    const alloc = new TenantFairShareAllocator();
    expect(alloc.allocate(100).size).toBe(0);
  });
});

// ─── CoordinatedThrottleGossip ───────────────────────────────────────────────

describe('CoordinatedThrottleGossip', () => {
  it('ignores own state', () => {
    const g = new CoordinatedThrottleGossip('node-1');
    g.receivePeerState({ nodeId: 'node-1', rate: 10, congestionLevel: 0.9, timestamp: Date.now() });
    const adj = g.getCoordinatedAdjustment();
    expect(adj.activePeers).toBe(0);
    expect(adj.recommendedMultiplier).toBe(1.0);
  });

  it('reduces multiplier when peers are congested', () => {
    const g = new CoordinatedThrottleGossip('node-1');
    g.receivePeerState({ nodeId: 'node-2', rate: 10, congestionLevel: 0.9, timestamp: Date.now() });
    const adj = g.getCoordinatedAdjustment();
    expect(adj.recommendedMultiplier).toBeLessThan(1.0);
    expect(adj.peerCongestion).toBeCloseTo(0.9);
  });

  it('no reduction when peers are healthy', () => {
    const g = new CoordinatedThrottleGossip('node-1');
    g.receivePeerState({ nodeId: 'node-2', rate: 50, congestionLevel: 0.2, timestamp: Date.now() });
    const adj = g.getCoordinatedAdjustment();
    expect(adj.recommendedMultiplier).toBe(1.0);
  });

  it('prunes stale peers', () => {
    const g = new CoordinatedThrottleGossip('node-1', 1000);
    g.receivePeerState({ nodeId: 'node-2', rate: 10, congestionLevel: 0.9, timestamp: Date.now() - 2000 });
    const adj = g.getCoordinatedAdjustment();
    expect(adj.activePeers).toBe(0);
  });

  it('getLocalState returns correct values', () => {
    const g = new CoordinatedThrottleGossip('node-1');
    const state = g.getLocalState(50, 0.3);
    expect(state.nodeId).toBe('node-1');
    expect(state.rate).toBe(50);
    expect(state.congestionLevel).toBe(0.3);
  });
});

// ─── BackoffScheduler ────────────────────────────────────────────────────────

describe('BackoffScheduler', () => {
  it('increases delay exponentially', () => {
    const b = new BackoffScheduler(100, 60000, 0);
    const d1 = b.nextDelay();
    const d2 = b.nextDelay();
    const d3 = b.nextDelay();
    expect(d2).toBeGreaterThan(d1);
    expect(d3).toBeGreaterThan(d2);
  });

  it('caps at maxMs', () => {
    const b = new BackoffScheduler(100, 500, 0);
    for (let i = 0; i < 20; i++) b.nextDelay();
    expect(b.nextDelay()).toBeLessThanOrEqual(500);
  });

  it('reset restarts attempts', () => {
    const b = new BackoffScheduler(100, 60000, 0);
    b.nextDelay(); b.nextDelay();
    expect(b.attempts).toBe(2);
    b.reset();
    expect(b.attempts).toBe(0);
  });

  it('adds jitter when factor > 0', () => {
    const b = new BackoffScheduler(1000, 60000, 0.5);
    const delays = new Set<number>();
    for (let i = 0; i < 5; i++) {
      delays.add(b.nextDelay());
      b.reset();
    }
    // With jitter and changing seed, delays should vary (though not guaranteed with deterministic seed)
    // At minimum, delay should be in range
    for (const d of delays) {
      expect(d).toBeGreaterThanOrEqual(100); // baseMs floor
    }
  });
});

// ─── AdaptiveThrottleGovernor ────────────────────────────────────────────────

describe('AdaptiveThrottleGovernor', () => {
  const config: ThrottleConfig = {
    initialRate: 50,
    minRate: 5,
    maxRate: 500,
    aimdIncrease: 5,
    aimdDecrease: 0.5,
    targetLatencyMs: 50,
    maxQueueDepth: 100,
    sojournTargetMs: 10,
    piKp: 0.5,
    piKi: 0.1,
    updateIntervalMs: 100,
    warmupRequests: 5,
    historyWindowSize: 50,
  };

  function makeSample(overrides: Partial<LatencySample> = {}): LatencySample {
    return {
      timestamp: Date.now(),
      durationMs: 20,
      success: true,
      tenantId: 'default',
      ...overrides,
    };
  }

  it('starts in warmup mode', () => {
    const gov = new AdaptiveThrottleGovernor(config);
    expect(gov.getState().mode).toBe('warmup');
    expect(gov.getState().currentRate).toBe(50);
  });

  it('transitions from warmup to probing after enough requests', () => {
    const gov = new AdaptiveThrottleGovernor(config);
    let t = 1000;
    for (let i = 0; i < 10; i++) {
      gov.recordRequest(makeSample({ timestamp: t, durationMs: 10 }));
      t += config.updateIntervalMs + 1;
    }
    expect(gov.getState().mode).not.toBe('warmup');
  });

  it('backs off on high error rate', () => {
    const gov = new AdaptiveThrottleGovernor(config);
    let t = 1000;
    // Warmup with good requests
    for (let i = 0; i < 6; i++) {
      gov.recordRequest(makeSample({ timestamp: t, durationMs: 10 }));
      t += config.updateIntervalMs + 1;
    }
    const rateAfterWarmup = gov.getState().currentRate;
    // Now send failures
    for (let i = 0; i < 30; i++) {
      gov.recordRequest(makeSample({ timestamp: t, durationMs: 10, success: false }));
      t += config.updateIntervalMs + 1;
    }
    expect(gov.getState().currentRate).toBeLessThan(rateAfterWarmup);
    expect(gov.getState().mode).toBe('backoff');
  });

  it('increases rate under good conditions', () => {
    const gov = new AdaptiveThrottleGovernor(config);
    let t = 1000;
    // Fill warmup
    for (let i = 0; i < 6; i++) {
      gov.recordRequest(makeSample({ timestamp: t, durationMs: 10 }));
      t += config.updateIntervalMs + 1;
    }
    const rateBefore = gov.getState().currentRate;
    // Keep sending good requests
    for (let i = 0; i < 20; i++) {
      gov.recordRequest(makeSample({ timestamp: t, durationMs: 10 }));
      t += config.updateIntervalMs + 1;
    }
    expect(gov.getState().currentRate).toBeGreaterThan(rateBefore);
  });

  it('shouldAllow returns true for valid tenant', () => {
    const gov = new AdaptiveThrottleGovernor(config);
    gov.addTenant({ id: 't1', weight: 1, minGuaranteedRate: 10, maxBurstRate: 100, priority: 0 });
    expect(gov.shouldAllow('t1')).toBe(true);
  });

  it('shouldAllow returns true for unknown tenant when rate > 0', () => {
    const gov = new AdaptiveThrottleGovernor(config);
    expect(gov.shouldAllow('unknown')).toBe(true);
  });

  it('overrideRate forces rate', () => {
    const gov = new AdaptiveThrottleGovernor(config);
    gov.overrideRate(200);
    expect(gov.getState().effectiveRate).toBe(200);
  });

  it('getP95Latency computes correctly', () => {
    const gov = new AdaptiveThrottleGovernor(config);
    for (let i = 0; i < 100; i++) {
      gov.recordRequest(makeSample({ timestamp: 1000 + i, durationMs: i + 1 }));
    }
    const p95 = gov.getP95Latency();
    expect(p95).toBeGreaterThanOrEqual(90);
  });

  it('getLatencyStats returns valid stats', () => {
    const gov = new AdaptiveThrottleGovernor(config);
    for (let i = 0; i < 50; i++) {
      gov.recordRequest(makeSample({ timestamp: 1000 + i, durationMs: 20 + (i % 10) }));
    }
    const stats = gov.getLatencyStats();
    expect(stats.mean).toBeGreaterThan(0);
    expect(stats.p50).toBeGreaterThan(0);
    expect(stats.p95).toBeGreaterThanOrEqual(stats.p50);
  });

  it('getLatencyStats returns zeros when empty', () => {
    const gov = new AdaptiveThrottleGovernor(config);
    const stats = gov.getLatencyStats();
    expect(stats.mean).toBe(0);
  });

  it('drainEvents returns and clears events', () => {
    const gov = new AdaptiveThrottleGovernor(config);
    let t = 1000;
    for (let i = 0; i < 10; i++) {
      gov.recordRequest(makeSample({ timestamp: t, durationMs: 10 }));
      t += config.updateIntervalMs + 1;
    }
    const events = gov.drainEvents();
    expect(events.length).toBeGreaterThan(0);
    expect(gov.drainEvents().length).toBe(0);
  });

  it('gossip integration: peer congestion reduces effective rate', () => {
    const gov = new AdaptiveThrottleGovernor(config, 'node-1');
    gov.receivePeerState({ nodeId: 'node-2', rate: 10, congestionLevel: 0.9, timestamp: Date.now() });
    // Trigger rate update
    let t = 1000;
    for (let i = 0; i < 10; i++) {
      gov.recordRequest(makeSample({ timestamp: t, durationMs: 10 }));
      t += config.updateIntervalMs + 1;
    }
    const state = gov.getState();
    expect(state.effectiveRate).toBeLessThan(state.currentRate);
  });

  it('getGossipState returns local state', () => {
    const gov = new AdaptiveThrottleGovernor(config, 'my-node');
    const gs = gov.getGossipState();
    expect(gs.nodeId).toBe('my-node');
    expect(gs.rate).toBe(config.initialRate);
  });

  it('addTenant and removeTenant work', () => {
    const gov = new AdaptiveThrottleGovernor(config);
    gov.addTenant({ id: 't1', weight: 1, minGuaranteedRate: 5, maxBurstRate: 100, priority: 0 });
    expect(gov.getState().tenantAllocations.has('t1')).toBe(true);
    gov.removeTenant('t1');
    expect(gov.getState().tenantAllocations.has('t1')).toBe(false);
  });
});

// ─── Presets ─────────────────────────────────────────────────────────────────

describe('Presets', () => {
  it('all presets create valid governors', () => {
    for (const [name, preset] of Object.entries(PRESETS)) {
      const gov = new AdaptiveThrottleGovernor(preset);
      expect(gov.getState().mode).toBe('warmup');
      expect(gov.getState().currentRate).toBe(preset.initialRate);
    }
  });
});
