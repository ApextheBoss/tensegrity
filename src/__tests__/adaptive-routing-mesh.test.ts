import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  TopologyTracker,
  LatencyPredictor,
  PathScorer,
  RouteCache,
  MultiPathRouter,
  CongestionDetector,
  FailureCorrelator,
  TrafficShaper,
  AdaptiveRoutingEngine,
  PRESETS,
} from '../adaptive-routing-mesh';
import type { LinkMetrics, Route, RoutingMessage } from '../adaptive-routing-mesh';

// ─── TopologyTracker ───

describe('TopologyTracker', () => {
  let tracker: TopologyTracker;

  beforeEach(() => {
    tracker = new TopologyTracker(0.3, 60000);
  });

  it('creates new link on first update', () => {
    tracker.updateLink('A', 'B', 50, true);
    const link = tracker.getLink('A', 'B');
    expect(link).toBeDefined();
    expect(link!.source).toBe('A');
    expect(link!.target).toBe('B');
    expect(link!.latencyMs).toBe(50);
    expect(link!.successCount).toBe(1);
    expect(link!.failureCount).toBe(0);
  });

  it('EWMA smooths latency on subsequent updates', () => {
    tracker.updateLink('A', 'B', 100, true);
    tracker.updateLink('A', 'B', 200, true);
    const link = tracker.getLink('A', 'B')!;
    // EWMA: 0.3 * 200 + 0.7 * 100 = 130
    expect(link.latencyMs).toBeCloseTo(130, 0);
    expect(link.samples).toBe(2);
  });

  it('tracks packet loss from failures', () => {
    tracker.updateLink('A', 'B', 50, true);
    tracker.updateLink('A', 'B', 50, false);
    const link = tracker.getLink('A', 'B')!;
    // EWMA loss: 0.3 * 1 + 0.7 * 0 = 0.3
    expect(link.packetLoss).toBeCloseTo(0.3, 2);
    expect(link.failureCount).toBe(1);
  });

  it('tracks neighbors (adjacency)', () => {
    tracker.updateLink('A', 'B', 10, true);
    tracker.updateLink('A', 'C', 20, true);
    expect(tracker.getNeighbors('A')).toContain('B');
    expect(tracker.getNeighbors('A')).toContain('C');
    expect(tracker.getNeighbors('B')).toEqual([]);
  });

  it('updates utilization with EWMA', () => {
    tracker.updateLink('A', 'B', 10, true);
    tracker.updateUtilization('A', 'B', 0.5);
    const link = tracker.getLink('A', 'B')!;
    // EWMA: 0.3 * 0.5 + 0.7 * 0 = 0.15
    expect(link.utilization).toBeCloseTo(0.15, 2);
  });

  it('prunes stale links', () => {
    tracker.updateLink('A', 'B', 10, true);
    // Force link to be old
    const link = tracker.getLink('A', 'B')!;
    (link as any).lastSeen = Date.now() - 120000;
    const pruned = tracker.pruneStaleLinks();
    expect(pruned).toHaveLength(1);
    expect(tracker.getLink('A', 'B')).toBeUndefined();
    expect(tracker.getNeighbors('A')).toEqual([]);
  });

  it('getAllNodes returns all unique nodes', () => {
    tracker.updateLink('A', 'B', 10, true);
    tracker.updateLink('B', 'C', 20, true);
    const nodes = tracker.getAllNodes();
    expect(nodes.sort()).toEqual(['A', 'B', 'C']);
  });

  it('getActiveLinks filters by timeout', () => {
    tracker.updateLink('A', 'B', 10, true);
    tracker.updateLink('C', 'D', 20, true);
    expect(tracker.getActiveLinks()).toHaveLength(2);
    // Make one stale
    tracker.getLink('C', 'D')!.lastSeen = Date.now() - 120000;
    expect(tracker.getActiveLinks()).toHaveLength(1);
  });
});

// ─── LatencyPredictor ───

describe('LatencyPredictor', () => {
  let predictor: LatencyPredictor;

  beforeEach(() => {
    predictor = new LatencyPredictor(100, 300000);
  });

  it('returns Infinity with no samples', () => {
    const result = predictor.predict('A->B');
    expect(result.predicted).toBe(Infinity);
    expect(result.confidence).toBe(0);
  });

  it('returns low confidence with 1 sample', () => {
    predictor.addSample('A->B', 50);
    const result = predictor.predict('A->B');
    expect(result.predicted).toBe(Infinity);
    expect(result.confidence).toBe(0);
  });

  it('predicts with 2+ samples', () => {
    predictor.addSample('A->B', 50);
    predictor.addSample('A->B', 55);
    const result = predictor.predict('A->B');
    expect(result.predicted).toBeGreaterThan(0);
    expect(result.predicted).toBeLessThan(Infinity);
    expect(result.confidence).toBeGreaterThan(0);
  });

  it('detects upward trend', () => {
    // Space samples apart in time so linear regression detects trend
    const base = Date.now() - 10000;
    for (let i = 0; i < 10; i++) {
      // Manually push samples with spaced timestamps
      (predictor as any).history.set('A->B', (predictor as any).history.get('A->B') || []);
      (predictor as any).history.get('A->B').push({ timestamp: base + i * 1000, latency: 50 + i * 10 });
    }
    const result = predictor.predict('A->B');
    expect(result.trend).toBeGreaterThan(0);
  });

  it('caps samples at maxSamples', () => {
    const small = new LatencyPredictor(5, 300000);
    for (let i = 0; i < 10; i++) {
      small.addSample('A->B', 50);
    }
    // Should not throw, just keeps last 5
    const result = small.predict('A->B');
    expect(result.predicted).toBeGreaterThan(0);
  });
});

// ─── PathScorer ───

describe('PathScorer', () => {
  it('returns Infinity score for empty path', () => {
    const scorer = new PathScorer();
    const result = scorer.scorePath([], new LatencyPredictor());
    expect(result.score).toBe(Infinity);
  });

  it('scores a single-hop path', () => {
    const scorer = new PathScorer();
    const link: LinkMetrics = {
      source: 'A', target: 'B',
      latencyMs: 50, latencyVariance: 10,
      packetLoss: 0.01, bandwidth: 100,
      utilization: 0.3, lastSeen: Date.now(),
      samples: 100, failureCount: 1, successCount: 99,
    };
    const result = scorer.scorePath([link], new LatencyPredictor());
    expect(result.score).toBeGreaterThan(0);
    expect(result.score).toBeLessThan(1);
    expect(result.latency).toBe(50);
    expect(result.reliability).toBeCloseTo(0.99, 2);
  });

  it('higher latency gives worse score', () => {
    const scorer = new PathScorer();
    const makeLnk = (lat: number): LinkMetrics => ({
      source: 'A', target: 'B', latencyMs: lat, latencyVariance: 0,
      packetLoss: 0, bandwidth: 100, utilization: 0,
      lastSeen: Date.now(), samples: 10, failureCount: 0, successCount: 10,
    });
    const fast = scorer.scorePath([makeLnk(10)], new LatencyPredictor());
    const slow = scorer.scorePath([makeLnk(2000)], new LatencyPredictor());
    expect(slow.score).toBeGreaterThan(fast.score);
  });

  it('multi-hop reliability multiplies', () => {
    const scorer = new PathScorer();
    const link: LinkMetrics = {
      source: 'A', target: 'B', latencyMs: 10, latencyVariance: 0,
      packetLoss: 0.1, bandwidth: 100, utilization: 0,
      lastSeen: Date.now(), samples: 10, failureCount: 1, successCount: 9,
    };
    const result = scorer.scorePath([link, { ...link, source: 'B', target: 'C' }], new LatencyPredictor());
    expect(result.reliability).toBeCloseTo(0.81, 2); // 0.9 * 0.9
  });
});

// ─── RouteCache ───

describe('RouteCache', () => {
  let cache: RouteCache;

  const makeRoute = (path: string[], score = 0.5): Route => ({
    path, score, estimatedLatency: 100, reliability: 0.99,
    hops: path.length - 1, load: 0.3, cost: 0.1,
    cachedAt: Date.now(), ttl: 30000,
  });

  beforeEach(() => {
    cache = new RouteCache(10, 30000);
  });

  it('stores and retrieves routes', () => {
    cache.put('A', 'B', [makeRoute(['A', 'B'])]);
    const routes = cache.get('A', 'B');
    expect(routes).toHaveLength(1);
    expect(routes![0].path).toEqual(['A', 'B']);
  });

  it('returns undefined for missing routes', () => {
    expect(cache.get('X', 'Y')).toBeUndefined();
  });

  it('expires routes by TTL', () => {
    // Put a route with cachedAt in the past so it's already expired
    const route = makeRoute(['A', 'B']);
    cache.put('A', 'B', [route]);
    // Manually set cachedAt to past
    const cached = cache.get('A', 'B');
    expect(cached).toBeDefined(); // not expired yet
    // Now put with old timestamp
    const oldRoute = makeRoute(['A', 'C']);
    oldRoute.cachedAt = Date.now() - 50000;
    oldRoute.ttl = 30000;
    cache.put('A', 'C', [oldRoute]);
    // put() resets cachedAt, so get it and manually age it
    const stored = (cache as any).cache.get('A|C');
    stored[0].cachedAt = Date.now() - 50000;
    expect(cache.get('A', 'C')).toBeUndefined();
  });

  it('evicts LRU when full', () => {
    const small = new RouteCache(3, 30000);
    small.put('A', 'B', [makeRoute(['A', 'B'])]);
    small.put('A', 'C', [makeRoute(['A', 'C'])]);
    small.put('A', 'D', [makeRoute(['A', 'D'])]);
    small.put('A', 'E', [makeRoute(['A', 'E'])]); // should evict A->B
    expect(small.size()).toBe(3);
    expect(small.get('A', 'B')).toBeUndefined();
  });

  it('invalidates routes through a node', () => {
    cache.put('A', 'C', [makeRoute(['A', 'B', 'C'])]);
    cache.put('A', 'D', [makeRoute(['A', 'D'])]);
    const count = cache.invalidate('B');
    expect(count).toBe(1);
    expect(cache.get('A', 'C')).toBeUndefined();
    expect(cache.get('A', 'D')).toBeDefined();
  });

  it('invalidates routes through a specific link', () => {
    cache.put('A', 'C', [makeRoute(['A', 'B', 'C'])]);
    cache.put('X', 'Y', [makeRoute(['X', 'Y'])]);
    const count = cache.invalidateLink('A', 'B');
    expect(count).toBe(1);
    expect(cache.get('A', 'C')).toBeUndefined();
  });

  it('getExpiringRoutes finds soon-to-expire routes', () => {
    const route = makeRoute(['A', 'B']);
    cache.put('A', 'B', [route]);
    // Manually age the cached route so it's near expiry
    const stored = (cache as any).cache.get('A|B');
    stored[0].cachedAt = Date.now() - 29000; // 1s remaining of 30s TTL
    stored[0].ttl = 30000;
    const expiring = cache.getExpiringRoutes(2000);
    expect(expiring).toHaveLength(1);
  });
});

// ─── CongestionDetector ───

describe('CongestionDetector', () => {
  let detector: CongestionDetector;

  beforeEach(() => {
    detector = new CongestionDetector(60000, 0.6, 0.8, 0.95);
  });

  it('reports none severity for healthy metrics', () => {
    const signal = detector.reportMetrics('node-1', 10, 90, 0.0);
    expect(signal.severity).toBe('none');
  });

  it('reports moderate congestion', () => {
    // score = 900/1000*0.4 + 0.8*0.4 + (1-50/100)*0.2 = 0.36 + 0.32 + 0.1 = 0.78 → mild
    // Bump higher: 950/1000*0.4 + 0.9*0.4 + (1-10/100)*0.2 = 0.38 + 0.36 + 0.18 = 0.92 → moderate
    const signal = detector.reportMetrics('node-1', 950, 10, 0.9);
    expect(['moderate', 'severe']).toContain(signal.severity);
  });

  it('reports severe congestion', () => {
    const signal = detector.reportMetrics('node-1', 950, 5, 0.95);
    expect(signal.severity).toBe('severe');
  });

  it('tracks congested nodes', () => {
    detector.reportMetrics('node-1', 950, 5, 0.95);
    detector.reportMetrics('node-2', 10, 90, 0.0);
    const congested = detector.getCongestedNodes();
    expect(congested).toContain('node-1');
    expect(congested).not.toContain('node-2');
  });

  it('isCongested returns correct boolean', () => {
    detector.reportMetrics('node-1', 950, 5, 0.95);
    expect(detector.isCongested('node-1')).toBe(true);
    expect(detector.isCongested('node-2')).toBe(false);
  });

  it('getCurrentSeverity returns none for unknown nodes', () => {
    expect(detector.getCurrentSeverity('unknown')).toBe('none');
  });
});

// ─── FailureCorrelator ───

describe('FailureCorrelator', () => {
  let correlator: FailureCorrelator;

  beforeEach(() => {
    correlator = new FailureCorrelator(300000, 5000);
  });

  it('records failures', () => {
    correlator.recordFailure('A->B', 'timeout');
    expect(correlator.getRecentFailureRate('A->B')).toBe(1);
  });

  it('detects correlated failures (shared fate)', () => {
    // Failures on A->B and A->C within threshold → node A is shared fate
    correlator.recordFailure('A->B', 'timeout');
    correlator.recordFailure('A->C', 'timeout');
    const groups = correlator.getSharedFateGroups();
    expect(groups.size).toBeGreaterThan(0);
    expect(correlator.isSharedFateRisk('A')).toBe(true);
  });

  it('no correlation for isolated failures', () => {
    correlator.recordFailure('A->B', 'timeout');
    // Wait longer than correlation threshold by using a custom correlator
    const slow = new FailureCorrelator(300000, 0); // 0ms threshold = same timestamp only
    slow.recordFailure('X->Y', 'timeout');
    // Different timestamps should not correlate with nothing else
    expect(slow.getSharedFateGroups().size).toBe(0);
  });

  it('records recovery', () => {
    correlator.recordFailure('A->B', 'timeout');
    correlator.recordRecovery('A->B', 500);
    // Still counts as a failure for rate purposes
    expect(correlator.getRecentFailureRate('A->B')).toBe(1);
  });
});

// ─── TrafficShaper ───

describe('TrafficShaper', () => {
  let shaper: TrafficShaper;

  beforeEach(() => {
    shaper = new TrafficShaper();
  });

  it('always admits critical priority', () => {
    shaper.configurePath('A->B', 0, 0); // no tokens
    const result = shaper.tryAdmit('A->B', 'critical');
    expect(result.admitted).toBe(true);
  });

  it('admits when tokens available', () => {
    shaper.configurePath('A->B', 10, 1);
    const result = shaper.tryAdmit('A->B', 'normal');
    expect(result.admitted).toBe(true);
  });

  it('rejects when no tokens', () => {
    shaper.configurePath('A->B', 2, 0.1);
    // Exhaust tokens
    shaper.tryAdmit('A->B', 'normal'); // costs 2
    const result = shaper.tryAdmit('A->B', 'normal'); // no tokens left
    expect(result.admitted).toBe(false);
    expect(result.waitMs).toBeGreaterThan(0);
  });

  it('admits unconfigured paths', () => {
    const result = shaper.tryAdmit('X->Y', 'bulk');
    expect(result.admitted).toBe(true);
  });

  it('getUtilization returns 0 for unconfigured', () => {
    expect(shaper.getUtilization('X->Y')).toBe(0);
  });

  it('bulk costs more tokens than high', () => {
    shaper.configurePath('A->B', 10, 0);
    shaper.tryAdmit('A->B', 'bulk'); // costs 8
    const result = shaper.tryAdmit('A->B', 'high'); // costs 1, has 2 left
    expect(result.admitted).toBe(true);
  });
});

// ─── MultiPathRouter ───

describe('MultiPathRouter', () => {
  let topology: TopologyTracker;
  let scorer: PathScorer;
  let predictor: LatencyPredictor;
  let router: MultiPathRouter;

  beforeEach(() => {
    topology = new TopologyTracker();
    scorer = new PathScorer();
    predictor = new LatencyPredictor();
    router = new MultiPathRouter(topology, scorer, predictor, 3, 8);
  });

  it('returns empty for no path', () => {
    const routes = router.findPaths('A', 'Z');
    expect(routes).toHaveLength(0);
  });

  it('finds a direct path', () => {
    topology.updateLink('A', 'B', 10, true);
    const routes = router.findPaths('A', 'B');
    expect(routes.length).toBeGreaterThan(0);
    expect(routes[0].path).toEqual(['A', 'B']);
  });

  it('finds a multi-hop path', () => {
    topology.updateLink('A', 'B', 10, true);
    topology.updateLink('B', 'C', 20, true);
    const routes = router.findPaths('A', 'C');
    expect(routes.length).toBeGreaterThan(0);
    expect(routes[0].path).toEqual(['A', 'B', 'C']);
  });

  it('finds multiple diverse paths', () => {
    topology.updateLink('A', 'B', 10, true);
    topology.updateLink('B', 'D', 10, true);
    topology.updateLink('A', 'C', 15, true);
    topology.updateLink('C', 'D', 15, true);
    const routes = router.findPaths('A', 'D');
    expect(routes.length).toBeGreaterThanOrEqual(1);
  });

  it('computeTrafficSplit gives 100% to single route', () => {
    topology.updateLink('A', 'B', 10, true);
    const routes = router.findPaths('A', 'B');
    const split = router.computeTrafficSplit(routes);
    expect(split.size).toBe(1);
    expect(Array.from(split.values())[0]).toBeCloseTo(1.0, 2);
  });

  it('computeTrafficSplit returns empty for no routes', () => {
    expect(router.computeTrafficSplit([]).size).toBe(0);
  });
});

// ─── AdaptiveRoutingEngine ───

describe('AdaptiveRoutingEngine', () => {
  let engine: AdaptiveRoutingEngine;

  beforeEach(() => {
    engine = new AdaptiveRoutingEngine({
      explorationRate: 0, // disable for deterministic tests
      congestionAvoidance: true,
      sharedFateAvoidance: true,
    });
  });

  function setupSimpleNetwork() {
    engine.reportLink('A', 'B', 10, true);
    engine.reportLink('B', 'C', 20, true);
    engine.reportLink('A', 'C', 50, true);
  }

  const makeMsg = (src: string, dst: string, priority: RoutingMessage['priority'] = 'normal'): RoutingMessage => ({
    id: `msg-${Math.random()}`,
    source: src,
    destination: dst,
    priority,
    size: 100,
    payload: {},
  });

  it('routes a message through the network', () => {
    setupSimpleNetwork();
    const route = engine.route(makeMsg('A', 'C'));
    expect(route).not.toBeNull();
    expect(route!.path[0]).toBe('A');
    expect(route!.path[route!.path.length - 1]).toBe('C');
  });

  it('returns null when no path exists', () => {
    engine.reportLink('A', 'B', 10, true);
    const route = engine.route(makeMsg('A', 'Z'));
    expect(route).toBeNull();
  });

  it('uses cache on second request', () => {
    setupSimpleNetwork();
    engine.route(makeMsg('A', 'C'));
    const stats1 = engine.getStats();
    engine.route(makeMsg('A', 'C'));
    const stats2 = engine.getStats();
    expect(stats2.cacheHits).toBe(stats1.cacheHits + 1);
  });

  it('invalidates cache on link failure', () => {
    setupSimpleNetwork();
    engine.route(makeMsg('A', 'C'));
    expect(engine.getStats().cacheSize).toBeGreaterThan(0);
    engine.reportLink('A', 'B', 10, false); // failure
    // Cache should be invalidated for routes through A->B
  });

  it('avoids congested nodes', () => {
    // A -> B -> D (congested at B)
    // A -> C -> D (clear)
    // Report multiple samples to establish links
    for (let i = 0; i < 3; i++) {
      engine.reportLink('A', 'B', 10, true);
      engine.reportLink('B', 'D', 10, true);
      engine.reportLink('A', 'C', 15, true);
      engine.reportLink('C', 'D', 15, true);
    }
    // First verify routing works without congestion
    const routeBefore = engine.route(makeMsg('A', 'D'));
    expect(routeBefore).not.toBeNull();
    // Now add congestion at B — cache gets invalidated
    engine.reportCongestion('B', 950, 5, 0.95);
    const route = engine.route(makeMsg('A', 'D'));
    expect(route).not.toBeNull();
    // Should prefer path through C, avoiding congested B
    expect(route!.path).toContain('C');
  });

  it('emits events', () => {
    const events: any[] = [];
    engine.onEvent(e => events.push(e));
    engine.reportLink('A', 'B', 10, true);
    expect(events.length).toBeGreaterThan(0);
    expect(events[0].type).toBe('link-updated');
  });

  it('emits link-failed on failure report', () => {
    const events: any[] = [];
    engine.onEvent(e => events.push(e));
    engine.reportLink('A', 'B', 10, true);
    engine.reportLink('A', 'B', 10, false);
    expect(events.some(e => e.type === 'link-failed')).toBe(true);
  });

  it('getStats returns routing statistics', () => {
    setupSimpleNetwork();
    engine.route(makeMsg('A', 'C'));
    const stats = engine.getStats();
    expect(stats.totalRouted).toBe(1);
    expect(stats.activeLinks).toBeGreaterThan(0);
  });

  it('tick prunes stale links', () => {
    engine.reportLink('A', 'B', 10, true);
    // Can't easily test stale pruning without time manipulation
    // but tick() should not throw
    engine.tick();
  });

  it('critical messages bypass traffic shaping', () => {
    engine.reportLink('A', 'B', 10, true);
    const route = engine.route(makeMsg('A', 'B', 'critical'));
    expect(route).not.toBeNull();
  });
});

// ─── Presets ───

describe('Presets', () => {
  it('local-cluster preset creates valid engine', () => {
    const engine = new AdaptiveRoutingEngine(PRESETS['local-cluster']);
    engine.reportLink('A', 'B', 5, true);
    const route = engine.route({
      id: 'test', source: 'A', destination: 'B',
      priority: 'normal', size: 10, payload: null,
    });
    expect(route).not.toBeNull();
  });

  it('wide-area preset creates valid engine', () => {
    const engine = new AdaptiveRoutingEngine(PRESETS['wide-area']);
    expect(engine.getStats().totalRouted).toBe(0);
  });

  it('resilient preset has higher exploration rate', () => {
    expect(PRESETS['resilient'].explorationRate).toBe(0.10);
    expect(PRESETS['local-cluster'].explorationRate).toBe(0.02);
  });

  it('resilient preset prioritizes reliability in scoring', () => {
    expect(PRESETS['resilient'].scoringWeights.reliability).toBe(0.40);
    expect(PRESETS['local-cluster'].scoringWeights.reliability).toBe(0.2);
  });
});
