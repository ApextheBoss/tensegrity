import { fnv1a } from './shared-utils';
/**
 * Adaptive Routing Mesh for Agent Networks
 * 
 * Intelligent message routing that learns from network topology changes,
 * latency patterns, and failure history to optimize delivery paths.
 * 
 * Components:
 * 1. TopologyTracker - Maintains live network graph with link quality metrics
 * 2. LatencyPredictor - EWMA + linear regression for path latency estimation
 * 3. PathScorer - Multi-factor path evaluation (latency, reliability, hops, load, cost)
 * 4. RouteCache - LRU route table with TTL-based invalidation and proactive refresh
 * 5. MultiPathRouter - Parallel path selection with weighted traffic splitting
 * 6. CongestionDetector - Queue depth + throughput monitoring with early warning
 * 7. FailureCorrelator - Temporal correlation of failures to detect shared-fate links
 * 8. TrafficShaper - Priority-based admission with token bucket per-path rate control
 * 9. AdaptiveRoutingEngine - Unified orchestrator with exploration/exploitation balance
 */

// ─── Types ────────────────────────────────────────────────────────────────────

interface LinkMetrics {
  source: string;
  target: string;
  latencyMs: number;       // EWMA smoothed
  latencyVariance: number; // Welford online variance
  packetLoss: number;      // 0-1 loss ratio
  bandwidth: number;       // messages/sec capacity
  utilization: number;     // 0-1 current load
  lastSeen: number;        // timestamp
  samples: number;
  failureCount: number;
  successCount: number;
}

interface Route {
  path: string[];        // ordered node IDs
  score: number;         // composite score (lower = better)
  estimatedLatency: number;
  reliability: number;   // 0-1
  hops: number;
  load: number;          // 0-1 average utilization
  cost: number;
  cachedAt: number;
  ttl: number;
}

interface RoutingMessage {
  id: string;
  source: string;
  destination: string;
  priority: 'critical' | 'high' | 'normal' | 'low' | 'bulk';
  size: number;
  deadline?: number;     // absolute timestamp
  payload: unknown;
}

interface CongestionSignal {
  node: string;
  queueDepth: number;
  throughput: number;    // messages/sec
  dropRate: number;
  severity: 'none' | 'mild' | 'moderate' | 'severe';
  timestamp: number;
}

interface FailureEvent {
  link: string;          // "source->target"
  timestamp: number;
  type: 'timeout' | 'reset' | 'unreachable' | 'overload';
  recovered: boolean;
  recoveryMs?: number;
}

interface RoutingEvent {
  type: 'route-computed' | 'route-cached' | 'route-invalidated' |
        'link-updated' | 'link-failed' | 'link-recovered' |
        'congestion-detected' | 'congestion-cleared' |
        'failure-correlated' | 'path-switched' |
        'exploration-probe' | 'traffic-shaped';
  timestamp: number;
  data: Record<string, unknown>;
}

type EventHandler = (event: RoutingEvent) => void;

// ─── FNV-1a Hash ──────────────────────────────────────────────────────────────

// ─── Topology Tracker ─────────────────────────────────────────────────────────

class TopologyTracker {
  private links: Map<string, LinkMetrics> = new Map();
  private neighbors: Map<string, Set<string>> = new Map();
  private readonly ewmaAlpha: number;
  private readonly linkTimeout: number;

  constructor(ewmaAlpha = 0.3, linkTimeoutMs = 60000) {
    this.ewmaAlpha = ewmaAlpha;
    this.linkTimeout = linkTimeoutMs;
  }

  private linkKey(source: string, target: string): string {
    return `${source}->${target}`;
  }

  updateLink(source: string, target: string, latencyMs: number, success: boolean): void {
    const key = this.linkKey(source, target);
    const existing = this.links.get(key);
    const now = Date.now();

    if (!existing) {
      this.links.set(key, {
        source, target,
        latencyMs,
        latencyVariance: 0,
        packetLoss: success ? 0 : 1,
        bandwidth: 100,
        utilization: 0,
        lastSeen: now,
        samples: 1,
        failureCount: success ? 0 : 1,
        successCount: success ? 1 : 0,
      });
    } else {
      // Welford online variance
      const n = existing.samples + 1;
      const delta = latencyMs - existing.latencyMs;
      const newMean = existing.latencyMs + delta / n;
      const delta2 = latencyMs - newMean;
      existing.latencyVariance = existing.latencyVariance + (delta * delta2 - existing.latencyVariance) / n;

      // EWMA smoothing
      existing.latencyMs = this.ewmaAlpha * latencyMs + (1 - this.ewmaAlpha) * existing.latencyMs;
      existing.packetLoss = this.ewmaAlpha * (success ? 0 : 1) + (1 - this.ewmaAlpha) * existing.packetLoss;
      existing.lastSeen = now;
      existing.samples = n;
      if (success) existing.successCount++; else existing.failureCount++;
    }

    // Update adjacency
    if (!this.neighbors.has(source)) this.neighbors.set(source, new Set());
    this.neighbors.get(source)!.add(target);
  }

  updateUtilization(source: string, target: string, utilization: number): void {
    const link = this.links.get(this.linkKey(source, target));
    if (link) {
      link.utilization = this.ewmaAlpha * utilization + (1 - this.ewmaAlpha) * link.utilization;
    }
  }

  getLink(source: string, target: string): LinkMetrics | undefined {
    return this.links.get(this.linkKey(source, target));
  }

  getNeighbors(node: string): string[] {
    return Array.from(this.neighbors.get(node) ?? []);
  }

  getActiveLinks(): LinkMetrics[] {
    const now = Date.now();
    return Array.from(this.links.values())
      .filter(l => now - l.lastSeen < this.linkTimeout);
  }

  pruneStaleLinks(): string[] {
    const now = Date.now();
    const pruned: string[] = [];
    for (const [key, link] of this.links) {
      if (now - link.lastSeen > this.linkTimeout) {
        this.links.delete(key);
        const neighbors = this.neighbors.get(link.source);
        if (neighbors) {
          neighbors.delete(link.target);
          if (neighbors.size === 0) this.neighbors.delete(link.source);
        }
        pruned.push(key);
      }
    }
    return pruned;
  }

  getAllNodes(): string[] {
    const nodes = new Set<string>();
    for (const link of this.links.values()) {
      nodes.add(link.source);
      nodes.add(link.target);
    }
    return Array.from(nodes);
  }
}

// ─── Latency Predictor ────────────────────────────────────────────────────────

class LatencyPredictor {
  private history: Map<string, { timestamp: number; latency: number }[]> = new Map();
  private readonly maxSamples: number;
  private readonly windowMs: number;

  constructor(maxSamples = 100, windowMs = 300000) {
    this.maxSamples = maxSamples;
    this.windowMs = windowMs;
  }

  addSample(pathKey: string, latency: number): void {
    if (!this.history.has(pathKey)) this.history.set(pathKey, []);
    const samples = this.history.get(pathKey)!;
    samples.push({ timestamp: Date.now(), latency });
    if (samples.length > this.maxSamples) samples.shift();
  }

  predict(pathKey: string): { predicted: number; confidence: number; trend: number } {
    const samples = this.history.get(pathKey);
    if (!samples || samples.length < 2) {
      return { predicted: Infinity, confidence: 0, trend: 0 };
    }

    const now = Date.now();
    const recent = samples.filter(s => now - s.timestamp < this.windowMs);
    if (recent.length < 2) {
      const last = samples[samples.length - 1];
      return { predicted: last.latency, confidence: 0.3, trend: 0 };
    }

    // Linear regression for trend
    const n = recent.length;
    let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
    const t0 = recent[0].timestamp;
    for (const s of recent) {
      const x = (s.timestamp - t0) / 1000; // seconds
      sumX += x;
      sumY += s.latency;
      sumXY += x * s.latency;
      sumX2 += x * x;
    }
    const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX || 1);
    const intercept = (sumY - slope * sumX) / n;

    // Predict at current time
    const xNow = (now - t0) / 1000;
    const predicted = Math.max(0, intercept + slope * xNow);

    // Confidence based on sample count and variance
    const mean = sumY / n;
    let variance = 0;
    for (const s of recent) variance += (s.latency - mean) ** 2;
    variance /= n;
    const cv = Math.sqrt(variance) / (mean || 1); // coefficient of variation
    const confidence = Math.min(1, n / 20) * Math.max(0, 1 - cv);

    return { predicted, confidence, trend: slope }; // slope: ms/sec change rate
  }
}

// ─── Path Scorer ──────────────────────────────────────────────────────────────

interface ScoringWeights {
  latency: number;
  reliability: number;
  hops: number;
  load: number;
  cost: number;
  variance: number;
}

class PathScorer {
  private readonly weights: ScoringWeights;

  constructor(weights: Partial<ScoringWeights> = {}) {
    this.weights = {
      latency: weights.latency ?? 0.35,
      reliability: weights.reliability ?? 0.25,
      hops: weights.hops ?? 0.10,
      load: weights.load ?? 0.15,
      cost: weights.cost ?? 0.05,
      variance: weights.variance ?? 0.10,
    };
  }

  scorePath(
    links: LinkMetrics[],
    predictor: LatencyPredictor
  ): { score: number; latency: number; reliability: number; load: number } {
    if (links.length === 0) return { score: Infinity, latency: Infinity, reliability: 0, load: 1 };

    let totalLatency = 0;
    let reliability = 1;
    let totalLoad = 0;
    let totalVariance = 0;

    for (const link of links) {
      totalLatency += link.latencyMs;
      reliability *= (1 - link.packetLoss);
      totalLoad += link.utilization;
      totalVariance += link.latencyVariance;
    }

    const avgLoad = totalLoad / links.length;
    const hops = links.length;

    // Normalize components to 0-1 range
    const latencyNorm = Math.min(1, totalLatency / 5000);  // 5s max
    const reliabilityNorm = 1 - reliability;                 // lower = better
    const hopsNorm = Math.min(1, hops / 10);                // 10 hops max
    const loadNorm = avgLoad;
    const costNorm = hops * 0.1;                            // simple hop-based cost
    const varianceNorm = Math.min(1, Math.sqrt(totalVariance) / 1000);

    const score =
      this.weights.latency * latencyNorm +
      this.weights.reliability * reliabilityNorm +
      this.weights.hops * hopsNorm +
      this.weights.load * loadNorm +
      this.weights.cost * costNorm +
      this.weights.variance * varianceNorm;

    return { score, latency: totalLatency, reliability, load: avgLoad };
  }
}

// ─── Route Cache ──────────────────────────────────────────────────────────────

class RouteCache {
  private cache: Map<string, Route[]> = new Map();
  private accessOrder: string[] = [];
  private readonly maxEntries: number;
  private readonly defaultTtl: number;

  constructor(maxEntries = 500, defaultTtlMs = 30000) {
    this.maxEntries = maxEntries;
    this.defaultTtl = defaultTtlMs;
  }

  private cacheKey(source: string, destination: string): string {
    return `${source}|${destination}`;
  }

  get(source: string, destination: string): Route[] | undefined {
    const key = this.cacheKey(source, destination);
    const routes = this.cache.get(key);
    if (!routes) return undefined;

    const now = Date.now();
    const valid = routes.filter(r => now - r.cachedAt < r.ttl);
    if (valid.length === 0) {
      this.cache.delete(key);
      return undefined;
    }

    // LRU touch
    this.accessOrder = this.accessOrder.filter(k => k !== key);
    this.accessOrder.push(key);
    return valid;
  }

  put(source: string, destination: string, routes: Route[]): void {
    const key = this.cacheKey(source, destination);
    const now = Date.now();

    for (const route of routes) {
      route.cachedAt = now;
      route.ttl = route.ttl || this.defaultTtl;
    }

    this.cache.set(key, routes);
    this.accessOrder = this.accessOrder.filter(k => k !== key);
    this.accessOrder.push(key);

    // Evict LRU
    while (this.cache.size > this.maxEntries) {
      const evict = this.accessOrder.shift();
      if (evict) this.cache.delete(evict);
    }
  }

  invalidate(nodeId: string): number {
    let count = 0;
    for (const [key, routes] of this.cache) {
      if (routes.some(r => r.path.includes(nodeId))) {
        this.cache.delete(key);
        count++;
      }
    }
    return count;
  }

  invalidateLink(source: string, target: string): number {
    let count = 0;
    for (const [key, routes] of this.cache) {
      const affected = routes.some(r => {
        for (let i = 0; i < r.path.length - 1; i++) {
          if (r.path[i] === source && r.path[i + 1] === target) return true;
        }
        return false;
      });
      if (affected) {
        this.cache.delete(key);
        count++;
      }
    }
    return count;
  }

  size(): number { return this.cache.size; }

  getExpiringRoutes(withinMs: number): Array<{ source: string; destination: string }> {
    const now = Date.now();
    const expiring: Array<{ source: string; destination: string }> = [];
    for (const [key, routes] of this.cache) {
      const minTtlRemaining = Math.min(...routes.map(r => r.ttl - (now - r.cachedAt)));
      if (minTtlRemaining < withinMs) {
        const [source, destination] = key.split('|');
        expiring.push({ source, destination });
      }
    }
    return expiring;
  }
}

// ─── Multi-Path Router ────────────────────────────────────────────────────────

class MultiPathRouter {
  private readonly topology: TopologyTracker;
  private readonly scorer: PathScorer;
  private readonly predictor: LatencyPredictor;
  private readonly maxPaths: number;
  private readonly maxHops: number;

  constructor(
    topology: TopologyTracker,
    scorer: PathScorer,
    predictor: LatencyPredictor,
    maxPaths = 3,
    maxHops = 8
  ) {
    this.topology = topology;
    this.scorer = scorer;
    this.predictor = predictor;
    this.maxPaths = maxPaths;
    this.maxHops = maxHops;
  }

  /**
   * Modified Dijkstra with Yen's k-shortest paths
   * Returns up to maxPaths diverse routes
   */
  findPaths(source: string, destination: string): Route[] {
    // First: shortest path via Dijkstra
    const shortest = this.dijkstra(source, destination, new Set());
    if (!shortest) return [];

    const routes: Route[] = [shortest];
    const candidates: Route[] = [];

    // Yen's algorithm for k-shortest paths
    for (let k = 1; k < this.maxPaths; k++) {
      const prevPath = routes[k - 1].path;

      for (let i = 0; i < prevPath.length - 1; i++) {
        const spurNode = prevPath[i];
        const rootPath = prevPath.slice(0, i + 1);

        // Exclude edges used by existing routes at the same spur
        const excluded = new Set<string>();
        for (const route of routes) {
          if (route.path.length > i && route.path.slice(0, i + 1).join(',') === rootPath.join(',')) {
            excluded.add(`${route.path[i]}->${route.path[i + 1]}`);
          }
        }

        // Also exclude root path nodes (except spur) to ensure diversity
        const excludedNodes = new Set(rootPath.slice(0, -1));

        const spurPath = this.dijkstra(spurNode, destination, excluded, excludedNodes);
        if (spurPath) {
          const totalPath = [...rootPath.slice(0, -1), ...spurPath.path];
          const links = this.getPathLinks(totalPath);
          if (links) {
            const { score, latency, reliability, load } = this.scorer.scorePath(links, this.predictor);
            candidates.push({
              path: totalPath,
              score,
              estimatedLatency: latency,
              reliability,
              hops: totalPath.length - 1,
              load,
              cost: (totalPath.length - 1) * 0.1,
              cachedAt: Date.now(),
              ttl: 30000,
            });
          }
        }
      }

      if (candidates.length === 0) break;

      // Pick best candidate
      candidates.sort((a, b) => a.score - b.score);
      const best = candidates.shift()!;

      // Check diversity — don't add near-duplicate paths
      const isDiverse = routes.every(r => {
        const overlap = r.path.filter(n => best.path.includes(n)).length;
        return overlap / Math.max(r.path.length, best.path.length) < 0.7;
      });

      if (isDiverse) {
        routes.push(best);
      }
    }

    return routes.sort((a, b) => a.score - b.score);
  }

  private dijkstra(
    source: string,
    destination: string,
    excludedEdges: Set<string>,
    excludedNodes: Set<string> = new Set()
  ): Route | null {
    const dist: Map<string, number> = new Map();
    const prev: Map<string, string> = new Map();
    const visited = new Set<string>();

    dist.set(source, 0);

    // Simple priority queue via sorted array
    const queue: Array<{ node: string; dist: number }> = [{ node: source, dist: 0 }];

    while (queue.length > 0) {
      queue.sort((a, b) => a.dist - b.dist);
      const { node } = queue.shift()!;

      if (visited.has(node)) continue;
      visited.add(node);

      if (node === destination) {
        // Reconstruct path
        const path: string[] = [];
        let current: string | undefined = destination;
        while (current) {
          path.unshift(current);
          current = prev.get(current);
        }
        const links = this.getPathLinks(path);
        if (!links) return null;
        const { score, latency, reliability, load } = this.scorer.scorePath(links, this.predictor);
        return {
          path, score, estimatedLatency: latency, reliability,
          hops: path.length - 1, load, cost: (path.length - 1) * 0.1,
          cachedAt: Date.now(), ttl: 30000,
        };
      }

      if ((dist.get(node) ?? 0) > this.maxHops) continue;

      for (const neighbor of this.topology.getNeighbors(node)) {
        if (excludedNodes.has(neighbor)) continue;
        if (excludedEdges.has(`${node}->${neighbor}`)) continue;

        const link = this.topology.getLink(node, neighbor);
        if (!link) continue;

        // Edge weight: composite of latency and loss
        const weight = link.latencyMs * (1 + link.packetLoss * 5) * (1 + link.utilization);
        const alt = (dist.get(node) ?? 0) + weight;

        if (alt < (dist.get(neighbor) ?? Infinity)) {
          dist.set(neighbor, alt);
          prev.set(neighbor, node);
          queue.push({ node: neighbor, dist: alt });
        }
      }
    }

    return null;
  }

  private getPathLinks(path: string[]): LinkMetrics[] | null {
    const links: LinkMetrics[] = [];
    for (let i = 0; i < path.length - 1; i++) {
      const link = this.topology.getLink(path[i], path[i + 1]);
      if (!link) return null;
      links.push(link);
    }
    return links;
  }

  /**
   * Split traffic across multiple paths weighted by inverse score
   */
  computeTrafficSplit(routes: Route[]): Map<string, number> {
    if (routes.length === 0) return new Map();
    if (routes.length === 1) return new Map([[routes[0].path.join('->'), 1.0]]);

    const totalInverse = routes.reduce((sum, r) => sum + 1 / (r.score + 0.001), 0);
    const split = new Map<string, number>();
    for (const route of routes) {
      const weight = (1 / (route.score + 0.001)) / totalInverse;
      split.set(route.path.join('->'), weight);
    }
    return split;
  }
}

// ─── Congestion Detector ──────────────────────────────────────────────────────

class CongestionDetector {
  private signals: Map<string, CongestionSignal[]> = new Map();
  private readonly windowMs: number;
  private readonly mildThreshold: number;
  private readonly moderateThreshold: number;
  private readonly severeThreshold: number;

  constructor(
    windowMs = 60000,
    mildThreshold = 0.6,
    moderateThreshold = 0.8,
    severeThreshold = 0.95
  ) {
    this.windowMs = windowMs;
    this.mildThreshold = mildThreshold;
    this.moderateThreshold = moderateThreshold;
    this.severeThreshold = severeThreshold;
  }

  reportMetrics(node: string, queueDepth: number, throughput: number, dropRate: number): CongestionSignal {
    const now = Date.now();
    
    // Composite congestion score
    const congestionScore = Math.min(1,
      queueDepth / 1000 * 0.4 +
      dropRate * 0.4 +
      (1 - Math.min(1, throughput / 100)) * 0.2
    );

    const severity: CongestionSignal['severity'] =
      congestionScore >= this.severeThreshold ? 'severe' :
      congestionScore >= this.moderateThreshold ? 'moderate' :
      congestionScore >= this.mildThreshold ? 'mild' : 'none';

    const signal: CongestionSignal = {
      node, queueDepth, throughput, dropRate, severity, timestamp: now,
    };

    if (!this.signals.has(node)) this.signals.set(node, []);
    const signals = this.signals.get(node)!;
    signals.push(signal);

    // Trim window
    const cutoff = now - this.windowMs;
    const trimmed = signals.filter(s => s.timestamp > cutoff);
    this.signals.set(node, trimmed);

    return signal;
  }

  getCurrentSeverity(node: string): CongestionSignal['severity'] {
    const signals = this.signals.get(node);
    if (!signals || signals.length === 0) return 'none';
    return signals[signals.length - 1].severity;
  }

  getCongestedNodes(): string[] {
    const congested: string[] = [];
    for (const [node] of this.signals) {
      const severity = this.getCurrentSeverity(node);
      if (severity !== 'none') congested.push(node);
    }
    return congested;
  }

  isCongested(node: string): boolean {
    return this.getCurrentSeverity(node) !== 'none';
  }
}

// ─── Failure Correlator ───────────────────────────────────────────────────────

class FailureCorrelator {
  private events: FailureEvent[] = [];
  private readonly windowMs: number;
  private readonly correlationThresholdMs: number;
  private sharedFateGroups: Map<string, Set<string>> = new Map();

  constructor(windowMs = 300000, correlationThresholdMs = 5000) {
    this.windowMs = windowMs;
    this.correlationThresholdMs = correlationThresholdMs;
  }

  recordFailure(link: string, type: FailureEvent['type']): void {
    this.events.push({
      link, timestamp: Date.now(), type, recovered: false,
    });
    this.pruneOldEvents();
    this.detectCorrelations();
  }

  recordRecovery(link: string, recoveryMs: number): void {
    // Mark most recent failure as recovered
    for (let i = this.events.length - 1; i >= 0; i--) {
      if (this.events[i].link === link && !this.events[i].recovered) {
        this.events[i].recovered = true;
        this.events[i].recoveryMs = recoveryMs;
        break;
      }
    }
  }

  private pruneOldEvents(): void {
    const cutoff = Date.now() - this.windowMs;
    this.events = this.events.filter(e => e.timestamp > cutoff);
  }

  private detectCorrelations(): void {
    this.sharedFateGroups.clear();

    // Group failures by time proximity
    const sorted = [...this.events].sort((a, b) => a.timestamp - b.timestamp);
    
    for (let i = 0; i < sorted.length; i++) {
      const group = new Set<string>([sorted[i].link]);
      
      for (let j = i + 1; j < sorted.length; j++) {
        if (sorted[j].timestamp - sorted[i].timestamp > this.correlationThresholdMs) break;
        group.add(sorted[j].link);
      }

      if (group.size > 1) {
        // Extract common node from correlated links
        const nodes = new Set<string>();
        for (const link of group) {
          const [src, tgt] = link.split('->');
          nodes.add(src);
          nodes.add(tgt);
        }
        
        // Find shared nodes (potential root cause)
        for (const node of nodes) {
          const linksWithNode = Array.from(group).filter(l => l.includes(node));
          if (linksWithNode.length > 1) {
            if (!this.sharedFateGroups.has(node)) {
              this.sharedFateGroups.set(node, new Set());
            }
            for (const l of linksWithNode) {
              this.sharedFateGroups.get(node)!.add(l);
            }
          }
        }
      }
    }
  }

  getSharedFateGroups(): Map<string, string[]> {
    const result = new Map<string, string[]>();
    for (const [node, links] of this.sharedFateGroups) {
      result.set(node, Array.from(links));
    }
    return result;
  }

  isSharedFateRisk(node: string): boolean {
    return this.sharedFateGroups.has(node);
  }

  getRecentFailureRate(link: string): number {
    const now = Date.now();
    const recent = this.events.filter(
      e => e.link === link && now - e.timestamp < 60000
    );
    return recent.length; // failures per minute
  }
}

// ─── Traffic Shaper ───────────────────────────────────────────────────────────

interface TokenBucket {
  tokens: number;
  maxTokens: number;
  refillRate: number;   // tokens/sec
  lastRefill: number;
}

class TrafficShaper {
  private buckets: Map<string, TokenBucket> = new Map();
  private readonly priorityCosts: Record<string, number>;

  constructor() {
    this.priorityCosts = {
      critical: 0,    // always admitted
      high: 1,
      normal: 2,
      low: 4,
      bulk: 8,
    };
  }

  configurePath(pathKey: string, maxTokens: number, refillRate: number): void {
    this.buckets.set(pathKey, {
      tokens: maxTokens,
      maxTokens,
      refillRate,
      lastRefill: Date.now(),
    });
  }

  tryAdmit(pathKey: string, priority: string): { admitted: boolean; waitMs: number } {
    const cost = this.priorityCosts[priority] ?? 2;
    if (cost === 0) return { admitted: true, waitMs: 0 }; // critical always passes

    const bucket = this.buckets.get(pathKey);
    if (!bucket) return { admitted: true, waitMs: 0 }; // unconfigured = unlimited

    // Refill tokens
    const now = Date.now();
    const elapsed = (now - bucket.lastRefill) / 1000;
    bucket.tokens = Math.min(bucket.maxTokens, bucket.tokens + elapsed * bucket.refillRate);
    bucket.lastRefill = now;

    if (bucket.tokens >= cost) {
      bucket.tokens -= cost;
      return { admitted: true, waitMs: 0 };
    }

    const waitMs = ((cost - bucket.tokens) / bucket.refillRate) * 1000;
    return { admitted: false, waitMs };
  }

  getUtilization(pathKey: string): number {
    const bucket = this.buckets.get(pathKey);
    if (!bucket) return 0;
    return 1 - (bucket.tokens / bucket.maxTokens);
  }
}

// ─── Adaptive Routing Engine ──────────────────────────────────────────────────

interface RoutingConfig {
  explorationRate: number;        // 0-1, fraction of traffic for probing
  routeCacheTtlMs: number;
  maxPaths: number;
  maxHops: number;
  congestionAvoidance: boolean;
  sharedFateAvoidance: boolean;
  proactiveRefresh: boolean;
  refreshAheadMs: number;
  scoringWeights: Partial<ScoringWeights>;
}

class AdaptiveRoutingEngine {
  private readonly topology: TopologyTracker;
  private readonly predictor: LatencyPredictor;
  private readonly scorer: PathScorer;
  private readonly cache: RouteCache;
  private readonly router: MultiPathRouter;
  private readonly congestion: CongestionDetector;
  private readonly correlator: FailureCorrelator;
  private readonly shaper: TrafficShaper;
  private readonly config: RoutingConfig;
  private readonly handlers: EventHandler[] = [];
  private routingStats: {
    totalRouted: number;
    cacheHits: number;
    cacheMisses: number;
    explorationProbes: number;
    pathSwitches: number;
    shapingDrops: number;
  };

  constructor(config: Partial<RoutingConfig> = {}) {
    this.config = {
      explorationRate: config.explorationRate ?? 0.05,
      routeCacheTtlMs: config.routeCacheTtlMs ?? 30000,
      maxPaths: config.maxPaths ?? 3,
      maxHops: config.maxHops ?? 8,
      congestionAvoidance: config.congestionAvoidance ?? true,
      sharedFateAvoidance: config.sharedFateAvoidance ?? true,
      proactiveRefresh: config.proactiveRefresh ?? true,
      refreshAheadMs: config.refreshAheadMs ?? 10000,
      scoringWeights: config.scoringWeights ?? {},
    };

    this.topology = new TopologyTracker();
    this.predictor = new LatencyPredictor();
    this.scorer = new PathScorer(this.config.scoringWeights);
    this.cache = new RouteCache(500, this.config.routeCacheTtlMs);
    this.router = new MultiPathRouter(
      this.topology, this.scorer, this.predictor,
      this.config.maxPaths, this.config.maxHops
    );
    this.congestion = new CongestionDetector();
    this.correlator = new FailureCorrelator();
    this.shaper = new TrafficShaper();
    this.routingStats = {
      totalRouted: 0, cacheHits: 0, cacheMisses: 0,
      explorationProbes: 0, pathSwitches: 0, shapingDrops: 0,
    };
  }

  onEvent(handler: EventHandler): void {
    this.handlers.push(handler);
  }

  private emit(event: RoutingEvent): void {
    for (const h of this.handlers) h(event);
  }

  // ── Link management ──

  reportLink(source: string, target: string, latencyMs: number, success: boolean): void {
    this.topology.updateLink(source, target, latencyMs, success);
    this.predictor.addSample(`${source}->${target}`, latencyMs);

    if (!success) {
      this.correlator.recordFailure(`${source}->${target}`, 'timeout');
      const invalidated = this.cache.invalidateLink(source, target);
      this.emit({
        type: 'link-failed', timestamp: Date.now(),
        data: { source, target, invalidated },
      });
    } else {
      this.emit({
        type: 'link-updated', timestamp: Date.now(),
        data: { source, target, latencyMs },
      });
    }
  }

  reportCongestion(node: string, queueDepth: number, throughput: number, dropRate: number): void {
    const signal = this.congestion.reportMetrics(node, queueDepth, throughput, dropRate);
    if (signal.severity !== 'none') {
      this.cache.invalidate(node);
      this.emit({
        type: 'congestion-detected', timestamp: Date.now(),
        data: { node, severity: signal.severity },
      });
    }
  }

  // ── Route computation ──

  route(message: RoutingMessage): Route | null {
    this.routingStats.totalRouted++;

    // Exploration: occasionally use a non-optimal path to discover better routes
    const isExploration = Math.random() < this.config.explorationRate &&
                          message.priority !== 'critical';

    // Check cache first
    let routes = this.cache.get(message.source, message.destination);

    if (routes && !isExploration) {
      this.routingStats.cacheHits++;
    } else {
      this.routingStats.cacheMisses++;
      routes = this.router.findPaths(message.source, message.destination);

      if (routes.length === 0) return null;

      this.cache.put(message.source, message.destination, routes);
      this.emit({
        type: 'route-computed', timestamp: Date.now(),
        data: { source: message.source, destination: message.destination, pathCount: routes.length },
      });
    }

    // Filter based on constraints
    let viable = routes;

    // Congestion avoidance
    if (this.config.congestionAvoidance) {
      const congested = new Set(this.congestion.getCongestedNodes());
      if (congested.size > 0) {
        const uncongested = viable.filter(r =>
          !r.path.some(n => congested.has(n) && n !== message.source && n !== message.destination)
        );
        if (uncongested.length > 0) viable = uncongested;
      }
    }

    // Shared-fate avoidance
    if (this.config.sharedFateAvoidance) {
      const riskyNodes = new Set<string>();
      for (const [node] of this.correlator.getSharedFateGroups()) {
        riskyNodes.add(node);
      }
      if (riskyNodes.size > 0) {
        const safe = viable.filter(r =>
          !r.path.some(n => riskyNodes.has(n) && n !== message.source && n !== message.destination)
        );
        if (safe.length > 0) viable = safe;
      }
    }

    // Deadline-aware: filter paths that can't meet deadline
    if (message.deadline) {
      const timeLeft = message.deadline - Date.now();
      const timely = viable.filter(r => r.estimatedLatency < timeLeft * 0.8); // 20% margin
      if (timely.length > 0) viable = timely;
    }

    // Traffic shaping
    if (message.priority !== 'critical') {
      for (const route of viable) {
        const pathKey = route.path.join('->');
        const { admitted } = this.shaper.tryAdmit(pathKey, message.priority);
        if (admitted) {
          if (isExploration) {
            this.routingStats.explorationProbes++;
            this.emit({
              type: 'exploration-probe', timestamp: Date.now(),
              data: { path: route.path },
            });
          }
          return route;
        }
      }
      // All shaped — try without shaping for higher priorities
      if (message.priority === 'high') {
        return viable[0] ?? null;
      }
      this.routingStats.shapingDrops++;
      this.emit({
        type: 'traffic-shaped', timestamp: Date.now(),
        data: { message: message.id, priority: message.priority },
      });
      return null;
    }

    // Exploration: pick a non-primary path
    if (isExploration && viable.length > 1) {
      const idx = (fnv1a(message.id) % (viable.length - 1)) + 1;
      this.routingStats.explorationProbes++;
      this.emit({
        type: 'exploration-probe', timestamp: Date.now(),
        data: { path: viable[idx].path },
      });
      return viable[idx];
    }

    return viable[0] ?? null;
  }

  // ── Proactive refresh ──

  tick(): void {
    // Prune stale topology
    const pruned = this.topology.pruneStaleLinks();
    for (const link of pruned) {
      const [source, target] = link.split('->');
      this.cache.invalidateLink(source, target);
    }

    // Proactive route refresh
    if (this.config.proactiveRefresh) {
      const expiring = this.cache.getExpiringRoutes(this.config.refreshAheadMs);
      for (const { source, destination } of expiring) {
        const routes = this.router.findPaths(source, destination);
        if (routes.length > 0) {
          this.cache.put(source, destination, routes);
        }
      }
    }
  }

  // ── Stats ──

  getStats(): typeof this.routingStats & {
    cacheSize: number;
    activeLinks: number;
    congestedNodes: string[];
    sharedFateGroups: Map<string, string[]>;
  } {
    return {
      ...this.routingStats,
      cacheSize: this.cache.size(),
      activeLinks: this.topology.getActiveLinks().length,
      congestedNodes: this.congestion.getCongestedNodes(),
      sharedFateGroups: this.correlator.getSharedFateGroups(),
    };
  }
}

// ─── Presets ──────────────────────────────────────────────────────────────────

const PRESETS = {
  /** Low-latency local cluster routing */
  'local-cluster': {
    explorationRate: 0.02,
    routeCacheTtlMs: 10000,
    maxPaths: 2,
    maxHops: 4,
    congestionAvoidance: true,
    sharedFateAvoidance: false,
    proactiveRefresh: true,
    refreshAheadMs: 5000,
    scoringWeights: { latency: 0.5, reliability: 0.2, hops: 0.1, load: 0.1, cost: 0, variance: 0.1 },
  } satisfies Partial<RoutingConfig>,

  /** Balanced wide-area routing */
  'wide-area': {
    explorationRate: 0.05,
    routeCacheTtlMs: 30000,
    maxPaths: 3,
    maxHops: 8,
    congestionAvoidance: true,
    sharedFateAvoidance: true,
    proactiveRefresh: true,
    refreshAheadMs: 15000,
    scoringWeights: { latency: 0.3, reliability: 0.3, hops: 0.1, load: 0.15, cost: 0.05, variance: 0.1 },
  } satisfies Partial<RoutingConfig>,

  /** Resilient routing for unreliable networks */
  'resilient': {
    explorationRate: 0.10,
    routeCacheTtlMs: 15000,
    maxPaths: 4,
    maxHops: 10,
    congestionAvoidance: true,
    sharedFateAvoidance: true,
    proactiveRefresh: true,
    refreshAheadMs: 10000,
    scoringWeights: { latency: 0.15, reliability: 0.40, hops: 0.05, load: 0.15, cost: 0.05, variance: 0.20 },
  } satisfies Partial<RoutingConfig>,
} as const;

export {
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
};

export type {
  LinkMetrics,
  Route,
  RoutingMessage,
  CongestionSignal,
  FailureEvent,
  RoutingEvent,
  RoutingConfig,
  ScoringWeights,
};
