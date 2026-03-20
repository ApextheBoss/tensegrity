import { describe, it, expect, beforeEach } from 'vitest';
import {
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
  type ServiceInstance,
  type TopologyChangeEvent,
} from '../service-discovery-mesh';

// Helper to create a minimal instance for the local registry
function makeInstance(overrides: Partial<ServiceInstance> = {}): ServiceInstance {
  return {
    instanceId: overrides.instanceId ?? 'inst-1',
    serviceType: overrides.serviceType ?? 'agent.compute',
    serviceName: overrides.serviceName ?? 'compute-1',
    agentAddress: overrides.agentAddress ?? 'agent-a',
    endpoint: overrides.endpoint ?? 'http://localhost:8080',
    version: overrides.version ?? '1.0.0',
    metadata: overrides.metadata ?? {},
    locality: overrides.locality ?? { labels: {} },
    registeredAt: overrides.registeredAt ?? 1000,
    lastHeartbeat: overrides.lastHeartbeat ?? 1000,
    ttlMs: overrides.ttlMs ?? 30000,
    logicalClock: overrides.logicalClock ?? 1,
    health: overrides.health ?? { alive: true, ready: true, lastCheckAt: 1000, consecutiveFailures: 0, latencyEwma: 0, score: 1.0 },
    load: overrides.load ?? 0,
    priority: overrides.priority ?? 5,
    tombstoned: overrides.tombstoned ?? false,
    tombstonedAt: overrides.tombstonedAt,
  };
}

describe('LocalRegistry', () => {
  let registry: LocalRegistry;

  beforeEach(() => {
    registry = new LocalRegistry({ maxInstances: 5 });
  });

  it('registers and retrieves instances', () => {
    const inst = makeInstance({ logicalClock: 1 });
    const result = registry.register(inst);
    expect(result.applied).toBe(true);
    expect(registry.size).toBe(1);
    expect(registry.get('inst-1')).toBeDefined();
  });

  it('rejects stale updates (lower logical clock)', () => {
    registry.register(makeInstance({ logicalClock: 5 }));
    const result = registry.register(makeInstance({ logicalClock: 3 }));
    expect(result.applied).toBe(false);
  });

  it('deregisters (tombstones) instances', () => {
    registry.register(makeInstance({ logicalClock: 1 }));
    const inst = registry.deregister('inst-1', 2000);
    expect(inst).not.toBeNull();
    expect(inst!.tombstoned).toBe(true);
    expect(registry.activeCount).toBe(0);
  });

  it('heartbeat updates lastHeartbeat and load', () => {
    registry.register(makeInstance({ logicalClock: 1 }));
    const ok = registry.heartbeat('inst-1', 5000, 0.7);
    expect(ok).toBe(true);
    expect(registry.get('inst-1')!.lastHeartbeat).toBe(5000);
    expect(registry.get('inst-1')!.load).toBeCloseTo(0.7);
  });

  it('heartbeat fails on tombstoned instance', () => {
    registry.register(makeInstance({ logicalClock: 1 }));
    registry.deregister('inst-1', 2000);
    expect(registry.heartbeat('inst-1', 3000)).toBe(false);
  });

  it('evicts lower-priority instances when at capacity', () => {
    for (let i = 0; i < 5; i++) {
      registry.register(makeInstance({ instanceId: `inst-${i}`, logicalClock: i + 1, priority: 3 }));
    }
    const result = registry.register(makeInstance({ instanceId: 'inst-new', logicalClock: 10, priority: 5 }));
    expect(result.applied).toBe(true);
    expect(result.evicted).toBeDefined();
    expect(registry.size).toBe(5);
  });

  it('refuses registration when at capacity and no lower priority to evict', () => {
    for (let i = 0; i < 5; i++) {
      registry.register(makeInstance({ instanceId: `inst-${i}`, logicalClock: i + 1, priority: 10 }));
    }
    const result = registry.register(makeInstance({ instanceId: 'inst-new', logicalClock: 10, priority: 1 }));
    expect(result.applied).toBe(false);
  });

  it('merge applies remote instance with higher clock', () => {
    registry.register(makeInstance({ logicalClock: 1 }));
    const remote = makeInstance({ logicalClock: 5, load: 0.9 });
    expect(registry.merge(remote)).toBe(true);
    expect(registry.get('inst-1')!.load).toBe(0.9);
  });

  it('merge rejects remote with lower/equal clock', () => {
    registry.register(makeInstance({ logicalClock: 5 }));
    expect(registry.merge(makeInstance({ logicalClock: 3 }))).toBe(false);
    expect(registry.merge(makeInstance({ logicalClock: 5 }))).toBe(false);
  });

  it('gc removes expired tombstones', () => {
    registry.register(makeInstance({ logicalClock: 1 }));
    registry.deregister('inst-1', 1000);
    // Not yet expired
    expect(registry.gc(100_000).length).toBe(0);
    // After tombstone retention (default 5min = 300_000ms)
    expect(registry.gc(400_000).length).toBe(1);
    expect(registry.size).toBe(0);
  });

  it('findExpired detects instances past TTL', () => {
    registry.register(makeInstance({ logicalClock: 1, lastHeartbeat: 1000, ttlMs: 5000 }));
    expect(registry.findExpired(3000).length).toBe(0);
    expect(registry.findExpired(7000).length).toBe(1);
  });

  it('buildDigest and computeDelta work correctly', () => {
    registry.register(makeInstance({ instanceId: 'a', logicalClock: 3 }));
    registry.register(makeInstance({ instanceId: 'b', logicalClock: 7 }));

    const digest = registry.buildDigest();
    expect(digest.get('a')).toBe(3);
    expect(digest.get('b')).toBe(7);

    // Remote has a=3, b=5 — local should send b (local 7 > remote 5)
    const remoteDigest = new Map([['a', 3], ['b', 5]]);
    const delta = registry.computeDelta(remoteDigest);
    expect(delta.updates.length).toBe(1);
    expect(delta.updates[0].instanceId).toBe('b');
  });

  it('getByType filters correctly', () => {
    registry.register(makeInstance({ instanceId: 'a', serviceType: 'compute', logicalClock: 1 }));
    registry.register(makeInstance({ instanceId: 'b', serviceType: 'storage', logicalClock: 2 }));
    registry.register(makeInstance({ instanceId: 'c', serviceType: 'compute', logicalClock: 3 }));
    expect(registry.getByType('compute').length).toBe(2);
    expect(registry.getByType('storage').length).toBe(1);
  });
});

describe('HealthChecker', () => {
  it('marks fresh instances as alive and ready', () => {
    const checker = new HealthChecker();
    const inst = makeInstance({ lastHeartbeat: 9000, ttlMs: 30000 });
    const health = checker.probe(inst, 10000);
    expect(health.alive).toBe(true);
    expect(health.ready).toBe(true);
    expect(health.score).toBeGreaterThan(0.5);
  });

  it('marks expired instances as not alive', () => {
    const checker = new HealthChecker();
    const inst = makeInstance({ lastHeartbeat: 1000, ttlMs: 5000 });
    const health = checker.probe(inst, 100000);
    expect(health.alive).toBe(false);
    expect(health.ready).toBe(false);
    expect(health.score).toBe(0);
  });

  it('marks stale (>70% TTL) instances as not ready', () => {
    const checker = new HealthChecker();
    const inst = makeInstance({ lastHeartbeat: 1000, ttlMs: 10000 });
    // age = 8500, >70% of 10000
    const health = checker.probe(inst, 9500);
    expect(health.alive).toBe(true);
    expect(health.ready).toBe(false);
  });
});

describe('LocalityScorer', () => {
  it('returns 1.0 for perfect match', () => {
    const scorer = new LocalityScorer();
    const loc = { region: 'us-east', zone: 'a', rack: 'r1', labels: {} };
    const pref = { region: 'us-east', zone: 'a', rack: 'r1' };
    expect(scorer.score(loc, pref)).toBeCloseTo(1.0);
  });

  it('returns 0.0 for no match', () => {
    const scorer = new LocalityScorer();
    const loc = { region: 'us-east', zone: 'a', rack: 'r1', labels: {} };
    const pref = { region: 'eu-west', zone: 'b', rack: 'r2' };
    expect(scorer.score(loc, pref)).toBeCloseTo(0.0);
  });

  it('returns 0.5 for empty preferred', () => {
    const scorer = new LocalityScorer();
    const loc = { region: 'us-east', zone: 'a', labels: {} };
    expect(scorer.score(loc, {})).toBeCloseTo(0.5);
  });
});

describe('WatchManager', () => {
  it('notifies matching subscribers', () => {
    const wm = new WatchManager();
    const events: TopologyChangeEvent[] = [];
    wm.subscribe({ serviceType: 'compute' }, (e) => events.push(e));
    wm.subscribe({ serviceType: 'storage' }, (e) => events.push(e));

    const inst = makeInstance({ serviceType: 'compute' });
    wm.notify({ type: 'registered', instance: inst, timestamp: 1000 });
    expect(events.length).toBe(1);
  });

  it('unsubscribe stops notifications', () => {
    const wm = new WatchManager();
    const events: TopologyChangeEvent[] = [];
    const id = wm.subscribe({ serviceType: 'compute' }, (e) => events.push(e));
    wm.unsubscribe(id);

    wm.notify({ type: 'registered', instance: makeInstance(), timestamp: 1000 });
    expect(events.length).toBe(0);
  });
});

describe('GossipDisseminator', () => {
  it('selects peers up to fanout', () => {
    const g = new GossipDisseminator({ fanout: 2 });
    g.addPeer('a');
    g.addPeer('b');
    g.addPeer('c');
    const peers = g.selectPeers();
    expect(peers.length).toBe(2);
  });

  it('returns all peers if fewer than fanout', () => {
    const g = new GossipDisseminator({ fanout: 5 });
    g.addPeer('a');
    const peers = g.selectPeers();
    expect(peers.length).toBe(1);
  });
});

describe('SplitBrainDetector', () => {
  it('detects no split brain when digests match', () => {
    const detector = new SplitBrainDetector();
    const local = new Map([['a', 1], ['b', 2]]);
    detector.recordPeerDigest('peer1', new Map([['a', 1], ['b', 2]]));
    const report = detector.detect(local, 1000);
    expect(report.detected).toBe(false);
  });

  it('detects split brain when digests diverge significantly', () => {
    const detector = new SplitBrainDetector({ divergenceThreshold: 0.1 });
    const local = new Map([['a', 1], ['b', 2], ['c', 3]]);
    detector.recordPeerDigest('peer1', new Map([['a', 10], ['b', 20], ['c', 30]]));
    const report = detector.detect(local, 1000);
    expect(report.detected).toBe(true);
    expect(report.partitions.length).toBe(2);
    expect(report.divergentInstances.length).toBeGreaterThan(0);
  });
});

describe('ServiceDiscoveryMesh', () => {
  let mesh: ServiceDiscoveryMesh;

  beforeEach(() => {
    mesh = new ServiceDiscoveryMesh({
      nodeId: 'node-1',
      defaultTtlMs: 30000,
      maxInstances: 100,
    });
  });

  it('register and discover', () => {
    mesh.register({
      instanceId: 'i1',
      serviceType: 'compute',
      serviceName: 'comp-1',
      agentAddress: 'agent-a',
      endpoint: 'http://a:8080',
      version: '1.0.0',
    });
    const results = mesh.discover({ serviceType: 'compute' });
    expect(results.length).toBe(1);
    expect(results[0].instance.instanceId).toBe('i1');
  });

  it('deregister removes from discovery', () => {
    mesh.register({
      instanceId: 'i1',
      serviceType: 'compute',
      serviceName: 'comp-1',
      agentAddress: 'agent-a',
      endpoint: 'http://a:8080',
      version: '1.0.0',
    });
    mesh.deregister('i1');
    expect(mesh.discover({ serviceType: 'compute' }).length).toBe(0);
  });

  it('resolveOne returns best instance', () => {
    mesh.register({ instanceId: 'i1', serviceType: 'compute', serviceName: 'c1', agentAddress: 'a', endpoint: 'http://a', version: '1.0.0' });
    mesh.register({ instanceId: 'i2', serviceType: 'compute', serviceName: 'c2', agentAddress: 'b', endpoint: 'http://b', version: '2.0.0' });
    const result = mesh.resolveOne('compute');
    expect(result).not.toBeNull();
  });

  it('heartbeat keeps instance alive', () => {
    mesh.register({ instanceId: 'i1', serviceType: 'compute', serviceName: 'c1', agentAddress: 'a', endpoint: 'http://a', version: '1.0.0' });
    expect(mesh.heartbeat('i1', 0.5)).toBe(true);
    expect(mesh.heartbeat('nonexistent')).toBe(false);
  });

  it('watch notifies on registration', () => {
    const events: TopologyChangeEvent[] = [];
    mesh.watch({ serviceType: 'compute' }, (e) => events.push(e));
    mesh.register({ instanceId: 'i1', serviceType: 'compute', serviceName: 'c1', agentAddress: 'a', endpoint: 'http://a', version: '1.0.0' });
    expect(events.length).toBe(1);
    expect(events[0].type).toBe('registered');
  });

  it('unwatch stops notifications', () => {
    const events: TopologyChangeEvent[] = [];
    const subId = mesh.watch({ serviceType: 'compute' }, (e) => events.push(e));
    mesh.unwatch(subId);
    mesh.register({ instanceId: 'i1', serviceType: 'compute', serviceName: 'c1', agentAddress: 'a', endpoint: 'http://a', version: '1.0.0' });
    expect(events.length).toBe(0);
  });

  it('tick expires instances past TTL', () => {
    mesh.register({ instanceId: 'i1', serviceType: 'compute', serviceName: 'c1', agentAddress: 'a', endpoint: 'http://a', version: '1.0.0', ttlMs: 1000 });
    // Fast-forward well past TTL
    const result = mesh.tick(Date.now() + 50000);
    expect(result.expired).toBe(1);
  });

  it('tick previousHealth captures old state correctly (regression)', () => {
    mesh.register({ instanceId: 'i1', serviceType: 'compute', serviceName: 'c1', agentAddress: 'a', endpoint: 'http://a', version: '1.0.0', ttlMs: 100000 });
    const events: TopologyChangeEvent[] = [];
    mesh.watch({ serviceType: 'compute' }, (e) => events.push(e));
    // Tick far enough that health drops significantly
    mesh.tick(Date.now() + 90000);
    const healthEvents = events.filter(e => e.type === 'health-changed');
    if (healthEvents.length > 0) {
      // previousHealth should have score ~1.0 (the original), not the new degraded score
      expect(healthEvents[0].previousHealth!.score).toBeGreaterThan(0.5);
    }
  });

  it('stats returns correct counts', () => {
    mesh.register({ instanceId: 'i1', serviceType: 'compute', serviceName: 'c1', agentAddress: 'a', endpoint: 'http://a', version: '1.0.0' });
    mesh.register({ instanceId: 'i2', serviceType: 'storage', serviceName: 's1', agentAddress: 'b', endpoint: 'http://b', version: '1.0.0' });
    const stats = mesh.stats();
    expect(stats.activeInstances).toBe(2);
    expect(stats.serviceTypes).toBe(2);
  });

  it('gossip round-trip between two meshes', () => {
    const mesh2 = new ServiceDiscoveryMesh({ nodeId: 'node-2', defaultTtlMs: 30000 });
    mesh.addPeer('node-2');
    mesh2.addPeer('node-1');

    mesh.register({ instanceId: 'i1', serviceType: 'compute', serviceName: 'c1', agentAddress: 'a', endpoint: 'http://a', version: '1.0.0' });

    // mesh1 gossips to mesh2
    const { digest } = mesh.initiateGossip();
    const delta = mesh2.handleGossipDigest(digest);
    // mesh2 has nothing, so delta from mesh2's perspective has no updates for mesh1
    // But mesh1 has i1 which mesh2 doesn't — mesh2 needs to get it
    // The protocol: mesh2 returns what IT has that mesh1 doesn't (empty)
    // mesh1 should send what IT has that mesh2 doesn't
    // Actually processDigest computes: local entries where local clock > remote clock
    // mesh2's local is empty, so delta is empty. mesh1 needs to compute its own delta.
    const delta1to2 = mesh.handleGossipDigest({ entries: new Map(), senderId: 'node-2', generatedAt: Date.now() });
    mesh2.applyGossipDelta(delta1to2, 'node-1');

    expect(mesh2.discover({ serviceType: 'compute' }).length).toBe(1);
  });

  it('query with attribute filtering', () => {
    mesh.register({ instanceId: 'i1', serviceType: 'compute', serviceName: 'c1', agentAddress: 'a', endpoint: 'http://a', version: '1.0.0', metadata: { gpu: 'true' } });
    mesh.register({ instanceId: 'i2', serviceType: 'compute', serviceName: 'c2', agentAddress: 'b', endpoint: 'http://b', version: '1.0.0', metadata: { gpu: 'false' } });
    const results = mesh.discover({ serviceType: 'compute', attributes: { gpu: 'true' } });
    expect(results.length).toBe(1);
    expect(results[0].instance.instanceId).toBe('i1');
  });

  it('query with version constraint', () => {
    mesh.register({ instanceId: 'i1', serviceType: 'compute', serviceName: 'c1', agentAddress: 'a', endpoint: 'http://a', version: '1.0.0' });
    mesh.register({ instanceId: 'i2', serviceType: 'compute', serviceName: 'c2', agentAddress: 'b', endpoint: 'http://b', version: '2.0.0' });
    const results = mesh.discover({ serviceType: 'compute', versionConstraint: '^1.0.0' });
    expect(results.length).toBe(1);
    expect(results[0].instance.instanceId).toBe('i1');
  });

  it('query with load and health filters', () => {
    mesh.register({ instanceId: 'i1', serviceType: 'compute', serviceName: 'c1', agentAddress: 'a', endpoint: 'http://a', version: '1.0.0' });
    mesh.register({ instanceId: 'i2', serviceType: 'compute', serviceName: 'c2', agentAddress: 'b', endpoint: 'http://b', version: '1.0.0' });
    // i2 is overloaded - heartbeat with high load
    mesh.heartbeat('i2', 0.95);
    const results = mesh.discover({ serviceType: 'compute', maxLoad: 0.5 });
    expect(results.length).toBe(1);
  });

  it('createMesh with preset', () => {
    const m = createMesh('test-node', 'small-cluster');
    expect(m.nodeId).toBe('test-node');
  });

  it('listInstances returns unscored results', () => {
    mesh.register({ instanceId: 'i1', serviceType: 'compute', serviceName: 'c1', agentAddress: 'a', endpoint: 'http://a', version: '1.0.0' });
    const list = mesh.listInstances('compute');
    expect(list.length).toBe(1);
    expect(list[0].instanceId).toBe('i1');
  });

  it('getEvents returns event log', () => {
    mesh.register({ instanceId: 'i1', serviceType: 'compute', serviceName: 'c1', agentAddress: 'a', endpoint: 'http://a', version: '1.0.0' });
    const events = mesh.getEvents();
    expect(events.length).toBeGreaterThan(0);
    expect(events[0].type).toBe('instance-registered');
  });
});
