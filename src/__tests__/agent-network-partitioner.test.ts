import { describe, it, expect, beforeEach } from 'vitest';
import {
  NetworkPartitioner, PartitionTopologyManager, PhiAccrualDetector,
  ControlledPartitionEngine, SplitBrainResolver, PartitionAwareRouter,
  HealingCoordinator, PartitionHistoryLogger, PRESETS,
  type AgentNode, type Partition,
} from '../agent-network-partitioner';

function makeNode(id: string, zone = 'us-east', priority = 1): AgentNode {
  return { id, zone, priority, capabilities: [], lastSeen: Date.now(), metadata: {} };
}

// ─── PhiAccrualDetector ──────────────────────────────────────────────
describe('PhiAccrualDetector', () => {
  it('returns 0 for unknown nodes', () => {
    const d = new PhiAccrualDetector();
    expect(d.phi('unknown', 1000)).toBe(0);
  });

  it('returns 0 with insufficient samples', () => {
    const d = new PhiAccrualDetector(100, 3);
    d.recordHeartbeat('a', 100);
    d.recordHeartbeat('a', 200);
    // Only 1 interval recorded, need 3
    expect(d.phi('a', 300)).toBe(0);
  });

  it('returns low phi for on-time heartbeats', () => {
    const d = new PhiAccrualDetector(100, 3);
    for (let t = 0; t <= 500; t += 100) d.recordHeartbeat('a', t);
    // Check phi right at expected time
    expect(d.phi('a', 600)).toBeLessThan(3);
  });

  it('returns high phi for late heartbeats', () => {
    const d = new PhiAccrualDetector(100, 3);
    for (let t = 0; t <= 500; t += 100) d.recordHeartbeat('a', t);
    // Way overdue
    expect(d.phi('a', 2000)).toBeGreaterThan(5);
  });

  it('removeNode clears state', () => {
    const d = new PhiAccrualDetector(100, 3);
    for (let t = 0; t <= 500; t += 100) d.recordHeartbeat('a', t);
    d.removeNode('a');
    expect(d.phi('a', 1000)).toBe(0);
  });
});

// ─── PartitionTopologyManager ────────────────────────────────────────
describe('PartitionTopologyManager', () => {
  let topo: PartitionTopologyManager;

  beforeEach(() => {
    topo = new PartitionTopologyManager();
  });

  it('adds and retrieves nodes', () => {
    topo.addNode(makeNode('a'));
    expect(topo.getNode('a')).toBeDefined();
    expect(topo.getAllNodes()).toHaveLength(1);
  });

  it('adds links and tracks adjacency', () => {
    topo.addNode(makeNode('a'));
    topo.addNode(makeNode('b'));
    topo.addLink('a', 'b', {});
    expect(topo.getNeighbors('a')).toContain('b');
    expect(topo.getNeighbors('b')).toContain('a');
    expect(topo.getLink('a', 'b')).toBeDefined();
  });

  it('removes node and its links', () => {
    topo.addNode(makeNode('a'));
    topo.addNode(makeNode('b'));
    topo.addLink('a', 'b', {});
    topo.removeNode('a');
    expect(topo.getNode('a')).toBeUndefined();
    expect(topo.getNeighbors('b')).not.toContain('a');
  });

  it('getConnectedComponents returns separate groups for failed links', () => {
    topo.addNode(makeNode('a'));
    topo.addNode(makeNode('b'));
    topo.addNode(makeNode('c'));
    topo.addLink('a', 'b', {});
    topo.addLink('b', 'c', {});
    // All connected
    expect(topo.getConnectedComponents()).toHaveLength(1);
    // Fail link b-c
    topo.updateLinkStatus('b', 'c', 'failed');
    const components = topo.getConnectedComponents();
    expect(components).toHaveLength(2);
  });

  it('detects bridge links', () => {
    topo.addNode(makeNode('a'));
    topo.addNode(makeNode('b'));
    topo.addNode(makeNode('c'));
    topo.addLink('a', 'b', {});
    topo.addLink('b', 'c', {});
    const bridges = topo.getBridgeLinks();
    expect(bridges).toHaveLength(2); // a-b and b-c are both bridges in a line
  });

  it('no bridges in a triangle', () => {
    topo.addNode(makeNode('a'));
    topo.addNode(makeNode('b'));
    topo.addNode(makeNode('c'));
    topo.addLink('a', 'b', {});
    topo.addLink('b', 'c', {});
    topo.addLink('a', 'c', {});
    expect(topo.getBridgeLinks()).toHaveLength(0);
  });
});

// ─── ControlledPartitionEngine ───────────────────────────────────────
describe('ControlledPartitionEngine', () => {
  it('creates and checks partitions', () => {
    const engine = new ControlledPartitionEngine();
    engine.createPartition('p1', [['a', 'b'], ['c', 'd']], 'test', 60000);
    expect(engine.isPartitioned('a', 'c')).toBe(true);
    expect(engine.isPartitioned('a', 'b')).toBe(false);
    expect(engine.isPartitioned('c', 'd')).toBe(false);
  });

  it('detects expired partitions', () => {
    const engine = new ControlledPartitionEngine();
    engine.createPartition('p1', [['a'], ['b']], 'test', 1000);
    expect(engine.getExpiredPartitions(Date.now() + 2000)).toContain('p1');
  });

  it('removes partitions', () => {
    const engine = new ControlledPartitionEngine();
    engine.createPartition('p1', [['a'], ['b']], 'test', 60000);
    expect(engine.removePartition('p1')).toBe(true);
    expect(engine.isPartitioned('a', 'b')).toBe(false);
  });
});

// ─── SplitBrainResolver ─────────────────────────────────────────────
describe('SplitBrainResolver', () => {
  const topo = new PartitionTopologyManager();
  const resolver = new SplitBrainResolver();

  function makePart(id: string, members: string[], opts: Partial<Partition> = {}): Partition {
    return { id, members: new Set(members), epoch: 1, createdAt: Date.now(), reason: 'detected', stateVersion: 1, ...opts };
  }

  beforeEach(() => {
    for (const id of ['a', 'b', 'c', 'd', 'e']) {
      topo.addNode(makeNode(id, 'us-east', id === 'c' ? 10 : 1));
    }
  });

  it('largest_partition picks biggest', () => {
    const res = resolver.resolve(
      [makePart('p1', ['a', 'b', 'c']), makePart('p2', ['d', 'e'])],
      'largest_partition', topo
    );
    expect(res.winningPartition).toBe('p1');
    expect(res.losingPartitions).toEqual(['p2']);
  });

  it('highest_priority picks partition with highest-priority node', () => {
    const res = resolver.resolve(
      [makePart('p1', ['a', 'b']), makePart('p2', ['c', 'd'])],
      'highest_priority', topo
    );
    // node 'c' has priority 10
    expect(res.winningPartition).toBe('p2');
  });

  it('most_recent_state picks highest stateVersion', () => {
    const res = resolver.resolve(
      [makePart('p1', ['a'], { stateVersion: 5 }), makePart('p2', ['b'], { stateVersion: 10 })],
      'most_recent_state', topo
    );
    expect(res.winningPartition).toBe('p2');
  });

  it('designated_leader picks partition with leader', () => {
    const res = resolver.resolve(
      [makePart('p1', ['a']), makePart('p2', ['b'], { leader: 'b' })],
      'designated_leader', topo
    );
    expect(res.winningPartition).toBe('p2');
  });

  it('quorum_based picks quorum partition', () => {
    const res = resolver.resolve(
      [makePart('p1', ['a', 'b', 'c']), makePart('p2', ['d', 'e'])],
      'quorum_based', topo
    );
    // 3 out of 5 = quorum
    expect(res.winningPartition).toBe('p1');
  });

  it('handles single partition', () => {
    const res = resolver.resolve([makePart('p1', ['a'])], 'largest_partition', topo);
    expect(res.winningPartition).toBe('p1');
    expect(res.losingPartitions).toEqual([]);
  });
});

// ─── PartitionAwareRouter ────────────────────────────────────────────
describe('PartitionAwareRouter', () => {
  it('blocks cross-partition routing', () => {
    const router = new PartitionAwareRouter();
    router.updateMembership([
      { id: 'p1', members: new Set(['a', 'b']), epoch: 1, createdAt: 0, reason: 'detected', stateVersion: 1 },
      { id: 'p2', members: new Set(['c', 'd']), epoch: 1, createdAt: 0, reason: 'detected', stateVersion: 1 },
    ]);
    expect(router.canRoute('a', 'b')).toBe(true);
    expect(router.canRoute('a', 'c')).toBe(false);
  });

  it('allows routing for unknown nodes', () => {
    const router = new PartitionAwareRouter();
    expect(router.canRoute('x', 'y')).toBe(true);
  });

  it('getReachableNodes returns same-partition nodes', () => {
    const router = new PartitionAwareRouter();
    router.updateMembership([
      { id: 'p1', members: new Set(['a', 'b']), epoch: 1, createdAt: 0, reason: 'detected', stateVersion: 1 },
    ]);
    const reachable = router.getReachableNodes('a');
    expect(reachable).toContain('a');
    expect(reachable).toContain('b');
  });

  it('routeMessage finds path within partition', () => {
    const router = new PartitionAwareRouter();
    const topo = new PartitionTopologyManager();
    topo.addNode(makeNode('a'));
    topo.addNode(makeNode('b'));
    topo.addNode(makeNode('c'));
    topo.addLink('a', 'b', {});
    topo.addLink('b', 'c', {});
    router.updateMembership([
      { id: 'p1', members: new Set(['a', 'b', 'c']), epoch: 1, createdAt: 0, reason: 'detected', stateVersion: 1 },
    ]);
    const result = router.routeMessage('a', 'c', {}, topo);
    expect(result.delivered).toBe(true);
    expect(result.hops).toEqual(['a', 'b', 'c']);
  });

  it('routeMessage fails cross-partition', () => {
    const router = new PartitionAwareRouter();
    const topo = new PartitionTopologyManager();
    router.updateMembership([
      { id: 'p1', members: new Set(['a']), epoch: 1, createdAt: 0, reason: 'detected', stateVersion: 1 },
      { id: 'p2', members: new Set(['b']), epoch: 1, createdAt: 0, reason: 'detected', stateVersion: 1 },
    ]);
    const result = router.routeMessage('a', 'b', {}, topo);
    expect(result.delivered).toBe(false);
    expect(result.reason).toBe('cross_partition');
  });
});

// ─── HealingCoordinator ─────────────────────────────────────────────
describe('HealingCoordinator', () => {
  it('creates and advances healing plans', () => {
    const hc = new HealingCoordinator(2);
    const plan = hc.createPlan('p1', 'p2', 5);
    expect(plan).not.toBeNull();
    expect(plan!.phase).toBe('quiesce');

    hc.advancePhase(plan!.id); // reconcile
    expect(hc.getActivePlans()[0].phase).toBe('reconcile');

    hc.advancePhase(plan!.id); // merge
    hc.advancePhase(plan!.id); // verify
    hc.advancePhase(plan!.id); // complete - removes from active
    expect(hc.getActivePlans()).toHaveLength(0);
  });

  it('respects max concurrent limit', () => {
    const hc = new HealingCoordinator(1);
    expect(hc.createPlan('p1', 'p2', 0)).not.toBeNull();
    expect(hc.createPlan('p3', 'p4', 0)).toBeNull();
  });

  it('cancels plans', () => {
    const hc = new HealingCoordinator(2);
    const plan = hc.createPlan('p1', 'p2', 0)!;
    expect(hc.cancelPlan(plan.id)).toBe(true);
    expect(hc.getActivePlans()).toHaveLength(0);
  });
});

// ─── PartitionHistoryLogger ──────────────────────────────────────────
describe('PartitionHistoryLogger', () => {
  it('logs and retrieves events', () => {
    const logger = new PartitionHistoryLogger(86400000);
    logger.log({ type: 'link_failed', timestamp: Date.now(), details: { src: 'a' } });
    logger.log({ type: 'partition_detected', timestamp: Date.now(), details: {} });
    expect(logger.getEvents()).toHaveLength(2);
    expect(logger.getEventsByType('link_failed')).toHaveLength(1);
  });

  it('verifies integrity of untampered log', () => {
    const now = Date.now();
    const logger = new PartitionHistoryLogger(86400000);
    logger.log({ type: 'link_failed', timestamp: now, details: { a: 1 } });
    logger.log({ type: 'link_restored', timestamp: now + 1000, details: { b: 2 } });
    expect(logger.verifyIntegrity()).toBe(true);
  });

  it('getEvents with since filter', () => {
    const now = Date.now();
    const logger = new PartitionHistoryLogger(86400000);
    logger.log({ type: 'link_failed', timestamp: now, details: {} });
    logger.log({ type: 'link_restored', timestamp: now + 1000, details: {} });
    expect(logger.getEvents(now + 500)).toHaveLength(1);
  });
});

// ─── NetworkPartitioner (Integration) ────────────────────────────────
describe('NetworkPartitioner', () => {
  let np: NetworkPartitioner;

  beforeEach(() => {
    np = new NetworkPartitioner({ autoHealEnabled: false, minPartitionSize: 1 });
  });

  function setupTriangle() {
    np.addNode(makeNode('a', 'us-east', 5));
    np.addNode(makeNode('b', 'us-east', 3));
    np.addNode(makeNode('c', 'eu-west', 1));
    np.addLink('a', 'b');
    np.addLink('b', 'c');
    np.addLink('a', 'c');
  }

  it('starts with single partition', () => {
    setupTriangle();
    np.tick();
    expect(np.getPartitions()).toHaveLength(1);
    expect(np.getPartitions()[0].members.size).toBe(3);
  });

  it('detects partitions after controlled split', () => {
    setupTriangle();
    np.createControlledPartition('test', [['a', 'b'], ['c']], 'maintenance');
    expect(np.getPartitions()).toHaveLength(2);
  });

  it('routes within partition, blocks across', () => {
    setupTriangle();
    np.createControlledPartition('test', [['a', 'b'], ['c']], 'test');
    np.tick(); // updates router membership
    const ok = np.routeMessage('a', 'b', { data: 1 });
    expect(ok.delivered).toBe(true);
    const fail = np.routeMessage('a', 'c', { data: 2 });
    expect(fail.delivered).toBe(false);
  });

  it('phi-based failure detection creates partition events', () => {
    np = new NetworkPartitioner({ autoHealEnabled: false, minPartitionSize: 1, phiThreshold: 4 });
    np.addNode(makeNode('a'));
    np.addNode(makeNode('b'));
    np.addLink('a', 'b');

    // Build heartbeat history
    for (let t = 0; t <= 500; t += 100) {
      np.recordHeartbeat('a', t);
      np.recordHeartbeat('b', t);
    }

    // Node b goes silent, tick much later
    const events = np.tick(5000);
    const linkFailed = events.filter(e => e.type === 'link_failed');
    expect(linkFailed.length).toBeGreaterThan(0);
  });

  it('auto-healing creates plans when enabled', () => {
    np = new NetworkPartitioner({ autoHealEnabled: true, minPartitionSize: 1 });
    np.addNode(makeNode('a'));
    np.addNode(makeNode('b'));
    np.addLink('a', 'b');
    np.createControlledPartition('test', [['a'], ['b']], 'test');
    // Tick triggers healing since there are 2 partitions
    const events = np.tick();
    const healingStarted = events.filter(e => e.type === 'healing_started');
    expect(healingStarted.length).toBeGreaterThan(0);
    expect(np.getHealingPlans().length).toBeGreaterThan(0);
  });

  it('getDashboard returns summary', () => {
    setupTriangle();
    np.tick();
    const dash = np.getDashboard();
    expect(dash.nodeCount).toBe(3);
    expect(dash.partitionCount).toBe(1);
    expect(dash.auditIntegrity).toBe(true);
  });

  it('verifyAuditIntegrity returns true', () => {
    setupTriangle();
    np.tick();
    expect(np.verifyAuditIntegrity()).toBe(true);
  });

  it('getHistory returns logged events', () => {
    setupTriangle();
    np.createControlledPartition('test', [['a'], ['b', 'c']], 'test');
    expect(np.getHistory().length).toBeGreaterThan(0);
  });

  it('removeNode updates partitions', () => {
    setupTriangle();
    np.tick();
    np.removeNode('c');
    np.tick();
    const parts = np.getPartitions();
    for (const p of parts) {
      expect(p.members.has('c')).toBe(false);
    }
  });

  it('getReachableFrom returns partition members', () => {
    setupTriangle();
    np.createControlledPartition('test', [['a', 'b'], ['c']], 'test');
    np.tick(); // updates router membership
    const reachable = np.getReachableFrom('a');
    expect(reachable).toContain('a');
    expect(reachable).toContain('b');
    expect(reachable).not.toContain('c');
  });

  it('getBridgeLinks delegates to topology', () => {
    np.addNode(makeNode('a'));
    np.addNode(makeNode('b'));
    np.addNode(makeNode('c'));
    np.addLink('a', 'b');
    np.addLink('b', 'c');
    expect(np.getBridgeLinks()).toHaveLength(2);
  });
});

// ─── Presets ─────────────────────────────────────────────────────────
describe('Presets', () => {
  it('all presets create valid partitioners', () => {
    for (const [name, config] of Object.entries(PRESETS)) {
      const np = new NetworkPartitioner(config);
      np.addNode(makeNode('a'));
      np.tick();
      expect(np.getPartitions().length).toBeGreaterThanOrEqual(0);
    }
  });

  it('chaos-testing preset has autoHeal disabled', () => {
    expect(PRESETS['chaos-testing'].autoHealEnabled).toBe(false);
  });
});
