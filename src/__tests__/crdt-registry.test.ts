import { describe, it, expect, beforeEach } from 'vitest';
import {
  CRDTRegistry,
  VectorClockUtil,
  LamportClock,
  MerkleTree,
  TombstoneGC,
  type RegistryConfig,
  type AgentMetadata,
  type AgentEntry,
  type VectorClock,
  type GCConfig,
} from '../crdt-registry';

// ─── Helpers ────────────────────────────────────────────────────────────

const defaultMeta: AgentMetadata = {
  name: 'test-agent',
  description: 'A test agent',
  version: '1.0.0',
  endpoints: ['http://localhost:3000'],
  tags: ['test'],
  customFields: new Map(),
};

function makeConfig(nodeId: string): RegistryConfig {
  return {
    nodeId,
    heartbeatTimeoutMs: 5000,
    gcConfig: { tombstoneRetentionMs: 1000, gcIntervalMs: 500, maxTombstonesBeforeForceGC: 100 },
    maxOpsPerSync: 500,
    enableAntiEntropy: true,
  };
}

// ─── VectorClockUtil ────────────────────────────────────────────────────

describe('VectorClockUtil', () => {
  it('creates empty clock', () => {
    const c = VectorClockUtil.create();
    expect(c.entries.size).toBe(0);
  });

  it('increments a dimension', () => {
    let c = VectorClockUtil.create();
    c = VectorClockUtil.increment(c, 'a');
    expect(c.entries.get('a')).toBe(1);
    c = VectorClockUtil.increment(c, 'a');
    expect(c.entries.get('a')).toBe(2);
  });

  it('merges two clocks (pointwise max)', () => {
    const a: VectorClock = { entries: new Map([['x', 3], ['y', 1]]) };
    const b: VectorClock = { entries: new Map([['x', 1], ['y', 5], ['z', 2]]) };
    const m = VectorClockUtil.merge(a, b);
    expect(m.entries.get('x')).toBe(3);
    expect(m.entries.get('y')).toBe(5);
    expect(m.entries.get('z')).toBe(2);
  });

  it('compares clocks: dominates', () => {
    const a: VectorClock = { entries: new Map([['x', 2], ['y', 3]]) };
    const b: VectorClock = { entries: new Map([['x', 1], ['y', 2]]) };
    expect(VectorClockUtil.compare(a, b)).toBe(1);
    expect(VectorClockUtil.dominates(a, b)).toBe(true);
  });

  it('compares clocks: concurrent', () => {
    const a: VectorClock = { entries: new Map([['x', 2], ['y', 1]]) };
    const b: VectorClock = { entries: new Map([['x', 1], ['y', 2]]) };
    expect(VectorClockUtil.compare(a, b)).toBe(0);
    expect(VectorClockUtil.concurrent(a, b)).toBe(true);
  });
});

// ─── LamportClock ───────────────────────────────────────────────────────

describe('LamportClock', () => {
  it('ticks monotonically', () => {
    const c = new LamportClock('node-1');
    const t1 = c.tick();
    const t2 = c.tick();
    expect(t2.counter).toBeGreaterThan(t1.counter);
    expect(t1.nodeId).toBe('node-1');
  });

  it('updates from remote and advances past it', () => {
    const c = new LamportClock('node-1');
    c.update({ counter: 10, nodeId: 'node-2' });
    const t = c.tick();
    expect(t.counter).toBeGreaterThan(10);
  });

  it('compares timestamps by counter then nodeId', () => {
    expect(LamportClock.compare({ counter: 1, nodeId: 'a' }, { counter: 2, nodeId: 'a' })).toBeLessThan(0);
    expect(LamportClock.compare({ counter: 2, nodeId: 'a' }, { counter: 2, nodeId: 'b' })).toBeLessThan(0);
    expect(LamportClock.compare({ counter: 2, nodeId: 'b' }, { counter: 2, nodeId: 'b' })).toBe(0);
  });
});

// ─── MerkleTree ─────────────────────────────────────────────────────────

describe('MerkleTree', () => {
  it('builds a tree from agent entries and diffs', () => {
    const reg1 = new CRDTRegistry(makeConfig('n1'));
    const reg2 = new CRDTRegistry(makeConfig('n2'));

    reg1.registerAgent('agent-a', 'addr-a', defaultMeta, ['cap-1']);
    reg1.registerAgent('agent-b', 'addr-b', defaultMeta, ['cap-2']);
    reg2.registerAgent('agent-a', 'addr-a', defaultMeta, ['cap-1']);

    const digest1 = reg1.getMerkleDigest();
    const digest2 = reg2.getMerkleDigest();

    const divergent = MerkleTree.diff(digest1, digest2);
    // agent-b only on reg1; agent-a may also show since metadata timestamps differ
    expect(divergent.length).toBeGreaterThan(0);
  });

  it('same tree yields no diff', () => {
    const agents = new Map<string, AgentEntry>();
    const tree = MerkleTree.build(agents);
    const diff = MerkleTree.diff(tree, tree);
    expect(diff.length).toBe(0);
  });
});

// ─── TombstoneGC ────────────────────────────────────────────────────────

describe('TombstoneGC', () => {
  it('collects expired tombstones', () => {
    const gc = new TombstoneGC({ tombstoneRetentionMs: 100, gcIntervalMs: 50, maxTombstonesBeforeForceGC: 10 });
    const set = {
      elements: new Map(),
      tombstones: new Map([['t1', { removedAt: 1000, removedBy: 'n1' }]]),
    };
    const cleaned = gc.collect(set, 2000); // 2000 - 1000 = 1000 > 100 retention
    expect(cleaned.tombstones.size).toBe(0);
  });

  it('keeps fresh tombstones', () => {
    const gc = new TombstoneGC({ tombstoneRetentionMs: 5000, gcIntervalMs: 50, maxTombstonesBeforeForceGC: 10 });
    const now = Date.now();
    const set = {
      elements: new Map(),
      tombstones: new Map([['t1', { removedAt: now, removedBy: 'n1' }]]),
    };
    const cleaned = gc.collect(set, now + 100);
    expect(cleaned.tombstones.size).toBe(1);
  });

  it('shouldRun respects interval and force threshold', () => {
    const gc = new TombstoneGC({ tombstoneRetentionMs: 5000, gcIntervalMs: 1000, maxTombstonesBeforeForceGC: 5 });
    expect(gc.shouldRun(6, 1000)).toBe(true); // over force threshold
    // After shouldRun with force, lastGC is still 0 (shouldRun doesn't update it)
    // But collect() does update lastGC. So shouldRun(1, 2000) checks 2000-0 >= 1000 → true
    expect(gc.shouldRun(1, 2000)).toBe(true); // interval elapsed from lastGC=0
  });
});

// ─── CRDTRegistry ───────────────────────────────────────────────────────

describe('CRDTRegistry', () => {
  let reg: CRDTRegistry;

  beforeEach(() => {
    reg = new CRDTRegistry(makeConfig('node-1'));
  });

  it('registers and retrieves an agent', () => {
    reg.registerAgent('agent-a', 'addr-a', defaultMeta, ['nlp', 'vision']);
    const agent = reg.getAgent('agent-a');
    expect(agent).toBeDefined();
    expect(agent!.agentId).toBe('agent-a');
    expect(agent!.status.value).toBe('online');
  });

  it('finds agents by capability', () => {
    reg.registerAgent('a1', 'addr', defaultMeta, ['nlp']);
    reg.registerAgent('a2', 'addr', defaultMeta, ['vision']);
    reg.registerAgent('a3', 'addr', defaultMeta, ['nlp', 'vision']);
    expect(reg.findByCapability('nlp').length).toBe(2);
    expect(reg.findByCapability('vision').length).toBe(2);
    expect(reg.findByCapability('audio').length).toBe(0);
  });

  it('finds agents by status', () => {
    reg.registerAgent('a1', 'addr', defaultMeta, []);
    expect(reg.findByStatus('online').length).toBe(1);
    expect(reg.findByStatus('offline').length).toBe(0);
  });

  it('finds agents by tag', () => {
    const taggedMeta = { ...defaultMeta, tags: ['gpu', 'fast'] };
    reg.registerAgent('a1', 'addr', taggedMeta, []);
    expect(reg.findByTag('gpu').length).toBe(1);
    expect(reg.findByTag('slow').length).toBe(0);
  });

  it('deregisters an agent', () => {
    reg.registerAgent('a1', 'addr', defaultMeta, ['cap']);
    const op = reg.deregisterAgent('a1');
    expect(op).not.toBeNull();
    expect(reg.getAgent('a1')).toBeUndefined();
  });

  it('updates metadata (LWW)', () => {
    reg.registerAgent('a1', 'addr', defaultMeta, []);
    const newMeta = { ...defaultMeta, name: 'updated-agent', version: '2.0.0' };
    reg.updateMetadata('a1', newMeta);
    expect(reg.getAgent('a1')!.metadata.value.name).toBe('updated-agent');
  });

  it('adds and removes capabilities', () => {
    reg.registerAgent('a1', 'addr', defaultMeta, ['nlp']);
    reg.addCapability('a1', 'vision');
    expect(reg.findByCapability('vision').length).toBe(1);
    reg.removeCapability('a1', 'vision');
    expect(reg.findByCapability('vision').length).toBe(0);
  });

  it('heartbeat updates lastSeen', () => {
    reg.registerAgent('a1', 'addr', defaultMeta, []);
    const before = reg.getAgent('a1')!.lastSeen;
    // Small delay
    reg.heartbeat('a1');
    const after = reg.getAgent('a1')!.lastSeen;
    expect(after).toBeGreaterThanOrEqual(before);
  });

  it('checkLiveness marks timed-out agents offline', () => {
    reg.registerAgent('a1', 'addr', defaultMeta, []);
    const timedOut = reg.checkLiveness(Date.now() + 10000); // 10s later, threshold 5s
    expect(timedOut).toContain('a1');
    expect(reg.getAgent('a1')!.status.value).toBe('offline');
  });

  it('replicates ops between two registries', () => {
    const reg2 = new CRDTRegistry(makeConfig('node-2'));

    const op = reg.registerAgent('a1', 'addr', defaultMeta, ['cap']);
    reg2.receiveOps([op], 'node-1');

    expect(reg2.getAgent('a1')).toBeDefined();
    expect(reg2.getAgent('a1')!.metadata.value.name).toBe('test-agent');
  });

  it('converges with concurrent updates (LWW metadata)', () => {
    const reg2 = new CRDTRegistry(makeConfig('node-2'));

    const op1 = reg.registerAgent('a1', 'addr', defaultMeta, []);
    reg2.receiveOps([op1], 'node-1');

    // Both update metadata concurrently
    const metaA = { ...defaultMeta, name: 'from-node-1' };
    const metaB = { ...defaultMeta, name: 'from-node-2' };
    const opA = reg.updateMetadata('a1', metaA)!;
    const opB = reg2.updateMetadata('a1', metaB)!;

    // Cross-apply
    reg.receiveOps([opB], 'node-2');
    reg2.receiveOps([opA], 'node-1');

    // Both should converge to same value (LWW by Lamport timestamp)
    expect(reg.getAgent('a1')!.metadata.value.name).toBe(reg2.getAgent('a1')!.metadata.value.name);
  });

  it('returns null ops for non-existent agents', () => {
    expect(reg.deregisterAgent('nope')).toBeNull();
    expect(reg.updateMetadata('nope', defaultMeta)).toBeNull();
    expect(reg.addCapability('nope', 'x')).toBeNull();
    expect(reg.removeCapability('nope', 'x')).toBeNull();
    expect(reg.heartbeat('nope')).toBeNull();
  });

  it('getStats returns correct counts', () => {
    reg.registerAgent('a1', 'addr', defaultMeta, ['cap']);
    const stats = reg.getStats();
    expect(stats.agentCount).toBe(1);
    expect(stats.onlineCount).toBe(1);
    expect(stats.opsProcessed).toBeGreaterThan(0);
  });

  it('getOpsSince returns delta ops', () => {
    const cursor = reg.getOpCursor();
    reg.registerAgent('a1', 'addr', defaultMeta, []);
    reg.registerAgent('a2', 'addr', defaultMeta, []);
    const ops = reg.getOpsSince('node-2', cursor);
    expect(ops.length).toBe(2);
  });

  it('bootstrapFromState merges entries', () => {
    reg.registerAgent('a1', 'addr', defaultMeta, ['cap']);
    const state = reg.getFullState();

    const reg2 = new CRDTRegistry(makeConfig('node-2'));
    reg2.bootstrapFromState(state, 'node-1');
    expect(reg2.getAgent('a1')).toBeDefined();
  });

  it('detectSplitBrain returns empty for healthy state', () => {
    reg.registerAgent('a1', 'addr', defaultMeta, []);
    const conflicts = reg.detectSplitBrain();
    expect(conflicts.length).toBe(0);
  });

  it('emits events via on()', () => {
    const events: string[] = [];
    reg.on('agent-added', (e) => events.push(e.detail));
    reg.registerAgent('a1', 'addr', defaultMeta, ['cap']);
    expect(events.length).toBe(1);
    expect(events[0]).toContain('Agent registered');
  });

  it('getAllAgents and getOnlineAgents', () => {
    reg.registerAgent('a1', 'addr', defaultMeta, []);
    reg.registerAgent('a2', 'addr', defaultMeta, []);
    expect(reg.getAllAgents().length).toBe(2);
    expect(reg.getOnlineAgents().length).toBe(2);
  });
});
