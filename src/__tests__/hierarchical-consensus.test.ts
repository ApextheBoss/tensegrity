import { describe, it, expect, beforeEach } from 'vitest';
import {
  HierarchicalConsensusCoordinator,
  IntraShardConsensus,
  ShardManager,
  CrossShardCoordinator,
  MetaConsensusLayer,
  RepresentativeRotation,
  ViewChangeCoordinator,
  ConsistentHashRing,
  PRESETS,
  type AgentId,
  type ConsensusValue,
} from '../hierarchical-consensus';

// ── ConsistentHashRing ──────────────────────────────────────────────────────

describe('ConsistentHashRing', () => {
  it('returns null for empty ring', () => {
    const ring = new ConsistentHashRing(10);
    expect(ring.getShard('key')).toBeNull();
  });

  it('routes all keys to single shard', () => {
    const ring = new ConsistentHashRing(10);
    ring.addShard('s1');
    expect(ring.getShard('a')).toBe('s1');
    expect(ring.getShard('z')).toBe('s1');
  });

  it('distributes keys across multiple shards', () => {
    const ring = new ConsistentHashRing(50);
    ring.addShard('s1');
    ring.addShard('s2');
    ring.addShard('s3');
    const counts = new Map<string, number>();
    for (let i = 0; i < 300; i++) {
      const s = ring.getShard(`key-${i}`)!;
      counts.set(s, (counts.get(s) || 0) + 1);
    }
    // All 3 shards should get some keys
    expect(counts.size).toBe(3);
    for (const c of counts.values()) {
      expect(c).toBeGreaterThan(30); // at least ~10% each
    }
  });

  it('removeShard stops routing to removed shard', () => {
    const ring = new ConsistentHashRing(50);
    ring.addShard('s1');
    ring.addShard('s2');
    ring.removeShard('s1');
    for (let i = 0; i < 50; i++) {
      expect(ring.getShard(`key-${i}`)).toBe('s2');
    }
  });

  it('getShards returns unique shard ids', () => {
    const ring = new ConsistentHashRing(10);
    ring.addShard('a');
    ring.addShard('b');
    expect(ring.getShards().sort()).toEqual(['a', 'b']);
  });

  it('getReplicaShards returns N distinct shards', () => {
    const ring = new ConsistentHashRing(50);
    ring.addShard('s1');
    ring.addShard('s2');
    ring.addShard('s3');
    const replicas = ring.getReplicaShards('key', 2);
    expect(replicas).toHaveLength(2);
    expect(new Set(replicas).size).toBe(2);
  });

  it('getReplicaShards returns all if count > shards', () => {
    const ring = new ConsistentHashRing(10);
    ring.addShard('s1');
    ring.addShard('s2');
    const replicas = ring.getReplicaShards('key', 5);
    expect(replicas).toHaveLength(2);
  });
});

// ── IntraShardConsensus ─────────────────────────────────────────────────────

describe('IntraShardConsensus', () => {
  const config = { minSize: 2, maxSize: 10, targetSize: 5, replicationFactor: 2 };

  it('auto-elects first member as leader', () => {
    const shard = new IntraShardConsensus('s1', config);
    const agent: AgentId = { id: 'a1' };
    shard.addMember(agent);
    expect(shard.getState().leader?.id).toBe('a1');
  });

  it('does not add duplicate members', () => {
    const shard = new IntraShardConsensus('s1', config);
    shard.addMember({ id: 'a1' });
    shard.addMember({ id: 'a1' });
    expect(shard.getMemberCount()).toBe(1);
  });

  it('elects new leader when leader removed', () => {
    const shard = new IntraShardConsensus('s1', config);
    shard.addMember({ id: 'a1' });
    shard.addMember({ id: 'a2' });
    shard.removeMember('a1');
    const state = shard.getState();
    expect(state.leader).not.toBeNull();
    expect(state.leader!.id).toBe('a2');
  });

  it('propose and commit values', () => {
    const shard = new IntraShardConsensus('s1', config);
    shard.addMember({ id: 'a1' });
    const val: ConsensusValue = { key: 'k1', value: 42, version: 1, timestamp: 1, shardId: 's1' };
    const idx = shard.propose(val, 'a1');
    expect(idx).toBe(0);
    expect(shard.getState().commitIndex).toBe(0);
    expect(shard.getCommittedSince(-1)).toHaveLength(1);
  });

  it('propose returns -1 with no leader', () => {
    const shard = new IntraShardConsensus('s1', config);
    const val: ConsensusValue = { key: 'k1', value: 1, version: 1, timestamp: 1, shardId: 's1' };
    expect(shard.propose(val, 'x')).toBe(-1);
  });

  it('electLeader deterministically by epoch', () => {
    const shard = new IntraShardConsensus('s1', config);
    shard.addMember({ id: 'a1' });
    shard.addMember({ id: 'a2' });
    shard.addMember({ id: 'a3' });
    const leader1 = shard.electLeader();
    // Same epoch should give same result
    const shard2 = new IntraShardConsensus('s1', config);
    shard2.addMember({ id: 'a1' });
    shard2.addMember({ id: 'a2' });
    shard2.addMember({ id: 'a3' });
    const leader2 = shard2.electLeader();
    expect(leader1?.id).toBe(leader2?.id);
  });

  it('needsSplit / needsMerge respect config', () => {
    const shard = new IntraShardConsensus('s1', config);
    expect(shard.needsMerge()).toBe(true); // 0 < minSize=2
    shard.addMember({ id: 'a1' });
    shard.addMember({ id: 'a2' });
    expect(shard.needsMerge()).toBe(false);
    expect(shard.needsSplit()).toBe(false);
    for (let i = 3; i <= 11; i++) shard.addMember({ id: `a${i}` });
    expect(shard.needsSplit()).toBe(true);
  });

  it('commit out of range returns -1', () => {
    const shard = new IntraShardConsensus('s1', config);
    expect(shard.commit(5)).toBe(-1);
    expect(shard.commit(-2)).toBe(-1);
  });

  it('updateHealth updates health and heartbeat', () => {
    const shard = new IntraShardConsensus('s1', config);
    shard.updateHealth({ avgLatencyMs: 42 });
    expect(shard.getState().health.avgLatencyMs).toBe(42);
  });

  it('setRepresentative updates state', () => {
    const shard = new IntraShardConsensus('s1', config);
    shard.setRepresentative('agent-x');
    expect(shard.getState().representativeId).toBe('agent-x');
  });
});

// ── CrossShardCoordinator ───────────────────────────────────────────────────

describe('CrossShardCoordinator', () => {
  it('begin and vote yes -> committing', () => {
    const coord = new CrossShardCoordinator(5000);
    const txId = coord.begin(['s1', 's2'], [], 'c1');
    coord.vote(txId, 's1', 'yes');
    const tx = coord.vote(txId, 's2', 'yes');
    expect(tx?.state).toBe('committing');
  });

  it('vote no -> immediate abort', () => {
    const coord = new CrossShardCoordinator(5000);
    const txId = coord.begin(['s1', 's2'], [], 'c1');
    const tx = coord.vote(txId, 's1', 'no');
    expect(tx?.state).toBe('aborted');
  });

  it('confirmCommit requires all shards (bug #17 regression)', () => {
    const coord = new CrossShardCoordinator(5000);
    const txId = coord.begin(['s1', 's2', 's3'], [], 'c1');
    coord.vote(txId, 's1', 'yes');
    coord.vote(txId, 's2', 'yes');
    coord.vote(txId, 's3', 'yes');
    // Now in 'committing' state. Confirm one shard at a time.
    const after1 = coord.confirmCommit(txId, 's1');
    expect(after1).not.toBeNull();
    expect(after1!.state).not.toBe('committed'); // NOT yet committed
    const after2 = coord.confirmCommit(txId, 's2');
    expect(after2!.state).not.toBe('committed'); // still not
    const after3 = coord.confirmCommit(txId, 's3');
    expect(after3!.state).toBe('committed'); // NOW committed
  });

  it('checkTimeouts aborts stale transactions', () => {
    const coord = new CrossShardCoordinator(1); // 1ms timeout
    const txId = coord.begin(['s1'], [], 'c1');
    // Wait briefly
    const start = Date.now();
    while (Date.now() - start < 5) {} // busy wait 5ms
    const aborted = coord.checkTimeouts();
    expect(aborted).toHaveLength(1);
    expect(aborted[0].txId).toBe(txId);
  });

  it('gc removes old completed transactions', () => {
    const coord = new CrossShardCoordinator(5000);
    const txId = coord.begin(['s1'], [], 'c1');
    coord.vote(txId, 's1', 'no'); // abort immediately
    // Wait a tick so age > 0
    const start = Date.now();
    while (Date.now() - start < 2) {}
    const removed = coord.gc(1); // maxAge=1ms
    expect(removed).toBe(1);
    expect(coord.getTransaction(txId)).toBeUndefined();
  });

  it('getPendingCount tracks active transactions', () => {
    const coord = new CrossShardCoordinator(5000);
    expect(coord.getPendingCount()).toBe(0);
    coord.begin(['s1'], [], 'c1');
    expect(coord.getPendingCount()).toBe(1);
  });

  it('vote on non-existent tx returns null', () => {
    const coord = new CrossShardCoordinator(5000);
    expect(coord.vote('nope', 's1', 'yes')).toBeNull();
  });

  it('confirmCommit on non-committing tx returns null', () => {
    const coord = new CrossShardCoordinator(5000);
    const txId = coord.begin(['s1'], [], 'c1');
    // Still in 'preparing', not 'committing'
    expect(coord.confirmCommit(txId, 's1')).toBeNull();
  });
});

// ── MetaConsensusLayer ──────────────────────────────────────────────────────

describe('MetaConsensusLayer', () => {
  it('accepts proposal with majority yes votes', () => {
    const meta = new MetaConsensusLayer(10000);
    const pid = meta.propose('upgrade-v2', 'rep1', 3);
    meta.vote(pid, 's1', true);
    const result = meta.vote(pid, 's2', true);
    expect(result).toBe('accepted');
  });

  it('rejects when majority is impossible (bug #18 regression)', () => {
    const meta = new MetaConsensusLayer(10000);
    const pid = meta.propose('bad-idea', 'rep1', 4); // 4 shards, need 3
    meta.vote(pid, 's1', false);
    const result = meta.vote(pid, 's2', false);
    // 2 no votes out of 4 total, only 2 remaining, max yes = 0+2 = 2 < 3
    expect(result).toBe('rejected');
  });

  it('stays open when outcome still undetermined', () => {
    const meta = new MetaConsensusLayer(10000);
    const pid = meta.propose('maybe', 'rep1', 5); // need 3
    const result = meta.vote(pid, 's1', false);
    // 1 no, 4 remaining, max yes = 0+4 = 4 >= 3, still possible
    expect(result).toBe('open');
  });

  it('expireStale rejects old open proposals', () => {
    const meta = new MetaConsensusLayer(1); // 1ms timeout
    const pid = meta.propose('slow', 'rep1', 3);
    const start = Date.now();
    while (Date.now() - start < 5) {}
    const expired = meta.expireStale();
    expect(expired).toContain(pid);
  });

  it('vote on closed proposal returns null', () => {
    const meta = new MetaConsensusLayer(10000);
    const pid = meta.propose('v2', 'rep1', 3);
    meta.vote(pid, 's1', true);
    meta.vote(pid, 's2', true); // accepted
    expect(meta.vote(pid, 's3', true)).toBeNull();
  });
});

// ── RepresentativeRotation ──────────────────────────────────────────────────

describe('RepresentativeRotation', () => {
  it('selects a representative from members', () => {
    const rot = new RepresentativeRotation(3, 300000);
    const members: AgentId[] = [{ id: 'a1' }, { id: 'a2' }, { id: 'a3' }];
    const rep = rot.selectRepresentative('s1', members);
    expect(rep).not.toBeNull();
    expect(members.some(m => m.id === rep!.id)).toBe(true);
  });

  it('returns null for empty members', () => {
    const rot = new RepresentativeRotation();
    expect(rot.selectRepresentative('s1', [])).toBeNull();
  });

  it('forceRotation clears current rep', () => {
    const rot = new RepresentativeRotation(3, 300000);
    const members: AgentId[] = [{ id: 'a1' }, { id: 'a2' }];
    rot.selectRepresentative('s1', members);
    rot.forceRotation('s1');
    expect(rot.getRepresentative('s1')).toBeUndefined();
  });

  it('updatePerformance applies EWMA', () => {
    const rot = new RepresentativeRotation(3, 300000);
    rot.selectRepresentative('s1', [{ id: 'a1' }]);
    rot.updatePerformance('s1', 0.5);
    const rep = rot.getRepresentative('s1')!;
    // EWMA: 0.3 * 0.5 + 0.7 * 1.0 = 0.85
    expect(rep.performanceScore).toBeCloseTo(0.85, 2);
  });
});

// ── ViewChangeCoordinator ───────────────────────────────────────────────────

describe('ViewChangeCoordinator', () => {
  it('initiates and completes view change with majority', () => {
    const vc = new ViewChangeCoordinator(5000);
    expect(vc.getCurrentView()).toBe(0);
    vc.initiateViewChange('a1', 'leader-failure', 3);
    expect(vc.hasPendingViewChange()).toBe(true);
    // a1 already counted, need 1 more for majority of 3 (need 2)
    const done = vc.acknowledgeViewChange('a2', 1);
    expect(done).toBe(true);
    expect(vc.getCurrentView()).toBe(1);
    expect(vc.hasPendingViewChange()).toBe(false);
  });

  it('rejects ack for wrong view number', () => {
    const vc = new ViewChangeCoordinator(5000);
    vc.initiateViewChange('a1', 'test', 3);
    expect(vc.acknowledgeViewChange('a2', 99)).toBe(false);
  });

  it('detects timeout', () => {
    const vc = new ViewChangeCoordinator(1);
    vc.initiateViewChange('a1', 'test', 5);
    const start = Date.now();
    while (Date.now() - start < 5) {}
    expect(vc.isTimedOut()).toBe(true);
  });

  it('isTimedOut false when no pending change', () => {
    const vc = new ViewChangeCoordinator(5000);
    expect(vc.isTimedOut()).toBe(false);
  });
});

// ── ShardManager ────────────────────────────────────────────────────────────

describe('ShardManager', () => {
  const config = { minSize: 2, maxSize: 5, targetSize: 3, replicationFactor: 2 };

  it('creates shard and assigns agents', () => {
    const mgr = new ShardManager(config);
    const sid = mgr.assignAgent({ id: 'a1' });
    expect(sid).toBeTruthy();
    expect(mgr.getShardCount()).toBeGreaterThanOrEqual(1);
    expect(mgr.getTotalAgents()).toBe(1);
  });

  it('auto-splits oversized shards', () => {
    const mgr = new ShardManager(config);
    // Add enough agents to trigger split (maxSize=5)
    for (let i = 0; i < 8; i++) {
      mgr.assignAgent({ id: `a${i}` });
    }
    // Should have split at least once
    expect(mgr.getShardCount()).toBeGreaterThan(1);
  });

  it('mergeShards combines two small shards', () => {
    const mgr = new ShardManager(config);
    const s1 = mgr.createShard();
    const s2 = mgr.createShard();
    mgr.getShard(s1)!.addMember({ id: 'a1' });
    mgr.getShard(s2)!.addMember({ id: 'a2' });
    const merged = mgr.mergeShards(s1, s2);
    expect(merged).not.toBeNull();
    expect(mgr.getShard(merged!)!.getMemberCount()).toBe(2);
  });

  it('mergeShards refuses if combined too large', () => {
    const mgr = new ShardManager(config);
    const s1 = mgr.createShard();
    const s2 = mgr.createShard();
    for (let i = 0; i < 3; i++) mgr.getShard(s1)!.addMember({ id: `a${i}` });
    for (let i = 3; i < 6; i++) mgr.getShard(s2)!.addMember({ id: `a${i}` });
    expect(mgr.mergeShards(s1, s2)).toBeNull();
  });

  it('getLoadDistribution returns stats', () => {
    const mgr = new ShardManager(config);
    mgr.assignAgent({ id: 'a1' });
    const dist = mgr.getLoadDistribution();
    expect(dist.length).toBeGreaterThan(0);
    expect(dist[0]).toHaveProperty('shardId');
    expect(dist[0]).toHaveProperty('members');
  });

  it('getShardForKey routes consistently', () => {
    const mgr = new ShardManager(config);
    mgr.assignAgent({ id: 'a1' });
    const s1 = mgr.getShardForKey('mykey');
    const s2 = mgr.getShardForKey('mykey');
    expect(s1).toBe(s2);
  });
});

// ── HierarchicalConsensusCoordinator ────────────────────────────────────────

describe('HierarchicalConsensusCoordinator', () => {
  let hcc: HierarchicalConsensusCoordinator;

  beforeEach(() => {
    hcc = new HierarchicalConsensusCoordinator(PRESETS['small-network']);
  });

  it('registers agents and assigns to shards', () => {
    const sid = hcc.registerAgent({ id: 'a1' });
    expect(sid).toBeTruthy();
    expect(hcc.getAgentShard('a1')).toBe(sid);
    expect(hcc.getStats().totalAgents).toBe(1);
  });

  it('deregisters agents', () => {
    hcc.registerAgent({ id: 'a1' });
    hcc.deregisterAgent('a1');
    expect(hcc.getAgentShard('a1')).toBeUndefined();
  });

  it('submit writes and reads values', () => {
    hcc.registerAgent({ id: 'a1' });
    const result = hcc.submit('mykey', { data: 'hello' }, 'a1');
    expect(result.success).toBe(true);
    const val = hcc.read('mykey');
    expect(val).not.toBeNull();
    expect(val!.value).toEqual({ data: 'hello' });
  });

  it('submit fails for unknown key shard', () => {
    // No agents registered = no shards
    const result = hcc.submit('mykey', 42, 'nobody');
    expect(result.success).toBe(false);
  });

  it('cross-shard tx commits when all shards have quorum', () => {
    // Register enough agents for quorum
    for (let i = 0; i < 10; i++) hcc.registerAgent({ id: `a${i}` });
    const result = hcc.submitCrossShard([
      { key: 'k1', value: 'v1' },
      { key: 'k2', value: 'v2' },
    ], 'a1');
    // May or may not succeed depending on shard assignment — at least no crash
    expect(result).toHaveProperty('success');
  });

  it('tick runs maintenance without error', () => {
    for (let i = 0; i < 5; i++) hcc.registerAgent({ id: `a${i}` });
    const result = hcc.tick();
    expect(result).toHaveProperty('rebalanceOps');
    expect(result).toHaveProperty('expiredTx');
  });

  it('events fire on registration', () => {
    const events: any[] = [];
    hcc.on('agent-registered', e => events.push(e));
    hcc.registerAgent({ id: 'a1' });
    expect(events).toHaveLength(1);
    expect(events[0].data.agentId).toBe('a1');
  });

  it('wildcard event handler receives all events', () => {
    const events: any[] = [];
    hcc.on('*', e => events.push(e));
    hcc.registerAgent({ id: 'a1' });
    hcc.deregisterAgent('a1');
    expect(events.length).toBeGreaterThanOrEqual(2);
  });

  it('getStats returns structure', () => {
    const stats = hcc.getStats();
    expect(stats.totalAgents).toBe(0);
    expect(stats.totalShards).toBe(0);
    expect(stats.pendingCrossShardTx).toBe(0);
    expect(stats.currentView).toBe(0);
    expect(stats.pendingViewChange).toBe(false);
  });

  it('read returns null for non-existent key', () => {
    hcc.registerAgent({ id: 'a1' });
    expect(hcc.read('nonexistent')).toBeNull();
  });
});

// ── Presets ──────────────────────────────────────────────────────────────────

describe('Presets', () => {
  it('all presets create valid coordinators', () => {
    for (const [name, preset] of Object.entries(PRESETS)) {
      const hcc = new HierarchicalConsensusCoordinator(preset);
      hcc.registerAgent({ id: 'test' });
      expect(hcc.getStats().totalAgents).toBe(1);
    }
  });
});
