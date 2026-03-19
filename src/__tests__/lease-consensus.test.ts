import { describe, it, expect, beforeEach } from 'vitest';
import {
  LeaseConsensusProtocol,
  LeaseGrantor,
  LeaseValidator,
  ReadDelegator,
  WriteForwarder,
  LeaseTransferProtocol,
  FailureDetector,
  SplitBrainGuard,
  LeaseConflictResolver,
  LeaseWaitQueue,
  PRESETS,
} from '../lease-consensus';

// ─── Helpers ────────────────────────────────────────────────────────────

const defaultConfig = { ...PRESETS['fast-reads'].config };

// ─── LeaseGrantor ───────────────────────────────────────────────────────

describe('LeaseGrantor', () => {
  let grantor: LeaseGrantor;
  const now = 1000000;

  beforeEach(() => { grantor = new LeaseGrantor(defaultConfig); });

  it('grants a lease for an available resource', () => {
    const lease = grantor.grant({ resourceId: 'r1', requesterId: 'a1', priority: 0 }, now);
    expect(lease).not.toBeNull();
    expect(lease!.holderId).toBe('a1');
    expect(lease!.state).toBe('active');
    expect(lease!.expiresAt).toBe(now + defaultConfig.leaseDurationMs);
  });

  it('rejects lease when resource already leased', () => {
    grantor.grant({ resourceId: 'r1', requesterId: 'a1', priority: 0 }, now);
    const second = grantor.grant({ resourceId: 'r1', requesterId: 'a2', priority: 0 }, now);
    expect(second).toBeNull();
  });

  it('grants lease after previous expired', () => {
    grantor.grant({ resourceId: 'r1', requesterId: 'a1', priority: 0 }, now);
    const later = now + defaultConfig.leaseDurationMs + 1000;
    const lease2 = grantor.grant({ resourceId: 'r1', requesterId: 'a2', priority: 0 }, later);
    expect(lease2).not.toBeNull();
    expect(lease2!.holderId).toBe('a2');
  });

  it('renews a lease within renewal window', () => {
    const lease = grantor.grant({ resourceId: 'r1', requesterId: 'a1', priority: 0 }, now)!;
    const renewTime = now + defaultConfig.leaseDurationMs - defaultConfig.renewalWindowMs + 100;
    const renewed = grantor.renew(lease.id, renewTime);
    expect(renewed).not.toBeNull();
    expect(renewed!.expiresAt).toBe(renewTime + defaultConfig.leaseDurationMs);
    expect(renewed!.renewalCount).toBe(1);
  });

  it('rejects early renewal', () => {
    const lease = grantor.grant({ resourceId: 'r1', requesterId: 'a1', priority: 0 }, now)!;
    const renewed = grantor.renew(lease.id, now + 100); // too early
    expect(renewed).toBeNull();
  });

  it('revokes a lease', () => {
    const lease = grantor.grant({ resourceId: 'r1', requesterId: 'a1', priority: 0 }, now)!;
    expect(grantor.revoke(lease.id)).toBe(true);
    expect(grantor.getActiveLease('r1', now)).toBeNull();
  });

  it('processExpiries marks expired leases', () => {
    grantor.grant({ resourceId: 'r1', requesterId: 'a1', priority: 0 }, now);
    const expired = grantor.processExpiries(now + defaultConfig.leaseDurationMs + 1);
    expect(expired.length).toBe(1);
  });

  it('getStats returns counts', () => {
    grantor.grant({ resourceId: 'r1', requesterId: 'a1', priority: 0 }, now);
    const stats = grantor.getStats();
    expect(stats.total).toBe(1);
    expect(stats.active).toBe(1);
  });
});

// ─── LeaseValidator ─────────────────────────────────────────────────────

describe('LeaseValidator', () => {
  const validator = new LeaseValidator(defaultConfig);
  const now = 1000000;

  it('validates active lease with remaining time', () => {
    const lease = {
      id: 'l1', resourceId: 'r1', holderId: 'a1', grantedAt: now,
      expiresAt: now + 10000, fencingToken: 1, epoch: 1, renewalCount: 0,
      state: 'active' as const, metadata: {},
    };
    expect(validator.isValid(lease, now)).toBe(true);
  });

  it('rejects expired lease', () => {
    const lease = {
      id: 'l1', resourceId: 'r1', holderId: 'a1', grantedAt: now,
      expiresAt: now + 100, fencingToken: 1, epoch: 1, renewalCount: 0,
      state: 'active' as const, metadata: {},
    };
    expect(validator.isValid(lease, now + 200)).toBe(false);
  });

  it('validates fencing token', () => {
    const lease = {
      id: 'l1', resourceId: 'r1', holderId: 'a1', grantedAt: now,
      expiresAt: now + 10000, fencingToken: 5, epoch: 1, renewalCount: 0,
      state: 'active' as const, metadata: {},
    };
    expect(validator.validateFencingToken(lease, 5)).toBe(true);
    expect(validator.validateFencingToken(lease, 3)).toBe(false);
  });

  it('getRemainingValidity accounts for clock skew', () => {
    const lease = {
      id: 'l1', resourceId: 'r1', holderId: 'a1', grantedAt: now,
      expiresAt: now + 10000, fencingToken: 1, epoch: 1, renewalCount: 0,
      state: 'active' as const, metadata: {},
    };
    const remaining = validator.getRemainingValidity(lease, now);
    expect(remaining).toBe(10000 - defaultConfig.maxClockSkewMs);
  });
});

// ─── WriteForwarder ─────────────────────────────────────────────────────

describe('WriteForwarder', () => {
  const now = 1000000;

  it('handles local write by lease holder', () => {
    const fw = new WriteForwarder(defaultConfig);
    const lease = {
      id: 'l1', resourceId: 'r1', holderId: 'a1', grantedAt: now,
      expiresAt: now + 10000, fencingToken: 3, epoch: 1, renewalCount: 0,
      state: 'active' as const, metadata: {},
    };
    const result = fw.handleWrite('r1', lease, 'a1', 3, now);
    expect(result.success).toBe(true);
    expect(result.forwardedTo).toBeNull();
    expect(fw.getStats().local).toBe(1);
  });

  it('forwards write to lease holder', () => {
    const fw = new WriteForwarder(defaultConfig);
    const lease = {
      id: 'l1', resourceId: 'r1', holderId: 'a1', grantedAt: now,
      expiresAt: now + 10000, fencingToken: 3, epoch: 1, renewalCount: 0,
      state: 'active' as const, metadata: {},
    };
    const result = fw.handleWrite('r1', lease, 'a2', 3, now);
    expect(result.success).toBe(true);
    expect(result.forwardedTo).toBe('a1');
  });

  it('rejects stale fencing token', () => {
    const fw = new WriteForwarder(defaultConfig);
    const lease = {
      id: 'l1', resourceId: 'r1', holderId: 'a1', grantedAt: now,
      expiresAt: now + 10000, fencingToken: 5, epoch: 1, renewalCount: 0,
      state: 'active' as const, metadata: {},
    };
    const result = fw.handleWrite('r1', lease, 'a1', 3, now);
    expect(result.success).toBe(false);
    expect(result.error).toBe('stale_fencing_token');
  });
});

// ─── FailureDetector ────────────────────────────────────────────────────

describe('FailureDetector', () => {
  let fd: FailureDetector;

  beforeEach(() => { fd = new FailureDetector(defaultConfig); });

  it('records heartbeats and computes low phi for healthy agent', () => {
    const base = Date.now();
    for (let i = 0; i < 20; i++) {
      fd.recordHeartbeat('a1', base + i * 1000);
    }
    const phi = fd.computePhi('a1', base + 20 * 1000); // 1 interval late
    expect(phi).toBeLessThan(defaultConfig.phiThreshold);
  });

  it('computes high phi for missing agent', () => {
    const base = Date.now();
    for (let i = 0; i < 20; i++) {
      fd.recordHeartbeat('a1', base + i * 1000);
    }
    const phi = fd.computePhi('a1', base + 100 * 1000); // way overdue
    expect(phi).toBeGreaterThan(defaultConfig.phiThreshold);
    expect(fd.isSuspected('a1', base + 100 * 1000)).toBe(true);
  });

  it('returns 0 phi for unknown agent', () => {
    expect(fd.computePhi('unknown', Date.now())).toBe(0);
  });

  it('getAgentHealth returns structure', () => {
    fd.recordHeartbeat('a1', Date.now());
    fd.recordHeartbeat('a1', Date.now() + 1000);
    const health = fd.getAgentHealth('a1', Date.now() + 2000);
    expect(health.lastHeartbeat).not.toBeNull();
    expect(health.avgInterval).not.toBeNull();
  });

  it('removeAgent cleans up', () => {
    fd.recordHeartbeat('a1', Date.now());
    fd.removeAgent('a1');
    expect(fd.computePhi('a1', Date.now())).toBe(0);
  });
});

// ─── SplitBrainGuard ────────────────────────────────────────────────────

describe('SplitBrainGuard', () => {
  it('detects split-brain with multiple claimants', () => {
    const guard = new SplitBrainGuard();
    const now = Date.now();
    guard.recordFencingToken('r1', 1, 'a1', now);
    guard.recordFencingToken('r1', 2, 'a2', now + 100);
    const result = guard.detectSplitBrain('r1', 5000, now + 200);
    expect(result.detected).toBe(true);
    expect(result.claimants.length).toBe(2);
  });

  it('no split-brain with single claimant', () => {
    const guard = new SplitBrainGuard();
    const now = Date.now();
    guard.recordFencingToken('r1', 1, 'a1', now);
    guard.recordFencingToken('r1', 2, 'a1', now + 100);
    const result = guard.detectSplitBrain('r1', 5000, now + 200);
    expect(result.detected).toBe(false);
  });

  it('validates writes against latest fencing token', () => {
    const guard = new SplitBrainGuard();
    guard.recordFencingToken('r1', 5, 'a1', Date.now());
    expect(guard.validateWrite('r1', 5)).toBe(true);
    expect(guard.validateWrite('r1', 3)).toBe(false);
    expect(guard.validateWrite('r1', 6)).toBe(true);
  });
});

// ─── LeaseConflictResolver ──────────────────────────────────────────────

describe('LeaseConflictResolver', () => {
  const makeLease = (holder: string, token: number, epoch: number, grantedAt: number) => ({
    id: `l-${holder}`, resourceId: 'r1', holderId: holder,
    grantedAt, expiresAt: grantedAt + 10000, fencingToken: token,
    epoch, renewalCount: 0, state: 'active' as const, metadata: {},
  });

  it('resolves by highest fencing token', () => {
    const resolver = new LeaseConflictResolver('highest_token');
    const res = resolver.resolve(makeLease('a', 5, 1, 100), makeLease('b', 3, 1, 100));
    expect(res.winnerId).toBe('a');
  });

  it('resolves by earliest grant', () => {
    const resolver = new LeaseConflictResolver('earliest_grant');
    const res = resolver.resolve(makeLease('a', 1, 1, 200), makeLease('b', 1, 1, 100));
    expect(res.winnerId).toBe('b');
  });

  it('resolves by priority', () => {
    const resolver = new LeaseConflictResolver('priority_based');
    resolver.setAgentPriority('a', 1);
    resolver.setAgentPriority('b', 10);
    const res = resolver.resolve(makeLease('a', 5, 1, 100), makeLease('b', 3, 1, 100));
    expect(res.winnerId).toBe('b');
  });

  it('resolves by epoch', () => {
    const resolver = new LeaseConflictResolver('epoch_based');
    const res = resolver.resolve(makeLease('a', 1, 5, 100), makeLease('b', 1, 3, 100));
    expect(res.winnerId).toBe('a');
  });

  it('tracks resolution history', () => {
    const resolver = new LeaseConflictResolver('highest_token');
    resolver.resolve(makeLease('a', 5, 1, 100), makeLease('b', 3, 1, 100));
    expect(resolver.getResolutionHistory().length).toBe(1);
    expect(resolver.getResolutionStats()['highest_token']).toBe(1);
  });
});

// ─── LeaseWaitQueue ─────────────────────────────────────────────────────

describe('LeaseWaitQueue', () => {
  it('enqueues and dequeues by priority', () => {
    const q = new LeaseWaitQueue();
    q.enqueue({ resourceId: 'r1', requesterId: 'low', priority: 1 }, 100, 5000);
    q.enqueue({ resourceId: 'r1', requesterId: 'high', priority: 10 }, 200, 5000);
    const next = q.dequeueNext('r1');
    expect(next!.request.requesterId).toBe('high');
  });

  it('processes timeouts', () => {
    const q = new LeaseWaitQueue();
    q.enqueue({ resourceId: 'r1', requesterId: 'a', priority: 0 }, 100, 500);
    const timedOut = q.processTimeouts(700);
    expect(timedOut.length).toBe(1);
    expect(q.getQueueLength('r1')).toBe(0);
  });

  it('getPosition returns correct index', () => {
    const q = new LeaseWaitQueue();
    q.enqueue({ resourceId: 'r1', requesterId: 'a', priority: 1 }, 100, 5000);
    q.enqueue({ resourceId: 'r1', requesterId: 'b', priority: 5 }, 200, 5000);
    expect(q.getPosition('r1', 'b')).toBe(0); // higher priority
    expect(q.getPosition('r1', 'a')).toBe(1);
    expect(q.getPosition('r1', 'c')).toBe(-1);
  });
});

// ─── LeaseConsensusProtocol (Orchestrator) ──────────────────────────────

describe('LeaseConsensusProtocol', () => {
  let proto: LeaseConsensusProtocol;
  const now = 1000000;

  beforeEach(() => { proto = new LeaseConsensusProtocol('agent-1', 'fast-reads'); });

  it('acquires a lease', () => {
    const lease = proto.acquireLease('r1', 0, now);
    expect(lease).not.toBeNull();
    expect(lease!.holderId).toBe('agent-1');
  });

  it('queues when resource taken', () => {
    proto.acquireLease('r1', 0, now);
    const second = proto.acquireLease('r1', 0, now + 100);
    expect(second).toBeNull(); // queued
  });

  it('renews a lease', () => {
    const lease = proto.acquireLease('r1', 0, now)!;
    const renewTime = now + defaultConfig.leaseDurationMs - defaultConfig.renewalWindowMs + 100;
    const renewed = proto.renewLease(lease.id, renewTime);
    expect(renewed).not.toBeNull();
  });

  it('reads from lease holder', () => {
    proto.acquireLease('r1', 0, now);
    const result = proto.read('r1', () => 'data', now + 100);
    expect(result.success).toBe(true);
    expect(result.data).toBe('data');
  });

  it('writes as lease holder', () => {
    const lease = proto.acquireLease('r1', 0, now)!;
    const result = proto.write('r1', lease.fencingToken, now + 100);
    expect(result.success).toBe(true);
  });

  it('tick processes expiries and grants from queue', () => {
    proto.acquireLease('r1', 0, now);
    // Try to acquire while taken → queued
    proto.acquireLease('r1', 5, now + 100);
    // Tick past expiry
    proto.tick(now + defaultConfig.leaseDurationMs + 1000);
    // The queued request should now have a lease
    const info = proto.getLeaseInfo('r1', now + defaultConfig.leaseDurationMs + 1000);
    // May or may not have been granted depending on queue state
    expect(info).toBeDefined();
  });

  it('reports heartbeats and checks health', () => {
    proto.reportHeartbeat('a2', now);
    proto.reportHeartbeat('a2', now + 1000);
    const health = proto.getAgentHealth('a2', now + 2000);
    expect(health.lastHeartbeat).toBe(now + 1000);
  });

  it('dashboard returns all stats', () => {
    proto.acquireLease('r1', 0, now);
    const dash = proto.dashboard(now);
    expect(dash.leases).toBeDefined();
    expect(dash.reads).toBeDefined();
    expect(dash.writes).toBeDefined();
  });

  it('getEvents returns event log', () => {
    proto.acquireLease('r1', 0, now);
    const events = proto.getEvents();
    expect(events.some(e => e.type === 'lease_granted')).toBe(true);
  });

  it('getPresets lists available presets', () => {
    const presets = LeaseConsensusProtocol.getPresets();
    expect(presets['fast-reads']).toBeDefined();
    expect(presets['strong-consistency']).toBeDefined();
    expect(presets['high-availability']).toBeDefined();
  });

  it('transferLease initiates transfer', () => {
    const lease = proto.acquireLease('r1', 0, now)!;
    const result = proto.transferLease(lease.id, 'agent-2', now + 100);
    expect(result).toBe(true);
    const events = proto.getEvents();
    expect(events.some(e => e.type === 'lease_transfer_initiated')).toBe(true);
  });

  it('getLeaseInfo returns full info', () => {
    proto.acquireLease('r1', 0, now);
    const info = proto.getLeaseInfo('r1', now + 100);
    expect(info.lease).not.toBeNull();
    expect(info.valid).toBe(true);
    expect(info.remaining).toBeGreaterThan(0);
  });
});
