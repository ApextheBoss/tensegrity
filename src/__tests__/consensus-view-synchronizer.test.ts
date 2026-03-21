import { describe, it, expect, vi } from 'vitest';
import {
  ConsensusViewSynchronizer,
  PacemakerTimer,
  ViewChangeCollector,
  HighestCertificateTracker,
  LeaderScheduler,
  CatchUpProtocol,
  OptimisticViewAdvance,
  TimeoutCertificateBuilder,
  ViewDivergenceDetector,
  PRESETS,
} from '../consensus-view-synchronizer';
import type {
  ViewSyncConfig,
  QuorumCertificate,
  TimeoutCertificate,
  ViewChangeMessage,
  AgentViewState,
} from '../consensus-view-synchronizer';

const baseConfig: ViewSyncConfig = {
  baseTimeoutMs: 1000,
  maxTimeoutMs: 10000,
  timeoutMultiplier: 2.0,
  quorumThreshold: 0.66,  // slightly below 2/3 so 2-of-3 reaches quorum
  maxByzantine: 1,
  catchUpThreshold: 3,
  viewHistoryLimit: 50,
  leaderRotation: 'round-robin',
  stickyLeaderViews: 3,
  optimisticAdvanceThreshold: 0.34,
  maxViewGap: 10,
  divergenceWindowMs: 5000,
};

function makeQC(view: number, now: number = Date.now()): QuorumCertificate {
  return {
    view,
    blockHash: `block-${view}`,
    signatures: new Map([['a1', 'sig1']]),
    aggregateWeight: 1,
    createdAt: now,
  };
}

function makeTC(view: number, now: number = Date.now()): TimeoutCertificate {
  return {
    view,
    timeoutVotes: new Map([['a1', { highestQCView: view - 1, signature: 'sig' }]]),
    aggregateWeight: 1,
    createdAt: now,
  };
}

// ─── PacemakerTimer ─────────────────────────────────────────────────────────
describe('PacemakerTimer', () => {
  it('starts with base timeout', () => {
    const p = new PacemakerTimer(baseConfig);
    expect(p.getCurrentTimeout()).toBe(1000);
    expect(p.getConsecutiveTimeouts()).toBe(0);
  });

  it('applies exponential backoff on consecutive timeouts', () => {
    const p = new PacemakerTimer(baseConfig);
    p.startView(0, 0, 'leader1');
    p.recordTimeout();
    p.startView(1, 1000, 'leader1');
    // multiplier=2, 1 timeout: 1000 * 2^1 * repFactor
    expect(p.getCurrentTimeout()).toBeGreaterThan(1000);
    p.recordTimeout();
    p.startView(2, 3000, 'leader1');
    expect(p.getCurrentTimeout()).toBeGreaterThan(2000);
  });

  it('caps timeout at maxTimeoutMs', () => {
    const p = new PacemakerTimer(baseConfig);
    for (let i = 0; i < 20; i++) p.recordTimeout();
    p.startView(20, 0, 'leader1');
    expect(p.getCurrentTimeout()).toBeLessThanOrEqual(10000);
  });

  it('resets consecutive timeouts on success', () => {
    const p = new PacemakerTimer(baseConfig);
    p.recordTimeout();
    p.recordTimeout();
    expect(p.getConsecutiveTimeouts()).toBe(2);
    p.recordSuccess('leader1');
    expect(p.getConsecutiveTimeouts()).toBe(0);
  });

  it('tracks leader reputation via EWMA', () => {
    const p = new PacemakerTimer(baseConfig);
    expect(p.getLeaderReputation('unknown')).toBe(0.5);
    p.recordSuccess('l1');
    expect(p.getLeaderReputation('l1')).toBeGreaterThan(0.5);
    p.recordFailure('l2');
    expect(p.getLeaderReputation('l2')).toBeLessThan(0.5);
  });

  it('isTimedOut returns true after timeout elapses', () => {
    const p = new PacemakerTimer(baseConfig);
    p.startView(0, 100, 'leader1');
    expect(p.isTimedOut(100)).toBe(false);
    expect(p.isTimedOut(100 + p.getCurrentTimeout())).toBe(true);
  });

  it('adjusts timeout for low-reputation leaders', () => {
    const p = new PacemakerTimer(baseConfig);
    // Good leader
    for (let i = 0; i < 10; i++) p.recordSuccess('good');
    p.startView(0, 0, 'good');
    const goodTimeout = p.getCurrentTimeout();
    // Bad leader
    for (let i = 0; i < 10; i++) p.recordFailure('bad');
    p.startView(1, 0, 'bad');
    const badTimeout = p.getCurrentTimeout();
    expect(badTimeout).toBeGreaterThan(goodTimeout);
  });

  it('reset() clears state', () => {
    const p = new PacemakerTimer(baseConfig);
    p.recordTimeout();
    p.recordTimeout();
    p.reset();
    expect(p.getConsecutiveTimeouts()).toBe(0);
    expect(p.getCurrentTimeout()).toBe(1000);
  });
});

// ─── ViewChangeCollector ────────────────────────────────────────────────────
describe('ViewChangeCollector', () => {
  it('detects quorum when enough weight accumulated', () => {
    const c = new ViewChangeCollector(baseConfig);
    c.registerAgent('a1', 1);
    c.registerAgent('a2', 1);
    c.registerAgent('a3', 1);

    const msg1: ViewChangeMessage = {
      fromAgent: 'a1', targetView: 1, highestQC: null, highestTC: null,
      reason: 'timeout', timestamp: 0,
    };
    const r1 = c.addMessage(msg1);
    expect(r1.quorumReached).toBe(false);

    const msg2: ViewChangeMessage = { ...msg1, fromAgent: 'a2' };
    const r2 = c.addMessage(msg2);
    // 2/3 = 0.667 >= 0.667
    expect(r2.quorumReached).toBe(true);
  });

  it('tracks highest QC and TC per view', () => {
    const c = new ViewChangeCollector(baseConfig);
    c.registerAgent('a1', 1);
    c.registerAgent('a2', 1);

    const qc5 = makeQC(5);
    const qc3 = makeQC(3);
    c.addMessage({ fromAgent: 'a1', targetView: 10, highestQC: qc3, highestTC: null, reason: 'timeout', timestamp: 0 });
    c.addMessage({ fromAgent: 'a2', targetView: 10, highestQC: qc5, highestTC: null, reason: 'timeout', timestamp: 0 });
    expect(c.getHighestQCForView(10)?.view).toBe(5);
  });

  it('returns null for unknown view', () => {
    const c = new ViewChangeCollector(baseConfig);
    expect(c.getHighestQCForView(999)).toBeNull();
    expect(c.getHighestTCForView(999)).toBeNull();
  });

  it('pruneBelow removes old views', () => {
    const c = new ViewChangeCollector(baseConfig);
    c.registerAgent('a1', 1);
    c.addMessage({ fromAgent: 'a1', targetView: 1, highestQC: null, highestTC: null, reason: 'timeout', timestamp: 0 });
    c.addMessage({ fromAgent: 'a1', targetView: 5, highestQC: null, highestTC: null, reason: 'timeout', timestamp: 0 });
    const pruned = c.pruneBelow(3);
    expect(pruned).toBe(1);
    expect(c.getVoterCount(1)).toBe(0);
    expect(c.getVoterCount(5)).toBe(1);
  });

  it('deduplicates messages from same agent per view', () => {
    const c = new ViewChangeCollector(baseConfig);
    c.registerAgent('a1', 1);
    c.registerAgent('a2', 1);
    c.registerAgent('a3', 1);
    c.addMessage({ fromAgent: 'a1', targetView: 1, highestQC: null, highestTC: null, reason: 'timeout', timestamp: 0 });
    c.addMessage({ fromAgent: 'a1', targetView: 1, highestQC: null, highestTC: null, reason: 'timeout', timestamp: 1 });
    expect(c.getVoterCount(1)).toBe(1);
  });
});

// ─── HighestCertificateTracker ──────────────────────────────────────────────
describe('HighestCertificateTracker', () => {
  it('tracks highest QC', () => {
    const t = new HighestCertificateTracker();
    expect(t.getHighestQC()).toBeNull();
    expect(t.updateQC(makeQC(3))).toBe(true);
    expect(t.updateQC(makeQC(1))).toBe(false); // lower
    expect(t.getHighestQC()?.view).toBe(3);
    expect(t.updateQC(makeQC(5))).toBe(true);
    expect(t.getHighestQC()?.view).toBe(5);
  });

  it('tracks highest TC', () => {
    const t = new HighestCertificateTracker();
    t.updateTC(makeTC(2));
    t.updateTC(makeTC(7));
    t.updateTC(makeTC(4)); // lower, ignored
    expect(t.getHighestTC()?.view).toBe(7);
  });

  it('getHighestCertifiedView returns max of QC and TC', () => {
    const t = new HighestCertificateTracker();
    expect(t.getHighestCertifiedView()).toBe(-1);
    t.updateQC(makeQC(3));
    expect(t.getHighestCertifiedView()).toBe(3);
    t.updateTC(makeTC(5));
    expect(t.getHighestCertifiedView()).toBe(5);
  });

  it('getQCForView finds specific QC', () => {
    const t = new HighestCertificateTracker();
    t.updateQC(makeQC(1));
    t.updateQC(makeQC(2));
    expect(t.getQCForView(1)?.view).toBe(1);
    expect(t.getQCForView(99)).toBeUndefined();
  });

  it('getProgressRate computes QCs per second', () => {
    const t = new HighestCertificateTracker();
    expect(t.getProgressRate(10)).toBe(0);
    t.updateQC(makeQC(1, 1000));
    expect(t.getProgressRate(10)).toBe(0); // need 2+
    t.updateQC(makeQC(2, 2000));
    t.updateQC(makeQC(3, 3000));
    // 2 intervals over 2s = 1 QC/s
    expect(t.getProgressRate(10)).toBe(1);
  });

  it('limits history size', () => {
    const t = new HighestCertificateTracker(5);
    for (let i = 0; i < 10; i++) t.updateQC(makeQC(i, i * 100));
    // Should still work, just trimmed internally
    expect(t.getHighestQC()?.view).toBe(9);
  });
});

// ─── LeaderScheduler ────────────────────────────────────────────────────────
describe('LeaderScheduler', () => {
  function makeAgentState(id: string, rep: number = 0.5): AgentViewState {
    return { agentId: id, currentView: 0, lastUpdate: 0, weight: 1, reputation: rep, consecutiveTimeouts: 0, viewHistory: [] };
  }

  it('round-robin rotates deterministically', () => {
    const s = new LeaderScheduler(baseConfig);
    s.registerAgent(makeAgentState('a'));
    s.registerAgent(makeAgentState('b'));
    s.registerAgent(makeAgentState('c'));

    const leaders = [0, 1, 2, 3, 4, 5].map(v => s.getLeader(v));
    // Should cycle through all 3
    const unique = new Set(leaders);
    expect(unique.size).toBe(3);
    // Periodic
    expect(leaders[0]).toBe(leaders[3]);
    expect(leaders[1]).toBe(leaders[4]);
  });

  it('returns null with no agents', () => {
    const s = new LeaderScheduler(baseConfig);
    expect(s.getLeader(0)).toBeNull();
  });

  it('sticky mode uses same leader for N views', () => {
    const cfg = { ...baseConfig, leaderRotation: 'sticky' as const, stickyLeaderViews: 3 };
    const s = new LeaderScheduler(cfg);
    s.registerAgent(makeAgentState('a'));
    s.registerAgent(makeAgentState('b'));
    // Views 0,1,2 should have same leader; 3,4,5 different
    expect(s.getLeader(0)).toBe(s.getLeader(1));
    expect(s.getLeader(1)).toBe(s.getLeader(2));
  });

  it('reputation-weighted prefers high-rep agents', () => {
    const cfg = { ...baseConfig, leaderRotation: 'reputation-weighted' as const };
    const s = new LeaderScheduler(cfg);
    s.registerAgent(makeAgentState('low', 0.01));
    s.registerAgent(makeAgentState('high', 10.0));
    // Over many views, 'high' should be leader much more often
    let highCount = 0;
    for (let v = 0; v < 100; v++) {
      if (s.getLeader(v) === 'high') highCount++;
    }
    expect(highCount).toBeGreaterThan(80);
  });

  it('removeAgent updates rotation', () => {
    const s = new LeaderScheduler(baseConfig);
    s.registerAgent(makeAgentState('a'));
    s.registerAgent(makeAgentState('b'));
    s.removeAgent('a');
    // All views should return 'b'
    expect(s.getLeader(0)).toBe('b');
    expect(s.getLeader(1)).toBe('b');
  });

  it('getNextLeaders returns sequence', () => {
    const s = new LeaderScheduler(baseConfig);
    s.registerAgent(makeAgentState('a'));
    s.registerAgent(makeAgentState('b'));
    const next = s.getNextLeaders(0, 4);
    expect(next).toHaveLength(4);
  });
});

// ─── CatchUpProtocol ────────────────────────────────────────────────────────
describe('CatchUpProtocol', () => {
  it('detects when catch-up is needed', () => {
    const c = new CatchUpProtocol(baseConfig); // catchUpThreshold=3
    expect(c.needsCatchUp(5, 7)).toBe(false); // gap=2
    expect(c.needsCatchUp(5, 8)).toBe(true);  // gap=3
  });

  it('builds catch-up package with certificates', () => {
    const c = new CatchUpProtocol(baseConfig);
    const tracker = new HighestCertificateTracker();
    tracker.updateQC(makeQC(10));
    tracker.updateTC(makeTC(9));

    c.requestCatchUp('agent1', 3, 10, 0);
    const pkg = c.buildCatchUpPackage('agent1', tracker);
    expect(pkg).not.toBeNull();
    expect(pkg!.targetView).toBe(10);
    expect(pkg!.highestQC?.view).toBe(10);
    expect(pkg!.highestTC?.view).toBe(9);

    // Second call returns null (fulfilled)
    expect(c.buildCatchUpPackage('agent1', tracker)).toBeNull();
  });

  it('tracks pending count and prunes completed', () => {
    const c = new CatchUpProtocol(baseConfig);
    const tracker = new HighestCertificateTracker();
    c.requestCatchUp('a1', 0, 10, 0);
    c.requestCatchUp('a2', 0, 10, 0);
    expect(c.getPendingCount()).toBe(2);
    c.buildCatchUpPackage('a1', tracker);
    expect(c.getPendingCount()).toBe(1);
    const pruned = c.pruneCompleted();
    expect(pruned).toBe(1);
  });
});

// ─── OptimisticViewAdvance ──────────────────────────────────────────────────
describe('OptimisticViewAdvance', () => {
  it('advances when f+1 threshold reached', () => {
    const o = new OptimisticViewAdvance(baseConfig); // threshold=0.34
    o.registerAgent('a1', 1);
    o.registerAgent('a2', 1);
    o.registerAgent('a3', 1);

    // 1/3 = 0.333 < 0.34, not enough
    expect(o.signalNextView('a1', 5)).toBe(false);
    // 2/3 = 0.667 >= 0.34, enough
    expect(o.signalNextView('a2', 5)).toBe(true);
  });

  it('tracks signal counts', () => {
    const o = new OptimisticViewAdvance(baseConfig);
    o.registerAgent('a1', 1);
    o.registerAgent('a2', 1);
    o.signalNextView('a1', 5);
    expect(o.getSignalCount(5)).toBe(1);
    expect(o.getSignalCount(6)).toBe(0);
  });

  it('prunes signals below view', () => {
    const o = new OptimisticViewAdvance(baseConfig);
    o.registerAgent('a1', 1);
    o.signalNextView('a1', 3);
    o.signalNextView('a1', 7);
    o.pruneBelow(5);
    expect(o.getSignalCount(3)).toBe(0);
    expect(o.getSignalCount(7)).toBe(1);
  });

  it('uses weight-based threshold', () => {
    const o = new OptimisticViewAdvance(baseConfig); // threshold=0.34
    o.registerAgent('heavy', 10);
    o.registerAgent('light1', 1);
    o.registerAgent('light2', 1);
    // heavy=10, total=12. 10/12=0.83 >= 0.34
    expect(o.signalNextView('heavy', 1)).toBe(true);
    // light1=1, total=12. 1/12=0.083 < 0.34
    const o2 = new OptimisticViewAdvance(baseConfig);
    o2.registerAgent('heavy', 10);
    o2.registerAgent('light1', 1);
    o2.registerAgent('light2', 1);
    expect(o2.signalNextView('light1', 1)).toBe(false);
  });
});

// ─── TimeoutCertificateBuilder ──────────────────────────────────────────────
describe('TimeoutCertificateBuilder', () => {
  it('builds TC when quorum reached', () => {
    const b = new TimeoutCertificateBuilder(baseConfig); // quorum=0.667
    b.registerAgent('a1', 1);
    b.registerAgent('a2', 1);
    b.registerAgent('a3', 1);

    expect(b.addTimeoutVote(5, 'a1', 4, 100)).toBeNull();
    const tc = b.addTimeoutVote(5, 'a2', 3, 200);
    expect(tc).not.toBeNull();
    expect(tc!.view).toBe(5);
    expect(tc!.timeoutVotes.size).toBe(2);
    expect(tc!.aggregateWeight).toBe(2);
  });

  it('tracks vote counts', () => {
    const b = new TimeoutCertificateBuilder(baseConfig);
    b.registerAgent('a1', 1);
    b.addTimeoutVote(5, 'a1', 4, 0);
    expect(b.getVoteCount(5)).toBe(1);
    expect(b.getVoteCount(6)).toBe(0);
  });

  it('prunes old views', () => {
    const b = new TimeoutCertificateBuilder(baseConfig);
    b.registerAgent('a1', 1);
    b.addTimeoutVote(2, 'a1', 1, 0);
    b.addTimeoutVote(8, 'a1', 7, 0);
    b.pruneBelow(5);
    expect(b.getVoteCount(2)).toBe(0);
    expect(b.getVoteCount(8)).toBe(1);
  });
});

// ─── ViewDivergenceDetector ─────────────────────────────────────────────────
describe('ViewDivergenceDetector', () => {
  it('reports no divergence when all agents on same view', () => {
    const d = new ViewDivergenceDetector(baseConfig);
    d.updateAgentView('a1', 5, 100);
    d.updateAgentView('a2', 5, 100);
    d.updateAgentView('a3', 5, 100);
    const result = d.checkDivergence(100);
    expect(result.divergent).toBe(false);
  });

  it('detects critical divergence with no majority', () => {
    const d = new ViewDivergenceDetector({ ...baseConfig, catchUpThreshold: 2 });
    d.updateAgentView('a1', 1, 100);
    d.updateAgentView('a2', 5, 100);
    d.updateAgentView('a3', 10, 100);
    const result = d.checkDivergence(100);
    expect(result.divergent).toBe(true);
    expect(result.severity).toBe('critical');
    expect(result.recommendation).toBe('emergency-view-sync');
  });

  it('detects high severity when majority exists but large gap', () => {
    const d = new ViewDivergenceDetector({ ...baseConfig, catchUpThreshold: 2 });
    d.updateAgentView('a1', 10, 100);
    d.updateAgentView('a2', 10, 100);
    d.updateAgentView('a3', 10, 100);
    d.updateAgentView('a4', 1, 100); // lagging far behind
    const result = d.checkDivergence(100);
    expect(result.divergent).toBe(true);
    expect(result.severity).toBe('high');
    expect(result.recommendation).toBe('force-catch-up-minority');
  });

  it('getMajorityView returns most common view', () => {
    const d = new ViewDivergenceDetector(baseConfig);
    d.updateAgentView('a1', 5, 100);
    d.updateAgentView('a2', 5, 100);
    d.updateAgentView('a3', 7, 100);
    expect(d.getMajorityView()).toBe(5);
  });

  it('excludes stale agents from divergence check', () => {
    const d = new ViewDivergenceDetector(baseConfig); // divergenceWindowMs=5000
    d.updateAgentView('a1', 5, 100);
    d.updateAgentView('a2', 100, 100);
    // Check at now=100, window*2=10000, both are within window
    const r1 = d.checkDivergence(100);
    expect(r1.divergent).toBe(true);
    // Check at now=20000, both should be stale
    const r2 = d.checkDivergence(20000);
    expect(r2.divergent).toBe(false);
  });

  it('tracks divergence alerts', () => {
    const d = new ViewDivergenceDetector({ ...baseConfig, catchUpThreshold: 2 });
    d.updateAgentView('a1', 1, 100);
    d.updateAgentView('a2', 50, 100);
    d.checkDivergence(100);
    const alerts = d.getRecentAlerts(10);
    expect(alerts.length).toBeGreaterThan(0);
  });

  it('removeAgent stops tracking', () => {
    const d = new ViewDivergenceDetector(baseConfig);
    d.updateAgentView('a1', 5, 100);
    d.updateAgentView('a2', 5, 100);
    d.removeAgent('a1');
    expect(d.getMajorityView()).toBe(5);
  });
});

// ─── ConsensusViewSynchronizer (orchestrator) ───────────────────────────────
describe('ConsensusViewSynchronizer', () => {
  it('starts at view 0', () => {
    const sync = new ConsensusViewSynchronizer(baseConfig);
    expect(sync.getCurrentView()).toBe(0);
    expect(sync.getHighestQC()).toBeNull();
    expect(sync.getHighestTC()).toBeNull();
  });

  it('registers and removes agents', () => {
    const sync = new ConsensusViewSynchronizer(baseConfig);
    sync.registerAgent('a1', 1);
    sync.registerAgent('a2', 1);
    expect(sync.getAgentState('a1')).toBeDefined();
    expect(sync.getStatus().agentCount).toBe(2);
    sync.removeAgent('a1');
    expect(sync.getAgentState('a1')).toBeUndefined();
    expect(sync.getStatus().agentCount).toBe(1);
  });

  it('advances view on QC received', () => {
    const sync = new ConsensusViewSynchronizer(baseConfig);
    sync.registerAgent('a1');
    sync.receiveQC(makeQC(0));
    expect(sync.getCurrentView()).toBe(1);
    expect(sync.getHighestQC()?.view).toBe(0);
  });

  it('does not go backward on old QC', () => {
    const sync = new ConsensusViewSynchronizer(baseConfig);
    sync.registerAgent('a1');
    sync.receiveQC(makeQC(5));
    expect(sync.getCurrentView()).toBe(6);
    sync.receiveQC(makeQC(3)); // old
    expect(sync.getCurrentView()).toBe(6);
  });

  it('handles timeout → TC → view advance', () => {
    const sync = new ConsensusViewSynchronizer(baseConfig);
    sync.registerAgent('a1', 1);
    sync.registerAgent('a2', 1);
    sync.registerAgent('a3', 1);

    // 2/3 timeout votes should form TC with quorum=0.667
    sync.handleTimeout('a1');
    expect(sync.getCurrentView()).toBe(0);
    sync.handleTimeout('a2');
    // TC formed, view should advance
    expect(sync.getCurrentView()).toBe(1);
    expect(sync.getHighestTC()).not.toBeNull();
  });

  it('signalNextView triggers optimistic advance', () => {
    const cfg = { ...baseConfig, optimisticAdvanceThreshold: 0.34 };
    const sync = new ConsensusViewSynchronizer(cfg);
    sync.registerAgent('a1', 1);
    sync.registerAgent('a2', 1);
    // 1/2 = 0.5 >= 0.34
    sync.signalNextView('a1');
    expect(sync.getCurrentView()).toBe(1);
  });

  it('receiveViewChange with quorum advances view', () => {
    const sync = new ConsensusViewSynchronizer(baseConfig);
    sync.registerAgent('a1', 1);
    sync.registerAgent('a2', 1);
    sync.registerAgent('a3', 1);

    const msg1: ViewChangeMessage = {
      fromAgent: 'a1', targetView: 5, highestQC: makeQC(4), highestTC: null,
      reason: 'qc_received', timestamp: 0,
    };
    sync.receiveViewChange(msg1);
    expect(sync.getCurrentView()).toBe(0); // no quorum yet

    sync.receiveViewChange({ ...msg1, fromAgent: 'a2' });
    // 2/3 quorum reached
    expect(sync.getCurrentView()).toBe(5);
  });

  it('applyCatchUp jumps to target view', () => {
    const sync = new ConsensusViewSynchronizer(baseConfig);
    sync.registerAgent('a1');
    sync.applyCatchUp(10, makeQC(9), makeTC(8));
    expect(sync.getCurrentView()).toBe(10);
    expect(sync.getHighestQC()?.view).toBe(9);
    expect(sync.getHighestTC()?.view).toBe(8);
  });

  it('applyCatchUp ignores backward jumps', () => {
    const sync = new ConsensusViewSynchronizer(baseConfig);
    sync.registerAgent('a1');
    sync.applyCatchUp(10, null, null);
    sync.applyCatchUp(5, null, null); // backward
    expect(sync.getCurrentView()).toBe(10);
  });

  it('tick detects view timeout', () => {
    const sync = new ConsensusViewSynchronizer(baseConfig);
    sync.registerAgent('a1');
    // Advance to start pacemaker
    sync.receiveQC(makeQC(0));
    const timeout = sync.getStatus().timeout;
    const events = sync.tick(Date.now() + timeout + 1000);
    const timeoutEvents = events.filter(e => e.type === 'view_timeout');
    expect(timeoutEvents.length).toBeGreaterThanOrEqual(1);
  });

  it('getStatus returns comprehensive state', () => {
    const sync = new ConsensusViewSynchronizer(baseConfig);
    sync.registerAgent('a1');
    sync.registerAgent('a2');
    const status = sync.getStatus();
    expect(status.currentView).toBe(0);
    expect(status.agentCount).toBe(2);
    expect(status.leader).not.toBeNull();
    expect(typeof status.timeout).toBe('number');
    expect(typeof status.progressRate).toBe('number');
  });

  it('getCurrentLeader returns leader for current view', () => {
    const sync = new ConsensusViewSynchronizer(baseConfig);
    sync.registerAgent('a1');
    expect(sync.getCurrentLeader()).toBe('a1');
  });

  it('getNextLeaders returns upcoming leaders', () => {
    const sync = new ConsensusViewSynchronizer(baseConfig);
    sync.registerAgent('a1');
    sync.registerAgent('a2');
    const next = sync.getNextLeaders(3);
    expect(next).toHaveLength(3);
  });

  it('onEvent fires for all emitted events', () => {
    const sync = new ConsensusViewSynchronizer(baseConfig);
    const events: any[] = [];
    sync.onEvent(e => events.push(e));
    sync.registerAgent('a1');
    expect(events.some(e => e.type === 'agent_registered')).toBe(true);
  });

  it('getEvents returns event history', () => {
    const sync = new ConsensusViewSynchronizer(baseConfig);
    sync.registerAgent('a1');
    sync.receiveQC(makeQC(0));
    const events = sync.getEvents();
    expect(events.length).toBeGreaterThan(0);
    const limited = sync.getEvents(1);
    expect(limited).toHaveLength(1);
  });

  it('view history is maintained per agent', () => {
    const sync = new ConsensusViewSynchronizer(baseConfig);
    sync.registerAgent('a1');
    sync.receiveQC(makeQC(0));
    sync.receiveQC(makeQC(1));
    const state = sync.getAgentState('a1');
    expect(state!.viewHistory.length).toBe(2);
    expect(state!.currentView).toBe(2);
  });

  it('consecutive timeouts tracked on agent state', () => {
    const sync = new ConsensusViewSynchronizer(baseConfig);
    sync.registerAgent('a1', 1);
    sync.registerAgent('a2', 1);
    sync.registerAgent('a3', 1);
    sync.handleTimeout('a1');
    expect(sync.getAgentState('a1')!.consecutiveTimeouts).toBe(1);
  });

  it('handleTimeout with unknown agent is no-op', () => {
    const sync = new ConsensusViewSynchronizer(baseConfig);
    sync.handleTimeout('unknown'); // should not throw
    expect(sync.getCurrentView()).toBe(0);
  });
});

// ─── Presets ────────────────────────────────────────────────────────────────
describe('PRESETS', () => {
  it('has 3 presets', () => {
    expect(Object.keys(PRESETS)).toHaveLength(3);
  });

  it('fast-consensus uses round-robin with low timeout', () => {
    const p = PRESETS['fast-consensus'];
    expect(p.leaderRotation).toBe('round-robin');
    expect(p.baseTimeoutMs).toBe(500);
  });

  it('byzantine-tolerant uses reputation-weighted with higher quorum', () => {
    const p = PRESETS['byzantine-tolerant'];
    expect(p.leaderRotation).toBe('reputation-weighted');
    expect(p.quorumThreshold).toBe(0.75);
  });

  it('high-throughput uses sticky leader', () => {
    const p = PRESETS['high-throughput'];
    expect(p.leaderRotation).toBe('sticky');
    expect(p.stickyLeaderViews).toBe(5);
  });

  it('all presets can construct a working synchronizer', () => {
    for (const [name, preset] of Object.entries(PRESETS)) {
      const sync = new ConsensusViewSynchronizer(preset);
      sync.registerAgent('a1');
      sync.registerAgent('a2');
      sync.registerAgent('a3');
      sync.registerAgent('a4');
      expect(sync.getStatus().agentCount).toBe(4);
    }
  });
});
