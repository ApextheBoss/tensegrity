import { describe, it, expect, beforeEach } from 'vitest';
import {
  UsageMeter,
  InMemoryUsageStore,
  SlidingWindowRateLimiter,
  PLAN_LIMITS,
  BillingEvent,
  PlanTier,
} from '../usage-metering';

// ============================================================
// SlidingWindowRateLimiter
// ============================================================

describe('SlidingWindowRateLimiter', () => {
  let rl: SlidingWindowRateLimiter;

  beforeEach(() => { rl = new SlidingWindowRateLimiter(); });

  it('allows actions within limit', () => {
    expect(rl.record('k', 3, 1000, 100)).toBe(true);
    expect(rl.record('k', 3, 1000, 200)).toBe(true);
    expect(rl.record('k', 3, 1000, 300)).toBe(true);
    expect(rl.record('k', 3, 1000, 400)).toBe(false);
  });

  it('expires old entries', () => {
    rl.record('k', 2, 1000, 100);
    rl.record('k', 2, 1000, 200);
    expect(rl.record('k', 2, 1000, 300)).toBe(false);
    // At t=1101, t=100 has expired
    expect(rl.record('k', 2, 1000, 1101)).toBe(true);
  });

  it('check does not consume capacity', () => {
    expect(rl.check('k', 1, 1000, 100)).toBe(true);
    expect(rl.check('k', 1, 1000, 100)).toBe(true);
    rl.record('k', 1, 1000, 100);
    expect(rl.check('k', 1, 1000, 100)).toBe(false);
  });

  it('count returns current window size', () => {
    expect(rl.count('k', 1000, 100)).toBe(0);
    rl.record('k', 10, 1000, 100);
    rl.record('k', 10, 1000, 200);
    expect(rl.count('k', 1000, 500)).toBe(2);
    expect(rl.count('k', 1000, 1099)).toBe(2); // both still valid
    expect(rl.count('k', 1000, 1101)).toBe(1); // t=100 expired
  });

  it('clear resets all windows', () => {
    rl.record('k', 10, 1000, 100);
    rl.clear();
    expect(rl.count('k', 1000, 100)).toBe(0);
  });

  it('isolates different keys', () => {
    rl.record('a', 1, 1000, 100);
    expect(rl.check('b', 1, 1000, 100)).toBe(true);
  });
});

// ============================================================
// InMemoryUsageStore
// ============================================================

describe('InMemoryUsageStore', () => {
  let store: InMemoryUsageStore;

  beforeEach(() => { store = new InMemoryUsageStore(); });

  it('stores and retrieves workspace usage', () => {
    expect(store.getWorkspaceUsage('ws1')).toBeUndefined();
    const usage = { workspaceId: 'ws1', plan: 'team' as PlanTier, currentAgents: 0, peakAgents: 0, tasksRoutedThisHour: 0, tasksRoutedThisMonth: 0, tasksCompletedThisMonth: 0, tasksFailedThisMonth: 0, apiCallsThisMonth: 0, billingPeriodStart: 1000, lastActivity: 1000 };
    store.setWorkspaceUsage('ws1', usage);
    expect(store.getWorkspaceUsage('ws1')).toEqual(usage);
  });

  it('records and counts by metric and time', () => {
    store.record({ workspaceId: 'ws1', metric: 'task_routed', value: 1, timestamp: 100 });
    store.record({ workspaceId: 'ws1', metric: 'task_routed', value: 1, timestamp: 200 });
    store.record({ workspaceId: 'ws1', metric: 'task_completed', value: 1, timestamp: 150 });
    store.record({ workspaceId: 'ws2', metric: 'task_routed', value: 1, timestamp: 120 });

    expect(store.countRecords('ws1', 'task_routed', 0)).toBe(2);
    expect(store.countRecords('ws1', 'task_routed', 150)).toBe(1);
    expect(store.countRecords('ws1', 'task_completed', 0)).toBe(1);
    expect(store.countRecords('ws2', 'task_routed', 0)).toBe(1);
  });

  it('lists workspaces', () => {
    const u = { workspaceId: '', plan: 'solo' as PlanTier, currentAgents: 0, peakAgents: 0, tasksRoutedThisHour: 0, tasksRoutedThisMonth: 0, tasksCompletedThisMonth: 0, tasksFailedThisMonth: 0, apiCallsThisMonth: 0, billingPeriodStart: 0, lastActivity: 0 };
    store.setWorkspaceUsage('a', { ...u, workspaceId: 'a' });
    store.setWorkspaceUsage('b', { ...u, workspaceId: 'b' });
    expect(store.listWorkspaces().sort()).toEqual(['a', 'b']);
  });

  it('prunes old records', () => {
    store.record({ workspaceId: 'ws1', metric: 'api_call', value: 1, timestamp: 100 });
    store.record({ workspaceId: 'ws1', metric: 'api_call', value: 1, timestamp: 500 });
    const pruned = store.prune(300);
    expect(pruned).toBe(1);
    expect(store.getAllRecords()).toHaveLength(1);
  });
});

// ============================================================
// UsageMeter
// ============================================================

describe('UsageMeter', () => {
  let meter: UsageMeter;
  let store: InMemoryUsageStore;
  let now: number;
  let events: BillingEvent[];

  beforeEach(() => {
    now = 1_000_000;
    store = new InMemoryUsageStore();
    meter = new UsageMeter({ store, nowFn: () => now });
    events = [];
    meter.onBillingEvent(e => events.push(e));
  });

  describe('workspace management', () => {
    it('initializes a workspace', () => {
      const usage = meter.initWorkspace('ws1', 'team');
      expect(usage.plan).toBe('team');
      expect(usage.currentAgents).toBe(0);
    });

    it('returns existing workspace on duplicate init', () => {
      meter.initWorkspace('ws1', 'team');
      const usage = meter.initWorkspace('ws1', 'pro'); // ignored
      expect(usage.plan).toBe('team');
    });

    it('changes plan', () => {
      meter.initWorkspace('ws1', 'solo');
      const result = meter.changePlan('ws1', 'team');
      expect(result.allowed).toBe(true);
      expect(meter.getUsageSummary('ws1')!.plan).toBe('team');
      expect(events.some(e => e.type === 'plan_change')).toBe(true);
    });

    it('rejects plan downgrade if agents exceed new limit', () => {
      meter.initWorkspace('ws1', 'team'); // max 25
      for (let i = 0; i < 6; i++) {
        meter.recordAgentConnection('ws1', `a${i}`);
      }
      const result = meter.changePlan('ws1', 'solo'); // max 5
      expect(result.allowed).toBe(false);
      expect(result.reason).toBe('current_agents_exceed_new_plan');
    });

    it('returns not found for unknown workspace', () => {
      expect(meter.changePlan('nope', 'team').allowed).toBe(false);
    });
  });

  describe('agent connections', () => {
    beforeEach(() => { meter.initWorkspace('ws1', 'solo'); }); // max 5

    it('records connection and updates count', () => {
      const r = meter.recordAgentConnection('ws1', 'a1');
      expect(r.allowed).toBe(true);
      expect(meter.getUsageSummary('ws1')!.currentAgents).toBe(1);
      expect(meter.getConnectedAgents('ws1')).toEqual(['a1']);
    });

    it('rejects connection at limit', () => {
      for (let i = 0; i < 5; i++) meter.recordAgentConnection('ws1', `a${i}`);
      const r = meter.recordAgentConnection('ws1', 'a5');
      expect(r.allowed).toBe(false);
      expect(r.reason).toBe('max_agents_reached');
      expect(events.some(e => e.type === 'limit_reached')).toBe(true);
    });

    it('emits warning at 80% utilization', () => {
      for (let i = 0; i < 4; i++) meter.recordAgentConnection('ws1', `a${i}`);
      expect(events.some(e => e.type === 'limit_warning')).toBe(true);
    });

    it('tracks peak agents', () => {
      meter.recordAgentConnection('ws1', 'a1');
      meter.recordAgentConnection('ws1', 'a2');
      meter.recordAgentConnection('ws1', 'a3');
      meter.recordAgentDisconnection('ws1', 'a2');
      expect(meter.getUsageSummary('ws1')!.peakAgents).toBe(3);
      expect(meter.getUsageSummary('ws1')!.currentAgents).toBe(2);
    });

    it('handles disconnect from unknown workspace', () => {
      meter.recordAgentDisconnection('nope', 'a1'); // no throw
    });

    it('duplicate agent connection does not double count', () => {
      meter.recordAgentConnection('ws1', 'a1');
      meter.recordAgentConnection('ws1', 'a1');
      expect(meter.getUsageSummary('ws1')!.currentAgents).toBe(1);
    });
  });

  describe('task routing', () => {
    beforeEach(() => { meter.initWorkspace('ws1', 'solo'); }); // 100/hr, 5000/mo

    it('records task and emits billing event', () => {
      const r = meter.recordTaskRouted('ws1', 't1', 'summarize');
      expect(r.allowed).toBe(true);
      expect(meter.getUsageSummary('ws1')!.tasksRoutedThisMonth).toBe(1);
      expect(events.some(e => e.type === 'usage_recorded' && (e.data as any).taskId === 't1')).toBe(true);
    });

    it('enforces hourly rate limit', () => {
      for (let i = 0; i < 100; i++) {
        now += 1;
        meter.recordTaskRouted('ws1', `t${i}`, 'cap');
      }
      const r = meter.recordTaskRouted('ws1', 't100', 'cap');
      expect(r.allowed).toBe(false);
      expect(r.reason).toBe('hourly_task_limit_reached');
    });

    it('enforces monthly limit', () => {
      const usage = meter.getUsageSummary('ws1')!;
      usage.tasksRoutedThisMonth = 4999;
      store.setWorkspaceUsage('ws1', usage);

      const r1 = meter.recordTaskRouted('ws1', 'last', 'cap');
      expect(r1.allowed).toBe(true);
      const r2 = meter.recordTaskRouted('ws1', 'over', 'cap');
      expect(r2.allowed).toBe(false);
      expect(r2.reason).toBe('monthly_task_limit_reached');
    });

    it('emits monthly warning at threshold', () => {
      const usage = meter.getUsageSummary('ws1')!;
      usage.tasksRoutedThisMonth = 3999; // 80% of 5000
      store.setWorkspaceUsage('ws1', usage);

      meter.recordTaskRouted('ws1', 'tw', 'cap');
      expect(events.some(e => e.type === 'limit_warning' && (e.data as any).metric === 'monthly_tasks')).toBe(true);
    });

    it('records completed and failed tasks', () => {
      meter.recordTaskCompleted('ws1', 't1');
      meter.recordTaskFailed('ws1', 't2');
      const usage = meter.getUsageSummary('ws1')!;
      expect(usage.tasksCompletedThisMonth).toBe(1);
      expect(usage.tasksFailedThisMonth).toBe(1);
    });

    it('ignores completed/failed for unknown workspace', () => {
      meter.recordTaskCompleted('nope', 't1'); // no throw
      meter.recordTaskFailed('nope', 't2');
    });
  });

  describe('API calls', () => {
    it('records api calls', () => {
      meter.initWorkspace('ws1', 'team');
      meter.recordApiCall('ws1');
      meter.recordApiCall('ws1');
      expect(meter.getUsageSummary('ws1')!.apiCallsThisMonth).toBe(2);
    });

    it('ignores unknown workspace', () => {
      meter.recordApiCall('nope'); // no throw
    });
  });

  describe('billing period reset', () => {
    it('resets monthly counters and emits event', () => {
      meter.initWorkspace('ws1', 'team');
      meter.recordTaskRouted('ws1', 't1', 'cap');
      meter.recordTaskCompleted('ws1', 't1');
      meter.recordApiCall('ws1');
      meter.recordAgentConnection('ws1', 'a1');

      now += 100_000;
      meter.resetBillingPeriod('ws1');

      const usage = meter.getUsageSummary('ws1')!;
      expect(usage.tasksRoutedThisMonth).toBe(0);
      expect(usage.tasksCompletedThisMonth).toBe(0);
      expect(usage.apiCallsThisMonth).toBe(0);
      expect(usage.currentAgents).toBe(1); // still connected
      expect(usage.peakAgents).toBe(1); // reset to current

      const reset = events.find(e => e.type === 'billing_period_reset');
      expect(reset).toBeDefined();
      expect((reset!.data as any).tasksRouted).toBe(1);
    });

    it('ignores unknown workspace', () => {
      meter.resetBillingPeriod('nope'); // no throw
    });
  });

  describe('features', () => {
    it('checks plan features', () => {
      meter.initWorkspace('ws1', 'solo');
      expect(meter.hasFeature('ws1', 'basic-routing')).toBe(true);
      expect(meter.hasFeature('ws1', 'dashboard')).toBe(false);

      meter.initWorkspace('ws2', 'team');
      expect(meter.hasFeature('ws2', 'dashboard')).toBe(true);
      expect(meter.hasFeature('ws2', 'sso')).toBe(false);
    });

    it('returns false for unknown workspace', () => {
      expect(meter.hasFeature('nope', 'anything')).toBe(false);
    });
  });

  describe('utilization report', () => {
    it('returns report for all workspaces', () => {
      meter.initWorkspace('ws1', 'solo');
      meter.initWorkspace('ws2', 'team');
      meter.recordAgentConnection('ws1', 'a1');
      meter.recordTaskRouted('ws2', 't1', 'cap');

      const report = meter.getUtilizationReport();
      expect(report).toHaveLength(2);
      const ws1 = report.find(r => r.workspaceId === 'ws1')!;
      expect(ws1.agentUtilization).toBe(1 / 5);
      expect(ws1.currentAgents).toBe(1);
    });

    it('handles enterprise infinity limits', () => {
      meter.initWorkspace('ws1', 'enterprise');
      meter.recordAgentConnection('ws1', 'a1');
      const report = meter.getUtilizationReport();
      expect(report[0].agentUtilization).toBe(0); // Infinity handled
    });
  });

  describe('billing listener management', () => {
    it('unsubscribe removes listener', () => {
      const captured: BillingEvent[] = [];
      const unsub = meter.onBillingEvent(e => captured.push(e));
      meter.initWorkspace('ws1', 'solo');
      meter.recordTaskRouted('ws1', 't1', 'cap');
      const before = captured.length;
      unsub();
      meter.recordTaskRouted('ws1', 't2', 'cap');
      expect(captured.length).toBe(before);
    });
  });

  describe('connected agents', () => {
    it('returns empty for unknown workspace', () => {
      expect(meter.getConnectedAgents('nope')).toEqual([]);
    });
  });

  describe('plan limits', () => {
    it('returns limits for each tier', () => {
      expect(meter.getPlanLimits('solo').maxAgents).toBe(5);
      expect(meter.getPlanLimits('team').maxAgents).toBe(25);
      expect(meter.getPlanLimits('pro').maxAgents).toBe(250);
      expect(meter.getPlanLimits('enterprise').maxAgents).toBe(Infinity);
    });
  });
});

// ============================================================
// PLAN_LIMITS
// ============================================================

describe('PLAN_LIMITS', () => {
  it('all tiers defined', () => {
    expect(Object.keys(PLAN_LIMITS)).toEqual(['solo', 'team', 'pro', 'enterprise']);
  });

  it('tiers are ordered by limits', () => {
    const tiers: PlanTier[] = ['solo', 'team', 'pro', 'enterprise'];
    for (let i = 1; i < tiers.length; i++) {
      expect(PLAN_LIMITS[tiers[i]].maxAgents).toBeGreaterThanOrEqual(PLAN_LIMITS[tiers[i - 1]].maxAgents);
      expect(PLAN_LIMITS[tiers[i]].maxTasksPerMonth).toBeGreaterThanOrEqual(PLAN_LIMITS[tiers[i - 1]].maxTasksPerMonth);
      expect(PLAN_LIMITS[tiers[i]].retentionDays).toBeGreaterThanOrEqual(PLAN_LIMITS[tiers[i - 1]].retentionDays);
    }
  });

  it('each tier has features that are a superset of lower tiers', () => {
    const tiers: PlanTier[] = ['solo', 'team', 'pro', 'enterprise'];
    for (let i = 1; i < tiers.length; i++) {
      for (const f of PLAN_LIMITS[tiers[i - 1]].features) {
        expect(PLAN_LIMITS[tiers[i]].features.has(f)).toBe(true);
      }
    }
  });
});
