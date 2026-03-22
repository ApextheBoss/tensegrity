/**
 * Tensegrity Cloud Usage Metering — Track agent usage, enforce plan limits,
 * and emit billing events for Stripe integration.
 *
 * Zero dependencies. Provides hooks/callbacks for billing provider integration
 * rather than importing Stripe SDK directly.
 *
 * Usage:
 *   import { UsageMeter, InMemoryUsageStore } from 'tensegrity/usage-metering';
 *   const meter = new UsageMeter({ store: new InMemoryUsageStore(), plan: 'team' });
 *   meter.onBillingEvent(event => sendToStripe(event));
 *   await meter.recordAgentConnection('workspace-1', 'agent-1');
 *   await meter.recordTaskRouted('workspace-1', 'task-1', 'summarize');
 *   const usage = await meter.getUsageSummary('workspace-1');
 */

// ============================================================
// Types
// ============================================================

export type PlanTier = 'solo' | 'team' | 'pro' | 'enterprise';

export interface PlanLimits {
  maxAgents: number;            // max concurrent connected agents
  maxTasksPerHour: number;      // task routing rate limit
  maxTasksPerMonth: number;     // monthly task cap
  retentionDays: number;        // metric retention
  features: Set<string>;        // enabled features
}

export const PLAN_LIMITS: Record<PlanTier, PlanLimits> = {
  solo: {
    maxAgents: 5,
    maxTasksPerHour: 100,
    maxTasksPerMonth: 5_000,
    retentionDays: 1,
    features: new Set(['basic-routing']),
  },
  team: {
    maxAgents: 25,
    maxTasksPerHour: 1_000,
    maxTasksPerMonth: 100_000,
    retentionDays: 7,
    features: new Set(['basic-routing', 'dashboard', 'alerts']),
  },
  pro: {
    maxAgents: 250,
    maxTasksPerHour: 10_000,
    maxTasksPerMonth: 1_000_000,
    retentionDays: 90,
    features: new Set(['basic-routing', 'dashboard', 'alerts', 'priority-support', 'sla']),
  },
  enterprise: {
    maxAgents: Infinity,
    maxTasksPerHour: Infinity,
    maxTasksPerMonth: Infinity,
    retentionDays: 365,
    features: new Set(['basic-routing', 'dashboard', 'alerts', 'priority-support', 'sla', 'sso', 'audit-logs', 'custom-sla']),
  },
};

export interface UsageRecord {
  workspaceId: string;
  metric: UsageMetric;
  value: number;
  timestamp: number;
}

export type UsageMetric =
  | 'agent_connection'
  | 'agent_disconnection'
  | 'task_routed'
  | 'task_completed'
  | 'task_failed'
  | 'api_call'
  | 'dashboard_view';

export interface WorkspaceUsage {
  workspaceId: string;
  plan: PlanTier;
  currentAgents: number;
  peakAgents: number;
  tasksRoutedThisHour: number;
  tasksRoutedThisMonth: number;
  tasksCompletedThisMonth: number;
  tasksFailedThisMonth: number;
  apiCallsThisMonth: number;
  billingPeriodStart: number;
  lastActivity: number;
}

export interface BillingEvent {
  type: BillingEventType;
  workspaceId: string;
  timestamp: number;
  data: Record<string, unknown>;
}

export type BillingEventType =
  | 'usage_recorded'
  | 'limit_warning'       // approaching limit (80%)
  | 'limit_reached'       // at limit, requests rejected
  | 'overage'             // over limit (enterprise can exceed)
  | 'plan_change'
  | 'billing_period_reset';

export interface LimitCheckResult {
  allowed: boolean;
  reason?: string;
  currentUsage?: number;
  limit?: number;
  utilizationPct?: number;
}

// ============================================================
// Usage Store Interface
// ============================================================

export interface UsageStore {
  /** Record a usage event */
  record(record: UsageRecord): void;

  /** Get workspace usage state */
  getWorkspaceUsage(workspaceId: string): WorkspaceUsage | undefined;

  /** Create or update workspace usage state */
  setWorkspaceUsage(workspaceId: string, usage: WorkspaceUsage): void;

  /** Count records matching metric within time range */
  countRecords(workspaceId: string, metric: UsageMetric, sinceMs: number): number;

  /** Get all workspace IDs */
  listWorkspaces(): string[];
}

// ============================================================
// InMemoryUsageStore
// ============================================================

export class InMemoryUsageStore implements UsageStore {
  private records: UsageRecord[] = [];
  private workspaces = new Map<string, WorkspaceUsage>();

  record(record: UsageRecord): void {
    this.records.push(record);
  }

  getWorkspaceUsage(workspaceId: string): WorkspaceUsage | undefined {
    return this.workspaces.get(workspaceId);
  }

  setWorkspaceUsage(workspaceId: string, usage: WorkspaceUsage): void {
    this.workspaces.set(workspaceId, usage);
  }

  countRecords(workspaceId: string, metric: UsageMetric, sinceMs: number): number {
    let count = 0;
    for (const r of this.records) {
      if (r.workspaceId === workspaceId && r.metric === metric && r.timestamp >= sinceMs) {
        count++;
      }
    }
    return count;
  }

  listWorkspaces(): string[] {
    return Array.from(this.workspaces.keys());
  }

  /** Get all raw records (for testing/debugging) */
  getAllRecords(): UsageRecord[] {
    return [...this.records];
  }

  /** Prune records older than cutoffMs */
  prune(cutoffMs: number): number {
    const before = this.records.length;
    this.records = this.records.filter(r => r.timestamp >= cutoffMs);
    return before - this.records.length;
  }
}

// ============================================================
// RateLimiter — sliding window rate limiting
// ============================================================

export class SlidingWindowRateLimiter {
  private windows = new Map<string, number[]>();

  /** Check if action is within rate limit. Returns true if allowed. */
  check(key: string, limit: number, windowMs: number, now: number): boolean {
    const timestamps = this.getWindow(key, windowMs, now);
    return timestamps.length < limit;
  }

  /** Record an action. Returns false if would exceed limit. */
  record(key: string, limit: number, windowMs: number, now: number): boolean {
    const timestamps = this.getWindow(key, windowMs, now);
    if (timestamps.length >= limit) return false;
    timestamps.push(now);
    return true;
  }

  /** Get current count within window */
  count(key: string, windowMs: number, now: number): number {
    return this.getWindow(key, windowMs, now).length;
  }

  /** Clear all windows */
  clear(): void {
    this.windows.clear();
  }

  private getWindow(key: string, windowMs: number, now: number): number[] {
    let timestamps = this.windows.get(key);
    if (!timestamps) {
      timestamps = [];
      this.windows.set(key, timestamps);
      return timestamps;
    }
    // Prune expired entries
    const cutoff = now - windowMs;
    while (timestamps.length > 0 && timestamps[0] <= cutoff) {
      timestamps.shift();
    }
    return timestamps;
  }
}

// ============================================================
// UsageMeter — Main orchestrator
// ============================================================

export interface UsageMeterConfig {
  store: UsageStore;
  /** Override now() for testing */
  nowFn?: () => number;
  /** Warning threshold as fraction (default: 0.8) */
  warningThreshold?: number;
}

export class UsageMeter {
  private store: UsageStore;
  private nowFn: () => number;
  private warningThreshold: number;
  private rateLimiter = new SlidingWindowRateLimiter();
  private billingListeners: Array<(event: BillingEvent) => void> = [];
  private connectedAgents = new Map<string, Set<string>>(); // workspaceId -> agentIds

  constructor(config: UsageMeterConfig) {
    this.store = config.store;
    this.nowFn = config.nowFn ?? (() => Date.now());
    this.warningThreshold = config.warningThreshold ?? 0.8;
  }

  // ----------------------------------------------------------
  // Billing event listeners
  // ----------------------------------------------------------

  onBillingEvent(listener: (event: BillingEvent) => void): () => void {
    this.billingListeners.push(listener);
    return () => {
      const idx = this.billingListeners.indexOf(listener);
      if (idx >= 0) this.billingListeners.splice(idx, 1);
    };
  }

  private emitBilling(event: BillingEvent): void {
    for (const listener of this.billingListeners) {
      listener(event);
    }
  }

  // ----------------------------------------------------------
  // Workspace management
  // ----------------------------------------------------------

  initWorkspace(workspaceId: string, plan: PlanTier): WorkspaceUsage {
    const now = this.nowFn();
    const existing = this.store.getWorkspaceUsage(workspaceId);
    if (existing) return existing;
    const usage: WorkspaceUsage = {
      workspaceId,
      plan,
      currentAgents: 0,
      peakAgents: 0,
      tasksRoutedThisHour: 0,
      tasksRoutedThisMonth: 0,
      tasksCompletedThisMonth: 0,
      tasksFailedThisMonth: 0,
      apiCallsThisMonth: 0,
      billingPeriodStart: now,
      lastActivity: now,
    };
    this.store.setWorkspaceUsage(workspaceId, usage);
    this.connectedAgents.set(workspaceId, new Set());
    return usage;
  }

  changePlan(workspaceId: string, newPlan: PlanTier): LimitCheckResult {
    const usage = this.store.getWorkspaceUsage(workspaceId);
    if (!usage) return { allowed: false, reason: 'workspace_not_found' };

    const newLimits = PLAN_LIMITS[newPlan];

    // Check if current usage exceeds new plan limits
    if (usage.currentAgents > newLimits.maxAgents) {
      return {
        allowed: false,
        reason: 'current_agents_exceed_new_plan',
        currentUsage: usage.currentAgents,
        limit: newLimits.maxAgents,
      };
    }

    const oldPlan = usage.plan;
    usage.plan = newPlan;
    this.store.setWorkspaceUsage(workspaceId, usage);

    this.emitBilling({
      type: 'plan_change',
      workspaceId,
      timestamp: this.nowFn(),
      data: { oldPlan, newPlan },
    });

    return { allowed: true };
  }

  // ----------------------------------------------------------
  // Agent connections
  // ----------------------------------------------------------

  checkAgentConnection(workspaceId: string): LimitCheckResult {
    const usage = this.store.getWorkspaceUsage(workspaceId);
    if (!usage) return { allowed: false, reason: 'workspace_not_found' };

    const limits = PLAN_LIMITS[usage.plan];
    const utilization = usage.currentAgents / limits.maxAgents;

    if (usage.currentAgents >= limits.maxAgents) {
      return {
        allowed: false,
        reason: 'max_agents_reached',
        currentUsage: usage.currentAgents,
        limit: limits.maxAgents,
        utilizationPct: 100,
      };
    }

    return {
      allowed: true,
      currentUsage: usage.currentAgents,
      limit: limits.maxAgents,
      utilizationPct: Math.round(utilization * 100),
    };
  }

  recordAgentConnection(workspaceId: string, agentId: string): LimitCheckResult {
    const check = this.checkAgentConnection(workspaceId);
    if (!check.allowed) {
      this.emitBilling({
        type: 'limit_reached',
        workspaceId,
        timestamp: this.nowFn(),
        data: { metric: 'agent_connection', agentId, ...check },
      });
      return check;
    }

    const usage = this.store.getWorkspaceUsage(workspaceId)!;
    const now = this.nowFn();

    // Track connected agent
    let agents = this.connectedAgents.get(workspaceId);
    if (!agents) {
      agents = new Set();
      this.connectedAgents.set(workspaceId, agents);
    }
    agents.add(agentId);

    usage.currentAgents = agents.size;
    if (usage.currentAgents > usage.peakAgents) {
      usage.peakAgents = usage.currentAgents;
    }
    usage.lastActivity = now;
    this.store.setWorkspaceUsage(workspaceId, usage);

    this.store.record({ workspaceId, metric: 'agent_connection', value: 1, timestamp: now });

    // Check for warning threshold
    const limits = PLAN_LIMITS[usage.plan];
    const utilization = usage.currentAgents / limits.maxAgents;
    if (utilization >= this.warningThreshold && utilization < 1) {
      this.emitBilling({
        type: 'limit_warning',
        workspaceId,
        timestamp: now,
        data: { metric: 'agent_connection', currentAgents: usage.currentAgents, maxAgents: limits.maxAgents, utilizationPct: Math.round(utilization * 100) },
      });
    }

    return { allowed: true, currentUsage: usage.currentAgents, limit: limits.maxAgents, utilizationPct: Math.round(utilization * 100) };
  }

  recordAgentDisconnection(workspaceId: string, agentId: string): void {
    const agents = this.connectedAgents.get(workspaceId);
    if (agents) {
      agents.delete(agentId);
    }

    const usage = this.store.getWorkspaceUsage(workspaceId);
    if (!usage) return;

    const now = this.nowFn();
    usage.currentAgents = agents ? agents.size : 0;
    usage.lastActivity = now;
    this.store.setWorkspaceUsage(workspaceId, usage);

    this.store.record({ workspaceId, metric: 'agent_disconnection', value: 1, timestamp: now });
  }

  // ----------------------------------------------------------
  // Task routing
  // ----------------------------------------------------------

  checkTaskRouting(workspaceId: string): LimitCheckResult {
    const usage = this.store.getWorkspaceUsage(workspaceId);
    if (!usage) return { allowed: false, reason: 'workspace_not_found' };

    const limits = PLAN_LIMITS[usage.plan];
    const now = this.nowFn();
    const oneHourAgo = now - 3_600_000;

    // Check hourly rate
    const hourlyCount = this.rateLimiter.count(`${workspaceId}:tasks_hourly`, 3_600_000, now);
    if (hourlyCount >= limits.maxTasksPerHour) {
      return {
        allowed: false,
        reason: 'hourly_task_limit_reached',
        currentUsage: hourlyCount,
        limit: limits.maxTasksPerHour,
        utilizationPct: 100,
      };
    }

    // Check monthly cap
    if (usage.tasksRoutedThisMonth >= limits.maxTasksPerMonth) {
      return {
        allowed: false,
        reason: 'monthly_task_limit_reached',
        currentUsage: usage.tasksRoutedThisMonth,
        limit: limits.maxTasksPerMonth,
        utilizationPct: 100,
      };
    }

    return { allowed: true, currentUsage: hourlyCount, limit: limits.maxTasksPerHour };
  }

  recordTaskRouted(workspaceId: string, taskId: string, capability: string): LimitCheckResult {
    const check = this.checkTaskRouting(workspaceId);
    if (!check.allowed) {
      this.emitBilling({
        type: 'limit_reached',
        workspaceId,
        timestamp: this.nowFn(),
        data: { metric: 'task_routed', taskId, capability, ...check },
      });
      return check;
    }

    const usage = this.store.getWorkspaceUsage(workspaceId)!;
    const now = this.nowFn();
    const limits = PLAN_LIMITS[usage.plan];

    this.rateLimiter.record(`${workspaceId}:tasks_hourly`, limits.maxTasksPerHour, 3_600_000, now);
    usage.tasksRoutedThisMonth++;
    usage.lastActivity = now;
    this.store.setWorkspaceUsage(workspaceId, usage);

    this.store.record({ workspaceId, metric: 'task_routed', value: 1, timestamp: now });

    this.emitBilling({
      type: 'usage_recorded',
      workspaceId,
      timestamp: now,
      data: { metric: 'task_routed', taskId, capability },
    });

    // Warning at threshold
    const monthlyUtilization = usage.tasksRoutedThisMonth / limits.maxTasksPerMonth;
    if (monthlyUtilization >= this.warningThreshold && monthlyUtilization < 1) {
      this.emitBilling({
        type: 'limit_warning',
        workspaceId,
        timestamp: now,
        data: { metric: 'monthly_tasks', current: usage.tasksRoutedThisMonth, limit: limits.maxTasksPerMonth, utilizationPct: Math.round(monthlyUtilization * 100) },
      });
    }

    return { allowed: true, currentUsage: usage.tasksRoutedThisMonth, limit: limits.maxTasksPerMonth };
  }

  recordTaskCompleted(workspaceId: string, taskId: string): void {
    const usage = this.store.getWorkspaceUsage(workspaceId);
    if (!usage) return;
    usage.tasksCompletedThisMonth++;
    usage.lastActivity = this.nowFn();
    this.store.setWorkspaceUsage(workspaceId, usage);
    this.store.record({ workspaceId, metric: 'task_completed', value: 1, timestamp: this.nowFn() });
  }

  recordTaskFailed(workspaceId: string, taskId: string): void {
    const usage = this.store.getWorkspaceUsage(workspaceId);
    if (!usage) return;
    usage.tasksFailedThisMonth++;
    usage.lastActivity = this.nowFn();
    this.store.setWorkspaceUsage(workspaceId, usage);
    this.store.record({ workspaceId, metric: 'task_failed', value: 1, timestamp: this.nowFn() });
  }

  // ----------------------------------------------------------
  // API calls
  // ----------------------------------------------------------

  recordApiCall(workspaceId: string): void {
    const usage = this.store.getWorkspaceUsage(workspaceId);
    if (!usage) return;
    usage.apiCallsThisMonth++;
    usage.lastActivity = this.nowFn();
    this.store.setWorkspaceUsage(workspaceId, usage);
    this.store.record({ workspaceId, metric: 'api_call', value: 1, timestamp: this.nowFn() });
  }

  // ----------------------------------------------------------
  // Billing period management
  // ----------------------------------------------------------

  resetBillingPeriod(workspaceId: string): void {
    const usage = this.store.getWorkspaceUsage(workspaceId);
    if (!usage) return;
    const now = this.nowFn();

    this.emitBilling({
      type: 'billing_period_reset',
      workspaceId,
      timestamp: now,
      data: {
        previousPeriodStart: usage.billingPeriodStart,
        tasksRouted: usage.tasksRoutedThisMonth,
        tasksCompleted: usage.tasksCompletedThisMonth,
        tasksFailed: usage.tasksFailedThisMonth,
        apiCalls: usage.apiCallsThisMonth,
        peakAgents: usage.peakAgents,
      },
    });

    usage.tasksRoutedThisMonth = 0;
    usage.tasksCompletedThisMonth = 0;
    usage.tasksFailedThisMonth = 0;
    usage.apiCallsThisMonth = 0;
    usage.peakAgents = usage.currentAgents;
    usage.billingPeriodStart = now;
    this.store.setWorkspaceUsage(workspaceId, usage);

    this.rateLimiter.clear();
  }

  // ----------------------------------------------------------
  // Usage queries
  // ----------------------------------------------------------

  getUsageSummary(workspaceId: string): WorkspaceUsage | undefined {
    return this.store.getWorkspaceUsage(workspaceId);
  }

  getConnectedAgents(workspaceId: string): string[] {
    const agents = this.connectedAgents.get(workspaceId);
    return agents ? Array.from(agents) : [];
  }

  getPlanLimits(plan: PlanTier): PlanLimits {
    return PLAN_LIMITS[plan];
  }

  /** Check if a feature is available on the workspace's plan */
  hasFeature(workspaceId: string, feature: string): boolean {
    const usage = this.store.getWorkspaceUsage(workspaceId);
    if (!usage) return false;
    return PLAN_LIMITS[usage.plan].features.has(feature);
  }

  /** Get utilization report for all workspaces */
  getUtilizationReport(): Array<{
    workspaceId: string;
    plan: PlanTier;
    agentUtilization: number;
    monthlyTaskUtilization: number;
    currentAgents: number;
    tasksThisMonth: number;
  }> {
    return this.store.listWorkspaces().map(wid => {
      const usage = this.store.getWorkspaceUsage(wid)!;
      const limits = PLAN_LIMITS[usage.plan];
      return {
        workspaceId: wid,
        plan: usage.plan,
        agentUtilization: limits.maxAgents === Infinity ? 0 : usage.currentAgents / limits.maxAgents,
        monthlyTaskUtilization: limits.maxTasksPerMonth === Infinity ? 0 : usage.tasksRoutedThisMonth / limits.maxTasksPerMonth,
        currentAgents: usage.currentAgents,
        tasksThisMonth: usage.tasksRoutedThisMonth,
      };
    });
  }
}
