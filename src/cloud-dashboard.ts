/**
 * Tensegrity Cloud Dashboard — Real-time agent monitoring and visualization.
 *
 * Tracks connected agents, task routing, circuit breaker states, failure rates,
 * and provides both a programmatic API and HTML dashboard endpoint.
 *
 * Usage:
 *   import { Dashboard, DashboardServer } from 'tensegrity/cloud-dashboard';
 *   const dashboard = new Dashboard();
 *   dashboard.recordAgentConnect({ agentId: 'a1', name: 'Summarizer', capabilities: ['summarize'] });
 *   dashboard.recordTaskRouted({ taskId: 't1', capability: 'summarize', agentId: 'a1' });
 *   dashboard.recordTaskCompleted({ taskId: 't1', agentId: 'a1', latencyMs: 120 });
 *   const snapshot = dashboard.getSnapshot();
 */

// ============================================================
// Types
// ============================================================

export interface AgentInfo {
  agentId: string;
  name: string;
  capabilities: string[];
  connectedAt: number;
  lastHeartbeat: number;
  status: 'connected' | 'degraded' | 'disconnected';
  metrics: AgentTaskMetrics;
}

export interface AgentTaskMetrics {
  tasksCompleted: number;
  tasksFailed: number;
  tasksInFlight: number;
  totalLatencyMs: number;
  avgLatencyMs: number;
  p95LatencyMs: number;
  latencies: number[];  // sliding window for percentile calc
  errorRate: number;     // rolling error rate (0-1)
}

export interface CircuitState {
  agentId: string;
  capability: string;
  state: 'closed' | 'open' | 'half-open';
  openedAt?: number;
  failureCount: number;
}

export interface TaskRecord {
  taskId: string;
  capability: string;
  agentId: string;
  routedAt: number;
  completedAt?: number;
  failedAt?: number;
  latencyMs?: number;
  status: 'in-flight' | 'completed' | 'failed';
}

export interface DashboardSnapshot {
  timestamp: number;
  agents: AgentInfo[];
  activeAgents: number;
  degradedAgents: number;
  disconnectedAgents: number;
  circuits: CircuitState[];
  openCircuits: number;
  taskSummary: TaskSummary;
  recentTasks: TaskRecord[];
  capabilityStats: Map<string, CapabilityStats>;
  alerts: DashboardAlert[];
}

export interface TaskSummary {
  totalRouted: number;
  totalCompleted: number;
  totalFailed: number;
  inFlight: number;
  avgLatencyMs: number;
  p95LatencyMs: number;
  throughputPerMin: number;
}

export interface CapabilityStats {
  capability: string;
  agentCount: number;
  agentIds: string[];
  tasksRouted: number;
  tasksCompleted: number;
  tasksFailed: number;
  avgLatencyMs: number;
}

export interface DashboardAlert {
  id: string;
  severity: 'info' | 'warning' | 'critical';
  message: string;
  timestamp: number;
  agentId?: string;
  capability?: string;
}

export interface DashboardConfig {
  /** Max recent tasks to keep (default: 1000) */
  maxRecentTasks: number;
  /** Max latencies to keep per agent for percentile calc (default: 500) */
  maxLatencyWindow: number;
  /** Agent considered degraded after this many ms without heartbeat (default: 45000) */
  degradedAfterMs: number;
  /** Agent considered disconnected after this many ms without heartbeat (default: 120000) */
  disconnectedAfterMs: number;
  /** Error rate threshold for alerts (default: 0.3) */
  errorRateAlertThreshold: number;
  /** Max alerts to retain (default: 200) */
  maxAlerts: number;
  /** Throughput window in ms (default: 60000) */
  throughputWindowMs: number;
}

export const DEFAULT_CONFIG: DashboardConfig = {
  maxRecentTasks: 1000,
  maxLatencyWindow: 500,
  degradedAfterMs: 45_000,
  disconnectedAfterMs: 120_000,
  errorRateAlertThreshold: 0.3,
  maxAlerts: 200,
  throughputWindowMs: 60_000,
};

export type DashboardEventType =
  | 'agent:connect'
  | 'agent:disconnect'
  | 'agent:degraded'
  | 'task:routed'
  | 'task:completed'
  | 'task:failed'
  | 'circuit:open'
  | 'circuit:close'
  | 'alert';

export type DashboardEventHandler = (event: DashboardEventType, data: unknown) => void;

// ============================================================
// Dashboard
// ============================================================

export class Dashboard {
  private config: DashboardConfig;
  private agents = new Map<string, AgentInfo>();
  private circuits = new Map<string, CircuitState>(); // key: agentId:capability
  private tasks = new Map<string, TaskRecord>();
  private recentCompletedTasks: TaskRecord[] = [];
  private completionTimestamps: number[] = []; // for throughput calc
  private globalLatencies: number[] = [];
  private totalRouted = 0;
  private totalCompleted = 0;
  private totalFailed = 0;
  private alerts: DashboardAlert[] = [];
  private alertCounter = 0;
  private listeners = new Set<DashboardEventHandler>();
  private capabilityStats = new Map<string, CapabilityStats>();

  constructor(config?: Partial<DashboardConfig>) {
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  // ---- Agent Management ----

  recordAgentConnect(info: { agentId: string; name?: string; capabilities?: string[] }): void {
    const now = Date.now();
    const existing = this.agents.get(info.agentId);
    const agent: AgentInfo = {
      agentId: info.agentId,
      name: info.name ?? info.agentId,
      capabilities: info.capabilities ?? [],
      connectedAt: existing?.connectedAt ?? now,
      lastHeartbeat: now,
      status: 'connected',
      metrics: existing?.metrics ?? this._newMetrics(),
    };
    if (info.capabilities) agent.capabilities = info.capabilities;
    this.agents.set(info.agentId, agent);
    this._updateCapabilityAgents();
    this._emit('agent:connect', agent);
  }

  recordAgentDisconnect(agentId: string): void {
    const agent = this.agents.get(agentId);
    if (agent) {
      agent.status = 'disconnected';
      agent.lastHeartbeat = Date.now();
      this._emit('agent:disconnect', agent);
    }
  }

  recordHeartbeat(agentId: string): void {
    const agent = this.agents.get(agentId);
    if (agent) {
      agent.lastHeartbeat = Date.now();
      if (agent.status === 'degraded') {
        agent.status = 'connected';
      }
    }
  }

  // ---- Task Tracking ----

  recordTaskRouted(info: { taskId: string; capability: string; agentId: string }): void {
    const now = Date.now();
    const record: TaskRecord = {
      taskId: info.taskId,
      capability: info.capability,
      agentId: info.agentId,
      routedAt: now,
      status: 'in-flight',
    };
    this.tasks.set(info.taskId, record);
    this.totalRouted++;

    const agent = this.agents.get(info.agentId);
    if (agent) agent.metrics.tasksInFlight++;

    const cap = this._getOrCreateCapability(info.capability);
    cap.tasksRouted++;

    this._emit('task:routed', record);
  }

  recordTaskCompleted(info: { taskId: string; agentId: string; latencyMs?: number }): void {
    const now = Date.now();
    const record = this.tasks.get(info.taskId);
    if (record) {
      record.status = 'completed';
      record.completedAt = now;
      record.latencyMs = info.latencyMs ?? (now - record.routedAt);
      this._addRecentTask(record);
      this.tasks.delete(info.taskId);

      const cap = this.capabilityStats.get(record.capability);
      if (cap) {
        cap.tasksCompleted++;
        const totalCap = cap.tasksCompleted + cap.tasksFailed;
        cap.avgLatencyMs = ((cap.avgLatencyMs * (totalCap - 1)) + record.latencyMs) / totalCap;
      }
    }

    this.totalCompleted++;
    this.completionTimestamps.push(now);
    const latency = info.latencyMs ?? (record ? now - record.routedAt : 0);
    this._addGlobalLatency(latency);

    const agent = this.agents.get(info.agentId);
    if (agent) {
      agent.metrics.tasksInFlight = Math.max(0, agent.metrics.tasksInFlight - 1);
      agent.metrics.tasksCompleted++;
      agent.metrics.totalLatencyMs += latency;
      this._addAgentLatency(agent, latency);
      this._updateAgentErrorRate(agent);
    }

    this._emit('task:completed', { taskId: info.taskId, agentId: info.agentId, latencyMs: latency });
  }

  recordTaskFailed(info: { taskId: string; agentId: string; reason?: string }): void {
    const now = Date.now();
    const record = this.tasks.get(info.taskId);
    if (record) {
      record.status = 'failed';
      record.failedAt = now;
      this._addRecentTask(record);
      this.tasks.delete(info.taskId);

      const cap = this.capabilityStats.get(record.capability);
      if (cap) cap.tasksFailed++;
    }

    this.totalFailed++;

    const agent = this.agents.get(info.agentId);
    if (agent) {
      agent.metrics.tasksInFlight = Math.max(0, agent.metrics.tasksInFlight - 1);
      agent.metrics.tasksFailed++;
      this._updateAgentErrorRate(agent);

      // Alert on high error rate
      if (agent.metrics.errorRate > this.config.errorRateAlertThreshold) {
        this._addAlert('warning', `Agent "${agent.name}" error rate is ${(agent.metrics.errorRate * 100).toFixed(1)}%`, info.agentId);
      }
    }

    this._emit('task:failed', { taskId: info.taskId, agentId: info.agentId, reason: info.reason });
  }

  // ---- Circuit Breaker Tracking ----

  recordCircuitOpen(agentId: string, capability: string): void {
    const key = `${agentId}:${capability}`;
    const existing = this.circuits.get(key);
    this.circuits.set(key, {
      agentId,
      capability,
      state: 'open',
      openedAt: Date.now(),
      failureCount: (existing?.failureCount ?? 0) + 1,
    });
    this._addAlert('critical', `Circuit breaker OPEN for "${capability}" on agent "${agentId}"`, agentId, capability);
    this._emit('circuit:open', { agentId, capability });
  }

  recordCircuitClose(agentId: string, capability: string): void {
    const key = `${agentId}:${capability}`;
    const existing = this.circuits.get(key);
    if (existing) {
      existing.state = 'closed';
      this._addAlert('info', `Circuit breaker closed for "${capability}" on agent "${agentId}"`, agentId, capability);
    }
    this._emit('circuit:close', { agentId, capability });
  }

  recordCircuitHalfOpen(agentId: string, capability: string): void {
    const key = `${agentId}:${capability}`;
    const existing = this.circuits.get(key);
    if (existing) {
      existing.state = 'half-open';
    }
  }

  // ---- Queries ----

  getSnapshot(): DashboardSnapshot {
    this._refreshAgentStatuses();
    const agents = Array.from(this.agents.values());
    const circuits = Array.from(this.circuits.values());

    return {
      timestamp: Date.now(),
      agents,
      activeAgents: agents.filter(a => a.status === 'connected').length,
      degradedAgents: agents.filter(a => a.status === 'degraded').length,
      disconnectedAgents: agents.filter(a => a.status === 'disconnected').length,
      circuits,
      openCircuits: circuits.filter(c => c.state === 'open').length,
      taskSummary: this._getTaskSummary(),
      recentTasks: this.recentCompletedTasks.slice(-50),
      capabilityStats: new Map(this.capabilityStats),
      alerts: this.alerts.slice(-50),
    };
  }

  getAgent(agentId: string): AgentInfo | undefined {
    return this.agents.get(agentId);
  }

  getAgents(): AgentInfo[] {
    this._refreshAgentStatuses();
    return Array.from(this.agents.values());
  }

  getAlerts(limit = 50): DashboardAlert[] {
    return this.alerts.slice(-limit);
  }

  getCapabilityStats(): Map<string, CapabilityStats> {
    return new Map(this.capabilityStats);
  }

  getInFlightTasks(): TaskRecord[] {
    return Array.from(this.tasks.values());
  }

  // ---- Events ----

  on(handler: DashboardEventHandler): () => void {
    this.listeners.add(handler);
    return () => this.listeners.delete(handler);
  }

  // ---- HTML Dashboard ----

  renderHTML(): string {
    const snapshot = this.getSnapshot();
    const capStats = Array.from(snapshot.capabilityStats.values());

    return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Tensegrity Dashboard</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif; background: #0a0e17; color: #e2e8f0; }
  .container { max-width: 1400px; margin: 0 auto; padding: 24px; }
  h1 { font-size: 24px; font-weight: 700; margin-bottom: 24px; color: #f8fafc; }
  h1 span { color: #818cf8; }
  .grid { display: grid; gap: 16px; margin-bottom: 24px; }
  .grid-4 { grid-template-columns: repeat(4, 1fr); }
  .grid-2 { grid-template-columns: repeat(2, 1fr); }
  .card { background: #1e293b; border-radius: 12px; padding: 20px; border: 1px solid #334155; }
  .stat-card { text-align: center; }
  .stat-value { font-size: 36px; font-weight: 700; color: #f8fafc; }
  .stat-label { font-size: 13px; color: #94a3b8; margin-top: 4px; text-transform: uppercase; letter-spacing: 0.5px; }
  .stat-value.green { color: #4ade80; }
  .stat-value.yellow { color: #fbbf24; }
  .stat-value.red { color: #f87171; }
  .stat-value.blue { color: #60a5fa; }
  h2 { font-size: 16px; font-weight: 600; margin-bottom: 16px; color: #cbd5e1; }
  table { width: 100%; border-collapse: collapse; }
  th { text-align: left; padding: 8px 12px; font-size: 12px; color: #64748b; text-transform: uppercase; letter-spacing: 0.5px; border-bottom: 1px solid #334155; }
  td { padding: 10px 12px; font-size: 14px; border-bottom: 1px solid #1e293b; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 9999px; font-size: 12px; font-weight: 500; }
  .badge-green { background: #064e3b; color: #4ade80; }
  .badge-yellow { background: #713f12; color: #fbbf24; }
  .badge-red { background: #7f1d1d; color: #f87171; }
  .alert-row { display: flex; align-items: center; gap: 12px; padding: 8px 0; border-bottom: 1px solid #1e293b; font-size: 14px; }
  .alert-row:last-child { border-bottom: none; }
  .alert-time { color: #64748b; font-size: 12px; min-width: 80px; }
  .empty { color: #475569; font-style: italic; padding: 20px 0; text-align: center; }
  @media (max-width: 768px) { .grid-4 { grid-template-columns: repeat(2, 1fr); } .grid-2 { grid-template-columns: 1fr; } }
</style>
</head>
<body>
<div class="container">
  <h1><span>⬡</span> Tensegrity Dashboard</h1>

  <div class="grid grid-4">
    <div class="card stat-card">
      <div class="stat-value green">${snapshot.activeAgents}</div>
      <div class="stat-label">Active Agents</div>
    </div>
    <div class="card stat-card">
      <div class="stat-value blue">${snapshot.taskSummary.inFlight}</div>
      <div class="stat-label">In-Flight Tasks</div>
    </div>
    <div class="card stat-card">
      <div class="stat-value ${snapshot.openCircuits > 0 ? 'red' : 'green'}">${snapshot.openCircuits}</div>
      <div class="stat-label">Open Circuits</div>
    </div>
    <div class="card stat-card">
      <div class="stat-value yellow">${snapshot.taskSummary.throughputPerMin.toFixed(1)}</div>
      <div class="stat-label">Tasks/min</div>
    </div>
  </div>

  <div class="grid grid-4">
    <div class="card stat-card">
      <div class="stat-value">${snapshot.taskSummary.totalRouted}</div>
      <div class="stat-label">Total Routed</div>
    </div>
    <div class="card stat-card">
      <div class="stat-value green">${snapshot.taskSummary.totalCompleted}</div>
      <div class="stat-label">Completed</div>
    </div>
    <div class="card stat-card">
      <div class="stat-value red">${snapshot.taskSummary.totalFailed}</div>
      <div class="stat-label">Failed</div>
    </div>
    <div class="card stat-card">
      <div class="stat-value blue">${snapshot.taskSummary.avgLatencyMs.toFixed(0)}ms</div>
      <div class="stat-label">Avg Latency</div>
    </div>
  </div>

  <div class="grid grid-2">
    <div class="card">
      <h2>Agents</h2>
      ${snapshot.agents.length === 0 ? '<div class="empty">No agents connected</div>' : `
      <table>
        <thead><tr><th>Agent</th><th>Status</th><th>Capabilities</th><th>Completed</th><th>Failed</th><th>Avg Latency</th></tr></thead>
        <tbody>
          ${snapshot.agents.map(a => `<tr>
            <td><strong>${esc(a.name)}</strong><br><span style="color:#64748b;font-size:12px">${esc(a.agentId)}</span></td>
            <td><span class="badge badge-${a.status === 'connected' ? 'green' : a.status === 'degraded' ? 'yellow' : 'red'}">${a.status}</span></td>
            <td style="font-size:12px">${a.capabilities.map(c => esc(c)).join(', ') || '—'}</td>
            <td>${a.metrics.tasksCompleted}</td>
            <td>${a.metrics.tasksFailed}</td>
            <td>${a.metrics.avgLatencyMs.toFixed(0)}ms</td>
          </tr>`).join('')}
        </tbody>
      </table>`}
    </div>

    <div class="card">
      <h2>Capabilities</h2>
      ${capStats.length === 0 ? '<div class="empty">No capabilities registered</div>' : `
      <table>
        <thead><tr><th>Capability</th><th>Agents</th><th>Routed</th><th>Completed</th><th>Failed</th><th>Avg Latency</th></tr></thead>
        <tbody>
          ${capStats.map(c => `<tr>
            <td><strong>${esc(c.capability)}</strong></td>
            <td>${c.agentCount}</td>
            <td>${c.tasksRouted}</td>
            <td>${c.tasksCompleted}</td>
            <td>${c.tasksFailed}</td>
            <td>${c.avgLatencyMs.toFixed(0)}ms</td>
          </tr>`).join('')}
        </tbody>
      </table>`}
    </div>
  </div>

  ${snapshot.circuits.length > 0 ? `
  <div class="card" style="margin-bottom:24px">
    <h2>Circuit Breakers</h2>
    <table>
      <thead><tr><th>Agent</th><th>Capability</th><th>State</th><th>Failures</th><th>Opened At</th></tr></thead>
      <tbody>
        ${snapshot.circuits.map(c => `<tr>
          <td>${esc(c.agentId)}</td>
          <td>${esc(c.capability)}</td>
          <td><span class="badge badge-${c.state === 'closed' ? 'green' : c.state === 'open' ? 'red' : 'yellow'}">${c.state}</span></td>
          <td>${c.failureCount}</td>
          <td>${c.openedAt ? new Date(c.openedAt).toISOString().slice(11, 19) : '—'}</td>
        </tr>`).join('')}
      </tbody>
    </table>
  </div>` : ''}

  <div class="card">
    <h2>Recent Alerts</h2>
    ${snapshot.alerts.length === 0 ? '<div class="empty">No alerts</div>' : `
    ${snapshot.alerts.slice().reverse().map(a => `
      <div class="alert-row">
        <span class="badge badge-${a.severity === 'critical' ? 'red' : a.severity === 'warning' ? 'yellow' : 'green'}">${a.severity}</span>
        <span class="alert-time">${new Date(a.timestamp).toISOString().slice(11, 19)}</span>
        <span>${esc(a.message)}</span>
      </div>
    `).join('')}`}
  </div>
</div>
</body>
</html>`;
  }

  /** Get a JSON-serializable snapshot (converts Maps to objects). */
  toJSON(): Record<string, unknown> {
    const snapshot = this.getSnapshot();
    const capStats: Record<string, CapabilityStats> = {};
    snapshot.capabilityStats.forEach((v, k) => { capStats[k] = v; });
    return {
      ...snapshot,
      capabilityStats: capStats,
    };
  }

  /** Reset all state. */
  reset(): void {
    this.agents.clear();
    this.circuits.clear();
    this.tasks.clear();
    this.recentCompletedTasks = [];
    this.completionTimestamps = [];
    this.globalLatencies = [];
    this.totalRouted = 0;
    this.totalCompleted = 0;
    this.totalFailed = 0;
    this.alerts = [];
    this.alertCounter = 0;
    this.capabilityStats.clear();
  }

  // ---- Internals ----

  private _newMetrics(): AgentTaskMetrics {
    return {
      tasksCompleted: 0,
      tasksFailed: 0,
      tasksInFlight: 0,
      totalLatencyMs: 0,
      avgLatencyMs: 0,
      p95LatencyMs: 0,
      latencies: [],
      errorRate: 0,
    };
  }

  private _addAgentLatency(agent: AgentInfo, latencyMs: number): void {
    agent.metrics.latencies.push(latencyMs);
    if (agent.metrics.latencies.length > this.config.maxLatencyWindow) {
      agent.metrics.latencies.shift();
    }
    const total = agent.metrics.tasksCompleted + agent.metrics.tasksFailed;
    agent.metrics.avgLatencyMs = total > 0 ? agent.metrics.totalLatencyMs / agent.metrics.tasksCompleted : 0;
    agent.metrics.p95LatencyMs = percentile(agent.metrics.latencies, 0.95);
  }

  private _addGlobalLatency(latencyMs: number): void {
    this.globalLatencies.push(latencyMs);
    if (this.globalLatencies.length > this.config.maxLatencyWindow) {
      this.globalLatencies.shift();
    }
  }

  private _updateAgentErrorRate(agent: AgentInfo): void {
    const total = agent.metrics.tasksCompleted + agent.metrics.tasksFailed;
    agent.metrics.errorRate = total > 0 ? agent.metrics.tasksFailed / total : 0;
  }

  private _addRecentTask(record: TaskRecord): void {
    this.recentCompletedTasks.push(record);
    if (this.recentCompletedTasks.length > this.config.maxRecentTasks) {
      this.recentCompletedTasks.shift();
    }
  }

  private _getTaskSummary(): TaskSummary {
    const now = Date.now();
    // Prune old completion timestamps
    const cutoff = now - this.config.throughputWindowMs;
    while (this.completionTimestamps.length > 0 && this.completionTimestamps[0] < cutoff) {
      this.completionTimestamps.shift();
    }
    const windowMinutes = this.config.throughputWindowMs / 60_000;
    const avgLatency = this.globalLatencies.length > 0
      ? this.globalLatencies.reduce((a, b) => a + b, 0) / this.globalLatencies.length
      : 0;

    return {
      totalRouted: this.totalRouted,
      totalCompleted: this.totalCompleted,
      totalFailed: this.totalFailed,
      inFlight: this.tasks.size,
      avgLatencyMs: avgLatency,
      p95LatencyMs: percentile(this.globalLatencies, 0.95),
      throughputPerMin: this.completionTimestamps.length / windowMinutes,
    };
  }

  private _refreshAgentStatuses(): void {
    const now = Date.now();
    for (const agent of this.agents.values()) {
      if (agent.status === 'disconnected') continue;
      const elapsed = now - agent.lastHeartbeat;
      if (elapsed > this.config.disconnectedAfterMs) {
        agent.status = 'disconnected';
        this._addAlert('critical', `Agent "${agent.name}" appears disconnected (no heartbeat for ${Math.round(elapsed / 1000)}s)`, agent.agentId);
        this._emit('agent:disconnect', agent);
      } else if (elapsed > this.config.degradedAfterMs) {
        if (agent.status !== 'degraded') {
          agent.status = 'degraded';
          this._addAlert('warning', `Agent "${agent.name}" heartbeat delayed (${Math.round(elapsed / 1000)}s)`, agent.agentId);
          this._emit('agent:degraded', agent);
        }
      }
    }
  }

  private _getOrCreateCapability(capability: string): CapabilityStats {
    let stats = this.capabilityStats.get(capability);
    if (!stats) {
      stats = {
        capability,
        agentCount: 0,
        agentIds: [],
        tasksRouted: 0,
        tasksCompleted: 0,
        tasksFailed: 0,
        avgLatencyMs: 0,
      };
      this.capabilityStats.set(capability, stats);
    }
    return stats;
  }

  private _updateCapabilityAgents(): void {
    // Rebuild agent counts from live agents
    const capAgents = new Map<string, Set<string>>();
    for (const agent of this.agents.values()) {
      for (const cap of agent.capabilities) {
        if (!capAgents.has(cap)) capAgents.set(cap, new Set());
        capAgents.get(cap)!.add(agent.agentId);
      }
    }
    for (const [cap, agentIds] of capAgents) {
      const stats = this._getOrCreateCapability(cap);
      stats.agentCount = agentIds.size;
      stats.agentIds = Array.from(agentIds);
    }
  }

  private _addAlert(severity: DashboardAlert['severity'], message: string, agentId?: string, capability?: string): void {
    const alert: DashboardAlert = {
      id: `alert-${++this.alertCounter}`,
      severity,
      message,
      timestamp: Date.now(),
      agentId,
      capability,
    };
    this.alerts.push(alert);
    if (this.alerts.length > this.config.maxAlerts) {
      this.alerts.shift();
    }
    this._emit('alert', alert);
  }

  private _emit(event: DashboardEventType, data: unknown): void {
    for (const handler of this.listeners) {
      try { handler(event, data); } catch (_) {}
    }
  }
}

// ============================================================
// Utilities
// ============================================================

function percentile(values: number[], p: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.ceil(p * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

function esc(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
