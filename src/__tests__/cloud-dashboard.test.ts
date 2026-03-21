import { describe, it, expect, beforeEach, vi } from 'vitest';
import { Dashboard, DashboardConfig, DEFAULT_CONFIG } from '../cloud-dashboard';

describe('Dashboard', () => {
  let dash: Dashboard;

  beforeEach(() => {
    dash = new Dashboard();
  });

  // ---- Agent Management ----

  describe('agent tracking', () => {
    it('records agent connect', () => {
      dash.recordAgentConnect({ agentId: 'a1', name: 'Agent One', capabilities: ['summarize'] });
      const agent = dash.getAgent('a1');
      expect(agent).toBeDefined();
      expect(agent!.name).toBe('Agent One');
      expect(agent!.capabilities).toEqual(['summarize']);
      expect(agent!.status).toBe('connected');
    });

    it('defaults name to agentId', () => {
      dash.recordAgentConnect({ agentId: 'a1' });
      expect(dash.getAgent('a1')!.name).toBe('a1');
    });

    it('reconnect preserves metrics', () => {
      dash.recordAgentConnect({ agentId: 'a1', capabilities: ['x'] });
      dash.recordTaskRouted({ taskId: 't1', capability: 'x', agentId: 'a1' });
      dash.recordTaskCompleted({ taskId: 't1', agentId: 'a1', latencyMs: 100 });
      expect(dash.getAgent('a1')!.metrics.tasksCompleted).toBe(1);

      // Reconnect
      dash.recordAgentConnect({ agentId: 'a1', capabilities: ['x', 'y'] });
      expect(dash.getAgent('a1')!.metrics.tasksCompleted).toBe(1);
      expect(dash.getAgent('a1')!.capabilities).toEqual(['x', 'y']);
    });

    it('records agent disconnect', () => {
      dash.recordAgentConnect({ agentId: 'a1' });
      dash.recordAgentDisconnect('a1');
      expect(dash.getAgent('a1')!.status).toBe('disconnected');
    });

    it('disconnect on unknown agent is a no-op', () => {
      dash.recordAgentDisconnect('unknown');
      expect(dash.getAgent('unknown')).toBeUndefined();
    });

    it('heartbeat updates lastHeartbeat and restores from degraded', () => {
      dash.recordAgentConnect({ agentId: 'a1' });
      const agent = dash.getAgent('a1')!;
      agent.status = 'degraded';
      dash.recordHeartbeat('a1');
      expect(dash.getAgent('a1')!.status).toBe('connected');
    });

    it('heartbeat on unknown agent is a no-op', () => {
      dash.recordHeartbeat('unknown'); // should not throw
    });
  });

  // ---- Task Tracking ----

  describe('task tracking', () => {
    beforeEach(() => {
      dash.recordAgentConnect({ agentId: 'a1', capabilities: ['summarize'] });
    });

    it('records task routed', () => {
      dash.recordTaskRouted({ taskId: 't1', capability: 'summarize', agentId: 'a1' });
      const inFlight = dash.getInFlightTasks();
      expect(inFlight).toHaveLength(1);
      expect(inFlight[0].status).toBe('in-flight');
      expect(dash.getAgent('a1')!.metrics.tasksInFlight).toBe(1);
    });

    it('records task completed', () => {
      dash.recordTaskRouted({ taskId: 't1', capability: 'summarize', agentId: 'a1' });
      dash.recordTaskCompleted({ taskId: 't1', agentId: 'a1', latencyMs: 150 });
      expect(dash.getInFlightTasks()).toHaveLength(0);
      expect(dash.getAgent('a1')!.metrics.tasksCompleted).toBe(1);
      expect(dash.getAgent('a1')!.metrics.tasksInFlight).toBe(0);
      expect(dash.getAgent('a1')!.metrics.avgLatencyMs).toBe(150);
    });

    it('records task failed', () => {
      dash.recordTaskRouted({ taskId: 't1', capability: 'summarize', agentId: 'a1' });
      dash.recordTaskFailed({ taskId: 't1', agentId: 'a1', reason: 'timeout' });
      expect(dash.getInFlightTasks()).toHaveLength(0);
      expect(dash.getAgent('a1')!.metrics.tasksFailed).toBe(1);
    });

    it('complete without prior route still increments totals', () => {
      dash.recordTaskCompleted({ taskId: 'orphan', agentId: 'a1', latencyMs: 50 });
      const snap = dash.getSnapshot();
      expect(snap.taskSummary.totalCompleted).toBe(1);
    });

    it('fail without prior route still increments totals', () => {
      dash.recordTaskFailed({ taskId: 'orphan', agentId: 'a1' });
      const snap = dash.getSnapshot();
      expect(snap.taskSummary.totalFailed).toBe(1);
    });

    it('tasksInFlight does not go below 0', () => {
      dash.recordTaskCompleted({ taskId: 'x', agentId: 'a1', latencyMs: 10 });
      expect(dash.getAgent('a1')!.metrics.tasksInFlight).toBe(0);
    });
  });

  // ---- Metrics ----

  describe('metrics', () => {
    beforeEach(() => {
      dash.recordAgentConnect({ agentId: 'a1', capabilities: ['x'] });
    });

    it('computes avg latency', () => {
      for (let i = 1; i <= 4; i++) {
        dash.recordTaskRouted({ taskId: `t${i}`, capability: 'x', agentId: 'a1' });
        dash.recordTaskCompleted({ taskId: `t${i}`, agentId: 'a1', latencyMs: i * 100 });
      }
      // avg = (100+200+300+400)/4 = 250
      expect(dash.getAgent('a1')!.metrics.avgLatencyMs).toBe(250);
    });

    it('computes p95 latency', () => {
      for (let i = 1; i <= 100; i++) {
        dash.recordTaskRouted({ taskId: `t${i}`, capability: 'x', agentId: 'a1' });
        dash.recordTaskCompleted({ taskId: `t${i}`, agentId: 'a1', latencyMs: i });
      }
      expect(dash.getAgent('a1')!.metrics.p95LatencyMs).toBe(95);
    });

    it('computes error rate', () => {
      for (let i = 0; i < 7; i++) {
        dash.recordTaskRouted({ taskId: `ok${i}`, capability: 'x', agentId: 'a1' });
        dash.recordTaskCompleted({ taskId: `ok${i}`, agentId: 'a1', latencyMs: 10 });
      }
      for (let i = 0; i < 3; i++) {
        dash.recordTaskRouted({ taskId: `fail${i}`, capability: 'x', agentId: 'a1' });
        dash.recordTaskFailed({ taskId: `fail${i}`, agentId: 'a1' });
      }
      expect(dash.getAgent('a1')!.metrics.errorRate).toBeCloseTo(0.3);
    });

    it('caps latency window at maxLatencyWindow', () => {
      const d = new Dashboard({ maxLatencyWindow: 5 });
      d.recordAgentConnect({ agentId: 'a1', capabilities: ['x'] });
      for (let i = 0; i < 10; i++) {
        d.recordTaskRouted({ taskId: `t${i}`, capability: 'x', agentId: 'a1' });
        d.recordTaskCompleted({ taskId: `t${i}`, agentId: 'a1', latencyMs: i * 10 });
      }
      expect(d.getAgent('a1')!.metrics.latencies).toHaveLength(5);
    });
  });

  // ---- Circuit Breakers ----

  describe('circuit breakers', () => {
    it('records circuit open', () => {
      dash.recordCircuitOpen('a1', 'summarize');
      const snap = dash.getSnapshot();
      expect(snap.openCircuits).toBe(1);
      expect(snap.circuits[0].state).toBe('open');
      expect(snap.circuits[0].failureCount).toBe(1);
    });

    it('accumulates failure count', () => {
      dash.recordCircuitOpen('a1', 'summarize');
      dash.recordCircuitClose('a1', 'summarize');
      dash.recordCircuitOpen('a1', 'summarize');
      const snap = dash.getSnapshot();
      expect(snap.circuits[0].failureCount).toBe(2);
    });

    it('records circuit close', () => {
      dash.recordCircuitOpen('a1', 'summarize');
      dash.recordCircuitClose('a1', 'summarize');
      const snap = dash.getSnapshot();
      expect(snap.openCircuits).toBe(0);
      expect(snap.circuits[0].state).toBe('closed');
    });

    it('records circuit half-open', () => {
      dash.recordCircuitOpen('a1', 'summarize');
      dash.recordCircuitHalfOpen('a1', 'summarize');
      expect(dash.getSnapshot().circuits[0].state).toBe('half-open');
    });

    it('close on unknown circuit is a no-op', () => {
      dash.recordCircuitClose('a1', 'x'); // should not throw
    });

    it('half-open on unknown circuit is a no-op', () => {
      dash.recordCircuitHalfOpen('a1', 'x'); // should not throw
    });
  });

  // ---- Capability Stats ----

  describe('capability stats', () => {
    it('tracks agents per capability', () => {
      dash.recordAgentConnect({ agentId: 'a1', capabilities: ['summarize', 'classify'] });
      dash.recordAgentConnect({ agentId: 'a2', capabilities: ['summarize'] });
      const stats = dash.getCapabilityStats();
      expect(stats.get('summarize')!.agentCount).toBe(2);
      expect(stats.get('classify')!.agentCount).toBe(1);
    });

    it('tracks task counts per capability', () => {
      dash.recordAgentConnect({ agentId: 'a1', capabilities: ['x'] });
      dash.recordTaskRouted({ taskId: 't1', capability: 'x', agentId: 'a1' });
      dash.recordTaskCompleted({ taskId: 't1', agentId: 'a1', latencyMs: 100 });
      dash.recordTaskRouted({ taskId: 't2', capability: 'x', agentId: 'a1' });
      dash.recordTaskFailed({ taskId: 't2', agentId: 'a1' });
      const stats = dash.getCapabilityStats();
      expect(stats.get('x')!.tasksRouted).toBe(2);
      expect(stats.get('x')!.tasksCompleted).toBe(1);
      expect(stats.get('x')!.tasksFailed).toBe(1);
    });

    it('computes avg latency per capability', () => {
      dash.recordAgentConnect({ agentId: 'a1', capabilities: ['x'] });
      dash.recordTaskRouted({ taskId: 't1', capability: 'x', agentId: 'a1' });
      dash.recordTaskCompleted({ taskId: 't1', agentId: 'a1', latencyMs: 100 });
      dash.recordTaskRouted({ taskId: 't2', capability: 'x', agentId: 'a1' });
      dash.recordTaskCompleted({ taskId: 't2', agentId: 'a1', latencyMs: 200 });
      expect(dash.getCapabilityStats().get('x')!.avgLatencyMs).toBe(150);
    });
  });

  // ---- Alerts ----

  describe('alerts', () => {
    it('generates alert on high error rate', () => {
      dash.recordAgentConnect({ agentId: 'a1', capabilities: ['x'] });
      // Need >30% error rate
      for (let i = 0; i < 2; i++) {
        dash.recordTaskRouted({ taskId: `ok${i}`, capability: 'x', agentId: 'a1' });
        dash.recordTaskCompleted({ taskId: `ok${i}`, agentId: 'a1', latencyMs: 10 });
      }
      for (let i = 0; i < 3; i++) {
        dash.recordTaskRouted({ taskId: `fail${i}`, capability: 'x', agentId: 'a1' });
        dash.recordTaskFailed({ taskId: `fail${i}`, agentId: 'a1' });
      }
      const alerts = dash.getAlerts();
      expect(alerts.some(a => a.severity === 'warning' && a.message.includes('error rate'))).toBe(true);
    });

    it('generates alert on circuit open', () => {
      dash.recordCircuitOpen('a1', 'summarize');
      const alerts = dash.getAlerts();
      expect(alerts.some(a => a.severity === 'critical' && a.message.includes('Circuit breaker OPEN'))).toBe(true);
    });

    it('generates info alert on circuit close', () => {
      dash.recordCircuitOpen('a1', 'x');
      dash.recordCircuitClose('a1', 'x');
      const alerts = dash.getAlerts();
      expect(alerts.some(a => a.severity === 'info' && a.message.includes('closed'))).toBe(true);
    });

    it('caps alerts at maxAlerts', () => {
      const d = new Dashboard({ maxAlerts: 3 });
      for (let i = 0; i < 10; i++) {
        d.recordCircuitOpen(`a${i}`, 'x');
      }
      expect(d.getAlerts().length).toBeLessThanOrEqual(3);
    });

    it('getAlerts respects limit', () => {
      for (let i = 0; i < 10; i++) {
        dash.recordCircuitOpen(`a${i}`, 'x');
      }
      expect(dash.getAlerts(3)).toHaveLength(3);
    });
  });

  // ---- Agent Status Refresh ----

  describe('agent status refresh', () => {
    it('marks agent as degraded after degradedAfterMs', () => {
      const d = new Dashboard({ degradedAfterMs: 100, disconnectedAfterMs: 500 });
      d.recordAgentConnect({ agentId: 'a1' });
      const agent = d.getAgent('a1')!;
      // Simulate old heartbeat
      agent.lastHeartbeat = Date.now() - 200;
      d.getAgents(); // triggers refresh
      expect(d.getAgent('a1')!.status).toBe('degraded');
    });

    it('marks agent as disconnected after disconnectedAfterMs', () => {
      const d = new Dashboard({ degradedAfterMs: 100, disconnectedAfterMs: 500 });
      d.recordAgentConnect({ agentId: 'a1' });
      const agent = d.getAgent('a1')!;
      agent.lastHeartbeat = Date.now() - 600;
      d.getAgents();
      expect(d.getAgent('a1')!.status).toBe('disconnected');
    });

    it('does not re-mark already disconnected agents', () => {
      const d = new Dashboard({ degradedAfterMs: 100, disconnectedAfterMs: 500 });
      d.recordAgentConnect({ agentId: 'a1' });
      d.recordAgentDisconnect('a1');
      const alertsBefore = d.getAlerts().length;
      d.getAgents(); // should not generate another alert
      expect(d.getAlerts().length).toBe(alertsBefore);
    });
  });

  // ---- Snapshot ----

  describe('snapshot', () => {
    it('returns complete snapshot', () => {
      dash.recordAgentConnect({ agentId: 'a1', capabilities: ['x'] });
      dash.recordAgentConnect({ agentId: 'a2', capabilities: ['y'] });
      dash.recordTaskRouted({ taskId: 't1', capability: 'x', agentId: 'a1' });
      dash.recordTaskCompleted({ taskId: 't1', agentId: 'a1', latencyMs: 100 });

      const snap = dash.getSnapshot();
      expect(snap.agents).toHaveLength(2);
      expect(snap.activeAgents).toBe(2);
      expect(snap.taskSummary.totalRouted).toBe(1);
      expect(snap.taskSummary.totalCompleted).toBe(1);
      expect(snap.timestamp).toBeGreaterThan(0);
    });

    it('limits recent tasks to 50 in snapshot', () => {
      dash.recordAgentConnect({ agentId: 'a1', capabilities: ['x'] });
      for (let i = 0; i < 100; i++) {
        dash.recordTaskRouted({ taskId: `t${i}`, capability: 'x', agentId: 'a1' });
        dash.recordTaskCompleted({ taskId: `t${i}`, agentId: 'a1', latencyMs: 10 });
      }
      expect(dash.getSnapshot().recentTasks.length).toBeLessThanOrEqual(50);
    });
  });

  // ---- Events ----

  describe('events', () => {
    it('emits events on agent connect', () => {
      const events: string[] = [];
      dash.on((type) => events.push(type));
      dash.recordAgentConnect({ agentId: 'a1' });
      expect(events).toContain('agent:connect');
    });

    it('emits events on task lifecycle', () => {
      const events: string[] = [];
      dash.on((type) => events.push(type));
      dash.recordAgentConnect({ agentId: 'a1', capabilities: ['x'] });
      dash.recordTaskRouted({ taskId: 't1', capability: 'x', agentId: 'a1' });
      dash.recordTaskCompleted({ taskId: 't1', agentId: 'a1', latencyMs: 50 });
      expect(events).toContain('task:routed');
      expect(events).toContain('task:completed');
    });

    it('emits events on task failure', () => {
      const events: string[] = [];
      dash.on((type) => events.push(type));
      dash.recordAgentConnect({ agentId: 'a1', capabilities: ['x'] });
      dash.recordTaskRouted({ taskId: 't1', capability: 'x', agentId: 'a1' });
      dash.recordTaskFailed({ taskId: 't1', agentId: 'a1' });
      expect(events).toContain('task:failed');
    });

    it('unsubscribe works', () => {
      const events: string[] = [];
      const unsub = dash.on((type) => events.push(type));
      dash.recordAgentConnect({ agentId: 'a1' });
      unsub();
      dash.recordAgentConnect({ agentId: 'a2' });
      expect(events).toHaveLength(1);
    });

    it('handler errors do not propagate', () => {
      dash.on(() => { throw new Error('boom'); });
      expect(() => dash.recordAgentConnect({ agentId: 'a1' })).not.toThrow();
    });
  });

  // ---- HTML Rendering ----

  describe('renderHTML', () => {
    it('returns valid HTML with no data', () => {
      const html = dash.renderHTML();
      expect(html).toContain('<!DOCTYPE html>');
      expect(html).toContain('Tensegrity Dashboard');
      expect(html).toContain('No agents connected');
    });

    it('includes agent info in HTML', () => {
      dash.recordAgentConnect({ agentId: 'a1', name: 'TestBot', capabilities: ['summarize'] });
      const html = dash.renderHTML();
      expect(html).toContain('TestBot');
      expect(html).toContain('summarize');
    });

    it('includes circuit breaker info in HTML', () => {
      dash.recordCircuitOpen('a1', 'classify');
      const html = dash.renderHTML();
      expect(html).toContain('Circuit Breakers');
      expect(html).toContain('classify');
    });

    it('escapes HTML entities', () => {
      dash.recordAgentConnect({ agentId: 'a1', name: '<script>alert("xss")</script>' });
      const html = dash.renderHTML();
      expect(html).not.toContain('<script>alert');
      expect(html).toContain('&lt;script&gt;');
    });
  });

  // ---- toJSON ----

  describe('toJSON', () => {
    it('returns serializable object', () => {
      dash.recordAgentConnect({ agentId: 'a1', capabilities: ['x'] });
      dash.recordTaskRouted({ taskId: 't1', capability: 'x', agentId: 'a1' });
      dash.recordTaskCompleted({ taskId: 't1', agentId: 'a1', latencyMs: 100 });
      const json = dash.toJSON();
      expect(json.agents).toBeDefined();
      expect(json.taskSummary).toBeDefined();
      // capabilityStats should be a plain object, not a Map
      expect(json.capabilityStats).toBeDefined();
      expect((json.capabilityStats as Record<string, unknown>)['x']).toBeDefined();
      // Should be JSON-serializable
      expect(() => JSON.stringify(json)).not.toThrow();
    });
  });

  // ---- Reset ----

  describe('reset', () => {
    it('clears all state', () => {
      dash.recordAgentConnect({ agentId: 'a1', capabilities: ['x'] });
      dash.recordTaskRouted({ taskId: 't1', capability: 'x', agentId: 'a1' });
      dash.recordCircuitOpen('a1', 'x');
      dash.reset();
      const snap = dash.getSnapshot();
      expect(snap.agents).toHaveLength(0);
      expect(snap.circuits).toHaveLength(0);
      expect(snap.taskSummary.totalRouted).toBe(0);
      expect(snap.alerts).toHaveLength(0);
    });
  });

  // ---- Throughput ----

  describe('throughput', () => {
    it('calculates throughput per minute', () => {
      dash.recordAgentConnect({ agentId: 'a1', capabilities: ['x'] });
      for (let i = 0; i < 10; i++) {
        dash.recordTaskRouted({ taskId: `t${i}`, capability: 'x', agentId: 'a1' });
        dash.recordTaskCompleted({ taskId: `t${i}`, agentId: 'a1', latencyMs: 10 });
      }
      const snap = dash.getSnapshot();
      // All completed within the throughput window, so throughput should be 10/min
      expect(snap.taskSummary.throughputPerMin).toBe(10);
    });
  });

  // ---- Config ----

  describe('config', () => {
    it('uses defaults when no config provided', () => {
      expect(DEFAULT_CONFIG.maxRecentTasks).toBe(1000);
      expect(DEFAULT_CONFIG.degradedAfterMs).toBe(45_000);
    });

    it('merges partial config', () => {
      const d = new Dashboard({ maxRecentTasks: 5 });
      d.recordAgentConnect({ agentId: 'a1', capabilities: ['x'] });
      for (let i = 0; i < 10; i++) {
        d.recordTaskRouted({ taskId: `t${i}`, capability: 'x', agentId: 'a1' });
        d.recordTaskCompleted({ taskId: `t${i}`, agentId: 'a1', latencyMs: 10 });
      }
      // Internal recentCompletedTasks should be capped
      const snap = d.getSnapshot();
      // recentTasks in snapshot is sliced to 50, but internal storage capped at 5
      expect(snap.recentTasks.length).toBeLessThanOrEqual(5);
    });
  });

  // ---- Percentile edge cases ----

  describe('percentile', () => {
    it('p95 with empty array returns 0', () => {
      const snap = dash.getSnapshot();
      expect(snap.taskSummary.p95LatencyMs).toBe(0);
    });

    it('p95 with single value returns that value', () => {
      dash.recordAgentConnect({ agentId: 'a1', capabilities: ['x'] });
      dash.recordTaskRouted({ taskId: 't1', capability: 'x', agentId: 'a1' });
      dash.recordTaskCompleted({ taskId: 't1', agentId: 'a1', latencyMs: 42 });
      expect(dash.getSnapshot().taskSummary.p95LatencyMs).toBe(42);
    });
  });
});
