/**
 * Tensegrity Cloud — Hosted coordination plane for multi-agent systems.
 * 
 * Agents connect via WebSocket, cloud handles coordination primitives,
 * dashboard shows health/routing/failures in real-time.
 * 
 * Architecture:
 * - Hono HTTP server for REST API + dashboard
 * - WebSocket for real-time agent connections
 * - In-memory state (SQLite persistence later)
 * - Per-workspace isolation
 */

import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { CircuitBreaker, CircuitBreakerRegistry } from '../src/circuit-breaker';
import { BackpressureController } from '../src/backpressure';

// Types
interface Agent {
  id: string;
  name: string;
  status: 'connected' | 'disconnected' | 'degraded';
  connectedAt: number;
  lastHeartbeat: number;
  capabilities: string[];
  metrics: {
    tasksCompleted: number;
    tasksFailed: number;
    avgLatencyMs: number;
    uptimeMs: number;
  };
}

interface Workspace {
  id: string;
  apiKey: string;
  name: string;
  plan: 'free' | 'pro' | 'enterprise';
  agents: Map<string, Agent>;
  breakers: CircuitBreakerRegistry;
  queues: Map<string, BackpressureController>;
  events: WorkspaceEvent[];
  createdAt: number;
}

interface WorkspaceEvent {
  timestamp: number;
  type: 'agent.connected' | 'agent.disconnected' | 'circuit.opened' | 'circuit.closed' | 'task.routed' | 'task.failed' | 'backpressure.activated';
  agentId?: string;
  data?: Record<string, unknown>;
}

// In-memory store
const workspaces = new Map<string, Workspace>();
const apiKeyIndex = new Map<string, string>(); // apiKey -> workspaceId

// Helper
function getWorkspaceByKey(apiKey: string): Workspace | null {
  const wsId = apiKeyIndex.get(apiKey);
  if (!wsId) return null;
  return workspaces.get(wsId) || null;
}

function generateId(): string {
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

function generateApiKey(): string {
  return 'tsg_' + Array.from({ length: 32 }, () => 
    'abcdefghijklmnopqrstuvwxyz0123456789'[Math.floor(Math.random() * 36)]
  ).join('');
}

// App
const app = new Hono();
app.use('*', cors());

// Auth middleware
function authMiddleware(c: any, next: any) {
  const auth = c.req.header('Authorization');
  if (!auth?.startsWith('Bearer tsg_')) {
    return c.json({ error: 'Missing or invalid API key. Use: Authorization: Bearer tsg_...' }, 401);
  }
  const apiKey = auth.replace('Bearer ', '');
  const ws = getWorkspaceByKey(apiKey);
  if (!ws) {
    return c.json({ error: 'Invalid API key' }, 401);
  }
  c.set('workspace', ws);
  return next();
}

// ============================================================
// Public routes
// ============================================================

app.get('/', (c) => {
  const totalAgents = Array.from(workspaces.values()).reduce((sum, ws) => sum + ws.agents.size, 0);
  const totalWorkspaces = workspaces.size;
  
  return c.html(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Tensegrity Cloud</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { background: #0a0a0a; color: #e0e0e0; font-family: 'Inter', -apple-system, sans-serif; }
    .hero { max-width: 800px; margin: 0 auto; padding: 80px 24px; }
    h1 { font-size: 48px; font-weight: 700; color: #fff; margin-bottom: 16px; }
    h1 span { color: #D4621A; }
    .subtitle { font-size: 20px; color: #888; margin-bottom: 48px; line-height: 1.5; }
    .stats { display: flex; gap: 32px; margin-bottom: 48px; }
    .stat { background: #1a1a1a; border: 1px solid #333; border-radius: 8px; padding: 24px; flex: 1; }
    .stat-value { font-size: 36px; font-weight: 700; color: #D4621A; }
    .stat-label { font-size: 14px; color: #666; margin-top: 4px; }
    .code { background: #1a1a1a; border: 1px solid #333; border-radius: 8px; padding: 24px; margin-bottom: 32px; overflow-x: auto; }
    .code pre { color: #ccc; font-family: 'JetBrains Mono', monospace; font-size: 14px; line-height: 1.6; }
    .code .comment { color: #666; }
    .code .string { color: #D4621A; }
    .code .keyword { color: #c792ea; }
    .endpoints { margin-top: 48px; }
    .endpoints h2 { font-size: 24px; color: #fff; margin-bottom: 24px; }
    .endpoint { background: #1a1a1a; border: 1px solid #333; border-radius: 8px; padding: 16px 20px; margin-bottom: 8px; display: flex; align-items: center; gap: 12px; }
    .method { font-size: 12px; font-weight: 700; padding: 4px 8px; border-radius: 4px; font-family: monospace; }
    .method.get { background: #1a3a1a; color: #4ade80; }
    .method.post { background: #3a2a1a; color: #D4621A; }
    .method.delete { background: #3a1a1a; color: #f87171; }
    .path { font-family: monospace; color: #ccc; }
    .desc { color: #666; margin-left: auto; font-size: 13px; }
    a { color: #D4621A; text-decoration: none; }
    a:hover { text-decoration: underline; }
    .footer { margin-top: 64px; padding-top: 24px; border-top: 1px solid #222; color: #444; font-size: 13px; }
  </style>
</head>
<body>
  <div class="hero">
    <h1><span>tensegrity</span> cloud</h1>
    <p class="subtitle">
      Hosted coordination plane for multi-agent systems.<br>
      Circuit breakers, backpressure, task routing, health monitoring.<br>
      Connect your agents. We handle the coordination.
    </p>
    
    <div class="stats">
      <div class="stat">
        <div class="stat-value">${totalWorkspaces}</div>
        <div class="stat-label">Workspaces</div>
      </div>
      <div class="stat">
        <div class="stat-value">${totalAgents}</div>
        <div class="stat-label">Connected Agents</div>
      </div>
      <div class="stat">
        <div class="stat-value">35</div>
        <div class="stat-label">Coordination Modules</div>
      </div>
    </div>

    <div class="code">
      <pre><span class="comment">// Connect your agents in 3 lines</span>
<span class="keyword">import</span> { TensegrityClient } <span class="keyword">from</span> <span class="string">'tensegrity/cloud'</span>;

<span class="keyword">const</span> client = <span class="keyword">new</span> TensegrityClient(<span class="string">'tsg_your_api_key'</span>);
<span class="keyword">await</span> client.register(<span class="string">'my-agent'</span>, [<span class="string">'summarize'</span>, <span class="string">'classify'</span>]);

<span class="comment">// Circuit breakers, backpressure, routing — all managed</span>
<span class="keyword">const</span> result = <span class="keyword">await</span> client.route(<span class="string">'summarize'</span>, payload);
</pre>
    </div>

    <div class="endpoints">
      <h2>API</h2>
      <div class="endpoint"><span class="method post">POST</span> <span class="path">/api/workspaces</span> <span class="desc">Create workspace & get API key</span></div>
      <div class="endpoint"><span class="method post">POST</span> <span class="path">/api/agents</span> <span class="desc">Register an agent</span></div>
      <div class="endpoint"><span class="method post">POST</span> <span class="path">/api/agents/:id/heartbeat</span> <span class="desc">Agent heartbeat</span></div>
      <div class="endpoint"><span class="method get">GET</span> <span class="path">/api/agents</span> <span class="desc">List agents + health</span></div>
      <div class="endpoint"><span class="method get">GET</span> <span class="path">/api/dashboard</span> <span class="desc">Full dashboard data</span></div>
      <div class="endpoint"><span class="method post">POST</span> <span class="path">/api/route</span> <span class="desc">Route a task to best agent</span></div>
      <div class="endpoint"><span class="method post">POST</span> <span class="path">/api/tasks/:id/complete</span> <span class="desc">Report task completion</span></div>
      <div class="endpoint"><span class="method post">POST</span> <span class="path">/api/tasks/:id/fail</span> <span class="desc">Report task failure</span></div>
      <div class="endpoint"><span class="method get">GET</span> <span class="path">/api/events</span> <span class="desc">Event stream</span></div>
      <div class="endpoint"><span class="method get">GET</span> <span class="path">/api/health</span> <span class="desc">Service health</span></div>
    </div>

    <div class="footer">
      Built by <a href="https://x.com/ApextheBossAI">Apex</a> · 
      <a href="https://github.com/ApextheBoss/tensegrity">GitHub</a> · 
      <a href="https://www.npmjs.com/package/tensegrity">npm</a>
    </div>
  </div>
</body>
</html>`);
});

app.get('/api/health', (c) => {
  return c.json({
    status: 'healthy',
    version: '0.1.0',
    uptime: process.uptime(),
    workspaces: workspaces.size,
    totalAgents: Array.from(workspaces.values()).reduce((sum, ws) => sum + ws.agents.size, 0),
  });
});

// ============================================================
// Workspace management (no auth required for creation)
// ============================================================

app.post('/api/workspaces', async (c) => {
  const body = await c.req.json().catch(() => ({}));
  const name = body.name || 'My Workspace';
  
  const id = generateId();
  const apiKey = generateApiKey();
  
  const workspace: Workspace = {
    id,
    apiKey,
    name,
    plan: 'free',
    agents: new Map(),
    breakers: new CircuitBreakerRegistry(),
    queues: new Map(),
    events: [],
    createdAt: Date.now(),
  };
  
  workspaces.set(id, workspace);
  apiKeyIndex.set(apiKey, id);
  
  return c.json({
    id,
    apiKey,
    name,
    plan: 'free',
    message: 'Workspace created. Use the API key in Authorization: Bearer <key> for all requests.',
  }, 201);
});

// ============================================================
// Authenticated routes
// ============================================================

// Register agent
app.post('/api/agents', authMiddleware, async (c) => {
  const ws: Workspace = c.get('workspace');
  const body = await c.req.json().catch(() => ({}));
  
  if (!body.id || !body.name) {
    return c.json({ error: 'id and name are required' }, 400);
  }
  
  const agent: Agent = {
    id: body.id,
    name: body.name,
    status: 'connected',
    connectedAt: Date.now(),
    lastHeartbeat: Date.now(),
    capabilities: body.capabilities || [],
    metrics: { tasksCompleted: 0, tasksFailed: 0, avgLatencyMs: 0, uptimeMs: 0 },
  };
  
  ws.agents.set(agent.id, agent);
  ws.events.push({ timestamp: Date.now(), type: 'agent.connected', agentId: agent.id });
  
  return c.json({ agent, circuitBreaker: ws.breakers.get(agent.id).getMetrics() }, 201);
});

// Agent heartbeat
app.post('/api/agents/:id/heartbeat', authMiddleware, async (c) => {
  const ws: Workspace = c.get('workspace');
  const agentId = c.req.param('id');
  const agent = ws.agents.get(agentId);
  
  if (!agent) return c.json({ error: 'Agent not found' }, 404);
  
  agent.lastHeartbeat = Date.now();
  agent.status = 'connected';
  agent.metrics.uptimeMs = Date.now() - agent.connectedAt;
  
  const body = await c.req.json().catch(() => ({}));
  if (body.metrics) {
    Object.assign(agent.metrics, body.metrics);
  }
  
  return c.json({ 
    status: 'ok', 
    agent,
    circuitBreaker: ws.breakers.get(agentId).getMetrics(),
  });
});

// List agents
app.get('/api/agents', authMiddleware, (c) => {
  const ws: Workspace = c.get('workspace');
  const now = Date.now();
  
  // Mark agents as degraded/disconnected based on heartbeat
  for (const agent of ws.agents.values()) {
    const sinceLast = now - agent.lastHeartbeat;
    if (sinceLast > 60000) agent.status = 'disconnected';
    else if (sinceLast > 30000) agent.status = 'degraded';
  }
  
  const agents = Array.from(ws.agents.values()).map(a => ({
    ...a,
    circuitBreaker: ws.breakers.get(a.id).getMetrics(),
  }));
  
  return c.json({ agents, total: agents.length });
});

// Remove agent
app.delete('/api/agents/:id', authMiddleware, (c) => {
  const ws: Workspace = c.get('workspace');
  const agentId = c.req.param('id');
  
  if (!ws.agents.has(agentId)) return c.json({ error: 'Agent not found' }, 404);
  
  ws.agents.delete(agentId);
  ws.events.push({ timestamp: Date.now(), type: 'agent.disconnected', agentId });
  
  return c.json({ status: 'removed' });
});

// Route a task to best agent
app.post('/api/route', authMiddleware, async (c) => {
  const ws: Workspace = c.get('workspace');
  const body = await c.req.json().catch(() => ({}));
  
  if (!body.capability) {
    return c.json({ error: 'capability is required' }, 400);
  }
  
  // Find connected agents with the requested capability
  const candidates = Array.from(ws.agents.values()).filter(a => 
    a.status === 'connected' && 
    a.capabilities.includes(body.capability) &&
    ws.breakers.get(a.id).getMetrics().state !== 'open'
  );
  
  if (candidates.length === 0) {
    return c.json({ error: 'No available agents for capability: ' + body.capability }, 503);
  }
  
  // Score by: success rate * (1/latency) — simple reputation routing
  const scored = candidates.map(a => {
    const total = a.metrics.tasksCompleted + a.metrics.tasksFailed;
    const successRate = total > 0 ? a.metrics.tasksCompleted / total : 0.5;
    const latencyScore = a.metrics.avgLatencyMs > 0 ? 1000 / a.metrics.avgLatencyMs : 1;
    return { agent: a, score: successRate * latencyScore };
  }).sort((a, b) => b.score - a.score);
  
  const selected = scored[0].agent;
  const taskId = generateId();
  
  ws.events.push({ 
    timestamp: Date.now(), type: 'task.routed', agentId: selected.id,
    data: { taskId, capability: body.capability }
  });
  
  return c.json({
    taskId,
    agent: { id: selected.id, name: selected.name },
    capability: body.capability,
    circuitBreaker: ws.breakers.get(selected.id).getMetrics(),
  });
});

// Report task completion
app.post('/api/tasks/:id/complete', authMiddleware, async (c) => {
  const ws: Workspace = c.get('workspace');
  const body = await c.req.json().catch(() => ({}));
  
  if (!body.agentId) return c.json({ error: 'agentId is required' }, 400);
  
  const agent = ws.agents.get(body.agentId);
  if (!agent) return c.json({ error: 'Agent not found' }, 404);
  
  agent.metrics.tasksCompleted++;
  if (body.latencyMs) {
    const total = agent.metrics.tasksCompleted + agent.metrics.tasksFailed;
    agent.metrics.avgLatencyMs = ((agent.metrics.avgLatencyMs * (total - 1)) + body.latencyMs) / total;
  }
  
  // Record success in circuit breaker
  await ws.breakers.get(agent.id).execute(() => Promise.resolve());
  
  return c.json({ status: 'recorded', metrics: agent.metrics });
});

// Report task failure
app.post('/api/tasks/:id/fail', authMiddleware, async (c) => {
  const ws: Workspace = c.get('workspace');
  const body = await c.req.json().catch(() => ({}));
  
  if (!body.agentId) return c.json({ error: 'agentId is required' }, 400);
  
  const agent = ws.agents.get(body.agentId);
  if (!agent) return c.json({ error: 'Agent not found' }, 404);
  
  agent.metrics.tasksFailed++;
  
  // Record failure in circuit breaker
  try {
    await ws.breakers.get(agent.id).execute(() => Promise.reject(new Error(body.reason || 'task failed')));
  } catch (_) {}
  
  const breakerState = ws.breakers.get(agent.id).getMetrics();
  if (breakerState.state === 'open') {
    ws.events.push({ timestamp: Date.now(), type: 'circuit.opened', agentId: agent.id });
  }
  
  ws.events.push({ 
    timestamp: Date.now(), type: 'task.failed', agentId: agent.id,
    data: { taskId: c.req.param('id'), reason: body.reason }
  });
  
  return c.json({ status: 'recorded', metrics: agent.metrics, circuitBreaker: breakerState });
});

// Dashboard data
app.get('/api/dashboard', authMiddleware, (c) => {
  const ws: Workspace = c.get('workspace');
  const now = Date.now();
  
  // Update agent statuses
  for (const agent of ws.agents.values()) {
    const sinceLast = now - agent.lastHeartbeat;
    if (sinceLast > 60000) agent.status = 'disconnected';
    else if (sinceLast > 30000) agent.status = 'degraded';
  }
  
  const agents = Array.from(ws.agents.values());
  const connected = agents.filter(a => a.status === 'connected').length;
  const degraded = agents.filter(a => a.status === 'degraded').length;
  const disconnected = agents.filter(a => a.status === 'disconnected').length;
  const openCircuits = ws.breakers.getOpenCircuits();
  
  const totalTasks = agents.reduce((s, a) => s + a.metrics.tasksCompleted + a.metrics.tasksFailed, 0);
  const totalSuccesses = agents.reduce((s, a) => s + a.metrics.tasksCompleted, 0);
  const avgLatency = agents.length > 0
    ? agents.reduce((s, a) => s + a.metrics.avgLatencyMs, 0) / agents.length
    : 0;
  
  return c.json({
    workspace: { id: ws.id, name: ws.name, plan: ws.plan, createdAt: ws.createdAt },
    summary: {
      totalAgents: agents.length,
      connected,
      degraded,
      disconnected,
      openCircuits: openCircuits.length,
      totalTasks,
      successRate: totalTasks > 0 ? (totalSuccesses / totalTasks * 100).toFixed(1) + '%' : 'N/A',
      avgLatencyMs: Math.round(avgLatency),
    },
    agents: agents.map(a => ({
      ...a,
      circuitBreaker: ws.breakers.get(a.id).getMetrics(),
    })),
    recentEvents: ws.events.slice(-50),
    openCircuits,
  });
});

// Event stream (last N events)
app.get('/api/events', authMiddleware, (c) => {
  const ws: Workspace = c.get('workspace');
  const limit = parseInt(c.req.query('limit') || '50');
  return c.json({ events: ws.events.slice(-limit) });
});

// ============================================================
// Start
// ============================================================

const port = parseInt(process.env.TENSEGRITY_PORT || '4100');

import { serve } from '@hono/node-server';

serve({ fetch: app.fetch, port }, (info) => {
  console.log(`Tensegrity Cloud running on http://localhost:${info.port}`);
});
