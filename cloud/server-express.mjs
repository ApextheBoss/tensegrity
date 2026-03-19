import express from 'express';

// Inline circuit breaker (minimal version for cloud)
class CircuitBreaker {
  constructor(id, config = {}) {
    this.id = id;
    this.state = 'closed';
    this.failures = 0;
    this.halfOpenSuccesses = 0;
    this.lastStateChangeMs = Date.now();
    this.totalRequests = 0;
    this.totalFailures = 0;
    this.failureTimestamps = [];
    this.config = {
      failureThreshold: config.failureThreshold || 3,
      resetTimeoutMs: config.resetTimeoutMs || 30000,
      halfOpenMaxAttempts: config.halfOpenMaxAttempts || 2,
      monitorWindowMs: config.monitorWindowMs || 60000,
    };
  }
  async execute(fn) {
    this.totalRequests++;
    if (this.state === 'open') {
      if (Date.now() - this.lastStateChangeMs >= this.config.resetTimeoutMs) {
        this.transitionTo('half_open');
      } else {
        throw new Error(`Circuit OPEN for ${this.id}`);
      }
    }
    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    }
  }
  onSuccess() {
    if (this.state === 'half_open') {
      this.halfOpenSuccesses++;
      if (this.halfOpenSuccesses >= this.config.halfOpenMaxAttempts) this.transitionTo('closed');
    } else {
      this.failures = 0;
      this.failureTimestamps = [];
    }
  }
  onFailure() {
    this.totalFailures++;
    this.failureTimestamps.push(Date.now());
    const cutoff = Date.now() - this.config.monitorWindowMs;
    this.failureTimestamps = this.failureTimestamps.filter(ts => ts > cutoff);
    this.failures = this.failureTimestamps.length;
    if (this.state === 'half_open' || this.failures >= this.config.failureThreshold) this.transitionTo('open');
  }
  transitionTo(s) {
    this.state = s;
    this.lastStateChangeMs = Date.now();
    if (s === 'closed') { this.failures = 0; this.halfOpenSuccesses = 0; this.failureTimestamps = []; }
    if (s === 'half_open') { this.halfOpenSuccesses = 0; }
  }
  getMetrics() {
    return { state: this.state, failures: this.failures, totalRequests: this.totalRequests, totalFailures: this.totalFailures };
  }
}

class CircuitBreakerRegistry {
  constructor() { this.breakers = new Map(); }
  get(id, config) {
    if (!this.breakers.has(id)) this.breakers.set(id, new CircuitBreaker(id, config));
    return this.breakers.get(id);
  }
  getOpenCircuits() {
    return [...this.breakers.entries()].filter(([_, b]) => b.state === 'open').map(([id]) => id);
  }
}

// State
const workspaces = new Map();
const apiKeyIndex = new Map();
const generateId = () => Math.random().toString(36).slice(2) + Date.now().toString(36);
const generateApiKey = () => 'tsg_' + Array.from({ length: 32 }, () => 'abcdefghijklmnopqrstuvwxyz0123456789'[Math.floor(Math.random() * 36)]).join('');

const app = express();
app.use(express.json());
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.sendStatus(200);
  next();
});

// Auth middleware
function auth(req, res, next) {
  const h = req.headers.authorization;
  if (!h || !h.startsWith('Bearer tsg_')) return res.status(401).json({ error: 'Missing API key' });
  const ws = workspaces.get(apiKeyIndex.get(h.replace('Bearer ', '')));
  if (!ws) return res.status(401).json({ error: 'Invalid API key' });
  req.workspace = ws;
  next();
}

// Landing page
app.get('/', (req, res) => {
  const totalAgents = [...workspaces.values()].reduce((s, ws) => s + ws.agents.size, 0);
  res.send(`<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Tensegrity Cloud</title>
<style>*{margin:0;padding:0;box-sizing:border-box}body{background:#0a0a0a;color:#e0e0e0;font-family:Inter,-apple-system,sans-serif}.hero{max-width:800px;margin:0 auto;padding:80px 24px}h1{font-size:48px;font-weight:700;color:#fff;margin-bottom:16px}h1 span{color:#D4621A}.subtitle{font-size:20px;color:#888;margin-bottom:48px;line-height:1.5}.stats{display:flex;gap:32px;margin-bottom:48px}.stat{background:#1a1a1a;border:1px solid #333;border-radius:8px;padding:24px;flex:1}.stat-value{font-size:36px;font-weight:700;color:#D4621A}.stat-label{font-size:14px;color:#666;margin-top:4px}a{color:#D4621A;text-decoration:none}a:hover{text-decoration:underline}.footer{margin-top:64px;padding-top:24px;border-top:1px solid #222;color:#444;font-size:13px}.code{background:#1a1a1a;border:1px solid #333;border-radius:8px;padding:24px;margin-bottom:32px;overflow-x:auto}.code pre{color:#ccc;font-family:'JetBrains Mono',monospace;font-size:14px;line-height:1.6}</style></head>
<body><div class="hero">
<h1><span>tensegrity</span> cloud</h1>
<p class="subtitle">Hosted coordination plane for multi-agent systems.<br>Circuit breakers, backpressure, task routing, health monitoring.<br>Connect your agents. We handle the coordination.</p>
<div class="stats"><div class="stat"><div class="stat-value">${workspaces.size}</div><div class="stat-label">Workspaces</div></div><div class="stat"><div class="stat-value">${totalAgents}</div><div class="stat-label">Connected Agents</div></div><div class="stat"><div class="stat-value">35</div><div class="stat-label">Coordination Modules</div></div></div>
<div class="code"><pre>npm install tensegrity

// Create workspace → register agents → route tasks
POST /api/workspaces           → { apiKey: "tsg_..." }
POST /api/agents               → { agent, circuitBreaker }
POST /api/route                → { taskId, agent }
GET  /api/dashboard            → { summary, agents, events }</pre></div>
<div class="footer">Built by <a href="https://x.com/ApextheBossAI">Apex</a> · <a href="https://github.com/ApextheBoss/tensegrity">GitHub</a> · <a href="https://www.npmjs.com/package/tensegrity">npm</a></div>
</div></body></html>`);
});

app.get('/api/health', (req, res) => {
  res.json({ status: 'healthy', version: '0.1.0', uptime: process.uptime(), workspaces: workspaces.size, totalAgents: [...workspaces.values()].reduce((s, ws) => s + ws.agents.size, 0) });
});

app.post('/api/workspaces', (req, res) => {
  const id = generateId(), apiKey = generateApiKey(), name = req.body?.name || 'My Workspace';
  const ws = { id, apiKey, name, plan: 'free', agents: new Map(), breakers: new CircuitBreakerRegistry(), events: [], createdAt: Date.now() };
  workspaces.set(id, ws);
  apiKeyIndex.set(apiKey, id);
  res.status(201).json({ id, apiKey, name, plan: 'free', message: 'Workspace created. Use the API key in Authorization: Bearer <key> for all requests.' });
});

app.post('/api/agents', auth, (req, res) => {
  const ws = req.workspace;
  if (!req.body?.id || !req.body?.name) return res.status(400).json({ error: 'id and name required' });
  const agent = { id: req.body.id, name: req.body.name, status: 'connected', connectedAt: Date.now(), lastHeartbeat: Date.now(), capabilities: req.body.capabilities || [], metrics: { tasksCompleted: 0, tasksFailed: 0, avgLatencyMs: 0, uptimeMs: 0 } };
  ws.agents.set(agent.id, agent);
  ws.events.push({ timestamp: Date.now(), type: 'agent.connected', agentId: agent.id });
  res.status(201).json({ agent, circuitBreaker: ws.breakers.get(agent.id).getMetrics() });
});

app.post('/api/agents/:id/heartbeat', auth, (req, res) => {
  const agent = req.workspace.agents.get(req.params.id);
  if (!agent) return res.status(404).json({ error: 'Agent not found' });
  agent.lastHeartbeat = Date.now();
  agent.status = 'connected';
  if (req.body?.metrics) Object.assign(agent.metrics, req.body.metrics);
  res.json({ status: 'ok', agent, circuitBreaker: req.workspace.breakers.get(agent.id).getMetrics() });
});

app.get('/api/agents', auth, (req, res) => {
  const ws = req.workspace, now = Date.now();
  for (const a of ws.agents.values()) {
    const since = now - a.lastHeartbeat;
    if (since > 60000) a.status = 'disconnected';
    else if (since > 30000) a.status = 'degraded';
  }
  res.json({ agents: [...ws.agents.values()].map(a => ({ ...a, circuitBreaker: ws.breakers.get(a.id).getMetrics() })), total: ws.agents.size });
});

app.delete('/api/agents/:id', auth, (req, res) => {
  const ws = req.workspace;
  if (!ws.agents.has(req.params.id)) return res.status(404).json({ error: 'Not found' });
  ws.agents.delete(req.params.id);
  ws.events.push({ timestamp: Date.now(), type: 'agent.disconnected', agentId: req.params.id });
  res.json({ status: 'removed' });
});

app.post('/api/route', auth, (req, res) => {
  const ws = req.workspace;
  if (!req.body?.capability) return res.status(400).json({ error: 'capability required' });
  const candidates = [...ws.agents.values()].filter(a => a.status === 'connected' && a.capabilities.includes(req.body.capability) && ws.breakers.get(a.id).getMetrics().state !== 'open');
  if (!candidates.length) return res.status(503).json({ error: 'No available agents for: ' + req.body.capability });
  const scored = candidates.map(a => {
    const total = a.metrics.tasksCompleted + a.metrics.tasksFailed;
    const rate = total > 0 ? a.metrics.tasksCompleted / total : 0.5;
    const latency = a.metrics.avgLatencyMs > 0 ? 1000 / a.metrics.avgLatencyMs : 1;
    return { agent: a, score: rate * latency };
  }).sort((a, b) => b.score - a.score);
  const selected = scored[0].agent, taskId = generateId();
  ws.events.push({ timestamp: Date.now(), type: 'task.routed', agentId: selected.id, data: { taskId, capability: req.body.capability } });
  res.json({ taskId, agent: { id: selected.id, name: selected.name }, capability: req.body.capability });
});

app.post('/api/tasks/:id/complete', auth, async (req, res) => {
  const agent = req.workspace.agents.get(req.body?.agentId);
  if (!agent) return res.status(404).json({ error: 'Agent not found' });
  agent.metrics.tasksCompleted++;
  if (req.body.latencyMs) {
    const total = agent.metrics.tasksCompleted + agent.metrics.tasksFailed;
    agent.metrics.avgLatencyMs = ((agent.metrics.avgLatencyMs * (total - 1)) + req.body.latencyMs) / total;
  }
  await req.workspace.breakers.get(agent.id).execute(() => Promise.resolve());
  res.json({ status: 'recorded', metrics: agent.metrics });
});

app.post('/api/tasks/:id/fail', auth, async (req, res) => {
  const agent = req.workspace.agents.get(req.body?.agentId);
  if (!agent) return res.status(404).json({ error: 'Agent not found' });
  agent.metrics.tasksFailed++;
  try { await req.workspace.breakers.get(agent.id).execute(() => Promise.reject(new Error(req.body?.reason || 'failed'))); } catch (_) {}
  const breaker = req.workspace.breakers.get(agent.id).getMetrics();
  if (breaker.state === 'open') req.workspace.events.push({ timestamp: Date.now(), type: 'circuit.opened', agentId: agent.id });
  req.workspace.events.push({ timestamp: Date.now(), type: 'task.failed', agentId: agent.id, data: { taskId: req.params.id, reason: req.body?.reason } });
  res.json({ status: 'recorded', metrics: agent.metrics, circuitBreaker: breaker });
});

app.get('/api/dashboard', auth, (req, res) => {
  const ws = req.workspace, now = Date.now();
  for (const a of ws.agents.values()) {
    const since = now - a.lastHeartbeat;
    if (since > 60000) a.status = 'disconnected';
    else if (since > 30000) a.status = 'degraded';
  }
  const agents = [...ws.agents.values()];
  const totalTasks = agents.reduce((s, a) => s + a.metrics.tasksCompleted + a.metrics.tasksFailed, 0);
  const successes = agents.reduce((s, a) => s + a.metrics.tasksCompleted, 0);
  res.json({
    workspace: { id: ws.id, name: ws.name, plan: ws.plan },
    summary: { totalAgents: agents.length, connected: agents.filter(a => a.status === 'connected').length, degraded: agents.filter(a => a.status === 'degraded').length, disconnected: agents.filter(a => a.status === 'disconnected').length, openCircuits: ws.breakers.getOpenCircuits().length, totalTasks, successRate: totalTasks > 0 ? (successes / totalTasks * 100).toFixed(1) + '%' : 'N/A' },
    agents: agents.map(a => ({ ...a, circuitBreaker: ws.breakers.get(a.id).getMetrics() })),
    recentEvents: ws.events.slice(-50),
  });
});

app.get('/api/events', auth, (req, res) => {
  res.json({ events: req.workspace.events.slice(-(parseInt(req.query.limit) || 50)) });
});

const port = parseInt(process.env.PORT || '4100');
app.listen(port, () => console.log(`Tensegrity Cloud running on http://localhost:${port}`));
