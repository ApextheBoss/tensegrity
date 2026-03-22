"use strict";
/**
 * Tensegrity Cloud — Hosted coordination plane for multi-agent systems.
 *
 * Pure Node.js HTTP server (zero framework dependencies).
 * Agents connect via REST API, cloud handles coordination primitives.
 */
Object.defineProperty(exports, "__esModule", { value: true });
const node_http_1 = require("node:http");
// ============================================================
// In-memory store
// ============================================================
const workspaces = new Map();
const apiKeyIndex = new Map();
function generateId() {
    return Math.random().toString(36).slice(2) + Date.now().toString(36);
}
function generateApiKey() {
    return 'tsg_' + Array.from({ length: 32 }, () => 'abcdefghijklmnopqrstuvwxyz0123456789'[Math.floor(Math.random() * 36)]).join('');
}
function getWorkspaceByKey(apiKey) {
    const wsId = apiKeyIndex.get(apiKey);
    return wsId ? workspaces.get(wsId) ?? null : null;
}
// ============================================================
// HTTP helpers
// ============================================================
function readBody(req) {
    return new Promise((resolve) => {
        if (req.complete) {
            resolve('');
            return;
        }
        const chunks = [];
        const timeout = setTimeout(() => resolve(Buffer.concat(chunks).toString()), 3000);
        req.on('data', (c) => chunks.push(c));
        req.on('end', () => { clearTimeout(timeout); resolve(Buffer.concat(chunks).toString()); });
        req.on('error', () => { clearTimeout(timeout); resolve(Buffer.concat(chunks).toString()); });
    });
}
function safeJson(str) {
    try {
        return JSON.parse(str);
    }
    catch {
        return {};
    }
}
function json(res, data, status = 200) {
    const body = JSON.stringify(data);
    res.writeHead(status, {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
    });
    res.end(body);
}
function html(res, body) {
    res.writeHead(200, {
        'Content-Type': 'text/html; charset=utf-8',
        'Access-Control-Allow-Origin': '*',
    });
    res.end(body);
}
function parseRoute(url) {
    const idx = url.indexOf('?');
    const path = idx >= 0 ? url.slice(0, idx) : url;
    const query = new URLSearchParams(idx >= 0 ? url.slice(idx + 1) : '');
    return { path, query };
}
function extractApiKey(req) {
    const auth = req.headers.authorization;
    if (auth?.startsWith('Bearer tsg_'))
        return auth.slice(7);
    return null;
}
function refreshAgentStatuses(ws) {
    const now = Date.now();
    for (const agent of ws.agents.values()) {
        if (agent.status === 'disconnected')
            continue;
        const elapsed = now - agent.lastHeartbeat;
        if (elapsed > 60000)
            agent.status = 'disconnected';
        else if (elapsed > 30000)
            agent.status = 'degraded';
    }
}
function esc(s) {
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
// ============================================================
// Route handler
// ============================================================
async function handleRequest(req, res) {
    const { path, query } = parseRoute(req.url || '/');
    const method = req.method || 'GET';
    // CORS preflight
    if (method === 'OPTIONS') {
        res.writeHead(204, {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
            'Access-Control-Max-Age': '86400',
        });
        res.end();
        return;
    }
    // ---- Public routes ----
    if (path === '/' && method === 'GET') {
        return html(res, renderLandingPage());
    }
    if (path === '/api/health' && method === 'GET') {
        const totalAgents = Array.from(workspaces.values()).reduce((s, ws) => s + ws.agents.size, 0);
        return json(res, {
            status: 'healthy',
            version: '0.2.0',
            uptime: process.uptime(),
            workspaces: workspaces.size,
            totalAgents,
        });
    }
    if (path === '/api/workspaces' && method === 'POST') {
        const body = safeJson(await readBody(req));
        const id = generateId();
        const apiKey = generateApiKey();
        const workspace = {
            id, apiKey, name: body.name || 'My Workspace', plan: 'free',
            agents: new Map(), tasks: new Map(), events: [], createdAt: Date.now(),
        };
        workspaces.set(id, workspace);
        apiKeyIndex.set(apiKey, id);
        return json(res, { id, apiKey, name: workspace.name, plan: 'free' }, 201);
    }
    // ---- Authenticated routes ----
    const apiKey = extractApiKey(req);
    if (!apiKey)
        return json(res, { error: 'Missing API key' }, 401);
    const ws = getWorkspaceByKey(apiKey);
    if (!ws)
        return json(res, { error: 'Invalid API key' }, 401);
    // Register agent
    if (path === '/api/agents' && method === 'POST') {
        const body = safeJson(await readBody(req));
        if (!body.id || !body.name)
            return json(res, { error: 'id and name required' }, 400);
        const agent = {
            id: body.id, name: body.name, status: 'connected',
            connectedAt: Date.now(), lastHeartbeat: Date.now(),
            capabilities: body.capabilities || [],
            metrics: { tasksCompleted: 0, tasksFailed: 0, avgLatencyMs: 0, uptimeMs: 0 },
        };
        ws.agents.set(agent.id, agent);
        ws.events.push({ timestamp: Date.now(), type: 'agent.connected', agentId: agent.id });
        return json(res, { agent }, 201);
    }
    // List agents
    if (path === '/api/agents' && method === 'GET') {
        refreshAgentStatuses(ws);
        return json(res, { agents: Array.from(ws.agents.values()), total: ws.agents.size });
    }
    // Agent heartbeat: /api/agents/:id/heartbeat
    const heartbeatMatch = path.match(/^\/api\/agents\/([^/]+)\/heartbeat$/);
    if (heartbeatMatch && method === 'POST') {
        const agent = ws.agents.get(heartbeatMatch[1]);
        if (!agent)
            return json(res, { error: 'Agent not found' }, 404);
        agent.lastHeartbeat = Date.now();
        agent.status = 'connected';
        agent.metrics.uptimeMs = Date.now() - agent.connectedAt;
        const body = safeJson(await readBody(req));
        if (body.metrics)
            Object.assign(agent.metrics, body.metrics);
        return json(res, { status: 'ok', agent });
    }
    // Delete agent: /api/agents/:id
    const agentDeleteMatch = path.match(/^\/api\/agents\/([^/]+)$/);
    if (agentDeleteMatch && method === 'DELETE') {
        if (!ws.agents.has(agentDeleteMatch[1]))
            return json(res, { error: 'Agent not found' }, 404);
        ws.agents.delete(agentDeleteMatch[1]);
        ws.events.push({ timestamp: Date.now(), type: 'agent.disconnected', agentId: agentDeleteMatch[1] });
        return json(res, { status: 'removed' });
    }
    // Route task
    if (path === '/api/route' && method === 'POST') {
        const body = safeJson(await readBody(req));
        if (!body.capability)
            return json(res, { error: 'capability required' }, 400);
        refreshAgentStatuses(ws);
        const candidates = Array.from(ws.agents.values()).filter(a => a.status === 'connected' && a.capabilities.includes(body.capability));
        if (candidates.length === 0)
            return json(res, { error: 'No available agents for: ' + body.capability }, 503);
        // Score: success rate * inverse latency
        const scored = candidates.map(a => {
            const total = a.metrics.tasksCompleted + a.metrics.tasksFailed;
            const successRate = total > 0 ? a.metrics.tasksCompleted / total : 0.5;
            const latencyScore = a.metrics.avgLatencyMs > 0 ? 1000 / a.metrics.avgLatencyMs : 1;
            return { agent: a, score: successRate * latencyScore };
        }).sort((a, b) => b.score - a.score);
        const selected = scored[0].agent;
        const taskId = generateId();
        const task = {
            taskId, capability: body.capability, agentId: selected.id,
            routedAt: Date.now(), status: 'in-flight',
        };
        ws.tasks.set(taskId, task);
        ws.events.push({ timestamp: Date.now(), type: 'task.routed', agentId: selected.id, data: { taskId, capability: body.capability } });
        return json(res, { taskId, agent: { id: selected.id, name: selected.name }, capability: body.capability });
    }
    // Task complete: /api/tasks/:id/complete
    const completeMatch = path.match(/^\/api\/tasks\/([^/]+)\/complete$/);
    if (completeMatch && method === 'POST') {
        const body = safeJson(await readBody(req));
        if (!body.agentId)
            return json(res, { error: 'agentId required' }, 400);
        const agent = ws.agents.get(body.agentId);
        if (!agent)
            return json(res, { error: 'Agent not found' }, 404);
        agent.metrics.tasksCompleted++;
        if (body.latencyMs) {
            const total = agent.metrics.tasksCompleted + agent.metrics.tasksFailed;
            agent.metrics.avgLatencyMs = ((agent.metrics.avgLatencyMs * (total - 1)) + body.latencyMs) / total;
        }
        const task = ws.tasks.get(completeMatch[1]);
        if (task) {
            task.status = 'completed';
            task.latencyMs = body.latencyMs;
        }
        return json(res, { status: 'recorded', metrics: agent.metrics });
    }
    // Task fail: /api/tasks/:id/fail
    const failMatch = path.match(/^\/api\/tasks\/([^/]+)\/fail$/);
    if (failMatch && method === 'POST') {
        const body = safeJson(await readBody(req));
        if (!body.agentId)
            return json(res, { error: 'agentId required' }, 400);
        const agent = ws.agents.get(body.agentId);
        if (!agent)
            return json(res, { error: 'Agent not found' }, 404);
        agent.metrics.tasksFailed++;
        const task = ws.tasks.get(failMatch[1]);
        if (task)
            task.status = 'failed';
        ws.events.push({ timestamp: Date.now(), type: 'task.failed', agentId: agent.id, data: { taskId: failMatch[1], reason: body.reason } });
        return json(res, { status: 'recorded', metrics: agent.metrics });
    }
    // Dashboard data
    if (path === '/api/dashboard' && method === 'GET') {
        refreshAgentStatuses(ws);
        const agents = Array.from(ws.agents.values());
        const connected = agents.filter(a => a.status === 'connected').length;
        const degraded = agents.filter(a => a.status === 'degraded').length;
        const disconnected = agents.filter(a => a.status === 'disconnected').length;
        const totalTasks = agents.reduce((s, a) => s + a.metrics.tasksCompleted + a.metrics.tasksFailed, 0);
        const totalSuccesses = agents.reduce((s, a) => s + a.metrics.tasksCompleted, 0);
        const avgLatency = agents.length > 0 ? agents.reduce((s, a) => s + a.metrics.avgLatencyMs, 0) / agents.length : 0;
        return json(res, {
            workspace: { id: ws.id, name: ws.name, plan: ws.plan, createdAt: ws.createdAt },
            summary: { totalAgents: agents.length, connected, degraded, disconnected, totalTasks, successRate: totalTasks > 0 ? (totalSuccesses / totalTasks * 100).toFixed(1) + '%' : 'N/A', avgLatencyMs: Math.round(avgLatency) },
            agents,
            recentEvents: ws.events.slice(-50),
        });
    }
    // Events
    if (path === '/api/events' && method === 'GET') {
        const limit = parseInt(query.get('limit') || '50');
        return json(res, { events: ws.events.slice(-limit) });
    }
    // 404
    return json(res, { error: 'Not found' }, 404);
}
// ============================================================
// Landing page
// ============================================================
function renderLandingPage() {
    const totalAgents = Array.from(workspaces.values()).reduce((s, ws) => s + ws.agents.size, 0);
    return `<!DOCTYPE html>
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
    .stat { background: #1a1a1a; border: 1px solid #333; border-radius: 8px; padding: 24px; flex: 1; text-align: center; }
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
      <div class="stat"><div class="stat-value">${workspaces.size}</div><div class="stat-label">Workspaces</div></div>
      <div class="stat"><div class="stat-value">${totalAgents}</div><div class="stat-label">Connected Agents</div></div>
      <div class="stat"><div class="stat-value">35</div><div class="stat-label">Coordination Modules</div></div>
    </div>
    <div class="code"><pre><span class="comment">// Connect your agents in 3 lines</span>
<span class="keyword">import</span> { TensegrityClient } <span class="keyword">from</span> <span class="string">'tensegrity'</span>;

<span class="keyword">const</span> client = <span class="keyword">new</span> TensegrityClient({
  url: <span class="string">'wss://tensegrity.vibekit.bot'</span>,
  apiKey: <span class="string">'tsg_your_api_key'</span>,
  agentId: <span class="string">'my-agent'</span>,
});
<span class="keyword">await</span> client.connect();
<span class="keyword">await</span> client.register([<span class="string">'summarize'</span>, <span class="string">'classify'</span>]);</pre></div>
    <div class="endpoints">
      <h2>API</h2>
      <div class="endpoint"><span class="method post">POST</span> <span class="path">/api/workspaces</span> <span class="desc">Create workspace &amp; get API key</span></div>
      <div class="endpoint"><span class="method post">POST</span> <span class="path">/api/agents</span> <span class="desc">Register an agent</span></div>
      <div class="endpoint"><span class="method post">POST</span> <span class="path">/api/agents/:id/heartbeat</span> <span class="desc">Agent heartbeat</span></div>
      <div class="endpoint"><span class="method get">GET</span> <span class="path">/api/agents</span> <span class="desc">List agents + health</span></div>
      <div class="endpoint"><span class="method get">GET</span> <span class="path">/api/dashboard</span> <span class="desc">Full dashboard data</span></div>
      <div class="endpoint"><span class="method post">POST</span> <span class="path">/api/route</span> <span class="desc">Route task to best agent</span></div>
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
</html>`;
}
// ============================================================
// Start server
// ============================================================
const port = parseInt(process.env.PORT || process.env.TENSEGRITY_PORT || '4003') || 4003;
const server = (0, node_http_1.createServer)(async (req, res) => {
    try {
        await handleRequest(req, res);
    }
    catch (err) {
        console.error('Request error:', err?.message);
        if (!res.headersSent) {
            json(res, { error: 'Internal server error' }, 500);
        }
    }
});
process.on('uncaughtException', (err) => {
    console.error('Uncaught exception:', err.message);
});
process.on('unhandledRejection', (err) => {
    console.error('Unhandled rejection:', err?.message || err);
});
server.listen(port, '0.0.0.0', () => {
    console.log(`Tensegrity Cloud running on http://0.0.0.0:${port}`);
});
