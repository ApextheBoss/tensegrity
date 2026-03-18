var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __commonJS = (cb, mod) => function __require() {
  return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));

// dist/circuit-breaker.js
var require_circuit_breaker = __commonJS({
  "dist/circuit-breaker.js"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.CircuitBreakerRegistry = exports.CircuitBreaker = void 0;
    var DEFAULT_CONFIG = {
      failureThreshold: 3,
      resetTimeoutMs: 3e4,
      // 30 seconds
      halfOpenMaxAttempts: 2,
      monitorWindowMs: 6e4
      // 1 minute window
    };
    var CircuitBreaker = class {
      agentAddress;
      state = "closed";
      failures = 0;
      halfOpenSuccesses = 0;
      lastFailureMs = 0;
      lastStateChangeMs = Date.now();
      totalRequests = 0;
      totalFailures = 0;
      failureTimestamps = [];
      config;
      constructor(agentAddress, config) {
        this.agentAddress = agentAddress;
        this.config = { ...DEFAULT_CONFIG, ...config };
      }
      /**
       * Execute a function through the circuit breaker.
       * Throws if circuit is open. Tracks success/failure for state transitions.
       */
      async execute(fn) {
        this.totalRequests++;
        if (this.state === "open") {
          if (Date.now() - this.lastStateChangeMs >= this.config.resetTimeoutMs) {
            this.transitionTo("half_open");
          } else {
            throw new Error(`Circuit OPEN for agent ${this.agentAddress}. Retry after ${this.remainingCooldownMs()}ms`);
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
        if (this.state === "half_open") {
          this.halfOpenSuccesses++;
          if (this.halfOpenSuccesses >= this.config.halfOpenMaxAttempts) {
            this.transitionTo("closed");
          }
        } else {
          this.failures = 0;
          this.failureTimestamps = [];
        }
      }
      onFailure() {
        this.totalFailures++;
        this.lastFailureMs = Date.now();
        this.failureTimestamps.push(Date.now());
        const cutoff = Date.now() - this.config.monitorWindowMs;
        this.failureTimestamps = this.failureTimestamps.filter((ts) => ts > cutoff);
        this.failures = this.failureTimestamps.length;
        if (this.state === "half_open") {
          this.transitionTo("open");
        } else if (this.failures >= this.config.failureThreshold) {
          this.transitionTo("open");
        }
      }
      transitionTo(newState) {
        this.state = newState;
        this.lastStateChangeMs = Date.now();
        if (newState === "closed") {
          this.failures = 0;
          this.halfOpenSuccesses = 0;
          this.failureTimestamps = [];
        } else if (newState === "half_open") {
          this.halfOpenSuccesses = 0;
        }
      }
      remainingCooldownMs() {
        return Math.max(0, this.config.resetTimeoutMs - (Date.now() - this.lastStateChangeMs));
      }
      getMetrics() {
        return {
          state: this.state,
          failures: this.failures,
          successes: this.halfOpenSuccesses,
          lastFailureMs: this.lastFailureMs,
          lastStateChangeMs: this.lastStateChangeMs,
          totalRequests: this.totalRequests,
          totalFailures: this.totalFailures
        };
      }
    };
    exports.CircuitBreaker = CircuitBreaker;
    var CircuitBreakerRegistry2 = class {
      breakers = /* @__PURE__ */ new Map();
      get(agentAddress, config) {
        if (!this.breakers.has(agentAddress)) {
          this.breakers.set(agentAddress, new CircuitBreaker(agentAddress, config));
        }
        return this.breakers.get(agentAddress);
      }
      getAll() {
        const metrics = /* @__PURE__ */ new Map();
        for (const [addr, breaker] of this.breakers) {
          metrics.set(addr, breaker.getMetrics());
        }
        return metrics;
      }
      getOpenCircuits() {
        return [...this.breakers.entries()].filter(([_, b]) => b.getMetrics().state === "open").map(([addr]) => addr);
      }
    };
    exports.CircuitBreakerRegistry = CircuitBreakerRegistry2;
  }
});

// cloud/server.ts
var import_circuit_breaker = __toESM(require_circuit_breaker());
import { Hono } from "hono";
import { cors } from "hono/cors";
import { serve } from "@hono/node-server";
var workspaces = /* @__PURE__ */ new Map();
var apiKeyIndex = /* @__PURE__ */ new Map();
function getWorkspaceByKey(apiKey) {
  const wsId = apiKeyIndex.get(apiKey);
  if (!wsId) return null;
  return workspaces.get(wsId) || null;
}
function generateId() {
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}
function generateApiKey() {
  return "tsg_" + Array.from(
    { length: 32 },
    () => "abcdefghijklmnopqrstuvwxyz0123456789"[Math.floor(Math.random() * 36)]
  ).join("");
}
var app = new Hono();
app.use("*", cors());
function authMiddleware(c, next) {
  const auth = c.req.header("Authorization");
  if (!auth?.startsWith("Bearer tsg_")) {
    return c.json({ error: "Missing or invalid API key. Use: Authorization: Bearer tsg_..." }, 401);
  }
  const apiKey = auth.replace("Bearer ", "");
  const ws = getWorkspaceByKey(apiKey);
  if (!ws) {
    return c.json({ error: "Invalid API key" }, 401);
  }
  c.set("workspace", ws);
  return next();
}
app.get("/", (c) => {
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

<span class="comment">// Circuit breakers, backpressure, routing \u2014 all managed</span>
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
      Built by <a href="https://x.com/ApextheBossAI">Apex</a> \xB7 
      <a href="https://github.com/ApextheBoss/tensegrity">GitHub</a> \xB7 
      <a href="https://www.npmjs.com/package/tensegrity">npm</a>
    </div>
  </div>
</body>
</html>`);
});
app.get("/api/health", (c) => {
  return c.json({
    status: "healthy",
    version: "0.1.0",
    uptime: process.uptime(),
    workspaces: workspaces.size,
    totalAgents: Array.from(workspaces.values()).reduce((sum, ws) => sum + ws.agents.size, 0)
  });
});
app.post("/api/workspaces", async (c) => {
  const body = await c.req.json().catch(() => ({}));
  const name = body.name || "My Workspace";
  const id = generateId();
  const apiKey = generateApiKey();
  const workspace = {
    id,
    apiKey,
    name,
    plan: "free",
    agents: /* @__PURE__ */ new Map(),
    breakers: new import_circuit_breaker.CircuitBreakerRegistry(),
    queues: /* @__PURE__ */ new Map(),
    events: [],
    createdAt: Date.now()
  };
  workspaces.set(id, workspace);
  apiKeyIndex.set(apiKey, id);
  return c.json({
    id,
    apiKey,
    name,
    plan: "free",
    message: "Workspace created. Use the API key in Authorization: Bearer <key> for all requests."
  }, 201);
});
app.post("/api/agents", authMiddleware, async (c) => {
  const ws = c.get("workspace");
  const body = await c.req.json().catch(() => ({}));
  if (!body.id || !body.name) {
    return c.json({ error: "id and name are required" }, 400);
  }
  const agent = {
    id: body.id,
    name: body.name,
    status: "connected",
    connectedAt: Date.now(),
    lastHeartbeat: Date.now(),
    capabilities: body.capabilities || [],
    metrics: { tasksCompleted: 0, tasksFailed: 0, avgLatencyMs: 0, uptimeMs: 0 }
  };
  ws.agents.set(agent.id, agent);
  ws.events.push({ timestamp: Date.now(), type: "agent.connected", agentId: agent.id });
  return c.json({ agent, circuitBreaker: ws.breakers.get(agent.id).getMetrics() }, 201);
});
app.post("/api/agents/:id/heartbeat", authMiddleware, async (c) => {
  const ws = c.get("workspace");
  const agentId = c.req.param("id");
  const agent = ws.agents.get(agentId);
  if (!agent) return c.json({ error: "Agent not found" }, 404);
  agent.lastHeartbeat = Date.now();
  agent.status = "connected";
  agent.metrics.uptimeMs = Date.now() - agent.connectedAt;
  const body = await c.req.json().catch(() => ({}));
  if (body.metrics) {
    Object.assign(agent.metrics, body.metrics);
  }
  return c.json({
    status: "ok",
    agent,
    circuitBreaker: ws.breakers.get(agentId).getMetrics()
  });
});
app.get("/api/agents", authMiddleware, (c) => {
  const ws = c.get("workspace");
  const now = Date.now();
  for (const agent of ws.agents.values()) {
    const sinceLast = now - agent.lastHeartbeat;
    if (sinceLast > 6e4) agent.status = "disconnected";
    else if (sinceLast > 3e4) agent.status = "degraded";
  }
  const agents = Array.from(ws.agents.values()).map((a) => ({
    ...a,
    circuitBreaker: ws.breakers.get(a.id).getMetrics()
  }));
  return c.json({ agents, total: agents.length });
});
app.delete("/api/agents/:id", authMiddleware, (c) => {
  const ws = c.get("workspace");
  const agentId = c.req.param("id");
  if (!ws.agents.has(agentId)) return c.json({ error: "Agent not found" }, 404);
  ws.agents.delete(agentId);
  ws.events.push({ timestamp: Date.now(), type: "agent.disconnected", agentId });
  return c.json({ status: "removed" });
});
app.post("/api/route", authMiddleware, async (c) => {
  const ws = c.get("workspace");
  const body = await c.req.json().catch(() => ({}));
  if (!body.capability) {
    return c.json({ error: "capability is required" }, 400);
  }
  const candidates = Array.from(ws.agents.values()).filter(
    (a) => a.status === "connected" && a.capabilities.includes(body.capability) && ws.breakers.get(a.id).getMetrics().state !== "open"
  );
  if (candidates.length === 0) {
    return c.json({ error: "No available agents for capability: " + body.capability }, 503);
  }
  const scored = candidates.map((a) => {
    const total = a.metrics.tasksCompleted + a.metrics.tasksFailed;
    const successRate = total > 0 ? a.metrics.tasksCompleted / total : 0.5;
    const latencyScore = a.metrics.avgLatencyMs > 0 ? 1e3 / a.metrics.avgLatencyMs : 1;
    return { agent: a, score: successRate * latencyScore };
  }).sort((a, b) => b.score - a.score);
  const selected = scored[0].agent;
  const taskId = generateId();
  ws.events.push({
    timestamp: Date.now(),
    type: "task.routed",
    agentId: selected.id,
    data: { taskId, capability: body.capability }
  });
  return c.json({
    taskId,
    agent: { id: selected.id, name: selected.name },
    capability: body.capability,
    circuitBreaker: ws.breakers.get(selected.id).getMetrics()
  });
});
app.post("/api/tasks/:id/complete", authMiddleware, async (c) => {
  const ws = c.get("workspace");
  const body = await c.req.json().catch(() => ({}));
  if (!body.agentId) return c.json({ error: "agentId is required" }, 400);
  const agent = ws.agents.get(body.agentId);
  if (!agent) return c.json({ error: "Agent not found" }, 404);
  agent.metrics.tasksCompleted++;
  if (body.latencyMs) {
    const total = agent.metrics.tasksCompleted + agent.metrics.tasksFailed;
    agent.metrics.avgLatencyMs = (agent.metrics.avgLatencyMs * (total - 1) + body.latencyMs) / total;
  }
  await ws.breakers.get(agent.id).execute(() => Promise.resolve());
  return c.json({ status: "recorded", metrics: agent.metrics });
});
app.post("/api/tasks/:id/fail", authMiddleware, async (c) => {
  const ws = c.get("workspace");
  const body = await c.req.json().catch(() => ({}));
  if (!body.agentId) return c.json({ error: "agentId is required" }, 400);
  const agent = ws.agents.get(body.agentId);
  if (!agent) return c.json({ error: "Agent not found" }, 404);
  agent.metrics.tasksFailed++;
  try {
    await ws.breakers.get(agent.id).execute(() => Promise.reject(new Error(body.reason || "task failed")));
  } catch (_) {
  }
  const breakerState = ws.breakers.get(agent.id).getMetrics();
  if (breakerState.state === "open") {
    ws.events.push({ timestamp: Date.now(), type: "circuit.opened", agentId: agent.id });
  }
  ws.events.push({
    timestamp: Date.now(),
    type: "task.failed",
    agentId: agent.id,
    data: { taskId: c.req.param("id"), reason: body.reason }
  });
  return c.json({ status: "recorded", metrics: agent.metrics, circuitBreaker: breakerState });
});
app.get("/api/dashboard", authMiddleware, (c) => {
  const ws = c.get("workspace");
  const now = Date.now();
  for (const agent of ws.agents.values()) {
    const sinceLast = now - agent.lastHeartbeat;
    if (sinceLast > 6e4) agent.status = "disconnected";
    else if (sinceLast > 3e4) agent.status = "degraded";
  }
  const agents = Array.from(ws.agents.values());
  const connected = agents.filter((a) => a.status === "connected").length;
  const degraded = agents.filter((a) => a.status === "degraded").length;
  const disconnected = agents.filter((a) => a.status === "disconnected").length;
  const openCircuits = ws.breakers.getOpenCircuits();
  const totalTasks = agents.reduce((s, a) => s + a.metrics.tasksCompleted + a.metrics.tasksFailed, 0);
  const totalSuccesses = agents.reduce((s, a) => s + a.metrics.tasksCompleted, 0);
  const avgLatency = agents.length > 0 ? agents.reduce((s, a) => s + a.metrics.avgLatencyMs, 0) / agents.length : 0;
  return c.json({
    workspace: { id: ws.id, name: ws.name, plan: ws.plan, createdAt: ws.createdAt },
    summary: {
      totalAgents: agents.length,
      connected,
      degraded,
      disconnected,
      openCircuits: openCircuits.length,
      totalTasks,
      successRate: totalTasks > 0 ? (totalSuccesses / totalTasks * 100).toFixed(1) + "%" : "N/A",
      avgLatencyMs: Math.round(avgLatency)
    },
    agents: agents.map((a) => ({
      ...a,
      circuitBreaker: ws.breakers.get(a.id).getMetrics()
    })),
    recentEvents: ws.events.slice(-50),
    openCircuits
  });
});
app.get("/api/events", authMiddleware, (c) => {
  const ws = c.get("workspace");
  const limit = parseInt(c.req.query("limit") || "50");
  return c.json({ events: ws.events.slice(-limit) });
});
var port = parseInt(process.env.PORT || process.env.TENSEGRITY_PORT || "4100");
serve({ fetch: app.fetch, port }, (info) => {
  console.log(`Tensegrity Cloud running on http://localhost:${info.port}`);
});
