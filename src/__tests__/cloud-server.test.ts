/**
 * Tests for Tensegrity Cloud Server
 * 
 * Spins up the server on a random port and tests all API endpoints.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createServer, IncomingMessage, ServerResponse, Server } from 'node:http';

// ============================================================
// Import server logic by re-implementing the core (since server.ts
// is a standalone script, we test by HTTP against it)
// ============================================================

let server: Server;
let port: number;
let baseUrl: string;

// We'll start the server via child_process
import { spawn, ChildProcess } from 'node:child_process';
import { join } from 'node:path';

let child: ChildProcess;

function findFreePort(): Promise<number> {
  return new Promise((resolve) => {
    const srv = createServer();
    srv.listen(0, () => {
      const addr = srv.address();
      const p = typeof addr === 'object' && addr ? addr.port : 0;
      srv.close(() => resolve(p));
    });
  });
}

async function fetch(path: string, opts: { method?: string; headers?: Record<string, string>; body?: unknown } = {}): Promise<{ status: number; data: any; text: string }> {
  const url = new URL(path, baseUrl);
  const method = opts.method || 'GET';
  
  return new Promise((resolve, reject) => {
    const reqModule = require('node:http');
    const req = reqModule.request(url.toString(), {
      method,
      headers: {
        'Content-Type': 'application/json',
        ...opts.headers,
      },
    }, (res: IncomingMessage) => {
      const chunks: Buffer[] = [];
      res.on('data', (c: Buffer) => chunks.push(c));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString();
        let data: any;
        try { data = JSON.parse(text); } catch { data = text; }
        resolve({ status: res.statusCode || 0, data, text });
      });
      res.on('error', reject);
    });
    req.on('error', reject);
    if (opts.body) req.write(JSON.stringify(opts.body));
    req.end();
  });
}

beforeAll(async () => {
  port = await findFreePort();
  baseUrl = `http://127.0.0.1:${port}`;
  
  const serverPath = join(__dirname, '../../cloud/dist/server.js');
  
  child = spawn('node', [serverPath], {
    env: { ...process.env, PORT: String(port) },
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  
  // Wait for server to be ready
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('Server startup timeout')), 5000);
    child.stdout?.on('data', (data: Buffer) => {
      if (data.toString().includes('running on')) {
        clearTimeout(timeout);
        resolve();
      }
    });
    child.stderr?.on('data', (data: Buffer) => {
      console.error('Server stderr:', data.toString());
    });
    child.on('error', (err) => {
      clearTimeout(timeout);
      reject(err);
    });
  });
}, 10000);

afterAll(() => {
  if (child) child.kill();
});

// ============================================================
// Tests
// ============================================================

describe('Cloud Server', () => {
  describe('Public routes', () => {
    it('GET / returns landing page HTML', async () => {
      const res = await fetch('/');
      expect(res.status).toBe(200);
      expect(res.text).toContain('tensegrity');
      expect(res.text).toContain('<!DOCTYPE html>');
    });

    it('GET /api/health returns healthy status', async () => {
      const res = await fetch('/api/health');
      expect(res.status).toBe(200);
      expect(res.data.status).toBe('healthy');
      expect(res.data.version).toBe('0.2.0');
      expect(typeof res.data.uptime).toBe('number');
    });

    it('POST /api/workspaces creates workspace with API key', async () => {
      const res = await fetch('/api/workspaces', {
        method: 'POST',
        body: { name: 'Test Workspace' },
      });
      expect(res.status).toBe(201);
      expect(res.data.apiKey).toMatch(/^tsg_/);
      expect(res.data.name).toBe('Test Workspace');
      expect(res.data.plan).toBe('free');
    });
  });

  describe('Auth', () => {
    it('rejects requests without API key', async () => {
      const res = await fetch('/api/agents');
      expect(res.status).toBe(401);
    });

    it('rejects requests with invalid API key', async () => {
      const res = await fetch('/api/agents', {
        headers: { Authorization: 'Bearer tsg_invalid' },
      });
      expect(res.status).toBe(401);
    });
  });

  describe('Agent lifecycle', () => {
    let apiKey: string;

    beforeAll(async () => {
      const ws = await fetch('/api/workspaces', { method: 'POST', body: { name: 'Agent Test' } });
      apiKey = ws.data.apiKey;
    });

    const auth = () => ({ Authorization: `Bearer ${apiKey}` });

    it('registers an agent', async () => {
      const res = await fetch('/api/agents', {
        method: 'POST',
        headers: auth(),
        body: { id: 'agent-1', name: 'Summarizer', capabilities: ['summarize', 'classify'] },
      });
      expect(res.status).toBe(201);
      expect(res.data.agent.id).toBe('agent-1');
      expect(res.data.agent.status).toBe('connected');
      expect(res.data.agent.capabilities).toEqual(['summarize', 'classify']);
    });

    it('lists agents', async () => {
      const res = await fetch('/api/agents', { headers: auth() });
      expect(res.status).toBe(200);
      expect(res.data.agents.length).toBe(1);
      expect(res.data.agents[0].id).toBe('agent-1');
    });

    it('heartbeat updates agent', async () => {
      const res = await fetch('/api/agents/agent-1/heartbeat', {
        method: 'POST',
        headers: auth(),
        body: {},
      });
      expect(res.status).toBe(200);
      expect(res.data.status).toBe('ok');
    });

    it('heartbeat 404s for unknown agent', async () => {
      const res = await fetch('/api/agents/unknown/heartbeat', {
        method: 'POST',
        headers: auth(),
        body: {},
      });
      expect(res.status).toBe(404);
    });

    it('deletes an agent', async () => {
      // Register another to delete
      await fetch('/api/agents', {
        method: 'POST',
        headers: auth(),
        body: { id: 'agent-del', name: 'Deleteme' },
      });
      const res = await fetch('/api/agents/agent-del', {
        method: 'DELETE',
        headers: auth(),
      });
      expect(res.status).toBe(200);
      expect(res.data.status).toBe('removed');
    });
  });

  describe('Task routing', () => {
    let apiKey: string;

    beforeAll(async () => {
      const ws = await fetch('/api/workspaces', { method: 'POST', body: { name: 'Routing Test' } });
      apiKey = ws.data.apiKey;
      await fetch('/api/agents', {
        method: 'POST',
        headers: { Authorization: `Bearer ${apiKey}` },
        body: { id: 'r1', name: 'Router1', capabilities: ['translate'] },
      });
    });

    const auth = () => ({ Authorization: `Bearer ${apiKey}` });

    it('routes task to available agent', async () => {
      const res = await fetch('/api/route', {
        method: 'POST',
        headers: auth(),
        body: { capability: 'translate' },
      });
      expect(res.status).toBe(200);
      expect(res.data.taskId).toBeTruthy();
      expect(res.data.agent.id).toBe('r1');
      expect(res.data.capability).toBe('translate');
    });

    it('returns 503 when no agents for capability', async () => {
      const res = await fetch('/api/route', {
        method: 'POST',
        headers: auth(),
        body: { capability: 'nonexistent' },
      });
      expect(res.status).toBe(503);
    });

    it('returns 400 without capability', async () => {
      const res = await fetch('/api/route', {
        method: 'POST',
        headers: auth(),
        body: {},
      });
      expect(res.status).toBe(400);
    });

    it('completes a task', async () => {
      const route = await fetch('/api/route', {
        method: 'POST',
        headers: auth(),
        body: { capability: 'translate' },
      });
      const res = await fetch(`/api/tasks/${route.data.taskId}/complete`, {
        method: 'POST',
        headers: auth(),
        body: { agentId: 'r1', latencyMs: 150 },
      });
      expect(res.status).toBe(200);
      expect(res.data.metrics.tasksCompleted).toBeGreaterThan(0);
    });

    it('fails a task', async () => {
      const route = await fetch('/api/route', {
        method: 'POST',
        headers: auth(),
        body: { capability: 'translate' },
      });
      const res = await fetch(`/api/tasks/${route.data.taskId}/fail`, {
        method: 'POST',
        headers: auth(),
        body: { agentId: 'r1', reason: 'timeout' },
      });
      expect(res.status).toBe(200);
      expect(res.data.metrics.tasksFailed).toBeGreaterThan(0);
    });
  });

  describe('Dashboard & Events', () => {
    let apiKey: string;

    beforeAll(async () => {
      const ws = await fetch('/api/workspaces', { method: 'POST', body: { name: 'Dashboard Test' } });
      apiKey = ws.data.apiKey;
      await fetch('/api/agents', {
        method: 'POST',
        headers: { Authorization: `Bearer ${apiKey}` },
        body: { id: 'd1', name: 'DashAgent', capabilities: ['analyze'] },
      });
    });

    const auth = () => ({ Authorization: `Bearer ${apiKey}` });

    it('returns dashboard data', async () => {
      const res = await fetch('/api/dashboard', { headers: auth() });
      expect(res.status).toBe(200);
      expect(res.data.workspace.name).toBe('Dashboard Test');
      expect(res.data.summary.totalAgents).toBe(1);
      expect(res.data.summary.connected).toBe(1);
      expect(res.data.agents).toHaveLength(1);
    });

    it('returns events', async () => {
      const res = await fetch('/api/events', { headers: auth() });
      expect(res.status).toBe(200);
      expect(Array.isArray(res.data.events)).toBe(true);
      expect(res.data.events.length).toBeGreaterThan(0);
    });

    it('respects event limit', async () => {
      const res = await fetch('/api/events?limit=1', { headers: auth() });
      expect(res.status).toBe(200);
      expect(res.data.events.length).toBeLessThanOrEqual(1);
    });
  });

  describe('CORS', () => {
    it('handles OPTIONS preflight', async () => {
      const url = new URL('/api/health', baseUrl);
      return new Promise<void>((resolve) => {
        const http = require('node:http');
        const req = http.request(url.toString(), { method: 'OPTIONS' }, (res: IncomingMessage) => {
          expect(res.statusCode).toBe(204);
          expect(res.headers['access-control-allow-origin']).toBe('*');
          resolve();
        });
        req.end();
      });
    });
  });

  describe('404', () => {
    it('returns 404 for unknown routes', async () => {
      const ws = await fetch('/api/workspaces', { method: 'POST', body: {} });
      const res = await fetch('/api/nonexistent', {
        headers: { Authorization: `Bearer ${ws.data.apiKey}` },
      });
      expect(res.status).toBe(404);
    });
  });

  describe('Reputation-based routing', () => {
    let apiKey: string;

    beforeAll(async () => {
      const ws = await fetch('/api/workspaces', { method: 'POST', body: { name: 'Reputation Test' } });
      apiKey = ws.data.apiKey;
      
      // Register two agents with same capability
      await fetch('/api/agents', {
        method: 'POST',
        headers: { Authorization: `Bearer ${apiKey}` },
        body: { id: 'fast', name: 'FastAgent', capabilities: ['process'] },
      });
      await fetch('/api/agents', {
        method: 'POST',
        headers: { Authorization: `Bearer ${apiKey}` },
        body: { id: 'slow', name: 'SlowAgent', capabilities: ['process'] },
      });

      const auth = { Authorization: `Bearer ${apiKey}` };
      
      // Give 'fast' agent good metrics
      for (let i = 0; i < 5; i++) {
        const r = await fetch('/api/route', { method: 'POST', headers: auth, body: { capability: 'process' } });
        await fetch(`/api/tasks/${r.data.taskId}/complete`, {
          method: 'POST', headers: auth, body: { agentId: 'fast', latencyMs: 50 },
        });
      }
      
      // Give 'slow' agent poor metrics
      for (let i = 0; i < 5; i++) {
        const r = await fetch('/api/route', { method: 'POST', headers: auth, body: { capability: 'process' } });
        await fetch(`/api/tasks/${r.data.taskId}/complete`, {
          method: 'POST', headers: auth, body: { agentId: 'slow', latencyMs: 2000 },
        });
      }
    });

    it('prefers higher-reputation agent', async () => {
      const auth = { Authorization: `Bearer ${apiKey}` };
      // Route multiple times — fast agent should be selected more
      const selections: string[] = [];
      for (let i = 0; i < 5; i++) {
        const r = await fetch('/api/route', { method: 'POST', headers: auth, body: { capability: 'process' } });
        selections.push(r.data.agent.id);
      }
      // Fast agent should be preferred (lower latency = higher score)
      const fastCount = selections.filter(s => s === 'fast').length;
      expect(fastCount).toBeGreaterThanOrEqual(3);
    });
  });
});
