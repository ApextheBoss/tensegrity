import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { TensegrityClient, CloudClientConfig, CloudTask } from '../cloud-client';

// ============================================================
// Mock WebSocket
// ============================================================

class MockWebSocket {
  static instances: MockWebSocket[] = [];
  url: string;
  readyState = 0; // CONNECTING
  sentMessages: string[] = [];
  private listeners = new Map<string, Set<Function>>();
  onclose: (() => void) | null = null;

  constructor(url: string) {
    this.url = url;
    MockWebSocket.instances.push(this);
  }

  addEventListener(event: string, handler: Function, opts?: { once?: boolean }) {
    if (!this.listeners.has(event)) this.listeners.set(event, new Set());
    if (opts?.once) {
      const orig = handler;
      handler = (...args: unknown[]) => {
        this.listeners.get(event)?.delete(handler as Function);
        (orig as Function)(...args);
      };
    }
    this.listeners.get(event)!.add(handler);
  }

  removeEventListener(event: string, handler: Function) {
    this.listeners.get(event)?.delete(handler);
  }

  send(data: string) {
    this.sentMessages.push(data);
  }

  close(_code?: number, _reason?: string) {
    this.readyState = 3;
    this._fire('close', {});
  }

  // Test helpers
  _fire(event: string, data: unknown) {
    const handlers = this.listeners.get(event);
    if (handlers) {
      for (const h of [...handlers]) h(data);
    }
  }

  _simulateOpen() {
    this.readyState = 1;
    this._fire('open', {});
  }

  _simulateMessage(msg: Record<string, unknown>) {
    this._fire('message', { data: JSON.stringify(msg) });
  }

  _simulateClose() {
    this.readyState = 3;
    this._fire('close', {});
  }

  _simulateError() {
    this._fire('error', new Event('error'));
  }
}

// Install mock
const originalWebSocket = globalThis.WebSocket;
beforeEach(() => {
  MockWebSocket.instances = [];
  (globalThis as any).WebSocket = MockWebSocket as any;
});
afterEach(() => {
  (globalThis as any).WebSocket = originalWebSocket;
});

function defaultConfig(overrides?: Partial<CloudClientConfig>): CloudClientConfig {
  return {
    url: 'ws://localhost:4100',
    apiKey: 'tsg_test123',
    agentId: 'agent-1',
    agentName: 'Test Agent',
    capabilities: ['summarize', 'classify'],
    autoReconnect: false, // disable for most tests
    ...overrides,
  };
}

function lastWs(): MockWebSocket {
  return MockWebSocket.instances[MockWebSocket.instances.length - 1];
}

// ============================================================
// Tests
// ============================================================

describe('TensegrityClient', () => {
  describe('constructor', () => {
    it('sets defaults', () => {
      const client = new TensegrityClient({ url: 'ws://localhost', apiKey: 'tsg_x', agentId: 'a' });
      expect(client.getState()).toBe('disconnected');
    });
  });

  describe('connect', () => {
    it('connects and registers on open', async () => {
      const client = new TensegrityClient(defaultConfig());
      const promise = client.connect();
      const ws = lastWs();
      expect(ws.url).toContain('apiKey=tsg_test123');
      expect(ws.url).toContain('agentId=agent-1');
      expect(client.getState()).toBe('connecting');

      ws._simulateOpen();
      await promise;

      expect(client.getState()).toBe('connected');
      // Should have auto-registered
      expect(ws.sentMessages.length).toBe(1);
      const reg = JSON.parse(ws.sentMessages[0]);
      expect(reg.type).toBe('register');
      expect(reg.capabilities).toEqual(['summarize', 'classify']);
    });

    it('rejects on connection error', async () => {
      const client = new TensegrityClient(defaultConfig());
      const promise = client.connect();
      lastWs()._simulateError();
      await expect(promise).rejects.toThrow('WebSocket connection failed');
    });

    it('is no-op if already connected', async () => {
      const client = new TensegrityClient(defaultConfig());
      const p = client.connect();
      lastWs()._simulateOpen();
      await p;
      await client.connect(); // should not throw
      expect(MockWebSocket.instances.length).toBe(1);
    });

    it('throws if destroyed', async () => {
      const client = new TensegrityClient(defaultConfig());
      client.destroy();
      await expect(client.connect()).rejects.toThrow('destroyed');
    });
  });

  describe('register', () => {
    it('sends register message', async () => {
      const client = new TensegrityClient(defaultConfig());
      const p = client.connect();
      lastWs()._simulateOpen();
      await p;

      lastWs().sentMessages = []; // clear auto-register
      await client.register(['translate', 'embed']);

      const msg = JSON.parse(lastWs().sentMessages[0]);
      expect(msg.type).toBe('register');
      expect(msg.capabilities).toEqual(['translate', 'embed']);
    });
  });

  describe('task handling', () => {
    it('delivers tasks to waiters', async () => {
      const client = new TensegrityClient(defaultConfig());
      const p = client.connect();
      lastWs()._simulateOpen();
      await p;

      const taskPromise = client.waitForTask();
      lastWs()._simulateMessage({
        type: 'task',
        taskId: 'task-1',
        capability: 'summarize',
        payload: { text: 'hello' },
      });

      const task = await taskPromise;
      expect(task.id).toBe('task-1');
      expect(task.capability).toBe('summarize');
      expect(task.payload).toEqual({ text: 'hello' });
    });

    it('queues tasks when no waiter', async () => {
      const client = new TensegrityClient(defaultConfig());
      const p = client.connect();
      lastWs()._simulateOpen();
      await p;

      lastWs()._simulateMessage({
        type: 'task',
        taskId: 'task-2',
        capability: 'classify',
        payload: {},
      });

      // Now wait — should get the queued task immediately
      const task = await client.waitForTask();
      expect(task.id).toBe('task-2');
    });

    it('waitForTask times out', async () => {
      const client = new TensegrityClient(defaultConfig());
      const p = client.connect();
      lastWs()._simulateOpen();
      await p;

      vi.useFakeTimers();
      const taskPromise = client.waitForTask(100);
      vi.advanceTimersByTime(150);
      await expect(taskPromise).rejects.toThrow('timed out');
      vi.useRealTimers();
    });
  });

  describe('completeTask / failTask', () => {
    it('sends task:complete', async () => {
      const client = new TensegrityClient(defaultConfig());
      const p = client.connect();
      lastWs()._simulateOpen();
      await p;

      lastWs().sentMessages = [];
      await client.completeTask('task-1', { summary: 'done' }, 150);

      const msg = JSON.parse(lastWs().sentMessages[0]);
      expect(msg.type).toBe('task:complete');
      expect(msg.taskId).toBe('task-1');
      expect(msg.latencyMs).toBe(150);
    });

    it('sends task:fail', async () => {
      const client = new TensegrityClient(defaultConfig());
      const p = client.connect();
      lastWs()._simulateOpen();
      await p;

      lastWs().sentMessages = [];
      await client.failTask('task-1', 'OOM');

      const msg = JSON.parse(lastWs().sentMessages[0]);
      expect(msg.type).toBe('task:fail');
      expect(msg.reason).toBe('OOM');
    });

    it('rejects when not connected', async () => {
      const client = new TensegrityClient(defaultConfig());
      await expect(client.completeTask('x')).rejects.toThrow('Not connected');
    });
  });

  describe('route', () => {
    it('sends route request and waits for response', async () => {
      const client = new TensegrityClient(defaultConfig());
      const p = client.connect();
      lastWs()._simulateOpen();
      await p;

      lastWs().sentMessages = [];
      const routePromise = client.route('summarize', { text: 'hi' });

      // Parse the sent request to get requestId
      const sent = JSON.parse(lastWs().sentMessages[0]);
      expect(sent.type).toBe('route');
      expect(sent.capability).toBe('summarize');

      // Simulate response
      lastWs()._simulateMessage({
        type: 'response',
        requestId: sent.requestId,
        data: { taskId: 'task-99', agentId: 'agent-2' },
      });

      const result = await routePromise;
      expect(result).toEqual({ taskId: 'task-99', agentId: 'agent-2' });
    });

    it('rejects on error response', async () => {
      const client = new TensegrityClient(defaultConfig());
      const p = client.connect();
      lastWs()._simulateOpen();
      await p;

      lastWs().sentMessages = [];
      const routePromise = client.route('nonexistent');

      const sent = JSON.parse(lastWs().sentMessages[0]);
      lastWs()._simulateMessage({
        type: 'response',
        requestId: sent.requestId,
        error: 'No agents available',
      });

      await expect(routePromise).rejects.toThrow('No agents available');
    });

    it('times out', async () => {
      const client = new TensegrityClient(defaultConfig());
      const p = client.connect();
      lastWs()._simulateOpen();
      await p;

      // Use a short real timeout
      const routePromise = client.route('summarize', {}, 50);
      await expect(routePromise).rejects.toThrow('timed out');
    });
  });

  describe('events', () => {
    it('emits connected/disconnected events', async () => {
      const client = new TensegrityClient(defaultConfig());
      const events: string[] = [];
      client.on('connected', () => events.push('connected'));
      client.on('disconnected', () => events.push('disconnected'));

      const p = client.connect();
      lastWs()._simulateOpen();
      await p;

      lastWs()._simulateClose();
      expect(events).toEqual(['connected', 'disconnected']);
    });

    it('emits circuit:open events', async () => {
      const client = new TensegrityClient(defaultConfig());
      const p = client.connect();
      lastWs()._simulateOpen();
      await p;

      const events: unknown[] = [];
      client.on('circuit:open', (d) => events.push(d));

      lastWs()._simulateMessage({ type: 'circuit:open', agentId: 'agent-1' });
      expect(events).toHaveLength(1);
    });

    it('on() returns unsubscribe function', async () => {
      const client = new TensegrityClient(defaultConfig());
      const handler = vi.fn();
      const unsub = client.on('task', handler);

      const p = client.connect();
      lastWs()._simulateOpen();
      await p;

      lastWs()._simulateMessage({ type: 'task', taskId: 't1', capability: 'x', payload: null });
      expect(handler).toHaveBeenCalledTimes(1);

      unsub();
      lastWs()._simulateMessage({ type: 'task', taskId: 't2', capability: 'x', payload: null });
      expect(handler).toHaveBeenCalledTimes(1); // not called again
    });
  });

  describe('heartbeat', () => {
    it('sends periodic heartbeats', async () => {
      vi.useFakeTimers();
      const client = new TensegrityClient(defaultConfig({ heartbeatIntervalMs: 100 }));
      const p = client.connect();
      lastWs()._simulateOpen();
      await p;

      lastWs().sentMessages = [];
      vi.advanceTimersByTime(350);

      const heartbeats = lastWs().sentMessages
        .map(m => JSON.parse(m))
        .filter(m => m.type === 'heartbeat');
      expect(heartbeats.length).toBe(3);
      expect(heartbeats[0].agentId).toBe('agent-1');
      vi.useRealTimers();
    });
  });

  describe('reconnect', () => {
    it('reconnects with exponential backoff', async () => {
      vi.useFakeTimers();
      const client = new TensegrityClient(defaultConfig({
        autoReconnect: true,
        reconnectBaseMs: 100,
        maxReconnectAttempts: 3,
      }));

      const events: string[] = [];
      client.on('reconnecting', () => events.push('reconnecting'));
      client.on('connected', () => events.push('connected'));

      const p = client.connect();
      lastWs()._simulateOpen();
      await p;

      // Disconnect
      lastWs()._simulateClose();
      expect(client.getState()).toBe('reconnecting');
      expect(events).toContain('reconnecting');

      // Advance past first backoff (100ms)
      vi.advanceTimersByTime(150);
      expect(MockWebSocket.instances.length).toBe(2);

      // Simulate successful reconnect
      lastWs()._simulateOpen();
      expect(client.getState()).toBe('connected');
      vi.useRealTimers();
    });
  });

  describe('destroy', () => {
    it('cleans up everything', async () => {
      const client = new TensegrityClient(defaultConfig());
      const p = client.connect();
      lastWs()._simulateOpen();
      await p;

      const routePromise = client.route('x');
      client.destroy();

      expect(client.getState()).toBe('disconnected');
      await expect(routePromise).rejects.toThrow('destroyed');
    });
  });
});
