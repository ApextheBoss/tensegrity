/**
 * Tensegrity Cloud Client — Connect agents to Tensegrity Cloud via WebSocket.
 *
 * Usage:
 *   import { TensegrityClient } from 'tensegrity/cloud-client';
 *   const client = new TensegrityClient({ url: 'wss://cloud.tensegrity.dev', apiKey: 'tsg_...' });
 *   await client.register('my-agent', ['summarize', 'classify']);
 *   const task = await client.waitForTask();
 *   await client.completeTask(task.id, result);
 */

// ============================================================
// Types
// ============================================================

export interface CloudClientConfig {
  /** WebSocket URL of the Tensegrity Cloud server */
  url: string;
  /** API key (tsg_...) */
  apiKey: string;
  /** Agent ID — must be unique within your workspace */
  agentId: string;
  /** Human-readable agent name */
  agentName?: string;
  /** Capabilities this agent can handle */
  capabilities?: string[];
  /** Heartbeat interval in ms (default: 15000) */
  heartbeatIntervalMs?: number;
  /** Reconnect on disconnect (default: true) */
  autoReconnect?: boolean;
  /** Max reconnect attempts (default: Infinity) */
  maxReconnectAttempts?: number;
  /** Base reconnect delay ms (default: 1000, exponential backoff) */
  reconnectBaseMs?: number;
}

export interface CloudTask {
  id: string;
  capability: string;
  payload: unknown;
  routedAt: number;
  timeoutMs?: number;
}

export interface CloudMessage {
  type: string;
  [key: string]: unknown;
}

export interface AgentMetrics {
  tasksCompleted: number;
  tasksFailed: number;
  avgLatencyMs: number;
}

export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'reconnecting';

export type CloudEventType =
  | 'connected'
  | 'disconnected'
  | 'reconnecting'
  | 'task'
  | 'circuit:open'
  | 'circuit:close'
  | 'error'
  | 'message';

export type CloudEventHandler = (data: unknown) => void;

// ============================================================
// Client
// ============================================================

export class TensegrityClient {
  private config: Required<CloudClientConfig>;
  private ws: WebSocket | null = null;
  private state: ConnectionState = 'disconnected';
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private listeners = new Map<CloudEventType, Set<CloudEventHandler>>();
  private pendingRequests = new Map<string, { resolve: (v: unknown) => void; reject: (e: Error) => void; timer: ReturnType<typeof setTimeout> }>();
  private requestCounter = 0;
  private taskQueue: CloudTask[] = [];
  private taskWaiters: Array<(task: CloudTask) => void> = [];
  private destroyed = false;

  constructor(config: CloudClientConfig) {
    this.config = {
      agentName: config.agentId,
      capabilities: [],
      heartbeatIntervalMs: 15000,
      autoReconnect: true,
      maxReconnectAttempts: Infinity,
      reconnectBaseMs: 1000,
      ...config,
    };
  }

  // ---- Public API ----

  /** Connect to Tensegrity Cloud and register this agent. */
  async connect(): Promise<void> {
    if (this.destroyed) throw new Error('Client has been destroyed');
    if (this.state === 'connected') return;
    return this._connect();
  }

  /** Register/update capabilities. Can be called after connect. */
  async register(capabilities: string[]): Promise<void> {
    this.config.capabilities = capabilities;
    await this._send('register', {
      agentId: this.config.agentId,
      name: this.config.agentName,
      capabilities,
    });
  }

  /** Wait for the next task routed to this agent. */
  waitForTask(timeoutMs?: number): Promise<CloudTask> {
    // Drain queue first
    if (this.taskQueue.length > 0) {
      return Promise.resolve(this.taskQueue.shift()!);
    }
    return new Promise((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const waiter = (task: CloudTask) => {
        if (timer) clearTimeout(timer);
        resolve(task);
      };
      this.taskWaiters.push(waiter);
      if (timeoutMs !== undefined) {
        timer = setTimeout(() => {
          const idx = this.taskWaiters.indexOf(waiter);
          if (idx >= 0) this.taskWaiters.splice(idx, 1);
          reject(new Error('waitForTask timed out'));
        }, timeoutMs);
      }
    });
  }

  /** Report task completion to the cloud. */
  async completeTask(taskId: string, result?: unknown, latencyMs?: number): Promise<void> {
    await this._send('task:complete', {
      taskId,
      agentId: this.config.agentId,
      result,
      latencyMs,
    });
  }

  /** Report task failure to the cloud. */
  async failTask(taskId: string, reason?: string): Promise<void> {
    await this._send('task:fail', {
      taskId,
      agentId: this.config.agentId,
      reason,
    });
  }

  /** Route a task to the best available agent (from coordinator perspective). */
  async route(capability: string, payload?: unknown, timeoutMs?: number): Promise<{ taskId: string; agentId: string }> {
    const resp = await this._request('route', { capability, payload }, timeoutMs);
    return resp as { taskId: string; agentId: string };
  }

  /** Subscribe to events. */
  on(event: CloudEventType, handler: CloudEventHandler): () => void {
    if (!this.listeners.has(event)) this.listeners.set(event, new Set());
    this.listeners.get(event)!.add(handler);
    return () => this.listeners.get(event)?.delete(handler);
  }

  /** Get current connection state. */
  getState(): ConnectionState {
    return this.state;
  }

  /** Disconnect and clean up. */
  destroy(): void {
    this.destroyed = true;
    this._clearTimers();
    if (this.ws) {
      this.ws.close(1000, 'client destroyed');
      this.ws = null;
    }
    this.state = 'disconnected';
    this.pendingRequests.forEach(p => {
      clearTimeout(p.timer);
      p.reject(new Error('Client destroyed'));
    });
    this.pendingRequests.clear();
    this.taskWaiters = [];
    this.listeners.clear();
  }

  // ---- Internals ----

  private _connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.state = 'connecting';
      const url = new URL(this.config.url);
      url.searchParams.set('apiKey', this.config.apiKey);
      url.searchParams.set('agentId', this.config.agentId);

      const ws = new WebSocket(url.toString());
      this.ws = ws;

      const onOpen = () => {
        ws.removeEventListener('error', onError);
        this.state = 'connected';
        this.reconnectAttempts = 0;
        this._startHeartbeat();
        this._emit('connected', undefined);
        // Auto-register on connect
        this.register(this.config.capabilities).catch(() => {});
        resolve();
      };

      const onError = () => {
        ws.removeEventListener('open', onOpen);
        reject(new Error('WebSocket connection failed'));
      };

      ws.addEventListener('open', onOpen, { once: true });
      ws.addEventListener('error', onError, { once: true });

      ws.addEventListener('message', (ev) => {
        try {
          const msg = JSON.parse(typeof ev.data === 'string' ? ev.data : new TextDecoder().decode(ev.data as ArrayBuffer)) as CloudMessage;
          this._handleMessage(msg);
        } catch (e) {
          this._emit('error', e);
        }
      });

      ws.addEventListener('close', () => {
        this._clearTimers();
        this.state = 'disconnected';
        this._emit('disconnected', undefined);
        if (!this.destroyed && this.config.autoReconnect && this.reconnectAttempts < this.config.maxReconnectAttempts) {
          this._scheduleReconnect();
        }
      });
    });
  }

  private _handleMessage(msg: CloudMessage): void {
    // Response to a request
    if (msg.type === 'response' && typeof msg.requestId === 'string') {
      const pending = this.pendingRequests.get(msg.requestId);
      if (pending) {
        this.pendingRequests.delete(msg.requestId);
        clearTimeout(pending.timer);
        if (msg.error) {
          pending.reject(new Error(msg.error as string));
        } else {
          pending.resolve(msg.data);
        }
      }
      return;
    }

    // Task routed to us
    if (msg.type === 'task') {
      const task: CloudTask = {
        id: msg.taskId as string,
        capability: msg.capability as string,
        payload: msg.payload,
        routedAt: Date.now(),
        timeoutMs: msg.timeoutMs as number | undefined,
      };
      if (this.taskWaiters.length > 0) {
        const waiter = this.taskWaiters.shift()!;
        waiter(task);
      } else {
        this.taskQueue.push(task);
      }
      this._emit('task', task);
      return;
    }

    // Circuit breaker notifications
    if (msg.type === 'circuit:open') {
      this._emit('circuit:open', msg);
      return;
    }
    if (msg.type === 'circuit:close') {
      this._emit('circuit:close', msg);
      return;
    }

    // Generic message
    this._emit('message', msg);
  }

  private _send(type: string, data: Record<string, unknown>): Promise<void> {
    if (!this.ws || this.state !== 'connected') {
      return Promise.reject(new Error('Not connected'));
    }
    this.ws.send(JSON.stringify({ type, ...data }));
    return Promise.resolve();
  }

  private _request(type: string, data: Record<string, unknown>, timeoutMs = 30000): Promise<unknown> {
    return new Promise((resolve, reject) => {
      if (!this.ws || this.state !== 'connected') {
        return reject(new Error('Not connected'));
      }
      const requestId = `${this.config.agentId}-${++this.requestCounter}`;
      const timer = setTimeout(() => {
        this.pendingRequests.delete(requestId);
        reject(new Error(`Request ${type} timed out`));
      }, timeoutMs);
      this.pendingRequests.set(requestId, { resolve, reject, timer });
      this.ws.send(JSON.stringify({ type, requestId, ...data }));
    });
  }

  private _startHeartbeat(): void {
    this._stopHeartbeat();
    this.heartbeatTimer = setInterval(() => {
      if (this.ws && this.state === 'connected') {
        this.ws.send(JSON.stringify({ type: 'heartbeat', agentId: this.config.agentId }));
      }
    }, this.config.heartbeatIntervalMs);
  }

  private _stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  private _scheduleReconnect(): void {
    this.state = 'reconnecting';
    this._emit('reconnecting', { attempt: this.reconnectAttempts + 1 });
    const delay = Math.min(
      this.config.reconnectBaseMs * Math.pow(2, this.reconnectAttempts),
      30000
    );
    this.reconnectAttempts++;
    this.reconnectTimer = setTimeout(() => {
      this._connect().catch(() => {});
    }, delay);
  }

  private _clearTimers(): void {
    this._stopHeartbeat();
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  private _emit(event: CloudEventType, data: unknown): void {
    const handlers = this.listeners.get(event);
    if (handlers) {
      for (const handler of handlers) {
        try { handler(data); } catch (_) {}
      }
    }
  }
}
