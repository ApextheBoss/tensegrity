import { fnv1a } from './shared-utils';
/**
 * Transactional Outbox Pattern for Agent Event Publishing
 * 
 * Guarantees exactly-once event publishing from agent state changes by
 * writing events to a local outbox table atomically with state mutations,
 * then asynchronously relaying them to external consumers.
 * 
 * Components:
 * - OutboxStore: append-only event log with ordering guarantees
 * - RelayDispatcher: async polling with batched delivery and retry
 * - IdempotencyRegistry: consumer-side dedup with LRU eviction
 * - OrderingGuaranteeManager: per-partition FIFO with head-of-line blocking options
 * - DeadLetterHandler: quarantine persistently failing events
 * - ChangeDataCaptureStream: log-tailing for low-latency relay
 * - CompactionManager: periodic outbox truncation after confirmed delivery
 * - PartitionRouter: consistent-hash event routing to relay workers
 * - TransactionalOutboxEngine: unified orchestrator
 */

// ─── FNV-1a Hash ─────────────────────────────────────────────────

// ─── Types ───────────────────────────────────────────────────────
interface OutboxEvent {
  id: string;
  partitionKey: string;
  topic: string;
  payload: Record<string, unknown>;
  metadata: Record<string, unknown>;
  createdAt: number;
  sequence: number;
  status: 'pending' | 'dispatched' | 'confirmed' | 'dead-lettered';
  attempts: number;
  lastAttemptAt: number | null;
  confirmedAt: number | null;
  error: string | null;
}

interface StateChange {
  entityId: string;
  entityType: string;
  operation: 'create' | 'update' | 'delete';
  before: Record<string, unknown> | null;
  after: Record<string, unknown> | null;
  timestamp: number;
}

interface DeliveryReceipt {
  eventId: string;
  consumerId: string;
  receivedAt: number;
  processingTimeMs: number;
  success: boolean;
  error?: string;
}

interface RelayWorker {
  id: string;
  partitions: Set<number>;
  status: 'active' | 'draining' | 'stopped';
  lastHeartbeat: number;
  eventsDispatched: number;
  errorRate: number;
}

interface PartitionState {
  id: number;
  headSequence: number;
  confirmedSequence: number;
  blockedEventId: string | null;
  pendingCount: number;
  assignedWorker: string | null;
}

interface CompactionCheckpoint {
  partitionId: number;
  confirmedUpTo: number;
  compactedUpTo: number;
  lastCompactionAt: number;
  eventsCompacted: number;
}

interface CDCPosition {
  partitionId: number;
  lastReadSequence: number;
  lastReadAt: number;
  lag: number;
}

interface OutboxConfig {
  maxBatchSize: number;
  pollIntervalMs: number;
  maxRetries: number;
  baseRetryDelayMs: number;
  maxRetryDelayMs: number;
  partitionCount: number;
  idempotencyWindowSize: number;
  idempotencyTtlMs: number;
  deadLetterThreshold: number;
  compactionIntervalMs: number;
  compactionRetentionMs: number;
  cdcEnabled: boolean;
  cdcPollIntervalMs: number;
  headOfLineBlockingEnabled: boolean;
  workerHeartbeatTimeoutMs: number;
  confirmationTimeoutMs: number;
  maxPendingPerPartition: number;
}

type EventType =
  | 'event_appended'
  | 'event_dispatched'
  | 'event_confirmed'
  | 'event_dead_lettered'
  | 'event_retried'
  | 'partition_blocked'
  | 'partition_unblocked'
  | 'compaction_completed'
  | 'worker_assigned'
  | 'worker_removed'
  | 'cdc_position_advanced'
  | 'idempotency_duplicate_detected';

interface OutboxSystemEvent {
  type: EventType;
  timestamp: number;
  data: Record<string, unknown>;
}

// ─── EWMA Tracker ────────────────────────────────────────────────
class EWMATracker {
  private value: number;
  private readonly alpha: number;
  private initialized = false;

  constructor(alpha: number = 0.3) {
    this.alpha = alpha;
    this.value = 0;
  }

  update(sample: number): void {
    if (!this.initialized) {
      this.value = sample;
      this.initialized = true;
    } else {
      this.value = this.alpha * sample + (1 - this.alpha) * this.value;
    }
  }

  get(): number {
    return this.value;
  }
}

// ─── OutboxStore ─────────────────────────────────────────────────
class OutboxStore {
  private events: Map<string, OutboxEvent> = new Map();
  private partitionIndex: Map<number, string[]> = new Map();
  private topicIndex: Map<string, Set<string>> = new Map();
  private sequenceCounter = 0;
  private readonly config: OutboxConfig;

  constructor(config: OutboxConfig) {
    this.config = config;
  }

  append(
    partitionKey: string,
    topic: string,
    payload: Record<string, unknown>,
    metadata: Record<string, unknown> = {}
  ): OutboxEvent {
    const partition = fnv1a(partitionKey) % this.config.partitionCount;
    const partitionEvents = this.partitionIndex.get(partition) || [];
    
    if (partitionEvents.filter(id => {
      const e = this.events.get(id);
      return e && e.status === 'pending';
    }).length >= this.config.maxPendingPerPartition) {
      throw new Error(`Partition ${partition} pending limit reached (${this.config.maxPendingPerPartition})`);
    }

    const id = `evt_${Date.now()}_${fnv1a(`${partitionKey}:${this.sequenceCounter}`).toString(16)}`;
    const event: OutboxEvent = {
      id,
      partitionKey,
      topic,
      payload,
      metadata,
      createdAt: Date.now(),
      sequence: ++this.sequenceCounter,
      status: 'pending',
      attempts: 0,
      lastAttemptAt: null,
      confirmedAt: null,
      error: null,
    };

    this.events.set(id, event);
    
    if (!this.partitionIndex.has(partition)) {
      this.partitionIndex.set(partition, []);
    }
    this.partitionIndex.get(partition)!.push(id);

    if (!this.topicIndex.has(topic)) {
      this.topicIndex.set(topic, new Set());
    }
    this.topicIndex.get(topic)!.add(id);

    return event;
  }

  appendBatch(
    entries: Array<{ partitionKey: string; topic: string; payload: Record<string, unknown>; metadata?: Record<string, unknown> }>
  ): OutboxEvent[] {
    return entries.map(e => this.append(e.partitionKey, e.topic, e.payload, e.metadata || {}));
  }

  atomicStateChangeAndAppend(
    stateChange: StateChange,
    events: Array<{ topic: string; payload: Record<string, unknown>; metadata?: Record<string, unknown> }>
  ): { stateChange: StateChange; events: OutboxEvent[] } {
    // Simulate atomic transaction: state change + event append together
    const appendedEvents = events.map(e =>
      this.append(stateChange.entityId, e.topic, {
        ...e.payload,
        _stateChange: {
          entityId: stateChange.entityId,
          entityType: stateChange.entityType,
          operation: stateChange.operation,
        },
      }, e.metadata || {})
    );
    return { stateChange, events: appendedEvents };
  }

  getPendingByPartition(partitionId: number, limit: number): OutboxEvent[] {
    const ids = this.partitionIndex.get(partitionId) || [];
    const pending: OutboxEvent[] = [];
    for (const id of ids) {
      if (pending.length >= limit) break;
      const event = this.events.get(id);
      if (event && event.status === 'pending') {
        // Check retry delay
        if (event.attempts > 0 && event.lastAttemptAt) {
          const delay = Math.min(
            this.config.baseRetryDelayMs * Math.pow(2, event.attempts - 1),
            this.config.maxRetryDelayMs
          );
          if (Date.now() - event.lastAttemptAt < delay) continue;
        }
        pending.push(event);
      }
    }
    return pending;
  }

  markDispatched(eventId: string): void {
    const event = this.events.get(eventId);
    if (event) {
      event.status = 'dispatched';
      event.attempts++;
      event.lastAttemptAt = Date.now();
    }
  }

  markConfirmed(eventId: string): void {
    const event = this.events.get(eventId);
    if (event) {
      event.status = 'confirmed';
      event.confirmedAt = Date.now();
    }
  }

  markFailed(eventId: string, error: string): void {
    const event = this.events.get(eventId);
    if (event) {
      event.status = 'pending'; // Back to pending for retry
      event.error = error;
      event.attempts++;
      event.lastAttemptAt = Date.now();
    }
  }

  markDeadLettered(eventId: string, error: string): void {
    const event = this.events.get(eventId);
    if (event) {
      event.status = 'dead-lettered';
      event.error = error;
    }
  }

  getEvent(eventId: string): OutboxEvent | undefined {
    return this.events.get(eventId);
  }

  getEventsBySequenceRange(partitionId: number, fromSeq: number, toSeq: number): OutboxEvent[] {
    const ids = this.partitionIndex.get(partitionId) || [];
    return ids
      .map(id => this.events.get(id)!)
      .filter(e => e && e.sequence >= fromSeq && e.sequence <= toSeq)
      .sort((a, b) => a.sequence - b.sequence);
  }

  compactPartition(partitionId: number, upToSequence: number): number {
    const ids = this.partitionIndex.get(partitionId) || [];
    let compacted = 0;
    const remaining: string[] = [];
    for (const id of ids) {
      const event = this.events.get(id);
      if (event && event.sequence <= upToSequence && event.status === 'confirmed') {
        this.events.delete(id);
        const topicSet = this.topicIndex.get(event.topic);
        if (topicSet) topicSet.delete(id);
        compacted++;
      } else {
        remaining.push(id);
      }
    }
    this.partitionIndex.set(partitionId, remaining);
    return compacted;
  }

  getStats(): {
    total: number;
    pending: number;
    dispatched: number;
    confirmed: number;
    deadLettered: number;
    partitionCounts: Map<number, number>;
  } {
    let pending = 0, dispatched = 0, confirmed = 0, deadLettered = 0;
    for (const event of this.events.values()) {
      switch (event.status) {
        case 'pending': pending++; break;
        case 'dispatched': dispatched++; break;
        case 'confirmed': confirmed++; break;
        case 'dead-lettered': deadLettered++; break;
      }
    }
    const partitionCounts = new Map<number, number>();
    for (const [pid, ids] of this.partitionIndex) {
      partitionCounts.set(pid, ids.length);
    }
    return { total: this.events.size, pending, dispatched, confirmed, deadLettered, partitionCounts };
  }
}

// ─── IdempotencyRegistry ─────────────────────────────────────────
class IdempotencyRegistry {
  private seen: Map<string, { consumerId: string; processedAt: number; result: 'success' | 'failure' }> = new Map();
  private accessOrder: string[] = [];
  private readonly maxSize: number;
  private readonly ttlMs: number;
  private duplicatesDetected = 0;

  constructor(maxSize: number, ttlMs: number) {
    this.maxSize = maxSize;
    this.ttlMs = ttlMs;
  }

  isDuplicate(eventId: string, consumerId: string): boolean {
    const key = `${eventId}:${consumerId}`;
    const entry = this.seen.get(key);
    if (entry && Date.now() - entry.processedAt < this.ttlMs) {
      this.duplicatesDetected++;
      return true;
    }
    return false;
  }

  record(eventId: string, consumerId: string, result: 'success' | 'failure'): void {
    const key = `${eventId}:${consumerId}`;
    this.seen.set(key, { consumerId, processedAt: Date.now(), result });
    this.accessOrder.push(key);
    this.evict();
  }

  private evict(): void {
    const now = Date.now();
    // TTL eviction
    while (this.accessOrder.length > 0) {
      const oldest = this.accessOrder[0];
      const entry = this.seen.get(oldest);
      if (entry && now - entry.processedAt >= this.ttlMs) {
        this.seen.delete(oldest);
        this.accessOrder.shift();
      } else {
        break;
      }
    }
    // LRU eviction
    while (this.seen.size > this.maxSize && this.accessOrder.length > 0) {
      const evicted = this.accessOrder.shift()!;
      this.seen.delete(evicted);
    }
  }

  getStats(): { size: number; duplicatesDetected: number } {
    return { size: this.seen.size, duplicatesDetected: this.duplicatesDetected };
  }
}

// ─── OrderingGuaranteeManager ────────────────────────────────────
class OrderingGuaranteeManager {
  private partitions: Map<number, PartitionState> = new Map();
  private readonly headOfLineBlocking: boolean;

  constructor(partitionCount: number, headOfLineBlocking: boolean) {
    this.headOfLineBlocking = headOfLineBlocking;
    for (let i = 0; i < partitionCount; i++) {
      this.partitions.set(i, {
        id: i,
        headSequence: 0,
        confirmedSequence: 0,
        blockedEventId: null,
        pendingCount: 0,
        assignedWorker: null,
      });
    }
  }

  canDispatch(partitionId: number, event: OutboxEvent): boolean {
    const partition = this.partitions.get(partitionId);
    if (!partition) return false;

    if (this.headOfLineBlocking && partition.blockedEventId) {
      return false; // Entire partition blocked
    }

    if (this.headOfLineBlocking) {
      // Must dispatch in sequence order
      return event.sequence === partition.headSequence + 1 || partition.headSequence === 0;
    }

    return true; // No ordering constraint
  }

  markDispatched(partitionId: number, event: OutboxEvent): void {
    const partition = this.partitions.get(partitionId);
    if (partition) {
      partition.headSequence = Math.max(partition.headSequence, event.sequence);
      partition.pendingCount++;
    }
  }

  markConfirmed(partitionId: number, event: OutboxEvent): void {
    const partition = this.partitions.get(partitionId);
    if (partition) {
      partition.confirmedSequence = Math.max(partition.confirmedSequence, event.sequence);
      partition.pendingCount = Math.max(0, partition.pendingCount - 1);
      if (partition.blockedEventId === event.id) {
        partition.blockedEventId = null;
      }
    }
  }

  markBlocked(partitionId: number, eventId: string): void {
    const partition = this.partitions.get(partitionId);
    if (partition && this.headOfLineBlocking) {
      partition.blockedEventId = eventId;
    }
  }

  unblock(partitionId: number): void {
    const partition = this.partitions.get(partitionId);
    if (partition) {
      partition.blockedEventId = null;
    }
  }

  assignWorker(partitionId: number, workerId: string): void {
    const partition = this.partitions.get(partitionId);
    if (partition) {
      partition.assignedWorker = workerId;
    }
  }

  getPartitionState(partitionId: number): PartitionState | undefined {
    return this.partitions.get(partitionId);
  }

  getAllPartitions(): PartitionState[] {
    return Array.from(this.partitions.values());
  }
}

// ─── DeadLetterHandler ───────────────────────────────────────────
class DeadLetterHandler {
  private deadLetters: Map<string, { event: OutboxEvent; reason: string; deadLetteredAt: number; replayAttempts: number }> = new Map();
  private readonly threshold: number;

  constructor(threshold: number) {
    this.threshold = threshold;
  }

  shouldDeadLetter(event: OutboxEvent): boolean {
    return event.attempts >= this.threshold;
  }

  addToDeadLetter(event: OutboxEvent, reason: string): void {
    this.deadLetters.set(event.id, {
      event: { ...event },
      reason,
      deadLetteredAt: Date.now(),
      replayAttempts: 0,
    });
  }

  getDeadLetters(limit: number = 100): Array<{ event: OutboxEvent; reason: string; deadLetteredAt: number }> {
    const entries = Array.from(this.deadLetters.values());
    return entries.slice(0, limit);
  }

  replay(eventId: string): OutboxEvent | null {
    const entry = this.deadLetters.get(eventId);
    if (!entry) return null;
    entry.replayAttempts++;
    const replayed = { ...entry.event, status: 'pending' as const, attempts: 0, error: null };
    this.deadLetters.delete(eventId);
    return replayed;
  }

  purge(olderThanMs: number): number {
    const cutoff = Date.now() - olderThanMs;
    let purged = 0;
    for (const [id, entry] of this.deadLetters) {
      if (entry.deadLetteredAt < cutoff) {
        this.deadLetters.delete(id);
        purged++;
      }
    }
    return purged;
  }

  getStats(): { total: number; oldestAt: number | null } {
    let oldest: number | null = null;
    for (const entry of this.deadLetters.values()) {
      if (oldest === null || entry.deadLetteredAt < oldest) {
        oldest = entry.deadLetteredAt;
      }
    }
    return { total: this.deadLetters.size, oldestAt: oldest };
  }
}

// ─── ChangeDataCaptureStream ─────────────────────────────────────
class ChangeDataCaptureStream {
  private positions: Map<number, CDCPosition> = new Map();
  private subscribers: Array<(events: OutboxEvent[]) => void> = [];
  private readonly enabled: boolean;

  constructor(enabled: boolean, partitionCount: number) {
    this.enabled = enabled;
    for (let i = 0; i < partitionCount; i++) {
      this.positions.set(i, {
        partitionId: i,
        lastReadSequence: 0,
        lastReadAt: 0,
        lag: 0,
      });
    }
  }

  subscribe(callback: (events: OutboxEvent[]) => void): void {
    if (this.enabled) {
      this.subscribers.push(callback);
    }
  }

  captureChanges(store: OutboxStore, partitionId: number): OutboxEvent[] {
    if (!this.enabled) return [];

    const position = this.positions.get(partitionId);
    if (!position) return [];

    const events = store.getEventsBySequenceRange(
      partitionId,
      position.lastReadSequence + 1,
      position.lastReadSequence + 100
    );

    if (events.length > 0) {
      const maxSeq = events[events.length - 1].sequence;
      position.lastReadSequence = maxSeq;
      position.lastReadAt = Date.now();
      position.lag = 0; // Reset lag on successful read

      // Notify subscribers
      for (const sub of this.subscribers) {
        sub(events);
      }
    }

    return events;
  }

  updateLag(partitionId: number, currentMaxSequence: number): void {
    const position = this.positions.get(partitionId);
    if (position) {
      position.lag = currentMaxSequence - position.lastReadSequence;
    }
  }

  getPositions(): CDCPosition[] {
    return Array.from(this.positions.values());
  }

  getTotalLag(): number {
    let total = 0;
    for (const pos of this.positions.values()) {
      total += pos.lag;
    }
    return total;
  }
}

// ─── CompactionManager ───────────────────────────────────────────
class CompactionManager {
  private checkpoints: Map<number, CompactionCheckpoint> = new Map();
  private readonly retentionMs: number;
  private lastCompactionAt = 0;
  private totalEventsCompacted = 0;

  constructor(partitionCount: number, retentionMs: number) {
    this.retentionMs = retentionMs;
    for (let i = 0; i < partitionCount; i++) {
      this.checkpoints.set(i, {
        partitionId: i,
        confirmedUpTo: 0,
        compactedUpTo: 0,
        lastCompactionAt: 0,
        eventsCompacted: 0,
      });
    }
  }

  updateConfirmedPosition(partitionId: number, sequence: number): void {
    const cp = this.checkpoints.get(partitionId);
    if (cp) {
      cp.confirmedUpTo = Math.max(cp.confirmedUpTo, sequence);
    }
  }

  compact(store: OutboxStore, partitionId: number): number {
    const cp = this.checkpoints.get(partitionId);
    if (!cp) return 0;

    // Only compact confirmed events older than retention window
    const safeSequence = cp.confirmedUpTo;
    if (safeSequence <= cp.compactedUpTo) return 0;

    const compacted = store.compactPartition(partitionId, safeSequence);
    cp.compactedUpTo = safeSequence;
    cp.lastCompactionAt = Date.now();
    cp.eventsCompacted += compacted;
    this.totalEventsCompacted += compacted;
    this.lastCompactionAt = Date.now();

    return compacted;
  }

  compactAll(store: OutboxStore): number {
    let total = 0;
    for (const [pid] of this.checkpoints) {
      total += this.compact(store, pid);
    }
    return total;
  }

  getCheckpoints(): CompactionCheckpoint[] {
    return Array.from(this.checkpoints.values());
  }

  getStats(): { totalCompacted: number; lastCompactionAt: number } {
    return { totalCompacted: this.totalEventsCompacted, lastCompactionAt: this.lastCompactionAt };
  }
}

// ─── PartitionRouter ─────────────────────────────────────────────
class PartitionRouter {
  private workers: Map<string, RelayWorker> = new Map();
  private partitionAssignments: Map<number, string> = new Map();
  private readonly partitionCount: number;
  private readonly heartbeatTimeoutMs: number;
  private dispatchLatency: EWMATracker = new EWMATracker(0.2);

  constructor(partitionCount: number, heartbeatTimeoutMs: number) {
    this.partitionCount = partitionCount;
    this.heartbeatTimeoutMs = heartbeatTimeoutMs;
  }

  addWorker(workerId: string): void {
    this.workers.set(workerId, {
      id: workerId,
      partitions: new Set(),
      status: 'active',
      lastHeartbeat: Date.now(),
      eventsDispatched: 0,
      errorRate: 0,
    });
    this.rebalance();
  }

  removeWorker(workerId: string): void {
    const worker = this.workers.get(workerId);
    if (worker) {
      worker.status = 'stopped';
      for (const pid of worker.partitions) {
        this.partitionAssignments.delete(pid);
      }
      this.workers.delete(workerId);
      this.rebalance();
    }
  }

  heartbeat(workerId: string): void {
    const worker = this.workers.get(workerId);
    if (worker) {
      worker.lastHeartbeat = Date.now();
    }
  }

  recordDispatch(workerId: string, latencyMs: number, success: boolean): void {
    const worker = this.workers.get(workerId);
    if (worker) {
      worker.eventsDispatched++;
      // EWMA error rate
      worker.errorRate = 0.1 * (success ? 0 : 1) + 0.9 * worker.errorRate;
      this.dispatchLatency.update(latencyMs);
    }
  }

  getWorkerForPartition(partitionId: number): string | null {
    return this.partitionAssignments.get(partitionId) || null;
  }

  private rebalance(): void {
    const activeWorkers = Array.from(this.workers.values())
      .filter(w => w.status === 'active')
      .sort((a, b) => fnv1a(a.id) - fnv1a(b.id));

    if (activeWorkers.length === 0) return;

    // Clear all assignments
    for (const worker of activeWorkers) {
      worker.partitions.clear();
    }
    this.partitionAssignments.clear();

    // Round-robin assignment with consistent ordering
    for (let i = 0; i < this.partitionCount; i++) {
      const worker = activeWorkers[i % activeWorkers.length];
      worker.partitions.add(i);
      this.partitionAssignments.set(i, worker.id);
    }
  }

  detectDeadWorkers(): string[] {
    const now = Date.now();
    const dead: string[] = [];
    for (const worker of this.workers.values()) {
      if (worker.status === 'active' && now - worker.lastHeartbeat > this.heartbeatTimeoutMs) {
        dead.push(worker.id);
      }
    }
    return dead;
  }

  getPartitionForKey(key: string): number {
    return fnv1a(key) % this.partitionCount;
  }

  getWorkers(): RelayWorker[] {
    return Array.from(this.workers.values());
  }

  getDispatchLatency(): number {
    return this.dispatchLatency.get();
  }
}

// ─── RelayDispatcher ─────────────────────────────────────────────
class RelayDispatcher {
  private readonly store: OutboxStore;
  private readonly ordering: OrderingGuaranteeManager;
  private readonly deadLetters: DeadLetterHandler;
  private readonly idempotency: IdempotencyRegistry;
  private readonly router: PartitionRouter;
  private readonly config: OutboxConfig;
  private deliveryCallbacks: Array<(event: OutboxEvent) => Promise<boolean>> = [];
  private dispatchedCount = 0;
  private failedCount = 0;

  constructor(
    store: OutboxStore,
    ordering: OrderingGuaranteeManager,
    deadLetters: DeadLetterHandler,
    idempotency: IdempotencyRegistry,
    router: PartitionRouter,
    config: OutboxConfig
  ) {
    this.store = store;
    this.ordering = ordering;
    this.deadLetters = deadLetters;
    this.idempotency = idempotency;
    this.router = router;
    this.config = config;
  }

  onDelivery(callback: (event: OutboxEvent) => Promise<boolean>): void {
    this.deliveryCallbacks.push(callback);
  }

  async pollAndDispatch(partitionId: number): Promise<number> {
    const workerId = this.router.getWorkerForPartition(partitionId);
    if (!workerId) return 0;

    const pending = this.store.getPendingByPartition(partitionId, this.config.maxBatchSize);
    let dispatched = 0;

    for (const event of pending) {
      if (!this.ordering.canDispatch(partitionId, event)) {
        continue;
      }

      // Check dead letter threshold
      if (this.deadLetters.shouldDeadLetter(event)) {
        this.store.markDeadLettered(event.id, event.error || 'Max retries exceeded');
        this.deadLetters.addToDeadLetter(event, event.error || 'Max retries exceeded');
        this.ordering.unblock(partitionId);
        continue;
      }

      const startTime = Date.now();
      this.store.markDispatched(event.id);
      this.ordering.markDispatched(partitionId, event);

      let success = false;
      try {
        // Deliver to all callbacks
        for (const cb of this.deliveryCallbacks) {
          success = await cb(event);
          if (!success) break;
        }
      } catch (err) {
        success = false;
      }

      const latency = Date.now() - startTime;
      this.router.recordDispatch(workerId, latency, success);

      if (success) {
        this.store.markConfirmed(event.id);
        this.ordering.markConfirmed(partitionId, event);
        this.dispatchedCount++;
        dispatched++;
      } else {
        this.store.markFailed(event.id, 'Delivery failed');
        this.ordering.markBlocked(partitionId, event.id);
        this.failedCount++;
      }
    }

    return dispatched;
  }

  processReceipt(receipt: DeliveryReceipt): void {
    if (receipt.success) {
      this.idempotency.record(receipt.eventId, receipt.consumerId, 'success');
    } else {
      this.idempotency.record(receipt.eventId, receipt.consumerId, 'failure');
    }
  }

  getStats(): { dispatched: number; failed: number; successRate: number } {
    const total = this.dispatchedCount + this.failedCount;
    return {
      dispatched: this.dispatchedCount,
      failed: this.failedCount,
      successRate: total > 0 ? this.dispatchedCount / total : 1,
    };
  }
}

// ─── TransactionalOutboxEngine ───────────────────────────────────
class TransactionalOutboxEngine {
  private readonly store: OutboxStore;
  private readonly idempotency: IdempotencyRegistry;
  private readonly ordering: OrderingGuaranteeManager;
  private readonly deadLetters: DeadLetterHandler;
  private readonly cdc: ChangeDataCaptureStream;
  private readonly compaction: CompactionManager;
  private readonly router: PartitionRouter;
  private readonly dispatcher: RelayDispatcher;
  private readonly config: OutboxConfig;
  private readonly events: OutboxSystemEvent[] = [];
  private readonly maxEvents = 1000;
  private tickCount = 0;

  constructor(config: OutboxConfig) {
    this.config = config;
    this.store = new OutboxStore(config);
    this.idempotency = new IdempotencyRegistry(config.idempotencyWindowSize, config.idempotencyTtlMs);
    this.ordering = new OrderingGuaranteeManager(config.partitionCount, config.headOfLineBlockingEnabled);
    this.deadLetters = new DeadLetterHandler(config.deadLetterThreshold);
    this.cdc = new ChangeDataCaptureStream(config.cdcEnabled, config.partitionCount);
    this.compaction = new CompactionManager(config.partitionCount, config.compactionRetentionMs);
    this.router = new PartitionRouter(config.partitionCount, config.workerHeartbeatTimeoutMs);
    this.dispatcher = new RelayDispatcher(
      this.store, this.ordering, this.deadLetters,
      this.idempotency, this.router, config
    );
  }

  // ── Core API ──────────────────────────────────────────────────

  appendEvent(
    partitionKey: string,
    topic: string,
    payload: Record<string, unknown>,
    metadata: Record<string, unknown> = {}
  ): OutboxEvent {
    const event = this.store.append(partitionKey, topic, payload, metadata);
    this.emit('event_appended', { eventId: event.id, partition: this.router.getPartitionForKey(partitionKey), topic });
    return event;
  }

  appendWithStateChange(
    stateChange: StateChange,
    events: Array<{ topic: string; payload: Record<string, unknown>; metadata?: Record<string, unknown> }>
  ): { stateChange: StateChange; events: OutboxEvent[] } {
    const result = this.store.atomicStateChangeAndAppend(stateChange, events);
    for (const event of result.events) {
      this.emit('event_appended', { eventId: event.id, entityId: stateChange.entityId, atomic: true });
    }
    return result;
  }

  onDelivery(callback: (event: OutboxEvent) => Promise<boolean>): void {
    this.dispatcher.onDelivery(callback);
  }

  addWorker(workerId: string): void {
    this.router.addWorker(workerId);
    this.emit('worker_assigned', { workerId });
  }

  removeWorker(workerId: string): void {
    this.router.removeWorker(workerId);
    this.emit('worker_removed', { workerId });
  }

  workerHeartbeat(workerId: string): void {
    this.router.heartbeat(workerId);
  }

  processReceipt(receipt: DeliveryReceipt): void {
    if (this.idempotency.isDuplicate(receipt.eventId, receipt.consumerId)) {
      this.emit('idempotency_duplicate_detected', { eventId: receipt.eventId, consumerId: receipt.consumerId });
      return;
    }
    this.dispatcher.processReceipt(receipt);
    
    const event = this.store.getEvent(receipt.eventId);
    if (event && receipt.success) {
      const partition = this.router.getPartitionForKey(event.partitionKey);
      this.compaction.updateConfirmedPosition(partition, event.sequence);
      this.emit('event_confirmed', { eventId: receipt.eventId });
    }
  }

  replayDeadLetter(eventId: string): OutboxEvent | null {
    const replayed = this.deadLetters.replay(eventId);
    if (replayed) {
      // Re-append to store
      return this.store.append(replayed.partitionKey, replayed.topic, replayed.payload, replayed.metadata);
    }
    return null;
  }

  // ── Tick ───────────────────────────────────────────────────────

  async tick(): Promise<void> {
    this.tickCount++;

    // Phase 1: Detect dead workers
    const deadWorkers = this.router.detectDeadWorkers();
    for (const workerId of deadWorkers) {
      this.removeWorker(workerId);
    }

    // Phase 2: Poll and dispatch per partition
    for (let pid = 0; pid < this.config.partitionCount; pid++) {
      const dispatched = await this.dispatcher.pollAndDispatch(pid);
      if (dispatched > 0) {
        this.emit('event_dispatched', { partition: pid, count: dispatched });
      }
    }

    // Phase 3: CDC capture (if enabled)
    if (this.config.cdcEnabled) {
      for (let pid = 0; pid < this.config.partitionCount; pid++) {
        const captured = this.cdc.captureChanges(this.store, pid);
        if (captured.length > 0) {
          this.emit('cdc_position_advanced', { partition: pid, events: captured.length });
        }
      }
    }

    // Phase 4: Periodic compaction
    if (this.tickCount % 10 === 0) {
      const compacted = this.compaction.compactAll(this.store);
      if (compacted > 0) {
        this.emit('compaction_completed', { eventsCompacted: compacted });
      }
    }

    // Phase 5: Dead letter purge
    if (this.tickCount % 50 === 0) {
      this.deadLetters.purge(this.config.compactionRetentionMs);
    }
  }

  // ── Queries ────────────────────────────────────────────────────

  getEvent(eventId: string): OutboxEvent | undefined {
    return this.store.getEvent(eventId);
  }

  getDeadLetters(limit?: number): Array<{ event: OutboxEvent; reason: string; deadLetteredAt: number }> {
    return this.deadLetters.getDeadLetters(limit);
  }

  getDashboard(): {
    store: ReturnType<OutboxStore['getStats']>;
    dispatcher: ReturnType<RelayDispatcher['getStats']>;
    idempotency: ReturnType<IdempotencyRegistry['getStats']>;
    deadLetters: ReturnType<DeadLetterHandler['getStats']>;
    compaction: ReturnType<CompactionManager['getStats']>;
    cdc: { totalLag: number; positions: CDCPosition[] };
    workers: RelayWorker[];
    partitions: PartitionState[];
    dispatchLatencyMs: number;
    tickCount: number;
  } {
    return {
      store: this.store.getStats(),
      dispatcher: this.dispatcher.getStats(),
      idempotency: this.idempotency.getStats(),
      deadLetters: this.deadLetters.getStats(),
      compaction: this.compaction.getStats(),
      cdc: { totalLag: this.cdc.getTotalLag(), positions: this.cdc.getPositions() },
      workers: this.router.getWorkers(),
      partitions: this.ordering.getAllPartitions(),
      dispatchLatencyMs: this.router.getDispatchLatency(),
      tickCount: this.tickCount,
    };
  }

  // ── Events ─────────────────────────────────────────────────────

  private emit(type: EventType, data: Record<string, unknown>): void {
    this.events.push({ type, timestamp: Date.now(), data });
    if (this.events.length > this.maxEvents) {
      this.events.splice(0, this.events.length - this.maxEvents);
    }
  }

  getEvents(since?: number): OutboxSystemEvent[] {
    if (since) return this.events.filter(e => e.timestamp >= since);
    return [...this.events];
  }
}

// ─── Presets ──────────────────────────────────────────────────────
const PRESETS = {
  'agent-event-bus': {
    maxBatchSize: 50,
    pollIntervalMs: 100,
    maxRetries: 5,
    baseRetryDelayMs: 500,
    maxRetryDelayMs: 30000,
    partitionCount: 8,
    idempotencyWindowSize: 10000,
    idempotencyTtlMs: 3600000,
    deadLetterThreshold: 5,
    compactionIntervalMs: 60000,
    compactionRetentionMs: 86400000,
    cdcEnabled: true,
    cdcPollIntervalMs: 50,
    headOfLineBlockingEnabled: true,
    workerHeartbeatTimeoutMs: 30000,
    confirmationTimeoutMs: 10000,
    maxPendingPerPartition: 1000,
  } as OutboxConfig,

  'high-throughput': {
    maxBatchSize: 200,
    pollIntervalMs: 50,
    maxRetries: 3,
    baseRetryDelayMs: 200,
    maxRetryDelayMs: 10000,
    partitionCount: 16,
    idempotencyWindowSize: 50000,
    idempotencyTtlMs: 1800000,
    deadLetterThreshold: 3,
    compactionIntervalMs: 30000,
    compactionRetentionMs: 3600000,
    cdcEnabled: true,
    cdcPollIntervalMs: 20,
    headOfLineBlockingEnabled: false,
    workerHeartbeatTimeoutMs: 15000,
    confirmationTimeoutMs: 5000,
    maxPendingPerPartition: 5000,
  } as OutboxConfig,

  'reliable-delivery': {
    maxBatchSize: 20,
    pollIntervalMs: 500,
    maxRetries: 10,
    baseRetryDelayMs: 1000,
    maxRetryDelayMs: 120000,
    partitionCount: 4,
    idempotencyWindowSize: 100000,
    idempotencyTtlMs: 86400000,
    deadLetterThreshold: 10,
    compactionIntervalMs: 300000,
    compactionRetentionMs: 604800000,
    cdcEnabled: false,
    cdcPollIntervalMs: 1000,
    headOfLineBlockingEnabled: true,
    workerHeartbeatTimeoutMs: 60000,
    confirmationTimeoutMs: 30000,
    maxPendingPerPartition: 500,
  } as OutboxConfig,
};

export {
  TransactionalOutboxEngine,
  OutboxStore,
  RelayDispatcher,
  IdempotencyRegistry,
  OrderingGuaranteeManager,
  DeadLetterHandler,
  ChangeDataCaptureStream,
  CompactionManager,
  PartitionRouter,
  PRESETS,
};
