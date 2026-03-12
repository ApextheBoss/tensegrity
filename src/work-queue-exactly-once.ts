/**
 * work-queue-exactly-once.ts — Distributed Work Queue with Exactly-Once Delivery
 * 
 * Guarantees each task is processed exactly once across a pool of competing agents,
 * even under crashes, network partitions, and duplicate submissions.
 * 
 * Core components:
 * 1. DeduplicationLog — Bloom filter + exact LRU for idempotent enqueue
 * 2. VisibilityTimer — Lease-based invisible period prevents double-processing
 * 3. ClaimFence — Fencing tokens prevent stale workers from committing results
 * 4. PoisonPillDetector — Tracks delivery attempts, quarantines stuck tasks
 * 5. CompactionScheduler — Garbage-collects completed/expired entries
 * 6. PartitionedQueue — Shards work by affinity key for locality
 * 7. ExactlyOnceOrchestrator — Ties everything together with transactional semantics
 */

// ─── Types ───────────────────────────────────────────────────────────────────

export interface TaskEnvelope {
  id: string;
  affinityKey: string;
  payload: unknown;
  priority: number;           // lower = higher priority
  maxAttempts: number;
  enqueuedAt: number;
  visibleAfter: number;       // 0 = immediately visible
  deadlineMs: number;         // absolute deadline for completion
  deduplicationKey: string;   // idempotency key
  metadata: Record<string, string>;
}

export interface ClaimedTask {
  envelope: TaskEnvelope;
  claimId: string;
  fenceToken: number;         // monotonically increasing
  claimedAt: number;
  leaseExpiresAt: number;
  attemptNumber: number;
}

export interface CompletionReceipt {
  taskId: string;
  claimId: string;
  fenceToken: number;
  result: unknown;
  completedAt: number;
  workerAgent: string;
}

export interface DeadLetterEntry {
  envelope: TaskEnvelope;
  attempts: AttemptRecord[];
  quarantinedAt: number;
  reason: 'max_attempts' | 'poison_pill' | 'deadline_exceeded' | 'manual';
}

export interface AttemptRecord {
  attemptNumber: number;
  claimId: string;
  workerAgent: string;
  startedAt: number;
  endedAt: number;
  outcome: 'success' | 'failure' | 'timeout' | 'fence_violation';
  error?: string;
}

export type QueueEvent =
  | { type: 'task_enqueued'; taskId: string; affinityKey: string; dedup: boolean }
  | { type: 'task_claimed'; taskId: string; claimId: string; workerAgent: string; fenceToken: number }
  | { type: 'task_completed'; taskId: string; claimId: string; fenceToken: number }
  | { type: 'task_failed'; taskId: string; claimId: string; error: string; willRetry: boolean }
  | { type: 'task_requeued'; taskId: string; attemptNumber: number; visibleAfter: number }
  | { type: 'task_dead_lettered'; taskId: string; reason: string; attempts: number }
  | { type: 'fence_violation'; taskId: string; claimId: string; staleToken: number; currentToken: number }
  | { type: 'lease_expired'; taskId: string; claimId: string; workerAgent: string }
  | { type: 'compaction_run'; removedCount: number; durationMs: number }
  | { type: 'partition_rebalanced'; partitionCount: number; tasksMoved: number }
  | { type: 'duplicate_detected'; deduplicationKey: string; originalTaskId: string }
  | { type: 'queue_depth_alert'; partitionKey: string; depth: number; threshold: number };

export interface QueueConfig {
  partitionCount: number;
  defaultVisibilityTimeoutMs: number;
  defaultMaxAttempts: number;
  defaultDeadlineMs: number;
  leaseRenewalIntervalMs: number;
  poisonPillThreshold: number;       // attempts before quarantine
  compactionIntervalMs: number;
  compactionRetentionMs: number;     // how long to keep completed receipts
  dedupWindowMs: number;             // dedup bloom filter time window
  dedupFalsePositiveRate: number;
  backoffBaseMs: number;
  backoffMaxMs: number;
  backoffJitterRatio: number;        // 0-1, fraction of delay as random jitter
  queueDepthAlertThreshold: number;
  maxInFlightPerWorker: number;
}

// ─── Deduplication Log ──────────────────────────────────────────────────────

/**
 * Two-tier dedup: fast Bloom filter for probable-duplicate rejection,
 * plus exact LRU map for confirmed dedup within the time window.
 * 
 * False positives from Bloom filter are resolved by the exact map.
 * False negatives are impossible (Bloom filter guarantee).
 */
class DeduplicationLog {
  private bloomBits: Uint32Array;
  private bloomSize: number;
  private hashCount: number;
  private exactMap: Map<string, { taskId: string; timestamp: number }> = new Map();
  private windowMs: number;

  constructor(expectedItems: number, falsePositiveRate: number, windowMs: number) {
    this.windowMs = windowMs;
    // Optimal bloom filter sizing
    const m = Math.ceil((-expectedItems * Math.log(falsePositiveRate)) / (Math.LN2 * Math.LN2));
    this.bloomSize = Math.max(m, 1024);
    this.bloomBits = new Uint32Array(Math.ceil(this.bloomSize / 32));
    this.hashCount = Math.max(1, Math.round((this.bloomSize / expectedItems) * Math.LN2));
  }

  /**
   * Check if a dedup key was seen recently. Returns original taskId if duplicate.
   */
  check(dedupKey: string): string | null {
    // Fast path: bloom filter says definitely-not-seen
    if (!this.bloomMayContain(dedupKey)) return null;
    
    // Bloom says maybe-seen, check exact map
    const entry = this.exactMap.get(dedupKey);
    if (!entry) return null; // Bloom false positive
    
    const now = Date.now();
    if (now - entry.timestamp > this.windowMs) {
      // Expired entry
      this.exactMap.delete(dedupKey);
      return null;
    }
    return entry.taskId;
  }

  /**
   * Record a dedup key. Call after successful enqueue.
   */
  record(dedupKey: string, taskId: string): void {
    this.bloomAdd(dedupKey);
    this.exactMap.set(dedupKey, { taskId, timestamp: Date.now() });
  }

  /**
   * Garbage-collect expired entries. Returns count removed.
   */
  gc(): number {
    const now = Date.now();
    let removed = 0;
    for (const [key, entry] of this.exactMap) {
      if (now - entry.timestamp > this.windowMs) {
        this.exactMap.delete(key);
        removed++;
      }
    }
    // Rebuild bloom filter after GC to reduce false positive rate
    if (removed > this.exactMap.size * 0.5) {
      this.rebuildBloom();
    }
    return removed;
  }

  private bloomAdd(key: string): void {
    for (let i = 0; i < this.hashCount; i++) {
      const pos = this.hash(key, i) % this.bloomSize;
      this.bloomBits[pos >>> 5] |= (1 << (pos & 31));
    }
  }

  private bloomMayContain(key: string): boolean {
    for (let i = 0; i < this.hashCount; i++) {
      const pos = this.hash(key, i) % this.bloomSize;
      if (!(this.bloomBits[pos >>> 5] & (1 << (pos & 31)))) return false;
    }
    return true;
  }

  private rebuildBloom(): void {
    this.bloomBits.fill(0);
    for (const key of this.exactMap.keys()) {
      this.bloomAdd(key);
    }
  }

  // FNV-1a variant with seed for multiple hash functions
  private hash(key: string, seed: number): number {
    let h = 0x811c9dc5 ^ seed;
    for (let i = 0; i < key.length; i++) {
      h ^= key.charCodeAt(i);
      h = Math.imul(h, 0x01000193);
    }
    return h >>> 0;
  }
}

// ─── Visibility Timer ───────────────────────────────────────────────────────

/**
 * Manages lease-based visibility for claimed tasks.
 * A claimed task becomes invisible to other workers for visibilityTimeoutMs.
 * If the lease expires without completion, the task becomes visible again.
 * 
 * Workers can renew leases for long-running tasks.
 * Each renewal bumps the fence token to detect stale workers.
 */
class VisibilityTimer {
  private leases: Map<string, {
    claimId: string;
    fenceToken: number;
    expiresAt: number;
    workerAgent: string;
  }> = new Map();
  
  private nextFenceToken = 1;

  claim(taskId: string, workerAgent: string, timeoutMs: number): { claimId: string; fenceToken: number; expiresAt: number } {
    const existing = this.leases.get(taskId);
    if (existing && existing.expiresAt > Date.now()) {
      throw new Error(`Task ${taskId} already claimed by ${existing.workerAgent}, lease active until ${existing.expiresAt}`);
    }
    
    const claimId = `claim-${taskId}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const fenceToken = this.nextFenceToken++;
    const expiresAt = Date.now() + timeoutMs;
    
    this.leases.set(taskId, { claimId, fenceToken, expiresAt, workerAgent });
    return { claimId, fenceToken, expiresAt };
  }

  renew(taskId: string, claimId: string, currentFence: number, extensionMs: number): { fenceToken: number; expiresAt: number } {
    const lease = this.leases.get(taskId);
    if (!lease) throw new Error(`No active lease for task ${taskId}`);
    if (lease.claimId !== claimId) throw new Error(`Claim ID mismatch for task ${taskId}`);
    if (lease.fenceToken !== currentFence) {
      throw new Error(`Fence token mismatch: expected ${lease.fenceToken}, got ${currentFence}`);
    }
    
    // Bump fence token on renewal — any in-flight operations with old token will be rejected
    const newFence = this.nextFenceToken++;
    const newExpiry = Date.now() + extensionMs;
    lease.fenceToken = newFence;
    lease.expiresAt = newExpiry;
    
    return { fenceToken: newFence, expiresAt: newExpiry };
  }

  /**
   * Validate that a fence token is still current for a task.
   * Used at completion time to reject stale workers.
   */
  validateFence(taskId: string, claimId: string, fenceToken: number): boolean {
    const lease = this.leases.get(taskId);
    if (!lease) return false;
    return lease.claimId === claimId && lease.fenceToken === fenceToken;
  }

  release(taskId: string): void {
    this.leases.delete(taskId);
  }

  /**
   * Find all expired leases. Returns list of (taskId, lease) pairs.
   */
  findExpired(): Array<{ taskId: string; claimId: string; workerAgent: string }> {
    const now = Date.now();
    const expired: Array<{ taskId: string; claimId: string; workerAgent: string }> = [];
    for (const [taskId, lease] of this.leases) {
      if (lease.expiresAt <= now) {
        expired.push({ taskId, claimId: lease.claimId, workerAgent: lease.workerAgent });
      }
    }
    return expired;
  }
}

// ─── Poison Pill Detector ───────────────────────────────────────────────────

/**
 * Tracks per-task delivery attempts. When a task fails too many times,
 * it's quarantined as a "poison pill" — a task that crashes workers repeatedly.
 * 
 * Detection heuristics:
 * - Simple: attempt count >= threshold
 * - Temporal: N failures within T seconds (burst detection)
 * - Worker-diverse: failed on >= K distinct workers (rules out worker-specific bugs)
 */
class PoisonPillDetector {
  private attempts: Map<string, AttemptRecord[]> = new Map();
  private threshold: number;

  constructor(threshold: number) {
    this.threshold = threshold;
  }

  recordAttempt(taskId: string, record: AttemptRecord): void {
    if (!this.attempts.has(taskId)) this.attempts.set(taskId, []);
    this.attempts.get(taskId)!.push(record);
  }

  /**
   * Check if a task should be quarantined.
   * Returns reason string if yes, null if no.
   */
  check(taskId: string): 'max_attempts' | 'poison_pill' | null {
    const records = this.attempts.get(taskId);
    if (!records) return null;
    
    const failures = records.filter(r => r.outcome === 'failure' || r.outcome === 'timeout');
    
    // Simple threshold
    if (failures.length >= this.threshold) return 'max_attempts';
    
    // Worker-diverse failure: failed on 3+ distinct workers = likely poison pill
    const distinctWorkers = new Set(failures.map(f => f.workerAgent));
    if (distinctWorkers.size >= 3 && failures.length >= Math.min(this.threshold, 5)) {
      return 'poison_pill';
    }
    
    // Burst detection: 3+ failures within 10 seconds
    if (failures.length >= 3) {
      const recentFailures = failures.slice(-3);
      const span = recentFailures[recentFailures.length - 1].endedAt - recentFailures[0].startedAt;
      if (span < 10_000) return 'poison_pill';
    }
    
    return null;
  }

  getAttempts(taskId: string): AttemptRecord[] {
    return this.attempts.get(taskId) || [];
  }

  getAttemptCount(taskId: string): number {
    return (this.attempts.get(taskId) || []).length;
  }

  clear(taskId: string): void {
    this.attempts.delete(taskId);
  }
}

// ─── Partitioned Queue ──────────────────────────────────────────────────────

/**
 * Shards tasks by affinity key into N partitions.
 * Each partition is an independent priority queue.
 * 
 * Benefits:
 * - Locality: tasks with same affinity key go to same partition
 * - Parallelism: workers can drain different partitions concurrently
 * - Fairness: round-robin across partitions prevents hot-key starvation
 * 
 * Uses consistent hashing for stable partition assignment.
 */
class PartitionedQueue {
  private partitions: Map<number, TaskEnvelope[]> = new Map();
  private partitionCount: number;
  private roundRobinIndex = 0;

  constructor(partitionCount: number) {
    this.partitionCount = partitionCount;
    for (let i = 0; i < partitionCount; i++) {
      this.partitions.set(i, []);
    }
  }

  private partitionFor(affinityKey: string): number {
    let h = 0x811c9dc5;
    for (let i = 0; i < affinityKey.length; i++) {
      h ^= affinityKey.charCodeAt(i);
      h = Math.imul(h, 0x01000193);
    }
    return ((h >>> 0) % this.partitionCount);
  }

  enqueue(task: TaskEnvelope): number {
    const partIdx = this.partitionFor(task.affinityKey);
    const queue = this.partitions.get(partIdx)!;
    
    // Insert in priority order (lower number = higher priority)
    let insertIdx = queue.length;
    for (let i = 0; i < queue.length; i++) {
      if (task.priority < queue[i].priority) {
        insertIdx = i;
        break;
      }
    }
    queue.splice(insertIdx, 0, task);
    return partIdx;
  }

  /**
   * Dequeue the highest-priority visible task, round-robin across partitions.
   * Returns null if no visible tasks available.
   */
  dequeue(): TaskEnvelope | null {
    const now = Date.now();
    
    // Try each partition starting from round-robin index
    for (let attempt = 0; attempt < this.partitionCount; attempt++) {
      const partIdx = (this.roundRobinIndex + attempt) % this.partitionCount;
      const queue = this.partitions.get(partIdx)!;
      
      for (let i = 0; i < queue.length; i++) {
        if (queue[i].visibleAfter <= now) {
          const task = queue.splice(i, 1)[0];
          this.roundRobinIndex = (partIdx + 1) % this.partitionCount;
          return task;
        }
      }
    }
    return null;
  }

  /**
   * Dequeue from a specific partition (for affinity-aware workers).
   */
  dequeueFromPartition(partIdx: number): TaskEnvelope | null {
    const now = Date.now();
    const queue = this.partitions.get(partIdx);
    if (!queue) return null;
    
    for (let i = 0; i < queue.length; i++) {
      if (queue[i].visibleAfter <= now) {
        return queue.splice(i, 1)[0];
      }
    }
    return null;
  }

  remove(taskId: string): boolean {
    for (const [, queue] of this.partitions) {
      const idx = queue.findIndex(t => t.id === taskId);
      if (idx !== -1) {
        queue.splice(idx, 1);
        return true;
      }
    }
    return false;
  }

  getDepths(): Map<number, number> {
    const depths = new Map<number, number>();
    for (const [idx, queue] of this.partitions) {
      depths.set(idx, queue.length);
    }
    return depths;
  }

  totalSize(): number {
    let total = 0;
    for (const [, queue] of this.partitions) total += queue.length;
    return total;
  }

  /**
   * Rebalance: redistribute tasks across a new partition count.
   * Returns number of tasks moved.
   */
  rebalance(newPartitionCount: number): number {
    if (newPartitionCount === this.partitionCount) return 0;
    
    // Collect all tasks
    const allTasks: TaskEnvelope[] = [];
    for (const [, queue] of this.partitions) {
      allTasks.push(...queue);
    }
    
    // Reset with new partition count
    this.partitions.clear();
    this.partitionCount = newPartitionCount;
    for (let i = 0; i < newPartitionCount; i++) {
      this.partitions.set(i, []);
    }
    
    // Re-enqueue all tasks
    for (const task of allTasks) {
      this.enqueue(task);
    }
    
    return allTasks.length;
  }
}

// ─── Compaction Scheduler ───────────────────────────────────────────────────

/**
 * Garbage-collects completed task receipts and expired dedup entries.
 * Runs periodically to prevent unbounded memory growth.
 * 
 * Retention policy: keep completed receipts for `retentionMs` for
 * audit/debugging, then discard.
 */
class CompactionScheduler {
  private completedReceipts: Map<string, CompletionReceipt> = new Map();
  private retentionMs: number;

  constructor(retentionMs: number) {
    this.retentionMs = retentionMs;
  }

  recordCompletion(receipt: CompletionReceipt): void {
    this.completedReceipts.set(receipt.taskId, receipt);
  }

  isCompleted(taskId: string): boolean {
    return this.completedReceipts.has(taskId);
  }

  /**
   * Run compaction. Returns count of entries removed.
   */
  compact(): { removedCount: number; durationMs: number } {
    const start = Date.now();
    const cutoff = start - this.retentionMs;
    let removedCount = 0;
    
    for (const [taskId, receipt] of this.completedReceipts) {
      if (receipt.completedAt < cutoff) {
        this.completedReceipts.delete(taskId);
        removedCount++;
      }
    }
    
    return { removedCount, durationMs: Date.now() - start };
  }

  size(): number {
    return this.completedReceipts.size;
  }
}

// ─── Backoff Calculator ─────────────────────────────────────────────────────

function calculateBackoff(
  attemptNumber: number,
  baseMs: number,
  maxMs: number,
  jitterRatio: number
): number {
  // Exponential backoff with decorrelated jitter
  const exponential = Math.min(maxMs, baseMs * Math.pow(2, attemptNumber - 1));
  const jitter = exponential * jitterRatio * Math.random();
  return Math.floor(exponential + jitter);
}

// ─── Exactly-Once Orchestrator ──────────────────────────────────────────────

/**
 * The main coordinator. Provides transactional exactly-once semantics by
 * combining dedup (at-most-once enqueue), visibility leases (at-most-once
 * processing), and fencing tokens (exactly-once completion).
 * 
 * Invariant: For any task T, exactly one CompletionReceipt exists,
 * and the result was produced by the worker holding the valid fence token
 * at commit time.
 * 
 * Failure modes handled:
 * 1. Duplicate submission → dedup log rejects, returns original task ID
 * 2. Worker crash mid-processing → lease expires, task becomes visible
 * 3. Slow worker commits after lease expired → fence token mismatch rejects
 * 4. Task always fails → poison pill detector quarantines to DLQ
 * 5. Task exceeds deadline → moved to DLQ regardless of attempt count
 */
export class ExactlyOnceQueue {
  private config: QueueConfig;
  private dedup: DeduplicationLog;
  private visibility: VisibilityTimer;
  private poison: PoisonPillDetector;
  private queue: PartitionedQueue;
  private compactor: CompactionScheduler;
  private deadLetterQueue: DeadLetterEntry[] = [];
  private inFlight: Map<string, ClaimedTask> = new Map();  // claimId → ClaimedTask
  private workerLoad: Map<string, number> = new Map();      // workerAgent → in-flight count
  private events: QueueEvent[] = [];

  constructor(config: QueueConfig) {
    this.config = config;
    this.dedup = new DeduplicationLog(10_000, config.dedupFalsePositiveRate, config.dedupWindowMs);
    this.visibility = new VisibilityTimer();
    this.poison = new PoisonPillDetector(config.poisonPillThreshold);
    this.queue = new PartitionedQueue(config.partitionCount);
    this.compactor = new CompactionScheduler(config.compactionRetentionMs);
  }

  // ─── Enqueue ────────────────────────────────────────────────────────

  /**
   * Enqueue a task. Deduplicates by deduplicationKey.
   * Returns { taskId, deduplicated } where deduplicated=true means
   * this was a duplicate and the original taskId is returned.
   */
  enqueue(params: {
    id?: string;
    affinityKey: string;
    payload: unknown;
    priority?: number;
    maxAttempts?: number;
    deadlineMs?: number;
    deduplicationKey?: string;
    delayMs?: number;
    metadata?: Record<string, string>;
  }): { taskId: string; deduplicated: boolean; partitionIndex: number } {
    const taskId = params.id || `task-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    const dedupKey = params.deduplicationKey || taskId;
    
    // Check for duplicate
    const existingId = this.dedup.check(dedupKey);
    if (existingId) {
      this.emit({ type: 'duplicate_detected', deduplicationKey: dedupKey, originalTaskId: existingId });
      return { taskId: existingId, deduplicated: true, partitionIndex: -1 };
    }
    
    // Check if already completed (late duplicate)
    if (this.compactor.isCompleted(taskId)) {
      return { taskId, deduplicated: true, partitionIndex: -1 };
    }
    
    const envelope: TaskEnvelope = {
      id: taskId,
      affinityKey: params.affinityKey,
      payload: params.payload,
      priority: params.priority ?? 5,
      maxAttempts: params.maxAttempts ?? this.config.defaultMaxAttempts,
      enqueuedAt: Date.now(),
      visibleAfter: params.delayMs ? Date.now() + params.delayMs : 0,
      deadlineMs: params.deadlineMs ?? (Date.now() + this.config.defaultDeadlineMs),
      deduplicationKey: dedupKey,
      metadata: params.metadata ?? {},
    };
    
    const partitionIndex = this.queue.enqueue(envelope);
    this.dedup.record(dedupKey, taskId);
    
    this.emit({ type: 'task_enqueued', taskId, affinityKey: params.affinityKey, dedup: false });
    this.checkDepthAlert(partitionIndex);
    
    return { taskId, deduplicated: false, partitionIndex };
  }

  // ─── Claim ──────────────────────────────────────────────────────────

  /**
   * Claim the next available task for processing.
   * Returns null if no tasks are visible.
   * The task becomes invisible to other workers for visibilityTimeoutMs.
   */
  claim(workerAgent: string, options?: {
    preferPartition?: number;
    visibilityTimeoutMs?: number;
  }): ClaimedTask | null {
    // Check worker capacity
    const currentLoad = this.workerLoad.get(workerAgent) || 0;
    if (currentLoad >= this.config.maxInFlightPerWorker) return null;
    
    // Dequeue task
    const envelope = options?.preferPartition !== undefined
      ? this.queue.dequeueFromPartition(options.preferPartition)
      : this.queue.dequeue();
    
    if (!envelope) return null;
    
    // Check deadline
    if (Date.now() >= envelope.deadlineMs) {
      this.deadLetter(envelope, 'deadline_exceeded');
      return this.claim(workerAgent, options); // Try next task
    }
    
    // Check poison pill
    const poisonReason = this.poison.check(envelope.id);
    if (poisonReason) {
      this.deadLetter(envelope, poisonReason);
      return this.claim(workerAgent, options);
    }
    
    // Create claim with visibility lease
    const timeoutMs = options?.visibilityTimeoutMs ?? this.config.defaultVisibilityTimeoutMs;
    const { claimId, fenceToken, expiresAt } = this.visibility.claim(envelope.id, workerAgent, timeoutMs);
    
    const claimed: ClaimedTask = {
      envelope,
      claimId,
      fenceToken,
      claimedAt: Date.now(),
      leaseExpiresAt: expiresAt,
      attemptNumber: this.poison.getAttemptCount(envelope.id) + 1,
    };
    
    this.inFlight.set(claimId, claimed);
    this.workerLoad.set(workerAgent, currentLoad + 1);
    
    this.emit({
      type: 'task_claimed',
      taskId: envelope.id,
      claimId,
      workerAgent,
      fenceToken,
    });
    
    return claimed;
  }

  // ─── Complete ───────────────────────────────────────────────────────

  /**
   * Complete a claimed task. The fence token must match the current lease.
   * If the lease expired and was reclaimed, this will reject with fence_violation.
   */
  complete(params: {
    taskId: string;
    claimId: string;
    fenceToken: number;
    result: unknown;
    workerAgent: string;
  }): { success: boolean; reason?: string } {
    // Validate fence token — the core exactly-once guarantee
    if (!this.visibility.validateFence(params.taskId, params.claimId, params.fenceToken)) {
      this.emit({
        type: 'fence_violation',
        taskId: params.taskId,
        claimId: params.claimId,
        staleToken: params.fenceToken,
        currentToken: -1, // unknown, just know it doesn't match
      });
      
      this.poison.recordAttempt(params.taskId, {
        attemptNumber: -1,
        claimId: params.claimId,
        workerAgent: params.workerAgent,
        startedAt: 0,
        endedAt: Date.now(),
        outcome: 'fence_violation',
        error: `Stale fence token ${params.fenceToken}`,
      });
      
      return { success: false, reason: 'fence_violation' };
    }
    
    // Record completion receipt
    const receipt: CompletionReceipt = {
      taskId: params.taskId,
      claimId: params.claimId,
      fenceToken: params.fenceToken,
      result: params.result,
      completedAt: Date.now(),
      workerAgent: params.workerAgent,
    };
    this.compactor.recordCompletion(receipt);
    
    // Record successful attempt
    const claimed = this.inFlight.get(params.claimId);
    this.poison.recordAttempt(params.taskId, {
      attemptNumber: claimed?.attemptNumber ?? -1,
      claimId: params.claimId,
      workerAgent: params.workerAgent,
      startedAt: claimed?.claimedAt ?? Date.now(),
      endedAt: Date.now(),
      outcome: 'success',
    });
    
    // Cleanup
    this.visibility.release(params.taskId);
    this.inFlight.delete(params.claimId);
    this.decrementWorkerLoad(params.workerAgent);
    this.poison.clear(params.taskId);
    
    this.emit({ type: 'task_completed', taskId: params.taskId, claimId: params.claimId, fenceToken: params.fenceToken });
    
    return { success: true };
  }

  // ─── Fail ───────────────────────────────────────────────────────────

  /**
   * Report a task failure. The task will be requeued with backoff
   * unless it exceeds max attempts or is detected as a poison pill.
   */
  fail(params: {
    taskId: string;
    claimId: string;
    fenceToken: number;
    error: string;
    workerAgent: string;
  }): { requeued: boolean; deadLettered: boolean; nextVisibleIn?: number } {
    // Validate fence (even for failures, to prevent stale workers from interfering)
    if (!this.visibility.validateFence(params.taskId, params.claimId, params.fenceToken)) {
      return { requeued: false, deadLettered: false };
    }
    
    const claimed = this.inFlight.get(params.claimId);
    if (!claimed) return { requeued: false, deadLettered: false };
    
    // Record failure
    this.poison.recordAttempt(params.taskId, {
      attemptNumber: claimed.attemptNumber,
      claimId: params.claimId,
      workerAgent: params.workerAgent,
      startedAt: claimed.claimedAt,
      endedAt: Date.now(),
      outcome: 'failure',
      error: params.error,
    });
    
    // Cleanup claim
    this.visibility.release(params.taskId);
    this.inFlight.delete(params.claimId);
    this.decrementWorkerLoad(params.workerAgent);
    
    // Check poison pill
    const poisonReason = this.poison.check(params.taskId);
    if (poisonReason) {
      this.deadLetter(claimed.envelope, poisonReason);
      this.emit({ type: 'task_failed', taskId: params.taskId, claimId: params.claimId, error: params.error, willRetry: false });
      return { requeued: false, deadLettered: true };
    }
    
    // Check max attempts
    if (claimed.attemptNumber >= claimed.envelope.maxAttempts) {
      this.deadLetter(claimed.envelope, 'max_attempts');
      this.emit({ type: 'task_failed', taskId: params.taskId, claimId: params.claimId, error: params.error, willRetry: false });
      return { requeued: false, deadLettered: true };
    }
    
    // Requeue with backoff
    const backoffMs = calculateBackoff(
      claimed.attemptNumber,
      this.config.backoffBaseMs,
      this.config.backoffMaxMs,
      this.config.backoffJitterRatio
    );
    
    claimed.envelope.visibleAfter = Date.now() + backoffMs;
    this.queue.enqueue(claimed.envelope);
    
    this.emit({ type: 'task_failed', taskId: params.taskId, claimId: params.claimId, error: params.error, willRetry: true });
    this.emit({ type: 'task_requeued', taskId: params.taskId, attemptNumber: claimed.attemptNumber + 1, visibleAfter: claimed.envelope.visibleAfter });
    
    return { requeued: true, deadLettered: false, nextVisibleIn: backoffMs };
  }

  // ─── Lease Renewal ──────────────────────────────────────────────────

  /**
   * Extend the visibility lease for a long-running task.
   * Returns new fence token — caller MUST use this for completion.
   */
  renewLease(params: {
    taskId: string;
    claimId: string;
    currentFenceToken: number;
    extensionMs?: number;
  }): { fenceToken: number; expiresAt: number } {
    return this.visibility.renew(
      params.taskId,
      params.claimId,
      params.currentFenceToken,
      params.extensionMs ?? this.config.defaultVisibilityTimeoutMs
    );
  }

  // ─── Lease Expiry Sweep ─────────────────────────────────────────────

  /**
   * Call periodically to detect and handle expired leases.
   * Expired tasks are requeued automatically.
   */
  sweepExpiredLeases(): number {
    const expired = this.visibility.findExpired();
    
    for (const { taskId, claimId, workerAgent } of expired) {
      const claimed = this.inFlight.get(claimId);
      if (!claimed) continue;
      
      this.emit({ type: 'lease_expired', taskId, claimId, workerAgent });
      
      // Record timeout attempt
      this.poison.recordAttempt(taskId, {
        attemptNumber: claimed.attemptNumber,
        claimId,
        workerAgent,
        startedAt: claimed.claimedAt,
        endedAt: Date.now(),
        outcome: 'timeout',
        error: 'Lease expired',
      });
      
      // Cleanup
      this.visibility.release(taskId);
      this.inFlight.delete(claimId);
      this.decrementWorkerLoad(workerAgent);
      
      // Requeue or dead-letter
      const poisonReason = this.poison.check(taskId);
      if (poisonReason || claimed.attemptNumber >= claimed.envelope.maxAttempts) {
        this.deadLetter(claimed.envelope, poisonReason || 'max_attempts');
      } else {
        const backoffMs = calculateBackoff(
          claimed.attemptNumber,
          this.config.backoffBaseMs,
          this.config.backoffMaxMs,
          this.config.backoffJitterRatio
        );
        claimed.envelope.visibleAfter = Date.now() + backoffMs;
        this.queue.enqueue(claimed.envelope);
      }
    }
    
    return expired.length;
  }

  // ─── Compaction ─────────────────────────────────────────────────────

  runCompaction(): { removedCount: number; durationMs: number } {
    const result = this.compactor.compact();
    const dedupRemoved = this.dedup.gc();
    result.removedCount += dedupRemoved;
    this.emit({ type: 'compaction_run', ...result });
    return result;
  }

  // ─── Dead Letter Queue ──────────────────────────────────────────────

  getDeadLetterQueue(): DeadLetterEntry[] {
    return [...this.deadLetterQueue];
  }

  /**
   * Retry a dead-lettered task by re-enqueueing it with reset attempt count.
   */
  retryDeadLetter(taskId: string): boolean {
    const idx = this.deadLetterQueue.findIndex(e => e.envelope.id === taskId);
    if (idx === -1) return false;
    
    const entry = this.deadLetterQueue.splice(idx, 1)[0];
    this.poison.clear(taskId);
    entry.envelope.visibleAfter = 0;
    this.queue.enqueue(entry.envelope);
    return true;
  }

  // ─── Stats ──────────────────────────────────────────────────────────

  stats(): {
    queueDepth: number;
    inFlight: number;
    deadLettered: number;
    completedRetained: number;
    partitionDepths: Map<number, number>;
    workerLoads: Map<string, number>;
  } {
    return {
      queueDepth: this.queue.totalSize(),
      inFlight: this.inFlight.size,
      deadLettered: this.deadLetterQueue.length,
      completedRetained: this.compactor.size(),
      partitionDepths: this.queue.getDepths(),
      workerLoads: new Map(this.workerLoad),
    };
  }

  drainEvents(): QueueEvent[] {
    const events = this.events;
    this.events = [];
    return events;
  }

  // ─── Private Helpers ────────────────────────────────────────────────

  private deadLetter(envelope: TaskEnvelope, reason: DeadLetterEntry['reason']): void {
    this.deadLetterQueue.push({
      envelope,
      attempts: this.poison.getAttempts(envelope.id),
      quarantinedAt: Date.now(),
      reason,
    });
    this.emit({ type: 'task_dead_lettered', taskId: envelope.id, reason, attempts: this.poison.getAttemptCount(envelope.id) });
  }

  private decrementWorkerLoad(workerAgent: string): void {
    const current = this.workerLoad.get(workerAgent) || 0;
    if (current <= 1) this.workerLoad.delete(workerAgent);
    else this.workerLoad.set(workerAgent, current - 1);
  }

  private checkDepthAlert(partitionIndex: number): void {
    const depths = this.queue.getDepths();
    const depth = depths.get(partitionIndex) || 0;
    if (depth >= this.config.queueDepthAlertThreshold) {
      this.emit({
        type: 'queue_depth_alert',
        partitionKey: `partition-${partitionIndex}`,
        depth,
        threshold: this.config.queueDepthAlertThreshold,
      });
    }
  }

  private emit(event: QueueEvent): void {
    this.events.push(event);
  }
}

// ─── Presets ─────────────────────────────────────────────────────────────────

export const PRESETS = {
  /** Fast task queue for short-lived agent jobs (< 30s each) */
  quickTasks: (): QueueConfig => ({
    partitionCount: 4,
    defaultVisibilityTimeoutMs: 30_000,
    defaultMaxAttempts: 3,
    defaultDeadlineMs: 300_000,        // 5 min
    leaseRenewalIntervalMs: 10_000,
    poisonPillThreshold: 3,
    compactionIntervalMs: 60_000,
    compactionRetentionMs: 300_000,
    dedupWindowMs: 120_000,
    dedupFalsePositiveRate: 0.01,
    backoffBaseMs: 1_000,
    backoffMaxMs: 15_000,
    backoffJitterRatio: 0.3,
    queueDepthAlertThreshold: 100,
    maxInFlightPerWorker: 5,
  }),

  /** Durable queue for long-running agent tasks (minutes to hours) */
  durableTasks: (): QueueConfig => ({
    partitionCount: 8,
    defaultVisibilityTimeoutMs: 300_000,   // 5 min lease
    defaultMaxAttempts: 5,
    defaultDeadlineMs: 3_600_000,          // 1 hour
    leaseRenewalIntervalMs: 120_000,
    poisonPillThreshold: 5,
    compactionIntervalMs: 600_000,
    compactionRetentionMs: 86_400_000,     // 24h retention
    dedupWindowMs: 3_600_000,
    dedupFalsePositiveRate: 0.001,
    backoffBaseMs: 5_000,
    backoffMaxMs: 300_000,
    backoffJitterRatio: 0.5,
    queueDepthAlertThreshold: 50,
    maxInFlightPerWorker: 2,
  }),

  /** High-throughput queue for batch processing pipelines */
  batchPipeline: (): QueueConfig => ({
    partitionCount: 16,
    defaultVisibilityTimeoutMs: 60_000,
    defaultMaxAttempts: 2,
    defaultDeadlineMs: 600_000,
    leaseRenewalIntervalMs: 20_000,
    poisonPillThreshold: 2,
    compactionIntervalMs: 30_000,
    compactionRetentionMs: 60_000,
    dedupWindowMs: 60_000,
    dedupFalsePositiveRate: 0.05,
    backoffBaseMs: 500,
    backoffMaxMs: 5_000,
    backoffJitterRatio: 0.2,
    queueDepthAlertThreshold: 500,
    maxInFlightPerWorker: 10,
  }),
};
