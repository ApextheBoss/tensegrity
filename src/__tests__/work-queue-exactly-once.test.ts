import { describe, it, expect, beforeEach, vi } from 'vitest';
import { ExactlyOnceQueue, PRESETS, type QueueConfig } from '../work-queue-exactly-once.js';

function testConfig(overrides?: Partial<QueueConfig>): QueueConfig {
  return {
    ...PRESETS.quickTasks(),
    // Make tests fast
    defaultVisibilityTimeoutMs: 500,
    backoffBaseMs: 10,
    backoffMaxMs: 50,
    backoffJitterRatio: 0,
    dedupWindowMs: 5000,
    compactionRetentionMs: 100,
    ...overrides,
  };
}

describe('ExactlyOnceQueue', () => {
  let q: ExactlyOnceQueue;

  beforeEach(() => {
    q = new ExactlyOnceQueue(testConfig());
  });

  // ── Enqueue ──────────────────────────────────────────────────────

  describe('enqueue', () => {
    it('enqueues a task and returns taskId', () => {
      const result = q.enqueue({ affinityKey: 'a', payload: { x: 1 } });
      expect(result.deduplicated).toBe(false);
      expect(result.taskId).toBeTruthy();
      expect(result.partitionIndex).toBeGreaterThanOrEqual(0);
      expect(q.stats().queueDepth).toBe(1);
    });

    it('uses provided id', () => {
      const result = q.enqueue({ id: 'my-task', affinityKey: 'a', payload: null });
      expect(result.taskId).toBe('my-task');
    });

    it('deduplicates by deduplicationKey', () => {
      const r1 = q.enqueue({ affinityKey: 'a', payload: 1, deduplicationKey: 'dup-1' });
      const r2 = q.enqueue({ affinityKey: 'a', payload: 2, deduplicationKey: 'dup-1' });
      expect(r1.deduplicated).toBe(false);
      expect(r2.deduplicated).toBe(true);
      expect(r2.taskId).toBe(r1.taskId);
      expect(q.stats().queueDepth).toBe(1);
    });

    it('emits duplicate_detected event', () => {
      q.enqueue({ affinityKey: 'a', payload: 1, deduplicationKey: 'dup-x' });
      q.drainEvents();
      q.enqueue({ affinityKey: 'a', payload: 2, deduplicationKey: 'dup-x' });
      const events = q.drainEvents();
      expect(events.some(e => e.type === 'duplicate_detected')).toBe(true);
    });

    it('emits queue_depth_alert when threshold exceeded', () => {
      const q2 = new ExactlyOnceQueue(testConfig({ queueDepthAlertThreshold: 2, partitionCount: 1 }));
      q2.enqueue({ affinityKey: 'a', payload: 1 });
      q2.enqueue({ affinityKey: 'a', payload: 2 });
      const events = q2.drainEvents();
      expect(events.some(e => e.type === 'queue_depth_alert')).toBe(true);
    });

    it('supports delayed tasks', () => {
      q.enqueue({ affinityKey: 'a', payload: 1, delayMs: 60000 });
      // Task is enqueued but not visible
      const claimed = q.claim('worker-1');
      expect(claimed).toBeNull();
    });
  });

  // ── Claim ────────────────────────────────────────────────────────

  describe('claim', () => {
    it('claims the next visible task', () => {
      q.enqueue({ id: 't1', affinityKey: 'a', payload: 'hello' });
      const claimed = q.claim('worker-1');
      expect(claimed).not.toBeNull();
      expect(claimed!.envelope.id).toBe('t1');
      expect(claimed!.envelope.payload).toBe('hello');
      expect(claimed!.fenceToken).toBeGreaterThan(0);
    });

    it('returns null when queue is empty', () => {
      expect(q.claim('worker-1')).toBeNull();
    });

    it('respects maxInFlightPerWorker', () => {
      const q2 = new ExactlyOnceQueue(testConfig({ maxInFlightPerWorker: 1 }));
      q2.enqueue({ id: 't1', affinityKey: 'a', payload: 1 });
      q2.enqueue({ id: 't2', affinityKey: 'a', payload: 2 });
      q2.claim('worker-1');
      expect(q2.claim('worker-1')).toBeNull();
    });

    it('claimed task is not available to other workers', () => {
      q.enqueue({ id: 't1', affinityKey: 'a', payload: 1 });
      q.claim('worker-1');
      expect(q.claim('worker-2')).toBeNull();
    });

    it('claims higher priority tasks first', () => {
      const q2 = new ExactlyOnceQueue(testConfig({ partitionCount: 1 }));
      q2.enqueue({ id: 'low', affinityKey: 'a', payload: 1, priority: 10 });
      q2.enqueue({ id: 'high', affinityKey: 'a', payload: 2, priority: 1 });
      const claimed = q2.claim('worker-1');
      expect(claimed!.envelope.id).toBe('high');
    });

    it('skips tasks past their deadline', () => {
      q.enqueue({ id: 't1', affinityKey: 'a', payload: 1, deadlineMs: Date.now() - 1000 });
      q.enqueue({ id: 't2', affinityKey: 'a', payload: 2 });
      const claimed = q.claim('worker-1');
      expect(claimed!.envelope.id).toBe('t2');
      expect(q.getDeadLetterQueue().length).toBe(1);
    });
  });

  // ── Complete ─────────────────────────────────────────────────────

  describe('complete', () => {
    it('completes a task successfully', () => {
      q.enqueue({ id: 't1', affinityKey: 'a', payload: 1 });
      const claimed = q.claim('worker-1')!;
      const result = q.complete({
        taskId: 't1',
        claimId: claimed.claimId,
        fenceToken: claimed.fenceToken,
        result: 'done',
        workerAgent: 'worker-1',
      });
      expect(result.success).toBe(true);
      expect(q.stats().inFlight).toBe(0);
    });

    it('rejects completion with stale fence token', () => {
      q.enqueue({ id: 't1', affinityKey: 'a', payload: 1 });
      const claimed = q.claim('worker-1')!;
      const result = q.complete({
        taskId: 't1',
        claimId: claimed.claimId,
        fenceToken: claimed.fenceToken + 999,
        result: 'done',
        workerAgent: 'worker-1',
      });
      expect(result.success).toBe(false);
      expect(result.reason).toBe('fence_violation');
    });

    it('emits task_completed event', () => {
      q.enqueue({ id: 't1', affinityKey: 'a', payload: 1 });
      const claimed = q.claim('worker-1')!;
      q.drainEvents();
      q.complete({
        taskId: 't1',
        claimId: claimed.claimId,
        fenceToken: claimed.fenceToken,
        result: 'done',
        workerAgent: 'worker-1',
      });
      const events = q.drainEvents();
      expect(events.some(e => e.type === 'task_completed')).toBe(true);
    });

    it('decrements worker load on completion', () => {
      q.enqueue({ id: 't1', affinityKey: 'a', payload: 1 });
      const claimed = q.claim('worker-1')!;
      expect(q.stats().workerLoads.get('worker-1')).toBe(1);
      q.complete({
        taskId: 't1',
        claimId: claimed.claimId,
        fenceToken: claimed.fenceToken,
        result: 'ok',
        workerAgent: 'worker-1',
      });
      expect(q.stats().workerLoads.has('worker-1')).toBe(false);
    });
  });

  // ── Fail & Retry ─────────────────────────────────────────────────

  describe('fail', () => {
    it('requeues task on failure', () => {
      q.enqueue({ id: 't1', affinityKey: 'a', payload: 1, maxAttempts: 3 });
      const claimed = q.claim('worker-1')!;
      const result = q.fail({
        taskId: 't1',
        claimId: claimed.claimId,
        fenceToken: claimed.fenceToken,
        error: 'oops',
        workerAgent: 'worker-1',
      });
      expect(result.requeued).toBe(true);
      expect(result.deadLettered).toBe(false);
    });

    it('dead-letters after max attempts', () => {
      const q2 = new ExactlyOnceQueue(testConfig({ defaultMaxAttempts: 1 }));
      q2.enqueue({ id: 't1', affinityKey: 'a', payload: 1 });
      const claimed = q2.claim('worker-1')!;
      const result = q2.fail({
        taskId: 't1',
        claimId: claimed.claimId,
        fenceToken: claimed.fenceToken,
        error: 'fail',
        workerAgent: 'worker-1',
      });
      expect(result.deadLettered).toBe(true);
      expect(q2.getDeadLetterQueue().length).toBe(1);
    });

    it('rejects fail with invalid fence token', () => {
      q.enqueue({ id: 't1', affinityKey: 'a', payload: 1 });
      const claimed = q.claim('worker-1')!;
      const result = q.fail({
        taskId: 't1',
        claimId: claimed.claimId,
        fenceToken: 9999,
        error: 'fail',
        workerAgent: 'worker-1',
      });
      expect(result.requeued).toBe(false);
      expect(result.deadLettered).toBe(false);
    });
  });

  // ── Lease Renewal ────────────────────────────────────────────────

  describe('renewLease', () => {
    it('renews lease and returns new fence token', () => {
      q.enqueue({ id: 't1', affinityKey: 'a', payload: 1 });
      const claimed = q.claim('worker-1')!;
      const renewed = q.renewLease({
        taskId: 't1',
        claimId: claimed.claimId,
        currentFenceToken: claimed.fenceToken,
      });
      expect(renewed.fenceToken).toBeGreaterThan(claimed.fenceToken);
      expect(renewed.expiresAt).toBeGreaterThanOrEqual(claimed.leaseExpiresAt);
    });

    it('old fence token is invalid after renewal', () => {
      q.enqueue({ id: 't1', affinityKey: 'a', payload: 1 });
      const claimed = q.claim('worker-1')!;
      const renewed = q.renewLease({
        taskId: 't1',
        claimId: claimed.claimId,
        currentFenceToken: claimed.fenceToken,
      });
      // Complete with old token should fail
      const result = q.complete({
        taskId: 't1',
        claimId: claimed.claimId,
        fenceToken: claimed.fenceToken,
        result: 'done',
        workerAgent: 'worker-1',
      });
      expect(result.success).toBe(false);

      // Complete with new token should succeed
      const result2 = q.complete({
        taskId: 't1',
        claimId: claimed.claimId,
        fenceToken: renewed.fenceToken,
        result: 'done',
        workerAgent: 'worker-1',
      });
      expect(result2.success).toBe(true);
    });
  });

  // ── Lease Expiry ─────────────────────────────────────────────────

  describe('sweepExpiredLeases', () => {
    it('requeues tasks with expired leases', async () => {
      const q2 = new ExactlyOnceQueue(testConfig({
        defaultVisibilityTimeoutMs: 50,
        maxInFlightPerWorker: 5,
        backoffBaseMs: 0,
        backoffMaxMs: 0,
      }));
      q2.enqueue({ id: 't1', affinityKey: 'a', payload: 1, maxAttempts: 3 });
      q2.claim('worker-1');
      
      await new Promise(r => setTimeout(r, 60));
      const swept = q2.sweepExpiredLeases();
      expect(swept).toBe(1);
      
      // Task should be available again (backoff=0 so immediately visible)
      const claimed2 = q2.claim('worker-2');
      expect(claimed2).not.toBeNull();
      expect(claimed2!.envelope.id).toBe('t1');
    });

    it('emits lease_expired event', async () => {
      const q2 = new ExactlyOnceQueue(testConfig({ defaultVisibilityTimeoutMs: 50 }));
      q2.enqueue({ id: 't1', affinityKey: 'a', payload: 1 });
      q2.claim('worker-1');
      q2.drainEvents();

      await new Promise(r => setTimeout(r, 60));
      q2.sweepExpiredLeases();
      const events = q2.drainEvents();
      expect(events.some(e => e.type === 'lease_expired')).toBe(true);
    });
  });

  // ── Compaction ───────────────────────────────────────────────────

  describe('compaction', () => {
    it('removes old completion receipts', async () => {
      const q2 = new ExactlyOnceQueue(testConfig({ compactionRetentionMs: 50 }));
      q2.enqueue({ id: 't1', affinityKey: 'a', payload: 1 });
      const claimed = q2.claim('worker-1')!;
      q2.complete({
        taskId: 't1',
        claimId: claimed.claimId,
        fenceToken: claimed.fenceToken,
        result: 'done',
        workerAgent: 'worker-1',
      });
      expect(q2.stats().completedRetained).toBe(1);

      await new Promise(r => setTimeout(r, 60));
      const result = q2.runCompaction();
      expect(result.removedCount).toBeGreaterThanOrEqual(1);
    });
  });

  // ── Dead Letter Queue ────────────────────────────────────────────

  describe('dead letter queue', () => {
    it('retries dead-lettered tasks', () => {
      const q2 = new ExactlyOnceQueue(testConfig({ defaultMaxAttempts: 1 }));
      q2.enqueue({ id: 't1', affinityKey: 'a', payload: 1 });
      const claimed = q2.claim('worker-1')!;
      q2.fail({
        taskId: 't1',
        claimId: claimed.claimId,
        fenceToken: claimed.fenceToken,
        error: 'fail',
        workerAgent: 'worker-1',
      });
      expect(q2.getDeadLetterQueue().length).toBe(1);

      const retried = q2.retryDeadLetter('t1');
      expect(retried).toBe(true);
      expect(q2.getDeadLetterQueue().length).toBe(0);
      expect(q2.stats().queueDepth).toBe(1);
    });

    it('returns false for unknown task', () => {
      expect(q.retryDeadLetter('nonexistent')).toBe(false);
    });
  });

  // ── Poison Pill Detection ────────────────────────────────────────

  describe('poison pill detection', () => {
    it('quarantines tasks that fail repeatedly', () => {
      const q2 = new ExactlyOnceQueue(testConfig({
        poisonPillThreshold: 2,
        defaultMaxAttempts: 10,
        maxInFlightPerWorker: 10,
        defaultVisibilityTimeoutMs: 10,
        backoffBaseMs: 0,
        backoffMaxMs: 0,
      }));
      q2.enqueue({ id: 't1', affinityKey: 'a', payload: 1, maxAttempts: 10 });

      // Fail twice — with backoff=0 the task is immediately re-claimable
      for (let i = 0; i < 2; i++) {
        const claimed = q2.claim(`worker-${i}`)!;
        expect(claimed).not.toBeNull();
        q2.fail({
          taskId: 't1',
          claimId: claimed.claimId,
          fenceToken: claimed.fenceToken,
          error: 'crash',
          workerAgent: `worker-${i}`,
        });
      }

      // The second failure should trigger poison pill (threshold=2), so task is dead-lettered
      expect(q2.getDeadLetterQueue().length).toBe(1);
      expect(q2.getDeadLetterQueue()[0].reason).toBe('max_attempts');
    });
  });

  // ── Partitioning ─────────────────────────────────────────────────

  describe('partitioning', () => {
    it('routes same affinity key to same partition', () => {
      const q2 = new ExactlyOnceQueue(testConfig({ partitionCount: 8 }));
      const r1 = q2.enqueue({ affinityKey: 'user-42', payload: 1 });
      const r2 = q2.enqueue({ affinityKey: 'user-42', payload: 2 });
      expect(r1.partitionIndex).toBe(r2.partitionIndex);
    });

    it('allows claiming from specific partition', () => {
      const q2 = new ExactlyOnceQueue(testConfig({ partitionCount: 4 }));
      const r1 = q2.enqueue({ affinityKey: 'a', payload: 1 });
      const claimed = q2.claim('worker-1', { preferPartition: r1.partitionIndex });
      expect(claimed).not.toBeNull();
    });
  });

  // ── Stats ────────────────────────────────────────────────────────

  describe('stats', () => {
    it('reports accurate stats', () => {
      q.enqueue({ id: 't1', affinityKey: 'a', payload: 1 });
      q.enqueue({ id: 't2', affinityKey: 'b', payload: 2 });
      const claimed = q.claim('worker-1')!;
      
      const stats = q.stats();
      expect(stats.queueDepth).toBe(1);
      expect(stats.inFlight).toBe(1);
      expect(stats.deadLettered).toBe(0);
      expect(stats.workerLoads.get('worker-1')).toBe(1);
    });
  });

  // ── Events ───────────────────────────────────────────────────────

  describe('events', () => {
    it('drainEvents returns and clears events', () => {
      q.enqueue({ affinityKey: 'a', payload: 1 });
      const events = q.drainEvents();
      expect(events.length).toBeGreaterThan(0);
      expect(q.drainEvents().length).toBe(0);
    });

    it('produces events for full lifecycle', () => {
      q.enqueue({ id: 't1', affinityKey: 'a', payload: 1 });
      const claimed = q.claim('worker-1')!;
      q.complete({
        taskId: 't1',
        claimId: claimed.claimId,
        fenceToken: claimed.fenceToken,
        result: 'ok',
        workerAgent: 'worker-1',
      });
      const events = q.drainEvents();
      const types = events.map(e => e.type);
      expect(types).toContain('task_enqueued');
      expect(types).toContain('task_claimed');
      expect(types).toContain('task_completed');
    });
  });

  // ── Presets ──────────────────────────────────────────────────────

  describe('presets', () => {
    it('quickTasks creates valid config', () => {
      const q2 = new ExactlyOnceQueue(PRESETS.quickTasks());
      expect(q2.stats().queueDepth).toBe(0);
    });

    it('durableTasks creates valid config', () => {
      const q2 = new ExactlyOnceQueue(PRESETS.durableTasks());
      expect(q2.stats().queueDepth).toBe(0);
    });

    it('batchPipeline creates valid config', () => {
      const q2 = new ExactlyOnceQueue(PRESETS.batchPipeline());
      expect(q2.stats().queueDepth).toBe(0);
    });
  });

  // ── Exactly-Once Guarantee ───────────────────────────────────────

  describe('exactly-once guarantee', () => {
    it('prevents double completion', () => {
      q.enqueue({ id: 't1', affinityKey: 'a', payload: 1 });
      const claimed = q.claim('worker-1')!;
      
      const r1 = q.complete({
        taskId: 't1',
        claimId: claimed.claimId,
        fenceToken: claimed.fenceToken,
        result: 'first',
        workerAgent: 'worker-1',
      });
      expect(r1.success).toBe(true);

      // Second completion should fail (lease released)
      const r2 = q.complete({
        taskId: 't1',
        claimId: claimed.claimId,
        fenceToken: claimed.fenceToken,
        result: 'second',
        workerAgent: 'worker-1',
      });
      expect(r2.success).toBe(false);
    });

    it('rejects completed task re-enqueue via dedup', () => {
      q.enqueue({ id: 't1', affinityKey: 'a', payload: 1, deduplicationKey: 'unique-op' });
      const claimed = q.claim('worker-1')!;
      q.complete({
        taskId: 't1',
        claimId: claimed.claimId,
        fenceToken: claimed.fenceToken,
        result: 'done',
        workerAgent: 'worker-1',
      });

      // Try to re-enqueue with same dedup key
      const r2 = q.enqueue({ affinityKey: 'a', payload: 2, deduplicationKey: 'unique-op' });
      expect(r2.deduplicated).toBe(true);
    });
  });
});
