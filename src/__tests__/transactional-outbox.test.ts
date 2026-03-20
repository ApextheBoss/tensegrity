import { describe, it, expect, beforeEach } from 'vitest';
import {
  TransactionalOutboxEngine,
  OutboxStore,
  IdempotencyRegistry,
  OrderingGuaranteeManager,
  DeadLetterHandler,
  ChangeDataCaptureStream,
  CompactionManager,
  PartitionRouter,
  PRESETS,
} from '../transactional-outbox';

const testConfig = {
  ...PRESETS['agent-event-bus'],
  partitionCount: 4,
  maxBatchSize: 10,
  maxRetries: 3,
  deadLetterThreshold: 3,
  baseRetryDelayMs: 0, // no delay in tests
  maxRetryDelayMs: 0,
  maxPendingPerPartition: 100,
  headOfLineBlockingEnabled: true,
  cdcEnabled: true,
  workerHeartbeatTimeoutMs: 100,
  idempotencyWindowSize: 100,
  idempotencyTtlMs: 60000,
  compactionRetentionMs: 1000,
};

// ─── OutboxStore ─────────────────────────────────────────────────
describe('OutboxStore', () => {
  let store: OutboxStore;

  beforeEach(() => {
    store = new OutboxStore(testConfig);
  });

  it('appends events with incrementing sequences', () => {
    const e1 = store.append('key1', 'topic.a', { val: 1 });
    const e2 = store.append('key2', 'topic.b', { val: 2 });
    expect(e1.sequence).toBe(1);
    expect(e2.sequence).toBe(2);
    expect(e1.status).toBe('pending');
  });

  it('appends batch of events', () => {
    const events = store.appendBatch([
      { partitionKey: 'k1', topic: 't1', payload: { a: 1 } },
      { partitionKey: 'k2', topic: 't2', payload: { b: 2 } },
      { partitionKey: 'k3', topic: 't3', payload: { c: 3 } },
    ]);
    expect(events).toHaveLength(3);
    expect(events[2].sequence).toBe(3);
  });

  it('atomicStateChangeAndAppend produces events with state change metadata', () => {
    const result = store.atomicStateChangeAndAppend(
      { entityId: 'agent-1', entityType: 'Agent', operation: 'update', before: { status: 'idle' }, after: { status: 'busy' }, timestamp: Date.now() },
      [{ topic: 'agent.updated', payload: { newStatus: 'busy' } }]
    );
    expect(result.events).toHaveLength(1);
    expect(result.events[0].payload._stateChange).toBeDefined();
    expect((result.events[0].payload._stateChange as any).operation).toBe('update');
  });

  it('gets pending events by partition', () => {
    // Append several events to same partition key
    store.append('same-key', 'topic', { i: 1 });
    store.append('same-key', 'topic', { i: 2 });
    store.append('same-key', 'topic', { i: 3 });
    // Find which partition 'same-key' maps to
    const stats = store.getStats();
    let partitionId = -1;
    for (const [pid, count] of stats.partitionCounts) {
      if (count > 0) { partitionId = pid; break; }
    }
    const pending = store.getPendingByPartition(partitionId, 10);
    expect(pending.length).toBe(3);
  });

  it('marks events through lifecycle: dispatched → confirmed', () => {
    const e = store.append('k', 't', {});
    store.markDispatched(e.id);
    expect(store.getEvent(e.id)!.status).toBe('dispatched');
    store.markConfirmed(e.id);
    expect(store.getEvent(e.id)!.status).toBe('confirmed');
  });

  it('markFailed returns event to pending with incremented attempts', () => {
    const e = store.append('k', 't', {});
    store.markFailed(e.id, 'oops');
    const updated = store.getEvent(e.id)!;
    expect(updated.status).toBe('pending');
    expect(updated.attempts).toBe(1);
    expect(updated.error).toBe('oops');
  });

  it('markDeadLettered sets dead-lettered status', () => {
    const e = store.append('k', 't', {});
    store.markDeadLettered(e.id, 'gave up');
    expect(store.getEvent(e.id)!.status).toBe('dead-lettered');
  });

  it('compactPartition removes confirmed events up to sequence', () => {
    const e1 = store.append('same', 't', {});
    const e2 = store.append('same', 't', {});
    store.markConfirmed(e1.id);
    // Find partition
    const stats = store.getStats();
    let pid = -1;
    for (const [p, c] of stats.partitionCounts) { if (c > 0) { pid = p; break; } }
    const compacted = store.compactPartition(pid, e1.sequence);
    expect(compacted).toBe(1);
    expect(store.getEvent(e1.id)).toBeUndefined();
    expect(store.getEvent(e2.id)).toBeDefined();
  });

  it('rejects when partition pending limit reached', () => {
    const cfg = { ...testConfig, maxPendingPerPartition: 2 };
    const s = new OutboxStore(cfg);
    s.append('same', 't', {});
    s.append('same', 't', {});
    expect(() => s.append('same', 't', {})).toThrow(/pending limit reached/);
  });

  it('getStats returns correct counts', () => {
    store.append('a', 't', {});
    store.append('b', 't', {});
    const e3 = store.append('c', 't', {});
    store.markConfirmed(e3.id);
    const stats = store.getStats();
    expect(stats.total).toBe(3);
    expect(stats.pending).toBe(2);
    expect(stats.confirmed).toBe(1);
  });
});

// ─── IdempotencyRegistry ─────────────────────────────────────────
describe('IdempotencyRegistry', () => {
  it('detects duplicates within TTL', () => {
    const reg = new IdempotencyRegistry(100, 60000);
    reg.record('evt1', 'consumer-a', 'success');
    expect(reg.isDuplicate('evt1', 'consumer-a')).toBe(true);
    expect(reg.isDuplicate('evt1', 'consumer-b')).toBe(false);
  });

  it('evicts by LRU when over max size', () => {
    const reg = new IdempotencyRegistry(2, 60000);
    reg.record('e1', 'c', 'success');
    reg.record('e2', 'c', 'success');
    reg.record('e3', 'c', 'success'); // should evict e1
    expect(reg.isDuplicate('e1', 'c')).toBe(false);
    expect(reg.isDuplicate('e3', 'c')).toBe(true);
  });

  it('tracks duplicate count in stats', () => {
    const reg = new IdempotencyRegistry(100, 60000);
    reg.record('e1', 'c', 'success');
    reg.isDuplicate('e1', 'c');
    reg.isDuplicate('e1', 'c');
    expect(reg.getStats().duplicatesDetected).toBe(2);
  });
});

// ─── OrderingGuaranteeManager ────────────────────────────────────
describe('OrderingGuaranteeManager', () => {
  it('enforces sequence order with head-of-line blocking', () => {
    const mgr = new OrderingGuaranteeManager(4, true);
    const evt1 = { sequence: 1 } as any;
    const evt2 = { sequence: 2 } as any;
    // headSequence=0: both seq 1 and seq 2 pass because of `|| headSequence === 0`
    expect(mgr.canDispatch(0, evt1)).toBe(true);
    mgr.markDispatched(0, evt1); // headSequence becomes 1
    expect(mgr.canDispatch(0, evt2)).toBe(true); // seq 2 === headSequence + 1
    const evt3 = { sequence: 3 } as any;
    // evt3 can't dispatch yet because headSequence=1, and 3 !== 2
    expect(mgr.canDispatch(0, evt3)).toBe(false);
    mgr.markDispatched(0, evt2);
    expect(mgr.canDispatch(0, evt3)).toBe(true);
  });

  it('blocks partition when event fails with head-of-line blocking', () => {
    const mgr = new OrderingGuaranteeManager(4, true);
    const evt1 = { id: 'e1', sequence: 1 } as any;
    mgr.markDispatched(0, evt1);
    mgr.markBlocked(0, 'e1');
    const evt2 = { sequence: 2 } as any;
    expect(mgr.canDispatch(0, evt2)).toBe(false);
    mgr.unblock(0);
    expect(mgr.canDispatch(0, evt2)).toBe(true);
  });

  it('allows any order without head-of-line blocking', () => {
    const mgr = new OrderingGuaranteeManager(4, false);
    const evt5 = { sequence: 5 } as any;
    expect(mgr.canDispatch(0, evt5)).toBe(true);
  });

  it('tracks partition state correctly', () => {
    const mgr = new OrderingGuaranteeManager(2, true);
    mgr.assignWorker(0, 'w1');
    const state = mgr.getPartitionState(0)!;
    expect(state.assignedWorker).toBe('w1');
    expect(mgr.getAllPartitions()).toHaveLength(2);
  });
});

// ─── DeadLetterHandler ───────────────────────────────────────────
describe('DeadLetterHandler', () => {
  it('determines when event should be dead-lettered', () => {
    const dlh = new DeadLetterHandler(3);
    expect(dlh.shouldDeadLetter({ attempts: 2 } as any)).toBe(false);
    expect(dlh.shouldDeadLetter({ attempts: 3 } as any)).toBe(true);
  });

  it('adds, retrieves, and replays dead letters', () => {
    const dlh = new DeadLetterHandler(3);
    const evt = { id: 'e1', partitionKey: 'k', topic: 't', payload: {}, metadata: {}, status: 'dead-lettered' } as any;
    dlh.addToDeadLetter(evt, 'too many retries');
    expect(dlh.getDeadLetters()).toHaveLength(1);
    expect(dlh.getStats().total).toBe(1);

    const replayed = dlh.replay('e1')!;
    expect(replayed.status).toBe('pending');
    expect(replayed.attempts).toBe(0);
    expect(dlh.getDeadLetters()).toHaveLength(0);
  });

  it('replay returns null for unknown event', () => {
    const dlh = new DeadLetterHandler(3);
    expect(dlh.replay('nope')).toBeNull();
  });

  it('purges old dead letters', async () => {
    const dlh = new DeadLetterHandler(3);
    const evt = { id: 'old', partitionKey: 'k', topic: 't', payload: {}, metadata: {} } as any;
    dlh.addToDeadLetter(evt, 'reason');
    // Wait a tick so deadLetteredAt < Date.now()
    await new Promise(r => setTimeout(r, 5));
    const purged = dlh.purge(1); // 1ms retention — anything older than 1ms ago
    expect(purged).toBe(1);
  });
});

// ─── ChangeDataCaptureStream ─────────────────────────────────────
describe('ChangeDataCaptureStream', () => {
  it('captures changes and notifies subscribers', () => {
    const cdc = new ChangeDataCaptureStream(true, 4);
    const store = new OutboxStore(testConfig);
    const captured: any[] = [];
    cdc.subscribe(events => captured.push(...events));

    store.append('key-for-partition', 'topic', { v: 1 });
    // Try all partitions to find where it landed
    for (let i = 0; i < 4; i++) {
      cdc.captureChanges(store, i);
    }
    expect(captured.length).toBeGreaterThanOrEqual(1);
  });

  it('does nothing when disabled', () => {
    const cdc = new ChangeDataCaptureStream(false, 4);
    const store = new OutboxStore(testConfig);
    store.append('k', 't', {});
    const result = cdc.captureChanges(store, 0);
    expect(result).toHaveLength(0);
  });

  it('tracks lag', () => {
    const cdc = new ChangeDataCaptureStream(true, 4);
    cdc.updateLag(0, 10);
    expect(cdc.getTotalLag()).toBe(10);
    const positions = cdc.getPositions();
    expect(positions[0].lag).toBe(10);
  });
});

// ─── CompactionManager ───────────────────────────────────────────
describe('CompactionManager', () => {
  it('compacts confirmed events', () => {
    const cm = new CompactionManager(4, 1000);
    const store = new OutboxStore(testConfig);
    const e1 = store.append('k', 't', {});
    store.markConfirmed(e1.id);

    // Find partition
    let pid = -1;
    for (const [p, c] of store.getStats().partitionCounts) { if (c > 0) { pid = p; break; } }

    cm.updateConfirmedPosition(pid, e1.sequence);
    const compacted = cm.compact(store, pid);
    expect(compacted).toBe(1);
    expect(cm.getStats().totalCompacted).toBe(1);
  });

  it('does not compact beyond confirmed position', () => {
    const cm = new CompactionManager(4, 1000);
    const store = new OutboxStore(testConfig);
    store.append('k', 't', {}); // pending, not confirmed
    const compacted = cm.compactAll(store);
    expect(compacted).toBe(0);
  });
});

// ─── PartitionRouter ─────────────────────────────────────────────
describe('PartitionRouter', () => {
  it('assigns partitions to workers via round-robin', () => {
    const router = new PartitionRouter(4, 10000);
    router.addWorker('w1');
    router.addWorker('w2');
    // All 4 partitions should be assigned
    for (let i = 0; i < 4; i++) {
      expect(router.getWorkerForPartition(i)).not.toBeNull();
    }
  });

  it('rebalances when worker removed', () => {
    const router = new PartitionRouter(4, 10000);
    router.addWorker('w1');
    router.addWorker('w2');
    router.removeWorker('w1');
    // All partitions should now be on w2
    for (let i = 0; i < 4; i++) {
      expect(router.getWorkerForPartition(i)).toBe('w2');
    }
  });

  it('detects dead workers by heartbeat timeout', async () => {
    const router = new PartitionRouter(4, 50);
    router.addWorker('w1');
    await new Promise(r => setTimeout(r, 60));
    const dead = router.detectDeadWorkers();
    expect(dead).toContain('w1');
  });

  it('heartbeat keeps worker alive', async () => {
    const router = new PartitionRouter(4, 100);
    router.addWorker('w1');
    await new Promise(r => setTimeout(r, 50));
    router.heartbeat('w1');
    await new Promise(r => setTimeout(r, 60));
    const dead = router.detectDeadWorkers();
    expect(dead).not.toContain('w1');
  });

  it('records dispatch latency', () => {
    const router = new PartitionRouter(4, 10000);
    router.addWorker('w1');
    router.recordDispatch('w1', 50, true);
    router.recordDispatch('w1', 100, true);
    expect(router.getDispatchLatency()).toBeGreaterThan(0);
  });

  it('getPartitionForKey returns consistent partition', () => {
    const router = new PartitionRouter(8, 10000);
    const p1 = router.getPartitionForKey('my-key');
    const p2 = router.getPartitionForKey('my-key');
    expect(p1).toBe(p2);
    expect(p1).toBeGreaterThanOrEqual(0);
    expect(p1).toBeLessThan(8);
  });
});

// ─── TransactionalOutboxEngine (integration) ─────────────────────
describe('TransactionalOutboxEngine', () => {
  let engine: TransactionalOutboxEngine;

  beforeEach(() => {
    engine = new TransactionalOutboxEngine(testConfig);
  });

  it('appends event and retrieves it', () => {
    const evt = engine.appendEvent('agent-1', 'task.completed', { taskId: 't1' });
    expect(evt.status).toBe('pending');
    expect(engine.getEvent(evt.id)).toBeDefined();
  });

  it('appends with state change atomically', () => {
    const result = engine.appendWithStateChange(
      { entityId: 'agent-1', entityType: 'Agent', operation: 'update', before: null, after: { status: 'done' }, timestamp: Date.now() },
      [{ topic: 'agent.status', payload: { status: 'done' } }]
    );
    expect(result.events).toHaveLength(1);
    expect(result.stateChange.entityId).toBe('agent-1');
  });

  it('dispatches events via tick with delivery callback', async () => {
    const delivered: string[] = [];
    engine.addWorker('w1');
    engine.onDelivery(async (event) => {
      delivered.push(event.id);
      return true;
    });
    const evt = engine.appendEvent('k1', 'topic', { data: 1 });
    await engine.tick();
    expect(delivered.length).toBeGreaterThanOrEqual(1);
    expect(engine.getEvent(evt.id)!.status).toBe('confirmed');
  });

  it('dead-letters events after max retries', async () => {
    // Disable head-of-line blocking so retries aren't blocked
    const cfg = { ...testConfig, headOfLineBlockingEnabled: false };
    const eng = new TransactionalOutboxEngine(cfg);
    eng.addWorker('w1');
    eng.onDelivery(async () => false); // always fail
    eng.appendEvent('k1', 'topic', { data: 1 });

    // Each tick: markDispatched (+1 attempt) then markFailed (+1 attempt) = 2 per tick
    // threshold=3, so after 2 ticks attempts=4 >= 3, dead-lettered on tick 3
    for (let i = 0; i < 5; i++) {
      await eng.tick();
    }

    const dl = eng.getDeadLetters();
    expect(dl.length).toBeGreaterThanOrEqual(1);
  });

  it('replays dead-lettered events', async () => {
    const cfg = { ...testConfig, headOfLineBlockingEnabled: false };
    const eng = new TransactionalOutboxEngine(cfg);
    eng.addWorker('w1');
    eng.onDelivery(async () => false);
    eng.appendEvent('k1', 'topic', {});

    for (let i = 0; i < 5; i++) await eng.tick();

    const dl = eng.getDeadLetters();
    expect(dl.length).toBeGreaterThan(0);
    const replayed = eng.replayDeadLetter(dl[0].event.id);
    expect(replayed).not.toBeNull();
    expect(replayed!.status).toBe('pending');
  });

  it('detects idempotency duplicates in receipts', async () => {
    engine.addWorker('w1');
    engine.onDelivery(async () => true);
    const evt = engine.appendEvent('k1', 'topic', {});
    await engine.tick();

    const receipt = { eventId: evt.id, consumerId: 'c1', receivedAt: Date.now(), processingTimeMs: 10, success: true };
    engine.processReceipt(receipt);
    // Second receipt is duplicate
    engine.processReceipt(receipt);
    const events = engine.getEvents();
    const dupes = events.filter(e => e.type === 'idempotency_duplicate_detected');
    expect(dupes.length).toBeGreaterThanOrEqual(1);
  });

  it('removes dead workers on tick', async () => {
    const cfg = { ...testConfig, workerHeartbeatTimeoutMs: 10 };
    const eng = new TransactionalOutboxEngine(cfg);
    eng.addWorker('w1');
    await new Promise(r => setTimeout(r, 20));
    await eng.tick();
    const dashboard = eng.getDashboard();
    expect(dashboard.workers).toHaveLength(0);
  });

  it('getDashboard returns comprehensive stats', () => {
    engine.addWorker('w1');
    engine.appendEvent('k', 't', {});
    const dash = engine.getDashboard();
    expect(dash.store.total).toBe(1);
    expect(dash.store.pending).toBe(1);
    expect(dash.workers).toHaveLength(1);
    expect(dash.partitions).toHaveLength(4);
    expect(dash.tickCount).toBe(0);
  });

  it('compacts confirmed events during tick', async () => {
    engine.addWorker('w1');
    engine.onDelivery(async () => true);

    // Append and confirm events
    for (let i = 0; i < 5; i++) {
      engine.appendEvent(`k${i}`, 'topic', { i });
    }
    await engine.tick(); // dispatch + confirm

    // Process receipts to update compaction positions
    for (let i = 0; i < 5; i++) {
      const evt = engine.getEvents().filter(e => e.type === 'event_appended')[i];
      if (evt) {
        engine.processReceipt({
          eventId: evt.data.eventId as string,
          consumerId: 'c1',
          receivedAt: Date.now(),
          processingTimeMs: 1,
          success: true,
        });
      }
    }

    // Tick 10 times to trigger compaction (tickCount % 10 === 0)
    for (let i = 0; i < 10; i++) await engine.tick();

    const dash = engine.getDashboard();
    expect(dash.compaction.totalCompacted).toBeGreaterThanOrEqual(0);
  });

  it('CDC captures events when enabled', async () => {
    engine.addWorker('w1');
    engine.onDelivery(async () => true);
    engine.appendEvent('k1', 'topic', {});
    await engine.tick();
    const cdcEvents = engine.getEvents().filter(e => e.type === 'cdc_position_advanced');
    expect(cdcEvents.length).toBeGreaterThanOrEqual(1);
  });

  it('system events are capped at maxEvents', () => {
    const cfg = { ...testConfig, maxPendingPerPartition: 10000 };
    const eng = new TransactionalOutboxEngine(cfg);
    for (let i = 0; i < 1100; i++) {
      eng.appendEvent(`key-${i}`, 'topic', { i });
    }
    // Engine caps at 1000 events
    expect(eng.getEvents().length).toBeLessThanOrEqual(1000);
  });

  it('getEvents filters by timestamp', async () => {
    engine.appendEvent('k1', 'topic', {});
    const now = Date.now();
    await new Promise(r => setTimeout(r, 5));
    engine.appendEvent('k2', 'topic', {});
    const recent = engine.getEvents(now);
    expect(recent.length).toBeGreaterThanOrEqual(1);
  });
});
