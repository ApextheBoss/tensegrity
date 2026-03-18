import { describe, it, expect, beforeEach } from 'vitest';
import { BackpressureController } from '../backpressure';

function msg(id: string, priority = 1) {
  return { id, payload: id, priority, enqueuedAt: Date.now(), sender: 'test' };
}

describe('BackpressureController', () => {
  it('enqueues and dequeues in priority order', async () => {
    const bp = new BackpressureController({ maxQueueDepth: 100 });
    await bp.enqueue(msg('low', 1));
    await bp.enqueue(msg('high', 10));
    await bp.enqueue(msg('mid', 5));

    expect(bp.dequeue()?.id).toBe('high');
    expect(bp.dequeue()?.id).toBe('mid');
    expect(bp.dequeue()?.id).toBe('low');
  });

  it('rejects when at max capacity (reject strategy)', async () => {
    const bp = new BackpressureController({
      maxQueueDepth: 3,
      highWaterMark: 0.5,
      strategy: 'reject',
    });

    expect(await bp.enqueue(msg('1'))).toBe(true);
    expect(await bp.enqueue(msg('2'))).toBe(true); // triggers pressure at 66%
    expect(await bp.enqueue(msg('3'))).toBe(false); // rejected
    expect(bp.getMetrics().rejected).toBe(1);
  });

  it('drops newest when full (drop-newest strategy)', async () => {
    const bp = new BackpressureController({
      maxQueueDepth: 2,
      highWaterMark: 0.5,
      strategy: 'drop-newest',
    });

    await bp.enqueue(msg('1'));
    await bp.enqueue(msg('2'));
    const accepted = await bp.enqueue(msg('3'));
    expect(accepted).toBe(false);
    expect(bp.getMetrics().dropped).toBe(1);
  });

  it('drops oldest when full (drop-oldest strategy)', async () => {
    const bp = new BackpressureController({
      maxQueueDepth: 2,
      highWaterMark: 0.5,
      strategy: 'drop-oldest',
    });

    await bp.enqueue(msg('1'));
    await bp.enqueue(msg('2'));
    await bp.enqueue(msg('3'));

    // '1' was dropped, '2' and '3' remain
    expect(bp.dequeue()?.id).toBe('2');
    expect(bp.dequeue()?.id).toBe('3');
  });

  it('releases pressure below low water mark', async () => {
    const bp = new BackpressureController({
      maxQueueDepth: 10,
      highWaterMark: 0.8,
      lowWaterMark: 0.3,
      strategy: 'reject',
    });

    // Fill to 90% (9/10) — pressure activates when utilization >= highWaterMark on enqueue
    for (let i = 0; i < 9; i++) {
      await bp.enqueue(msg(`${i}`));
    }
    expect(bp.getMetrics().pressureActive).toBe(true);

    // Drain to below 30% (need depth <= 3)
    for (let i = 0; i < 7; i++) {
      bp.dequeue();
    }
    expect(bp.getMetrics().pressureActive).toBe(false);
  });

  it('peek returns next without removing', async () => {
    const bp = new BackpressureController();
    await bp.enqueue(msg('first'));
    expect(bp.peek()?.id).toBe('first');
    expect(bp.peek()?.id).toBe('first'); // still there
    expect(bp.dequeue()?.id).toBe('first');
    expect(bp.peek()).toBeUndefined();
  });

  it('drain removes matching messages', async () => {
    const bp = new BackpressureController();
    await bp.enqueue({ ...msg('a'), sender: 'alice' });
    await bp.enqueue({ ...msg('b'), sender: 'bob' });
    await bp.enqueue({ ...msg('c'), sender: 'alice' });

    const drained = bp.drain(m => m.sender === 'alice');
    expect(drained.length).toBe(2);
    expect(bp.dequeue()?.sender).toBe('bob');
    expect(bp.dequeue()).toBeUndefined();
  });

  it('tracks metrics correctly', async () => {
    const bp = new BackpressureController({ maxQueueDepth: 5 });
    await bp.enqueue(msg('1'));
    await bp.enqueue(msg('2'));
    bp.dequeue();

    const m = bp.getMetrics();
    expect(m.depth).toBe(1);
    expect(m.utilizationPct).toBe(20);
  });

  it('adaptive throttle returns 0 when no pressure', () => {
    const bp = new BackpressureController();
    expect(bp.getAdaptiveThrottle()).toBe(0);
  });
});
