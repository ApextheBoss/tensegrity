import { describe, it, expect } from 'vitest';
import {
  createClock, tick, merge, compare, happensBefore, isConcurrent, dominates,
  createDVV, dvvAdd, dvvContains, dvvMerge, dvvFindConflicts,
  createMatrixClock, matrixTick, matrixMerge, matrixStableFrontier,
  CausalBarrierManager, CausalEventLog,
  StabilityDetector, reconstructClock,
  lwwStrategy, deterministicStrategy, priorityStrategy,
  clockToString, clockSize,
  type VectorClock, type Dot,
} from '../vector-clock-causality';

// ─── Vector Clock Operations ─────────────────────────────────────

describe('VectorClock basics', () => {
  it('createClock initializes origin at 0', () => {
    const c = createClock('a');
    expect(c.origin).toBe('a');
    expect(c.entries.get('a')).toBe(0);
  });

  it('tick increments origin counter', () => {
    const c = tick(createClock('a'));
    expect(c.entries.get('a')).toBe(1);
    const c2 = tick(c);
    expect(c2.entries.get('a')).toBe(2);
  });

  it('merge takes component-wise max and ticks own counter', () => {
    let a = tick(tick(createClock('a'))); // a:2
    let b = tick(createClock('b'));       // b:1
    const m = merge(a, b);
    expect(m.origin).toBe('a');
    expect(m.entries.get('a')).toBe(3); // 2+1 from merge tick
    expect(m.entries.get('b')).toBe(1);
  });
});

describe('VectorClock compare', () => {
  it('equal clocks', () => {
    const a = createClock('a');
    const b = createClock('a');
    expect(compare(a, b)).toBe('equal');
  });

  it('a before b', () => {
    const a = tick(createClock('a')); // a:1
    const b = tick(tick(createClock('a'))); // a:2
    expect(compare(a, b)).toBe('before');
    expect(happensBefore(a, b)).toBe(true);
  });

  it('a after b', () => {
    const a = tick(tick(createClock('a')));
    const b = tick(createClock('a'));
    expect(compare(a, b)).toBe('after');
  });

  it('concurrent clocks', () => {
    const a = tick(createClock('a')); // a:1, b:0
    const b = tick(createClock('b')); // a:0, b:1
    expect(compare(a, b)).toBe('concurrent');
    expect(isConcurrent(a, b)).toBe(true);
  });

  it('dominates', () => {
    let a = tick(tick(createClock('a')));
    const entries = new Map(a.entries);
    entries.set('b', 1);
    a = { entries, origin: 'a' };
    const b = tick(createClock('a')); // a:1
    expect(dominates(a, b)).toBe(true);
    expect(dominates(b, a)).toBe(false);
  });

  it('equal clocks do not dominate', () => {
    const a = tick(createClock('a'));
    const b = tick(createClock('a'));
    expect(dominates(a, b)).toBe(false);
  });
});

// ─── Dotted Version Vectors ──────────────────────────────────────

describe('DottedVersionVector', () => {
  it('add contiguous dots collapses into base', () => {
    let dvv = createDVV();
    dvv = dvvAdd(dvv, { agent: 'a', counter: 1 });
    dvv = dvvAdd(dvv, { agent: 'a', counter: 2 });
    expect(dvv.base.get('a')).toBe(2);
    expect(dvv.dots.size).toBe(0);
  });

  it('add sparse dot stays in dots set', () => {
    let dvv = createDVV();
    dvv = dvvAdd(dvv, { agent: 'a', counter: 3 }); // skip 1,2
    expect(dvv.base.get('a') ?? 0).toBe(0);
    expect(dvv.dots.has('a:3')).toBe(true);
  });

  it('filling gap collapses sparse dots', () => {
    let dvv = createDVV();
    dvv = dvvAdd(dvv, { agent: 'a', counter: 1 });
    dvv = dvvAdd(dvv, { agent: 'a', counter: 3 }); // sparse
    dvv = dvvAdd(dvv, { agent: 'a', counter: 2 }); // fills gap
    expect(dvv.base.get('a')).toBe(3);
    expect(dvv.dots.size).toBe(0);
  });

  it('dvvContains checks base and dots', () => {
    let dvv = createDVV();
    dvv = dvvAdd(dvv, { agent: 'a', counter: 1 });
    dvv = dvvAdd(dvv, { agent: 'a', counter: 3 });
    expect(dvvContains(dvv, { agent: 'a', counter: 1 })).toBe(true);
    expect(dvvContains(dvv, { agent: 'a', counter: 2 })).toBe(false);
    expect(dvvContains(dvv, { agent: 'a', counter: 3 })).toBe(true);
  });

  it('dvvMerge takes max base and merges dots', () => {
    let a = createDVV();
    a = dvvAdd(a, { agent: 'x', counter: 1 });
    a = dvvAdd(a, { agent: 'x', counter: 2 });
    let b = createDVV();
    b = dvvAdd(b, { agent: 'x', counter: 1 });
    b = dvvAdd(b, { agent: 'x', counter: 4 }); // sparse
    const merged = dvvMerge(a, b);
    expect(merged.base.get('x')).toBe(2);
    expect(merged.dots.has('x:4')).toBe(true);
  });

  it('dvvFindConflicts finds dots unknown to other side', () => {
    let a = createDVV();
    a = dvvAdd(a, { agent: 'x', counter: 3 }); // sparse
    let b = createDVV();
    b = dvvAdd(b, { agent: 'y', counter: 2 }); // sparse
    const conflicts = dvvFindConflicts(a, b);
    expect(conflicts.length).toBe(2);
  });

  it('adding already-known dot is no-op', () => {
    let dvv = createDVV();
    dvv = dvvAdd(dvv, { agent: 'a', counter: 1 });
    const dvv2 = dvvAdd(dvv, { agent: 'a', counter: 1 });
    expect(dvv2).toBe(dvv); // same reference returned
  });
});

// ─── Matrix Clocks ───────────────────────────────────────────────

describe('MatrixClock', () => {
  it('creates zero matrix', () => {
    const mc = createMatrixClock('a', ['a', 'b']);
    const row = mc.rows.get('a')!;
    expect(row.get('a')).toBe(0);
    expect(row.get('b')).toBe(0);
  });

  it('matrixTick increments own counter', () => {
    const mc = matrixTick(createMatrixClock('a', ['a', 'b']));
    expect(mc.rows.get('a')!.get('a')).toBe(1);
  });

  it('matrixStableFrontier returns min across rows', () => {
    let mc = createMatrixClock('a', ['a', 'b']);
    // Manually set rows
    const rows = new Map<string, Map<string, number>>();
    rows.set('a', new Map([['a', 3], ['b', 1]]));
    rows.set('b', new Map([['a', 2], ['b', 4]]));
    mc = { rows, origin: 'a' };
    const frontier = matrixStableFrontier(mc);
    expect(frontier.get('a')).toBe(2);
    expect(frontier.get('b')).toBe(1);
  });

  it('empty matrix returns empty frontier', () => {
    const mc = { rows: new Map(), origin: 'a' };
    expect(matrixStableFrontier(mc).size).toBe(0);
  });
});

// ─── CausalBarrierManager ────────────────────────────────────────

describe('CausalBarrierManager', () => {
  it('immediately fires callback when already satisfied', () => {
    const mgr = new CausalBarrierManager('a');
    let fired = false;
    const required = createClock('a'); // a:0, already satisfied
    mgr.waitFor(required, () => { fired = true; });
    expect(fired).toBe(true);
    mgr.destroy();
  });

  it('fires callback when clock advances past barrier', () => {
    const mgr = new CausalBarrierManager('a');
    let fired = false;
    const entries = new Map([['b', 2]]);
    const required: VectorClock = { entries, origin: 'b' };
    mgr.waitFor(required, () => { fired = true; });
    expect(fired).toBe(false);

    // Advance with a clock that has b:3
    const advanceEntries = new Map([['b', 3]]);
    mgr.advance({ entries: advanceEntries, origin: 'b' });
    expect(fired).toBe(true);
    mgr.destroy();
  });

  it('cancel removes pending barrier', () => {
    const mgr = new CausalBarrierManager('a');
    const entries = new Map([['b', 99]]);
    const id = mgr.waitFor({ entries, origin: 'b' }, () => {});
    expect(mgr.pendingCount).toBe(1);
    mgr.cancel(id);
    expect(mgr.pendingCount).toBe(0);
    mgr.destroy();
  });

  it('destroy clears all timers and barriers', () => {
    const mgr = new CausalBarrierManager('a');
    const entries = new Map([['b', 99]]);
    mgr.waitFor({ entries, origin: 'b' }, () => {});
    mgr.waitFor({ entries, origin: 'b' }, () => {});
    mgr.destroy();
    expect(mgr.pendingCount).toBe(0);
  });
});

// ─── CausalEventLog ─────────────────────────────────────────────

describe('CausalEventLog', () => {
  it('emit creates event with incremented clock', () => {
    const log = new CausalEventLog('a');
    const e = log.emit('hello');
    expect(e.origin).toBe('a');
    expect(e.clock.entries.get('a')).toBe(1);
    expect(e.payload).toBe('hello');
    expect(log.deliveredEvents.length).toBe(1);
  });

  it('delivers causally-ready remote events', () => {
    const logA = new CausalEventLog<string>('a');
    const logB = new CausalEventLog<string>('b');

    const e1 = logA.emit('from-a-1');
    logB.receive(e1);
    expect(logB.deliveredEvents.length).toBe(1);
    expect(logB.pendingCount).toBe(0);
  });

  it('buffers events with unmet causal dependencies', () => {
    const logA = new CausalEventLog<string>('a');
    const logB = new CausalEventLog<string>('b');

    const e1 = logA.emit('first');
    const e2 = logA.emit('second');

    // Deliver e2 before e1 — should buffer e2
    logB.receive(e2);
    expect(logB.pendingCount).toBe(1);
    expect(logB.deliveredEvents.length).toBe(0);

    // Now deliver e1 — should trigger delivery of both
    logB.receive(e1);
    expect(logB.pendingCount).toBe(0);
    expect(logB.deliveredEvents.length).toBe(2);
  });

  it('onDeliver callback fires for each delivered event', () => {
    const log = new CausalEventLog<string>('a');
    const delivered: string[] = [];
    log.onDeliver(e => delivered.push(e.payload));
    log.emit('one');
    log.emit('two');
    expect(delivered).toEqual(['one', 'two']);
  });

  it('enforces maxHistory limit', () => {
    const log = new CausalEventLog<number>('a', 5);
    for (let i = 0; i < 10; i++) log.emit(i);
    expect(log.deliveredEvents.length).toBe(5);
  });
});

// ─── StabilityDetector ───────────────────────────────────────────

describe('StabilityDetector', () => {
  it('does not advance until all agents report', () => {
    const detector = new StabilityDetector(new Set(['a', 'b']));
    let advanced = false;
    detector.onAdvance(() => { advanced = true; });

    detector.reportClock('a', tick(createClock('a')));
    expect(advanced).toBe(false);
  });

  it('advances when all agents report clocks with overlapping knowledge', () => {
    const detector = new StabilityDetector(new Set(['a', 'b']));
    let frontier: Map<string, number> | null = null;
    detector.onAdvance(f => { frontier = f; });

    // Both agents know about each other (simulating after message exchange)
    const clockA: VectorClock = { entries: new Map([['a', 2], ['b', 1]]), origin: 'a' };
    const clockB: VectorClock = { entries: new Map([['a', 1], ['b', 3]]), origin: 'b' };
    detector.reportClock('a', clockA);
    detector.reportClock('b', clockB);
    // frontier = min across rows: a=min(2,1)=1, b=min(1,3)=1
    expect(frontier).not.toBeNull();
    expect(frontier!.get('a')).toBe(1);
    expect(frontier!.get('b')).toBe(1);
  });

  it('isStable checks event against frontier', () => {
    const detector = new StabilityDetector(new Set(['a', 'b']));
    const clockA = tick(tick(createClock('a')));
    const clockB = tick(createClock('b'));
    detector.reportClock('a', clockA);
    detector.reportClock('b', clockB);

    // Event from a at time 1 should be stable (both know a >= 0, but a's frontier is min)
    const event = {
      id: 'e1', origin: 'a',
      clock: tick(createClock('a')),
      payload: null, timestamp: 0,
    };
    // Frontier for 'a' = min(clockA[a], clockB[a]) = min(2, 0) = 0
    // Event time for 'a' = 1, so 1 > 0 => not stable
    expect(detector.isStable(event)).toBe(false);
  });
});

// ─── reconstructClock ────────────────────────────────────────────

describe('reconstructClock', () => {
  it('takes component-wise max of observed messages', () => {
    const m1Entries = new Map([['a', 3], ['b', 1]]);
    const m2Entries = new Map([['a', 1], ['b', 5]]);
    const clock = reconstructClock(
      [
        { origin: 'a', clock: { entries: m1Entries, origin: 'a' } },
        { origin: 'b', clock: { entries: m2Entries, origin: 'b' } },
      ],
      'joiner'
    );
    expect(clock.origin).toBe('joiner');
    expect(clock.entries.get('a')).toBe(3);
    expect(clock.entries.get('b')).toBe(5);
    expect(clock.entries.get('joiner')).toBe(0);
  });
});

// ─── Conflict Strategies ─────────────────────────────────────────

describe('Conflict strategies', () => {
  const makeEvent = (origin: string, ts: number) => ({
    id: `${origin}-1`, origin,
    clock: tick(createClock(origin)),
    payload: origin, timestamp: ts,
  });

  it('lww picks latest timestamp', () => {
    const strategy = lwwStrategy<string>();
    const e1 = makeEvent('a', 100);
    const e2 = makeEvent('b', 200);
    expect(strategy([e1, e2], { currentClock: createClock('x') })).toBe(e2);
  });

  it('deterministic picks lowest agent ID', () => {
    const strategy = deterministicStrategy<string>();
    const e1 = makeEvent('beta', 100);
    const e2 = makeEvent('alpha', 200);
    expect(strategy([e1, e2], { currentClock: createClock('x') }).origin).toBe('alpha');
  });

  it('priority picks lowest priority number', () => {
    const priorities = new Map([['a', 10], ['b', 1]]);
    const strategy = priorityStrategy<string>(priorities);
    const e1 = makeEvent('a', 100);
    const e2 = makeEvent('b', 100);
    expect(strategy([e1, e2], { currentClock: createClock('x') }).origin).toBe('b');
  });
});

// ─── Utilities ───────────────────────────────────────────────────

describe('Utilities', () => {
  it('clockToString formats sorted entries', () => {
    const entries = new Map([['beta', 2], ['alpha', 1]]);
    const s = clockToString({ entries, origin: 'alpha' });
    expect(s).toContain('alpha');
    expect(s).toContain('beta');
    expect(s.indexOf('alpha')).toBeLessThan(s.indexOf('beta'));
  });

  it('clockSize returns entry count', () => {
    const c = tick(createClock('a'));
    expect(clockSize(c)).toBe(1);
  });
});
