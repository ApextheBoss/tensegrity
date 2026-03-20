import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  ObservableStateMachine,
  StateMachineBuilder,
  StateRegistry,
  TransitionEngine,
  ObserverManager,
  TimeoutManager,
  InvariantChecker,
  TransitionLog,
  SnapshotManager,
  DeadlockDetector,
  ParallelRegionCoordinator,
  createAgentLifecycleMachine,
  createProtocolHandshakeMachine,
  createTaskWorkflowMachine,
  type MachineContext,
  type StateChangeEvent,
} from '../observable-state-machine';

// ─── Helper ───

function simpleMachine() {
  return new StateMachineBuilder({ maxTransitions: 1000, snapshotInterval: 5 })
    .state('idle', { initial: true })
    .state('running')
    .state('done')
    .transition('idle', 'running', 'START')
    .transition('running', 'done', 'FINISH')
    .transition('running', 'idle', 'RESET')
    .build();
}

// ─── StateRegistry ───

describe('StateRegistry', () => {
  it('registers and retrieves states', () => {
    const reg = new StateRegistry();
    reg.register({ name: 'a', invariants: [], metadata: {} });
    expect(reg.get('a')).toBeDefined();
    expect(reg.get('b')).toBeUndefined();
  });

  it('tracks parent-child relationships', () => {
    const reg = new StateRegistry();
    reg.register({ name: 'parent', invariants: [], metadata: {} });
    reg.register({ name: 'child1', parent: 'parent', initial: true, invariants: [], metadata: {} });
    reg.register({ name: 'child2', parent: 'parent', invariants: [], metadata: {} });
    expect(reg.getChildren('parent')).toEqual(['child1', 'child2']);
    expect(reg.getInitialChild('parent')).toBe('child1');
  });

  it('getInitialChild falls back to first child', () => {
    const reg = new StateRegistry();
    reg.register({ name: 'p', invariants: [], metadata: {} });
    reg.register({ name: 'c1', parent: 'p', invariants: [], metadata: {} });
    expect(reg.getInitialChild('p')).toBe('c1');
  });

  it('computes ancestors', () => {
    const reg = new StateRegistry();
    reg.register({ name: 'root', invariants: [], metadata: {} });
    reg.register({ name: 'mid', parent: 'root', invariants: [], metadata: {} });
    reg.register({ name: 'leaf', parent: 'mid', invariants: [], metadata: {} });
    expect(reg.getAncestors('leaf')).toEqual(['mid', 'root']);
    expect(reg.isDescendant('leaf', 'root')).toBe(true);
    expect(reg.isDescendant('root', 'leaf')).toBe(false);
  });

  it('getRootStates returns only parentless states', () => {
    const reg = new StateRegistry();
    reg.register({ name: 'a', invariants: [], metadata: {} });
    reg.register({ name: 'b', parent: 'a', invariants: [], metadata: {} });
    reg.register({ name: 'c', invariants: [], metadata: {} });
    expect(reg.getRootStates()).toEqual(['a', 'c']);
  });

  it('getStatesByRegion groups correctly', () => {
    const reg = new StateRegistry();
    reg.register({ name: 'a', region: 'r1', invariants: [], metadata: {} });
    reg.register({ name: 'b', region: 'r1', invariants: [], metadata: {} });
    reg.register({ name: 'c', region: 'r2', invariants: [], metadata: {} });
    const regions = reg.getStatesByRegion();
    expect(regions.get('r1')).toEqual(['a', 'b']);
    expect(regions.get('r2')).toEqual(['c']);
  });
});

// ─── TransitionEngine ───

describe('TransitionEngine', () => {
  it('finds matching transitions sorted by priority', () => {
    const engine = new TransitionEngine();
    const ctx: MachineContext = {
      currentStates: new Map(), history: new Map(), data: {},
      enteredAt: new Map(), transitionCount: 0, startedAt: 0,
    };
    engine.addTransition({ from: 'a', to: 'b', event: 'GO', priority: 1, metadata: {} });
    engine.addTransition({ from: 'a', to: 'c', event: 'GO', priority: 10, metadata: {} });
    const results = engine.findTransitions('a', 'GO', ctx);
    expect(results.length).toBe(2);
    expect(results[0].to).toBe('c'); // higher priority first
  });

  it('wildcard from matches any state', () => {
    const engine = new TransitionEngine();
    const ctx: MachineContext = {
      currentStates: new Map(), history: new Map(), data: {},
      enteredAt: new Map(), transitionCount: 0, startedAt: 0,
    };
    engine.addTransition({ from: '*', to: 'error', event: 'CRASH', priority: 0, metadata: {} });
    expect(engine.findTransitions('anything', 'CRASH', ctx).length).toBe(1);
  });

  it('guard blocks transition when false', () => {
    const engine = new TransitionEngine();
    const ctx: MachineContext = {
      currentStates: new Map(), history: new Map(), data: { allowed: false },
      enteredAt: new Map(), transitionCount: 0, startedAt: 0,
    };
    engine.addTransition({ from: 'a', to: 'b', event: 'GO', guard: 'check', priority: 0, metadata: {} });
    engine.registerGuard('check', (c) => c.data.allowed as boolean);
    expect(engine.findTransitions('a', 'GO', ctx).length).toBe(0);
    ctx.data.allowed = true;
    expect(engine.findTransitions('a', 'GO', ctx).length).toBe(1);
  });

  it('executes actions', () => {
    const engine = new TransitionEngine();
    const ctx: MachineContext = {
      currentStates: new Map(), history: new Map(), data: {},
      enteredAt: new Map(), transitionCount: 0, startedAt: 0,
    };
    engine.registerAction('setFlag', (c) => { c.data.flag = true; });
    engine.executeAction('setFlag', ctx, 'test');
    expect(ctx.data.flag).toBe(true);
  });

  it('getAllEvents returns unique events', () => {
    const engine = new TransitionEngine();
    engine.addTransition({ from: 'a', to: 'b', event: 'GO', priority: 0, metadata: {} });
    engine.addTransition({ from: 'b', to: 'c', event: 'GO', priority: 0, metadata: {} });
    engine.addTransition({ from: 'c', to: 'a', event: 'RESET', priority: 0, metadata: {} });
    expect(engine.getAllEvents()).toEqual(new Set(['GO', 'RESET']));
  });
});

// ─── ObserverManager ───

describe('ObserverManager', () => {
  it('notifies matching observers', () => {
    const mgr = new ObserverManager();
    const calls: string[] = [];
    mgr.subscribe('idle', (e) => calls.push(e.state));
    mgr.subscribe('running', (e) => calls.push(e.state));
    mgr.notify({ type: 'enter', state: 'idle', region: 'default', timestamp: 0, context: {} });
    expect(calls).toEqual(['idle']);
  });

  it('wildcard matches everything', () => {
    const mgr = new ObserverManager();
    const calls: string[] = [];
    mgr.subscribe('*', (e) => calls.push(e.state));
    mgr.notify({ type: 'enter', state: 'anything', region: 'default', timestamp: 0, context: {} });
    expect(calls).toEqual(['anything']);
  });

  it('glob pattern busy.* matches children', () => {
    const mgr = new ObserverManager();
    const calls: string[] = [];
    mgr.subscribe('busy.*', (e) => calls.push(e.state));
    mgr.notify({ type: 'enter', state: 'busy.processing', region: 'default', timestamp: 0, context: {} });
    mgr.notify({ type: 'enter', state: 'busy', region: 'default', timestamp: 0, context: {} });
    mgr.notify({ type: 'enter', state: 'idle', region: 'default', timestamp: 0, context: {} });
    expect(calls).toEqual(['busy.processing', 'busy']);
  });

  it('unsubscribe removes observer', () => {
    const mgr = new ObserverManager();
    const calls: string[] = [];
    const id = mgr.subscribe('*', (e) => calls.push(e.state));
    mgr.unsubscribe(id);
    mgr.notify({ type: 'enter', state: 'x', region: 'default', timestamp: 0, context: {} });
    expect(calls).toEqual([]);
  });

  it('observer errors do not break notification', () => {
    const mgr = new ObserverManager();
    const calls: string[] = [];
    mgr.subscribe('*', () => { throw new Error('boom'); });
    mgr.subscribe('*', (e) => calls.push(e.state));
    mgr.notify({ type: 'enter', state: 'x', region: 'default', timestamp: 0, context: {} });
    expect(calls).toEqual(['x']);
  });
});

// ─── TimeoutManager ───

describe('TimeoutManager', () => {
  it('tracks and fires expired timers', () => {
    const mgr = new TimeoutManager();
    mgr.setTimer('idle', { durationMs: 100, event: 'TIMEOUT', resetOnReentry: false }, 1000);
    expect(mgr.checkExpired(1050)).toEqual([]);
    expect(mgr.checkExpired(1100)).toEqual([{ state: 'idle', event: 'TIMEOUT' }]);
    // Should be cleared after firing
    expect(mgr.checkExpired(1200)).toEqual([]);
  });

  it('clearTimer prevents firing', () => {
    const mgr = new TimeoutManager();
    mgr.setTimer('s', { durationMs: 100, event: 'T', resetOnReentry: false }, 0);
    mgr.clearTimer('s');
    expect(mgr.checkExpired(200)).toEqual([]);
  });

  it('getDeadline returns correct value', () => {
    const mgr = new TimeoutManager();
    mgr.setTimer('s', { durationMs: 500, event: 'T', resetOnReentry: false }, 1000);
    expect(mgr.getDeadline('s')).toBe(1500);
    expect(mgr.getDeadline('other')).toBeUndefined();
  });
});

// ─── InvariantChecker ───

describe('InvariantChecker', () => {
  it('records violations and returns them', () => {
    const checker = new InvariantChecker();
    const state = {
      name: 'test', invariants: [
        { name: 'positive', condition: (ctx: MachineContext) => (ctx.data.x as number) > 0, severity: 'error' as const, message: 'x must be positive' },
      ], metadata: {},
    };
    const ctx: MachineContext = {
      currentStates: new Map(), history: new Map(), data: { x: -1 },
      enteredAt: new Map(), transitionCount: 0, startedAt: 0,
    };
    const result = checker.check(state, ctx, 100);
    expect(result).toEqual({ anyViolation: true, fatal: false });
    expect(checker.getViolationCount()).toBe(1);
    expect(checker.getViolations()[0].invariant).toBe('positive');
  });

  it('fatal severity returns false', () => {
    const checker = new InvariantChecker();
    const state = {
      name: 'test', invariants: [
        { name: 'fatal-check', condition: () => false, severity: 'fatal' as const, message: 'fatal' },
      ], metadata: {},
    };
    const ctx: MachineContext = {
      currentStates: new Map(), history: new Map(), data: {},
      enteredAt: new Map(), transitionCount: 0, startedAt: 0,
    };
    expect(checker.check(state, ctx, 0)).toEqual({ anyViolation: true, fatal: true });
  });

  it('clearViolations works', () => {
    const checker = new InvariantChecker();
    const state = {
      name: 'test', invariants: [
        { name: 'x', condition: () => false, severity: 'warning' as const, message: 'w' },
      ], metadata: {},
    };
    const ctx: MachineContext = {
      currentStates: new Map(), history: new Map(), data: {},
      enteredAt: new Map(), transitionCount: 0, startedAt: 0,
    };
    checker.check(state, ctx, 0);
    checker.clearViolations();
    expect(checker.getViolationCount()).toBe(0);
  });

  it('getViolations since timestamp', () => {
    const checker = new InvariantChecker();
    const state = {
      name: 'test', invariants: [
        { name: 'x', condition: () => false, severity: 'warning' as const, message: 'w' },
      ], metadata: {},
    };
    const ctx: MachineContext = {
      currentStates: new Map(), history: new Map(), data: {},
      enteredAt: new Map(), transitionCount: 0, startedAt: 0,
    };
    checker.check(state, ctx, 100);
    checker.check(state, ctx, 200);
    expect(checker.getViolations(150).length).toBe(1);
    expect(checker.getViolations(150)[0].timestamp).toBe(200);
  });
});

// ─── TransitionLog ───

describe('TransitionLog', () => {
  it('records and retrieves transitions', () => {
    const log = new TransitionLog(5);
    log.record('a', 'b', 'GO', 'default', {});
    log.record('b', 'c', 'NEXT', 'default', {});
    expect(log.size()).toBe(2);
    expect(log.getRecent(1)[0].event).toBe('NEXT');
  });

  it('trims to max depth', () => {
    const log = new TransitionLog(3);
    for (let i = 0; i < 5; i++) log.record('a', 'b', `E${i}`, 'default', {});
    expect(log.size()).toBe(3);
    expect(log.getAll()[0].event).toBe('E2');
  });

  it('filters by state and event', () => {
    const log = new TransitionLog(100);
    log.record('a', 'b', 'GO', 'default', {});
    log.record('b', 'c', 'NEXT', 'default', {});
    expect(log.getByState('b').length).toBe(2);
    expect(log.getByEvent('GO').length).toBe(1);
  });
});

// ─── SnapshotManager ───

describe('SnapshotManager', () => {
  it('captures and restores snapshots', () => {
    const mgr = new SnapshotManager(5);
    const ctx: MachineContext = {
      currentStates: new Map([['default', 'idle']]),
      history: new Map(), data: { x: 1 },
      enteredAt: new Map(), transitionCount: 3, startedAt: 1000,
    };
    const snap = mgr.capture(ctx);
    expect(snap.version).toBe(1);

    // Modify original
    ctx.data.x = 99;
    ctx.transitionCount = 50;

    // Restore
    const restored = mgr.restore(1);
    expect(restored).toBeDefined();
    expect(restored!.data.x).toBe(1);
    expect(restored!.transitionCount).toBe(3);
  });

  it('getLatest returns most recent', () => {
    const mgr = new SnapshotManager(5);
    const ctx: MachineContext = {
      currentStates: new Map(), history: new Map(), data: {},
      enteredAt: new Map(), transitionCount: 0, startedAt: 0,
    };
    mgr.capture(ctx);
    ctx.transitionCount = 10;
    mgr.capture(ctx);
    expect(mgr.getLatest()!.version).toBe(2);
  });

  it('returns undefined for invalid version', () => {
    const mgr = new SnapshotManager(5);
    expect(mgr.restore(999)).toBeUndefined();
  });

  it('trims old snapshots', () => {
    const mgr = new SnapshotManager(2);
    const ctx: MachineContext = {
      currentStates: new Map(), history: new Map(), data: {},
      enteredAt: new Map(), transitionCount: 0, startedAt: 0,
    };
    mgr.capture(ctx); // v1
    mgr.capture(ctx); // v2
    mgr.capture(ctx); // v3
    expect(mgr.restore(1)).toBeUndefined(); // trimmed
    expect(mgr.restore(3)).toBeDefined();
  });
});

// ─── DeadlockDetector ───

describe('DeadlockDetector', () => {
  it('detects state with no outgoing transitions', () => {
    const detector = new DeadlockDetector();
    const reg = new StateRegistry();
    reg.register({ name: 'stuck', invariants: [], metadata: {} });
    const engine = new TransitionEngine();
    const ctx: MachineContext = {
      currentStates: new Map([['default', 'stuck']]),
      history: new Map(), data: {},
      enteredAt: new Map(), transitionCount: 0, startedAt: 0,
    };
    expect(detector.detectPotentialDeadlocks(reg, engine, ctx)).toEqual(['stuck']);
  });

  it('does not flag states with timeout', () => {
    const detector = new DeadlockDetector();
    const reg = new StateRegistry();
    reg.register({ name: 'waiting', timeout: { durationMs: 1000, event: 'T', resetOnReentry: false }, invariants: [], metadata: {} });
    const engine = new TransitionEngine();
    const ctx: MachineContext = {
      currentStates: new Map([['default', 'waiting']]),
      history: new Map(), data: {},
      enteredAt: new Map(), transitionCount: 0, startedAt: 0,
    };
    expect(detector.detectPotentialDeadlocks(reg, engine, ctx)).toEqual([]);
  });

  it('detects livelock from cycling transitions', () => {
    const detector = new DeadlockDetector();
    const log = new TransitionLog(100);
    for (let i = 0; i < 20; i++) {
      log.record(i % 2 === 0 ? 'a' : 'b', i % 2 === 0 ? 'b' : 'a', 'FLIP', 'default', {});
    }
    expect(detector.detectLivelock(log, 20)).toBe(true);
  });

  it('no livelock with diverse states', () => {
    const detector = new DeadlockDetector();
    const log = new TransitionLog(100);
    for (let i = 0; i < 20; i++) {
      log.record(`s${i}`, `s${i + 1}`, 'GO', 'default', {});
    }
    expect(detector.detectLivelock(log, 20)).toBe(false);
  });
});

// ─── ObservableStateMachine (core) ───

describe('ObservableStateMachine', () => {
  it('starts in initial state', () => {
    const m = simpleMachine();
    m.start();
    expect(m.getCurrentState()).toBe('idle');
    expect(m.isRunning()).toBe(true);
  });

  it('transitions on send', () => {
    const m = simpleMachine();
    m.start();
    expect(m.send('START')).toBe(true);
    expect(m.getCurrentState()).toBe('running');
  });

  it('rejects invalid events', () => {
    const m = simpleMachine();
    m.start();
    expect(m.send('NONEXISTENT')).toBe(false);
    expect(m.getCurrentState()).toBe('idle');
  });

  it('send returns false when not running', () => {
    const m = simpleMachine();
    expect(m.send('START')).toBe(false);
  });

  it('stop exits all states', () => {
    const m = simpleMachine();
    m.start();
    m.send('START');
    m.stop();
    expect(m.isRunning()).toBe(false);
  });

  it('start is idempotent', () => {
    const m = simpleMachine();
    m.start();
    m.start(); // should not throw or reset
    expect(m.getCurrentState()).toBe('idle');
  });

  it('respects maxTransitions', () => {
    const m = new StateMachineBuilder({ maxTransitions: 2, snapshotInterval: 100 })
      .state('a', { initial: true })
      .state('b')
      .transition('a', 'b', 'GO')
      .transition('b', 'a', 'BACK')
      .build();
    m.start();
    m.send('GO');   // 1
    m.send('BACK'); // 2
    expect(m.send('GO')).toBe(false); // at limit
  });

  it('tracks transition count', () => {
    const m = simpleMachine();
    m.start();
    m.send('START');
    m.send('FINISH');
    expect(m.getContext().transitionCount).toBe(2);
  });

  it('canSend checks available transitions', () => {
    const m = simpleMachine();
    m.start();
    expect(m.canSend('START')).toBe(true);
    expect(m.canSend('FINISH')).toBe(false);
  });

  it('getAvailableEvents lists current events', () => {
    const m = simpleMachine();
    m.start();
    expect(m.getAvailableEvents()).toEqual(['START']);
  });

  it('getData/setData work', () => {
    const m = simpleMachine();
    m.start();
    m.setData('key', 42);
    expect(m.getData('key')).toBe(42);
  });

  it('isInState checks descendants too', () => {
    const m = new StateMachineBuilder()
      .state('parent', { initial: true })
      .state('child', { parent: 'parent', initial: true })
      .build();
    m.start();
    // Current state should be 'child' (entered via parent → initial child)
    expect(m.isInState('child')).toBe(true);
    expect(m.isInState('parent')).toBe(true); // isDescendant check
  });

  it('getStats returns correct info', () => {
    const m = simpleMachine();
    m.start();
    m.send('START');
    const stats = m.getStats();
    expect(stats.transitionCount).toBe(1);
    expect(stats.currentStates).toEqual({ default: 'running' });
    expect(stats.logSize).toBeGreaterThan(0);
  });
});

// ─── Guards and Actions ───

describe('Guards and Actions', () => {
  it('guard blocks transition', () => {
    const m = new StateMachineBuilder()
      .state('a', { initial: true })
      .state('b')
      .transition('a', 'b', 'GO', { guard: 'allowed' })
      .guard('allowed', (ctx) => ctx.data.ok as boolean)
      .build();
    m.start({ ok: false });
    expect(m.send('GO')).toBe(false);
    m.setData('ok', true);
    expect(m.send('GO')).toBe(true);
    expect(m.getCurrentState()).toBe('b');
  });

  it('action executes on transition', () => {
    const m = new StateMachineBuilder()
      .state('a', { initial: true })
      .state('b')
      .transition('a', 'b', 'GO', { action: 'increment' })
      .action('increment', (ctx) => { ctx.data.count = ((ctx.data.count as number) || 0) + 1; })
      .build();
    m.start();
    m.send('GO');
    expect(m.getData('count')).toBe(1);
  });

  it('onEnter/onExit actions fire', () => {
    const log: string[] = [];
    const m = new StateMachineBuilder()
      .state('a', { initial: true, onExit: 'exitA' })
      .state('b', { onEnter: 'enterB' })
      .transition('a', 'b', 'GO')
      .action('exitA', () => log.push('exitA'))
      .action('enterB', () => log.push('enterB'))
      .build();
    m.start();
    m.send('GO');
    expect(log).toContain('exitA');
    expect(log).toContain('enterB');
  });

  it('internal transition does not fire enter/exit', () => {
    const log: string[] = [];
    const m = new StateMachineBuilder()
      .state('a', { initial: true, onEnter: 'enter', onExit: 'exit' })
      .transition('a', 'a', 'TICK', { internal: true, action: 'doTick' })
      .action('enter', () => log.push('enter'))
      .action('exit', () => log.push('exit'))
      .action('doTick', () => log.push('tick'))
      .build();
    m.start();
    log.length = 0; // clear initial enter
    m.send('TICK');
    expect(log).toEqual(['tick']);
  });
});

// ─── Observers ───

describe('Observers', () => {
  it('notifies on state transitions', () => {
    const events: StateChangeEvent[] = [];
    const m = simpleMachine();
    m.onStateChange('*', (e) => events.push(e));
    m.start();
    m.send('START');
    const types = events.map(e => `${e.type}:${e.state}`);
    expect(types).toContain('enter:idle');
    expect(types).toContain('exit:idle');
    expect(types).toContain('enter:running');
    expect(types).toContain('transition:running');
  });

  it('removeObserver stops notifications', () => {
    const events: StateChangeEvent[] = [];
    const m = simpleMachine();
    const id = m.onStateChange('*', (e) => events.push(e));
    m.start();
    m.removeObserver(id);
    events.length = 0;
    m.send('START');
    expect(events.length).toBe(0);
  });
});

// ─── Timeouts ───

describe('Timeouts', () => {
  it('tick fires timeout event and triggers transition', () => {
    const m = new StateMachineBuilder({ snapshotInterval: 100 })
      .state('waiting', { initial: true, timeout: { durationMs: 100, event: 'EXPIRED', resetOnReentry: false } })
      .state('done')
      .transition('waiting', 'done', 'EXPIRED')
      .build();
    m.start();
    expect(m.getCurrentState()).toBe('waiting');
    // Simulate time passing
    m.tick(Date.now() + 200);
    expect(m.getCurrentState()).toBe('done');
  });

  it('timeout cleared when exiting state', () => {
    const events: StateChangeEvent[] = [];
    const m = new StateMachineBuilder({ snapshotInterval: 100 })
      .state('a', { initial: true, timeout: { durationMs: 100, event: 'TIMEOUT', resetOnReentry: false } })
      .state('b')
      .transition('a', 'b', 'GO')
      .transition('a', 'b', 'TIMEOUT')
      .build();
    m.start();
    m.onStateChange('*', (e) => events.push(e));
    m.send('GO'); // exits a, clears timeout
    m.tick(Date.now() + 200); // timeout should not fire
    expect(m.getCurrentState()).toBe('b');
  });
});

// ─── Hierarchical States ───

describe('Hierarchical States', () => {
  it('entering parent enters initial child', () => {
    const m = new StateMachineBuilder()
      .state('idle', { initial: true })
      .state('busy')
      .state('busy.processing', { parent: 'busy', initial: true })
      .state('busy.waiting', { parent: 'busy' })
      .transition('idle', 'busy', 'START')
      .transition('busy.processing', 'busy.waiting', 'WAIT')
      .build();
    m.start();
    m.send('START');
    // Should be in busy.processing (initial child of busy)
    expect(m.getCurrentState()).toBe('busy.processing');
    expect(m.isInState('busy')).toBe(true);
  });

  it('history remembers last child state', () => {
    const m = new StateMachineBuilder()
      .state('idle', { initial: true })
      .state('busy', { history: 'shallow' })
      .state('busy.a', { parent: 'busy', initial: true })
      .state('busy.b', { parent: 'busy' })
      .transition('idle', 'busy', 'START')
      .transition('busy.a', 'busy.b', 'NEXT')
      .transition('busy.b', 'idle', 'STOP')
      .build();
    m.start();
    m.send('START');      // → busy.a
    m.send('NEXT');       // → busy.b
    m.send('STOP');       // → idle (history saves busy.b)
    m.send('START');      // → busy → should restore to busy.b
    expect(m.getCurrentState()).toBe('busy.b');
  });
});

// ─── Snapshots and Restore ───

describe('Snapshots', () => {
  it('auto-snapshots at configured interval', () => {
    const m = new StateMachineBuilder({ snapshotInterval: 2 })
      .state('a', { initial: true })
      .state('b')
      .state('c')
      .transition('a', 'b', 'GO')
      .transition('b', 'c', 'GO')
      .build();
    m.start(); // initial snapshot (v1)
    m.send('GO'); // transition 1
    m.send('GO'); // transition 2 → auto snapshot (v2)
    const snap = m.getSnapshot();
    expect(snap).toBeDefined();
    expect(snap!.version).toBeGreaterThanOrEqual(2);
  });

  it('restoreFromSnapshot reverts state', () => {
    const m = new StateMachineBuilder({ snapshotInterval: 100 })
      .state('a', { initial: true })
      .state('b')
      .state('c')
      .transition('a', 'b', 'GO')
      .transition('b', 'c', 'GO')
      .build();
    m.start(); // snap v1
    m.send('GO');
    m.send('GO');
    expect(m.getCurrentState()).toBe('c');
    // Restore to initial
    expect(m.restoreFromSnapshot(1)).toBe(true);
    expect(m.getCurrentState()).toBe('a');
  });

  it('restoreFromSnapshot returns false for bad version', () => {
    const m = simpleMachine();
    m.start();
    expect(m.restoreFromSnapshot(999)).toBe(false);
  });
});

// ─── Invariants ───

describe('Invariants', () => {
  it('tick checks invariants and notifies on violation', () => {
    const violations: StateChangeEvent[] = [];
    const m = new StateMachineBuilder({ deadlockDetectionEnabled: false })
      .state('active', {
        initial: true,
        invariants: [{
          name: 'must-be-positive',
          condition: (ctx) => (ctx.data.value as number) > 0,
          severity: 'fatal',
          message: 'value must be positive',
        }],
      })
      .build();
    m.start({ value: 5 });
    m.onStateChange('*', (e) => { if (e.type === 'invariant-violation') violations.push(e); });
    m.tick(); // passes
    expect(violations.length).toBe(0);
    m.setData('value', -1);
    m.tick();
    expect(violations.length).toBe(1);
  });
  it('non-fatal invariant violations DO trigger observer notification (bug fixed)', () => {
    const violations: StateChangeEvent[] = [];
    const m = new StateMachineBuilder({ deadlockDetectionEnabled: false })
      .state('active', {
        initial: true,
        invariants: [{
          name: 'should-notify',
          condition: () => false,
          severity: 'error',
          message: 'error-level violation',
        }],
      })
      .build();
    m.start();
    m.onStateChange('*', (e) => { if (e.type === 'invariant-violation') violations.push(e); });
    m.tick();
    // Violation IS recorded internally...
    expect(m.getInvariantViolations().length).toBeGreaterThan(0);
    // ...and observers ARE notified for all severities (fixed)
    expect(violations.length).toBe(1);
  });
});

// ─── Deadlock Detection ───

describe('Deadlock Detection in tick', () => {
  it('notifies on deadlocked states', () => {
    const violations: StateChangeEvent[] = [];
    const m = new StateMachineBuilder({ deadlockDetectionEnabled: true })
      .state('trap', { initial: true })
      // No transitions from trap!
      .build();
    m.onStateChange('*', (e) => { if (e.type === 'invariant-violation') violations.push(e); });
    m.start();
    m.tick();
    const deadlockViolation = violations.find(v => v.region === 'deadlock-detector' && v.state === 'trap');
    expect(deadlockViolation).toBeDefined();
  });
});

// ─── Priority / Conflict Resolution ───

describe('Transition Priority', () => {
  it('higher priority wins when multiple transitions match', () => {
    const m = new StateMachineBuilder()
      .state('a', { initial: true })
      .state('b')
      .state('c')
      .transition('a', 'b', 'GO', { priority: 1 })
      .transition('a', 'c', 'GO', { priority: 10 })
      .build();
    m.start();
    m.send('GO');
    expect(m.getCurrentState()).toBe('c'); // priority 10 wins
  });
});

// ─── Wildcard Transitions ───

describe('Wildcard Transitions', () => {
  it('from * matches any current state', () => {
    const m = new StateMachineBuilder()
      .state('a', { initial: true })
      .state('b')
      .state('error')
      .transition('a', 'b', 'GO')
      .transition('*', 'error', 'CRASH')
      .build();
    m.start();
    m.send('GO');
    expect(m.getCurrentState()).toBe('b');
    m.send('CRASH');
    expect(m.getCurrentState()).toBe('error');
  });
});

// ─── Preset Machines ───

describe('createAgentLifecycleMachine', () => {
  it('follows basic lifecycle', () => {
    const m = createAgentLifecycleMachine();
    m.start();
    expect(m.getCurrentState()).toBe('initializing');
    m.send('INITIALIZED');
    expect(m.getCurrentState()).toBe('ready');
    expect(m.getData('readyAt')).toBeDefined();
    m.send('TASK_ASSIGNED');
    expect(m.getCurrentState()).toBe('busy.processing');
    m.send('TASK_COMPLETED');
    expect(m.getCurrentState()).toBe('ready');
    expect(m.getData('tasksCompleted')).toBe(1);
    m.send('SHUTDOWN');
    expect(m.getCurrentState()).toBe('shutting-down');
    m.send('CLEANUP_DONE');
    expect(m.getCurrentState()).toBe('terminated');
  });

  it('handles degraded → recovery flow', () => {
    const m = createAgentLifecycleMachine();
    m.start({ errorCount: 0 });
    m.send('INITIALIZED');
    m.send('TASK_ASSIGNED');
    m.send('ERROR'); // recoverable (errorCount < 3)
    expect(m.getCurrentState()).toBe('degraded');
    m.send('RECOVERED');
    expect(m.getCurrentState()).toBe('ready');
    expect(m.getData('recoveries')).toBe(1);
  });

  it('guard blocks ERROR when errorCount >= 3', () => {
    const m = createAgentLifecycleMachine();
    m.start({ errorCount: 3 });
    m.send('INITIALIZED');
    m.send('TASK_ASSIGNED');
    expect(m.send('ERROR')).toBe(false); // guard fails
    expect(m.getCurrentState()).toBe('busy.processing');
  });

  it('busy substates work with history', () => {
    const m = createAgentLifecycleMachine();
    m.start();
    m.send('INITIALIZED');
    m.send('TASK_ASSIGNED');
    expect(m.getCurrentState()).toBe('busy.processing');
    m.send('AWAIT_DEPENDENCY');
    expect(m.getCurrentState()).toBe('busy.waiting');
    m.send('DEPENDENCY_RESOLVED');
    expect(m.getCurrentState()).toBe('busy.processing');
  });
});

describe('createProtocolHandshakeMachine', () => {
  it('follows happy path', () => {
    const m = createProtocolHandshakeMachine();
    m.start();
    expect(m.getCurrentState()).toBe('disconnected');
    m.send('CONNECT');
    expect(m.getCurrentState()).toBe('connecting');
    m.send('SYN_ACK');
    expect(m.getCurrentState()).toBe('version-negotiation');
    m.send('VERSION_AGREED');
    expect(m.getCurrentState()).toBe('authenticating');
    m.send('AUTH_SUCCESS');
    expect(m.getCurrentState()).toBe('connected');
    expect(m.getData('connectedAt')).toBeDefined();
  });

  it('handles reconnection', () => {
    const m = createProtocolHandshakeMachine();
    m.start();
    m.send('CONNECT');
    m.send('SYN_ACK');
    m.send('VERSION_AGREED');
    m.send('AUTH_SUCCESS');
    m.send('DISCONNECTED');
    expect(m.getCurrentState()).toBe('reconnecting');
    m.send('RETRY');
    expect(m.getCurrentState()).toBe('connecting');
  });
});

describe('createTaskWorkflowMachine', () => {
  it('follows full workflow', () => {
    const m = createTaskWorkflowMachine();
    m.start();
    expect(m.getCurrentState()).toBe('draft');
    m.send('SUBMIT');
    m.send('VALIDATE');
    m.send('VALID');
    m.send('ASSIGN');
    m.send('START');
    expect(m.getCurrentState()).toBe('in-progress.active');
    m.send('REVIEW');
    expect(m.getCurrentState()).toBe('in-progress.reviewing');
    m.send('APPROVE');
    expect(m.getCurrentState()).toBe('completed');
  });

  it('handles validation failure loop', () => {
    const m = createTaskWorkflowMachine();
    m.start();
    m.send('SUBMIT');
    m.send('VALIDATE');
    m.send('INVALID');
    expect(m.getCurrentState()).toBe('draft');
  });

  it('pause and resume', () => {
    const m = createTaskWorkflowMachine();
    m.start();
    m.send('SUBMIT');
    m.send('VALIDATE');
    m.send('VALID');
    m.send('ASSIGN');
    m.send('START');
    m.send('PAUSE');
    expect(m.getCurrentState()).toBe('in-progress.paused');
    m.send('RESUME');
    expect(m.getCurrentState()).toBe('in-progress.active');
  });
});

// ─── Transition History ───

describe('Transition History', () => {
  it('getTransitionHistory returns records', () => {
    const m = simpleMachine();
    m.start();
    m.send('START');
    m.send('FINISH');
    const history = m.getTransitionHistory();
    expect(history.length).toBeGreaterThanOrEqual(2);
  });

  it('getTransitionHistory with count limits', () => {
    const m = simpleMachine();
    m.start();
    m.send('START');
    m.send('FINISH');
    expect(m.getTransitionHistory(1).length).toBe(1);
  });
});

// ─── ParallelRegionCoordinator ───

describe('ParallelRegionCoordinator', () => {
  it('join condition checks all regions', () => {
    const coord = new ParallelRegionCoordinator();
    const ctx: MachineContext = {
      currentStates: new Map([['r1', 'done'], ['r2', 'done']]),
      history: new Map(), data: {},
      enteredAt: new Map(), transitionCount: 0, startedAt: 0,
    };
    const target = new Map([['r1', 'done'], ['r2', 'done']]);
    expect(coord.computeJoinCondition(['r1', 'r2'], target, ctx)).toBe(true);
    ctx.currentStates.set('r2', 'working');
    expect(coord.computeJoinCondition(['r1', 'r2'], target, ctx)).toBe(false);
  });

  it('forkToRegions sets initial states', () => {
    const coord = new ParallelRegionCoordinator();
    const ctx: MachineContext = {
      currentStates: new Map(),
      history: new Map(), data: {},
      enteredAt: new Map(), transitionCount: 0, startedAt: 0,
    };
    coord.forkToRegions(['r1', 'r2'], new Map([['r1', 'a'], ['r2', 'b']]), ctx);
    expect(ctx.currentStates.get('r1')).toBe('a');
    expect(ctx.currentStates.get('r2')).toBe('b');
  });
});
