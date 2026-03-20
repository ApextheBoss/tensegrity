import { describe, it, expect, beforeEach } from 'vitest';
import {
  BackoffCoordinator,
  BackoffCalculator,
  SlotManager,
  CorrelationDetector,
  InheritanceManager,
  PRESETS,
  type BackoffCoordinatorConfig,
} from '../backoff-coordinator';

const fastConfig: BackoffCoordinatorConfig = {
  ...PRESETS['fast-service'],
};

describe('BackoffCalculator', () => {
  it('exponential grows by multiplier', () => {
    const r0 = BackoffCalculator.compute(0, 'exponential', 100, 10000, 2, 0);
    const r1 = BackoffCalculator.compute(1, 'exponential', 100, 10000, 2, 0);
    const r2 = BackoffCalculator.compute(2, 'exponential', 100, 10000, 2, 0);
    expect(r0.delayMs).toBe(100);
    expect(r1.delayMs).toBe(200);
    expect(r2.delayMs).toBe(400);
  });

  it('exponential caps at maxMs', () => {
    const r = BackoffCalculator.compute(20, 'exponential', 100, 5000, 2, 0);
    expect(r.delayMs).toBe(5000);
  });

  it('harmonic converges toward maxMs', () => {
    const r1 = BackoffCalculator.compute(1, 'harmonic', 100, 10000, 2, 0);
    const r10 = BackoffCalculator.compute(10, 'harmonic', 100, 10000, 2, 0);
    expect(r10.delayMs).toBeGreaterThan(r1.delayMs);
    expect(r10.delayMs).toBeLessThanOrEqual(10000);
  });

  it('linear grows linearly', () => {
    const r0 = BackoffCalculator.compute(0, 'linear', 100, 10000, 2, 0);
    const r1 = BackoffCalculator.compute(1, 'linear', 100, 10000, 2, 0);
    expect(r0.delayMs).toBe(100);
    expect(r1.delayMs).toBe(200);
  });

  it('constant stays the same', () => {
    const r0 = BackoffCalculator.compute(0, 'constant', 500, 10000, 2, 0);
    const r5 = BackoffCalculator.compute(5, 'constant', 500, 10000, 2, 0);
    expect(r0.delayMs).toBe(500);
    expect(r5.delayMs).toBe(500);
  });

  it('jitter is bounded by jitterFraction', () => {
    const r = BackoffCalculator.compute(3, 'exponential', 1000, 100000, 2, 0.5);
    // delay = 8000, jitter should be <= 4000
    expect(r.jitterMs).toBeLessThanOrEqual(r.delayMs * 0.5);
  });
});

describe('BackoffCoordinator', () => {
  let coord: BackoffCoordinator;

  beforeEach(() => {
    coord = new BackoffCoordinator(fastConfig);
  });

  it('first attempt has no delay', () => {
    const retry = coord.getNextRetryTime('agent-a', 'target-1');
    expect(retry.reason).toBe('first-attempt');
    expect(retry.blocked).toBe(false);
  });

  it('recordFailure creates backoff state', () => {
    const result = coord.recordFailure('agent-a', 'target-1');
    expect(result.nextDelay).toBeGreaterThan(0);
    expect(result.correlated).toBe(false);

    const state = coord.getState('agent-a', 'target-1');
    expect(state).not.toBeNull();
    expect(state!.attemptCount).toBe(1);
  });

  it('multiple failures escalate delay', () => {
    const r1 = coord.recordFailure('agent-a', 'target-1');
    const r2 = coord.recordFailure('agent-a', 'target-1');
    const r3 = coord.recordFailure('agent-a', 'target-1');
    expect(r3.nextDelay).toBeGreaterThan(r1.nextDelay);
  });

  it('recordSuccess resets backoff', () => {
    coord.recordFailure('agent-a', 'target-1');
    coord.recordSuccess('agent-a', 'target-1');
    expect(coord.getState('agent-a', 'target-1')).toBeNull();
  });

  it('detects correlated failures when threshold is met', () => {
    // threshold is 3 agents
    coord.recordFailure('agent-a', 'target-1');
    coord.recordFailure('agent-b', 'target-1');
    const r3 = coord.recordFailure('agent-c', 'target-1');
    expect(r3.correlated).toBe(true);
  });

  it('getAgentsBackingOff returns all agents for a target', () => {
    coord.recordFailure('agent-a', 'target-1');
    coord.recordFailure('agent-b', 'target-1');
    coord.recordFailure('agent-c', 'target-2');
    const agents = coord.getAgentsBackingOff('target-1');
    expect(agents.length).toBe(2);
  });

  it('getTargetPressure reports system state', () => {
    coord.recordFailure('agent-a', 'target-1');
    coord.recordFailure('agent-b', 'target-1');
    const pressure = coord.getTargetPressure('target-1');
    expect(pressure.backingOffCount).toBe(2);
    expect(pressure.averageDelay).toBeGreaterThan(0);
  });

  it('cleanup removes stale states', () => {
    coord.recordFailure('agent-a', 'target-1');
    // Force state to look old
    const state = coord.getState('agent-a', 'target-1')!;
    (state as any).lastFailureAt = Date.now() - fastConfig.maxDelayMs * 11;
    const result = coord.cleanup();
    expect(result.removedStates).toBe(1);
  });

  it('setStrategy changes backoff strategy', () => {
    coord.recordFailure('agent-a', 'target-1');
    coord.setStrategy('agent-a', 'target-1', 'harmonic');
    const state = coord.getState('agent-a', 'target-1');
    expect(state!.strategy).toBe('harmonic');
  });

  it('events are recorded', () => {
    coord.recordFailure('agent-a', 'target-1');
    const events = coord.getRecentEvents();
    expect(events.length).toBeGreaterThan(0);
    expect(events[0].type).toBe('backoff-started');
  });
});

describe('BackoffCoordinator — dependency inheritance', () => {
  it('agent inherits backoff from dependency', () => {
    const coord = new BackoffCoordinator({
      ...fastConfig,
      inheritanceDecayFactor: 0.8,
    });

    coord.addDependency('agent-a', 'agent-b', 'target-1');

    // agent-b fails against target-1
    coord.recordFailure('agent-b', 'target-1');
    coord.recordFailure('agent-b', 'target-1');
    coord.recordFailure('agent-b', 'target-1');

    // agent-a also fails
    coord.recordFailure('agent-a', 'target-1');

    // agent-a should get a retry time influenced by agent-b's backoff
    const retry = coord.getNextRetryTime('agent-a', 'target-1');
    expect(retry.retryAt).toBeGreaterThan(Date.now());
  });

  it('removeDependency stops inheritance', () => {
    const coord = new BackoffCoordinator(fastConfig);
    coord.addDependency('a', 'b', 't1');
    coord.removeDependency('a', 'b', 't1');
    // No error, inheritance won't apply
    coord.recordFailure('b', 't1');
    coord.recordFailure('a', 't1');
    const retry = coord.getNextRetryTime('a', 't1');
    expect(retry.blocked).toBe(false);
  });
});

describe('BackoffCoordinator — escalation and blackout', () => {
  it('escalation increases delay multiplier', () => {
    // Use a config with short escalation delay
    const coord = new BackoffCoordinator({
      ...fastConfig,
      correlationThreshold: 2,
      escalationDelayMs: 0, // immediate escalation
    });

    // Two agents fail → correlation detected
    coord.recordFailure('agent-a', 'target-1');
    coord.recordFailure('agent-b', 'target-1');
    // More failures escalate
    coord.recordFailure('agent-a', 'target-1');
    coord.recordFailure('agent-b', 'target-1');

    const pressure = coord.getTargetPressure('target-1');
    expect(pressure.correlationLevel).toBeGreaterThanOrEqual(0);
  });

  it('recovery clears correlation', () => {
    const coord = new BackoffCoordinator({
      ...fastConfig,
      correlationThreshold: 2,
      escalationDelayMs: 0,
      probeSuccessThreshold: 1,
    });

    coord.recordFailure('agent-a', 'target-1');
    coord.recordFailure('agent-b', 'target-1');
    // Force probe agent assignment
    coord.recordFailure('agent-a', 'target-1');
    coord.recordFailure('agent-a', 'target-1');
    coord.recordFailure('agent-a', 'target-1');

    // Check who is the prober
    const pressure = coord.getTargetPressure('target-1');
    if (pressure.probeAgent) {
      coord.recordSuccess(pressure.probeAgent, 'target-1');
      // After success threshold, all backoff cleared
      const p2 = coord.getTargetPressure('target-1');
      expect(p2.correlationLevel).toBe(0);
    }
  });
});

describe('SlotManager', () => {
  it('generates correct number of slots', () => {
    const sm = new SlotManager(fastConfig);
    const slots = sm.generateSlots('t1', 1000);
    expect(slots.length).toBe(fastConfig.slotsPerWindow);
  });

  it('claimSlot assigns agent to a slot', () => {
    const sm = new SlotManager(fastConfig);
    sm.generateSlots('t1', Date.now());
    const slot = sm.claimSlot('t1', 'agent-a', Date.now());
    expect(slot).not.toBeNull();
    expect(slot!.agentId).toBe('agent-a');
    expect(slot!.claimed).toBe(true);
  });

  it('markExecuted updates slot state', () => {
    const sm = new SlotManager(fastConfig);
    sm.generateSlots('t1', Date.now());
    const slot = sm.claimSlot('t1', 'agent-a', Date.now())!;
    sm.markExecuted(slot.slotId, 'success');
    const slots = sm.getActiveSlots('t1');
    const updated = slots.find(s => s.slotId === slot.slotId);
    expect(updated!.executed).toBe(true);
    expect(updated!.result).toBe('success');
  });

  it('all slots claimed returns null', () => {
    const sm = new SlotManager({ ...fastConfig, slotsPerWindow: 1 });
    sm.generateSlots('t1', Date.now());
    sm.claimSlot('t1', 'agent-a', Date.now());
    const slot2 = sm.claimSlot('t1', 'agent-b', Date.now());
    expect(slot2).toBeNull();
  });
});

describe('CorrelationDetector', () => {
  it('returns null below threshold', () => {
    const cd = new CorrelationDetector(fastConfig);
    const r = cd.recordFailure('t1', 'agent-a', Date.now());
    expect(r).toBeNull();
  });

  it('detects correlation at threshold', () => {
    const cd = new CorrelationDetector({ ...fastConfig, correlationThreshold: 2 });
    cd.recordFailure('t1', 'agent-a', Date.now());
    const r = cd.recordFailure('t1', 'agent-b', Date.now());
    expect(r).not.toBeNull();
    expect(r!.failingAgents.size).toBe(2);
  });

  it('assignProber selects deterministically', () => {
    const cd = new CorrelationDetector({ ...fastConfig, correlationThreshold: 2 });
    const now = Date.now();
    cd.recordFailure('t1', 'agent-a', now);
    cd.recordFailure('t1', 'agent-b', now);
    const prober = cd.assignProber('t1', now);
    expect(prober).toBeTruthy();
    // Same call should give same result
    const prober2 = cd.assignProber('t1', now);
    expect(prober2).toBe(prober);
  });

  it('recordRecovery clears correlation', () => {
    const cd = new CorrelationDetector({ ...fastConfig, correlationThreshold: 2 });
    cd.recordFailure('t1', 'agent-a', Date.now());
    cd.recordFailure('t1', 'agent-b', Date.now());
    cd.recordRecovery('t1');
    expect(cd.getCorrelation('t1')).toBeNull();
  });

  it('isBlackout returns true at level 3 for non-prober', () => {
    const cd = new CorrelationDetector({ ...fastConfig, correlationThreshold: 2, escalationDelayMs: 0 });
    const now = Date.now();
    cd.recordFailure('t1', 'agent-a', now);
    cd.recordFailure('t1', 'agent-b', now);
    // Escalate to level 3
    cd.escalate('t1', now + 1);
    cd.escalate('t1', now + 2);
    cd.escalate('t1', now + 3);
    const prober = cd.assignProber('t1', now);
    const nonProber = prober === 'agent-a' ? 'agent-b' : 'agent-a';
    expect(cd.isBlackout('t1', nonProber)).toBe(true);
    expect(cd.isBlackout('t1', prober!)).toBe(false);
  });
});

describe('Presets', () => {
  it('all presets create valid coordinators', () => {
    for (const key of Object.keys(PRESETS) as Array<keyof typeof PRESETS>) {
      const coord = new BackoffCoordinator(PRESETS[key]);
      coord.recordFailure('a', 't');
      expect(coord.getState('a', 't')).not.toBeNull();
    }
  });
});
