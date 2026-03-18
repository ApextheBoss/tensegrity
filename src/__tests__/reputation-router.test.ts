import { describe, it, expect } from 'vitest';
import { routeTask, routeTaskWithFallback, decayedReputation, Agent, Task } from '../reputation-router';

function makeAgent(overrides: Partial<Agent> = {}): Agent {
  return {
    address: 'agent-1',
    name: 'Agent One',
    reputationByDomain: { coding: 800 },
    availability: true,
    lastActiveMs: Date.now() - 3600_000, // 1 hour ago
    ...overrides,
  };
}

function makeTask(overrides: Partial<Task> = {}): Task {
  return {
    id: 'task-1',
    domain: 'coding',
    complexity: 'tier1',
    requiredMinReputation: 100,
    deadline: Date.now() + 3600_000,
    ...overrides,
  };
}

describe('decayedReputation', () => {
  it('returns base reputation for recently active agents', () => {
    const agent = makeAgent({ lastActiveMs: Date.now() });
    const rep = decayedReputation(agent, 'coding', Date.now());
    expect(rep).toBe(800);
  });

  it('decays reputation over time', () => {
    const thirtyDaysAgo = Date.now() - 30 * 24 * 3600_000;
    const agent = makeAgent({ lastActiveMs: thirtyDaysAgo });
    const rep = decayedReputation(agent, 'coding', Date.now());
    expect(rep).toBeLessThan(800);
    expect(rep).toBeGreaterThan(0);
  });

  it('returns 0 for unknown domain', () => {
    const agent = makeAgent();
    expect(decayedReputation(agent, 'unknown', Date.now())).toBe(0);
  });
});

describe('routeTask', () => {
  it('returns null when no agents available', () => {
    expect(routeTask([], makeTask())).toBeNull();
  });

  it('returns null when all agents are unavailable', () => {
    const agent = makeAgent({ availability: false });
    expect(routeTask([agent], makeTask())).toBeNull();
  });

  it('returns null when no agent meets min reputation', () => {
    const agent = makeAgent({ reputationByDomain: { coding: 10 } });
    expect(routeTask([agent], makeTask({ requiredMinReputation: 500 }))).toBeNull();
  });

  it('selects the best agent by score', () => {
    const agents = [
      makeAgent({ address: 'low', reputationByDomain: { coding: 200 }, lastActiveMs: Date.now() - 86400_000 * 7 }),
      makeAgent({ address: 'high', reputationByDomain: { coding: 900 }, lastActiveMs: Date.now() - 3600_000 }),
    ];
    const result = routeTask(agents, makeTask());
    expect(result).not.toBeNull();
    expect(result!.agent.address).toBe('high');
    expect(result!.score).toBeGreaterThan(0);
    expect(result!.reason).toContain('Selected');
  });

  it('prefers recently active agents', () => {
    const agents = [
      makeAgent({ address: 'old', reputationByDomain: { coding: 800 }, lastActiveMs: Date.now() - 86400_000 * 30 }),
      makeAgent({ address: 'new', reputationByDomain: { coding: 750 }, lastActiveMs: Date.now() - 60_000 }),
    ];
    const result = routeTask(agents, makeTask());
    expect(result!.agent.address).toBe('new');
  });

  it('filters out unavailable agents even if they score highest', () => {
    const agents = [
      makeAgent({ address: 'best-unavail', reputationByDomain: { coding: 999 }, availability: false }),
      makeAgent({ address: 'ok-avail', reputationByDomain: { coding: 300 } }),
    ];
    const result = routeTask(agents, makeTask());
    expect(result!.agent.address).toBe('ok-avail');
  });
});

describe('routeTaskWithFallback', () => {
  it('returns normal result when available', () => {
    const agent = makeAgent({ reputationByDomain: { coding: 500 } });
    const result = routeTaskWithFallback([agent], makeTask({ requiredMinReputation: 200 }));
    expect(result).not.toBeNull();
    expect(result!.reason).not.toContain('FALLBACK');
  });

  it('falls back with relaxed reputation requirement', () => {
    const agent = makeAgent({ reputationByDomain: { coding: 80 } });
    const task = makeTask({ requiredMinReputation: 100 });
    // 80 < 100 → normal fails. Fallback: 100 * 0.5 = 50, 80 > 50 → matches
    const result = routeTaskWithFallback([agent], task);
    expect(result).not.toBeNull();
    expect(result!.reason).toContain('FALLBACK');
  });

  it('returns null if even fallback fails', () => {
    const agent = makeAgent({ reputationByDomain: { coding: 10 } });
    const task = makeTask({ requiredMinReputation: 100 });
    // 10 < 50 (fallback threshold) → null
    const result = routeTaskWithFallback([agent], task);
    expect(result).toBeNull();
  });
});
