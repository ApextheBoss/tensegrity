import { describe, it, expect, beforeEach } from 'vitest';
import { CircuitBreaker, CircuitBreakerRegistry } from '../circuit-breaker';

describe('CircuitBreaker', () => {
  let breaker: CircuitBreaker;

  beforeEach(() => {
    breaker = new CircuitBreaker('agent-0x1', {
      failureThreshold: 3,
      resetTimeoutMs: 100,
      halfOpenMaxAttempts: 2,
      monitorWindowMs: 5000,
    });
  });

  it('starts in closed state', () => {
    expect(breaker.getMetrics().state).toBe('closed');
  });

  it('stays closed on success', async () => {
    await breaker.execute(() => Promise.resolve('ok'));
    expect(breaker.getMetrics().state).toBe('closed');
    expect(breaker.getMetrics().totalRequests).toBe(1);
  });

  it('opens after threshold failures', async () => {
    const fail = () => Promise.reject(new Error('down'));
    for (let i = 0; i < 3; i++) {
      await breaker.execute(fail).catch(() => {});
    }
    expect(breaker.getMetrics().state).toBe('open');
    expect(breaker.getMetrics().totalFailures).toBe(3);
  });

  it('rejects calls when open', async () => {
    const fail = () => Promise.reject(new Error('down'));
    for (let i = 0; i < 3; i++) {
      await breaker.execute(fail).catch(() => {});
    }

    await expect(breaker.execute(() => Promise.resolve('ok')))
      .rejects.toThrow(/Circuit OPEN/);
  });

  it('transitions to half-open after reset timeout', async () => {
    const fail = () => Promise.reject(new Error('down'));
    for (let i = 0; i < 3; i++) {
      await breaker.execute(fail).catch(() => {});
    }

    // Wait for reset timeout
    await new Promise(r => setTimeout(r, 150));

    // Next call should go through (half-open)
    const result = await breaker.execute(() => Promise.resolve('recovered'));
    expect(result).toBe('recovered');
    expect(breaker.getMetrics().state).toBe('half_open');
  });

  it('closes after enough half-open successes', async () => {
    const fail = () => Promise.reject(new Error('down'));
    for (let i = 0; i < 3; i++) {
      await breaker.execute(fail).catch(() => {});
    }

    await new Promise(r => setTimeout(r, 150));

    // 2 successes in half-open → closed
    await breaker.execute(() => Promise.resolve('ok'));
    await breaker.execute(() => Promise.resolve('ok'));
    expect(breaker.getMetrics().state).toBe('closed');
  });

  it('reopens on failure in half-open', async () => {
    const fail = () => Promise.reject(new Error('down'));
    for (let i = 0; i < 3; i++) {
      await breaker.execute(fail).catch(() => {});
    }

    await new Promise(r => setTimeout(r, 150));

    // Fail in half-open → back to open
    await breaker.execute(fail).catch(() => {});
    expect(breaker.getMetrics().state).toBe('open');
  });

  it('does NOT reset failure count on success in closed state (sliding window)', async () => {
    const fail = () => Promise.reject(new Error('down'));
    await breaker.execute(fail).catch(() => {});
    await breaker.execute(fail).catch(() => {});

    // Success does NOT reset the window — failures persist until they age out
    await breaker.execute(() => Promise.resolve('ok'));
    expect(breaker.getMetrics().failures).toBe(2);

    // One more failure trips the breaker (2 prior + 1 = 3 = threshold)
    await breaker.execute(fail).catch(() => {});
    expect(breaker.getMetrics().state).toBe('open');
  });

  it('opens on bursty failure pattern interleaved with successes', async () => {
    const fail = () => Promise.reject(new Error('down'));
    const ok = () => Promise.resolve('ok');

    // fail, success, fail, success, fail → should trip at threshold=3
    await breaker.execute(fail).catch(() => {});
    await breaker.execute(ok);
    await breaker.execute(fail).catch(() => {});
    await breaker.execute(ok);
    await breaker.execute(fail).catch(() => {});

    expect(breaker.getMetrics().state).toBe('open');
  });

  it('failures age out of sliding window', async () => {
    // Use a very short window
    const shortBreaker = new CircuitBreaker('agent-short', {
      failureThreshold: 3,
      resetTimeoutMs: 100,
      halfOpenMaxAttempts: 2,
      monitorWindowMs: 100, // 100ms window
    });

    const fail = () => Promise.reject(new Error('down'));
    await shortBreaker.execute(fail).catch(() => {});
    await shortBreaker.execute(fail).catch(() => {});

    // Wait for failures to age out
    await new Promise(r => setTimeout(r, 150));

    // This failure should be the only one in the window now
    await shortBreaker.execute(fail).catch(() => {});
    expect(shortBreaker.getMetrics().failures).toBe(1);
    expect(shortBreaker.getMetrics().state).toBe('closed');
  });
});

describe('CircuitBreakerRegistry', () => {
  it('creates breakers on demand', () => {
    const reg = new CircuitBreakerRegistry();
    const b1 = reg.get('agent-a');
    const b2 = reg.get('agent-a');
    expect(b1).toBe(b2); // same instance
  });

  it('throws when passing config to existing breaker via get()', () => {
    const reg = new CircuitBreakerRegistry();
    reg.get('agent-a', { failureThreshold: 5 });
    expect(() => reg.get('agent-a', { failureThreshold: 1 })).toThrow(/already exists/);
  });

  it('getOrCreate silently ignores config on existing breaker', () => {
    const reg = new CircuitBreakerRegistry();
    const b1 = reg.getOrCreate('agent-a', { failureThreshold: 5 });
    const b2 = reg.getOrCreate('agent-a', { failureThreshold: 1 });
    expect(b1).toBe(b2);
  });

  it('tracks open circuits', async () => {
    const reg = new CircuitBreakerRegistry();
    const b = reg.get('agent-broken', { failureThreshold: 1 });
    await b.execute(() => Promise.reject(new Error('fail'))).catch(() => {});
    expect(reg.getOpenCircuits()).toEqual(['agent-broken']);
  });
});
