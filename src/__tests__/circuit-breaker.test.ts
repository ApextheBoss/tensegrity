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

  it('resets failure count on success in closed state', async () => {
    const fail = () => Promise.reject(new Error('down'));
    await breaker.execute(fail).catch(() => {});
    await breaker.execute(fail).catch(() => {});

    // Success resets counter
    await breaker.execute(() => Promise.resolve('ok'));
    expect(breaker.getMetrics().failures).toBe(0);

    // Need 3 more failures to open, not 1
    await breaker.execute(fail).catch(() => {});
    expect(breaker.getMetrics().state).toBe('closed');
  });
});

describe('CircuitBreakerRegistry', () => {
  it('creates breakers on demand', () => {
    const reg = new CircuitBreakerRegistry();
    const b1 = reg.get('agent-a');
    const b2 = reg.get('agent-a');
    expect(b1).toBe(b2); // same instance
  });

  it('tracks open circuits', async () => {
    const reg = new CircuitBreakerRegistry();
    const b = reg.get('agent-broken', { failureThreshold: 1 });
    await b.execute(() => Promise.reject(new Error('fail'))).catch(() => {});
    expect(reg.getOpenCircuits()).toEqual(['agent-broken']);
  });
});
