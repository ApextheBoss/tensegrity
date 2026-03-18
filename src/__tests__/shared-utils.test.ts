import { describe, it, expect } from 'vitest';
import {
  fnv1a, fnv1aHash, WelfordStats, EWMATracker,
  createEWMA, updateEWMA, createWelford, updateWelford, getVariance, getStdDev
} from '../shared-utils';

describe('fnv1a', () => {
  it('returns consistent hash for same input', () => {
    expect(fnv1a('hello')).toBe(fnv1a('hello'));
  });

  it('returns different hashes for different inputs', () => {
    expect(fnv1a('hello')).not.toBe(fnv1a('world'));
  });

  it('returns a 32-bit unsigned integer', () => {
    const h = fnv1a('test');
    expect(h).toBeGreaterThanOrEqual(0);
    expect(h).toBeLessThanOrEqual(0xFFFFFFFF);
  });

  it('fnv1aHash is an alias for fnv1a', () => {
    expect(fnv1aHash('test')).toBe(fnv1a('test'));
  });
});

describe('WelfordStats', () => {
  it('computes mean correctly', () => {
    const s = new WelfordStats();
    s.add(10); s.add(20); s.add(30);
    expect(s.mean).toBe(20);
  });

  it('computes variance and stddev', () => {
    const s = new WelfordStats();
    [2, 4, 4, 4, 5, 5, 7, 9].forEach(v => s.add(v));
    expect(s.variance).toBeCloseTo(32 / 7, 5);
    expect(s.stddev).toBeCloseTo(Math.sqrt(32 / 7), 5);
  });

  it('tracks min/max/n', () => {
    const s = new WelfordStats();
    s.add(5); s.add(1); s.add(9);
    expect(s.min).toBe(1);
    expect(s.max).toBe(9);
    expect(s.n).toBe(3);
  });
});

describe('EWMATracker', () => {
  it('first sample becomes the value', () => {
    const e = new EWMATracker(0.5);
    e.update(10);
    expect(e.current).toBe(10);
  });

  it('smooths subsequent samples', () => {
    const e = new EWMATracker(0.5);
    e.update(10);
    e.update(20);
    expect(e.current).toBe(15); // 0.5*20 + 0.5*10
  });

  it('reset clears state', () => {
    const e = new EWMATracker();
    e.update(100);
    e.reset();
    expect(e.current).toBe(0);
  });
});

describe('functional EWMA', () => {
  it('tracks values', () => {
    const t = createEWMA(0.5);
    updateEWMA(t, 10);
    expect(t.value).toBe(10);
    updateEWMA(t, 20);
    expect(t.value).toBe(15);
  });
});

describe('functional Welford', () => {
  it('computes stats', () => {
    const s = createWelford();
    [2, 4, 4, 4, 5, 5, 7, 9].forEach(v => updateWelford(s, v));
    expect(s.mean).toBe(5);
    expect(getVariance(s)).toBeCloseTo(32 / 7, 5);
    expect(getStdDev(s)).toBeCloseTo(Math.sqrt(32 / 7), 5);
  });
});
