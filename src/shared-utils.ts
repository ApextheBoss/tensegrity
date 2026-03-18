// ─── Shared Utilities ────────────────────────────────────────────────────────
// Extracted from duplicated implementations across modules.
// fnv1a: 32-bit FNV-1a hash
// WelfordStats: Online variance/stddev (class-based)
// EWMATracker: Exponentially Weighted Moving Average (class-based)
// Functional variants for modules using plain-object style.

// ─── FNV-1a Hash ─────────────────────────────────────────────────────────────

export function fnv1a(input: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = (hash * 0x01000193) >>> 0;
  }
  return hash;
}

/** Alias used by some modules */
export const fnv1aHash = fnv1a;

// ─── Welford Online Statistics (Class) ───────────────────────────────────────

export class WelfordStats {
  private count = 0;
  private mean_ = 0;
  private m2 = 0;
  private min_ = Infinity;
  private max_ = -Infinity;

  add(value: number): void {
    this.count++;
    const delta = value - this.mean_;
    this.mean_ += delta / this.count;
    const delta2 = value - this.mean_;
    this.m2 += delta * delta2;
    this.min_ = Math.min(this.min_, value);
    this.max_ = Math.max(this.max_, value);
  }

  get mean(): number { return this.mean_; }
  get variance(): number { return this.count > 1 ? this.m2 / (this.count - 1) : 0; }
  get stddev(): number { return Math.sqrt(this.variance); }
  get min(): number { return this.min_; }
  get max(): number { return this.max_; }
  get n(): number { return this.count; }
}

// ─── EWMA Tracker (Class) ────────────────────────────────────────────────────

export class EWMATracker {
  private value: number | null = null;
  private readonly alpha: number;

  constructor(alpha: number = 0.3) {
    this.alpha = alpha;
  }

  update(sample: number): number {
    if (this.value === null) {
      this.value = sample;
    } else {
      this.value = this.alpha * sample + (1 - this.alpha) * this.value;
    }
    return this.value;
  }

  get current(): number { return this.value ?? 0; }
  reset(): void { this.value = null; }
}

// ─── Functional EWMA ─────────────────────────────────────────────────────────

export interface EWMAState {
  value: number;
  alpha: number;
  count: number;
}

export function createEWMA(alpha: number): EWMAState {
  return { value: 0, alpha, count: 0 };
}

export function updateEWMA(tracker: EWMAState, sample: number): number {
  if (tracker.count === 0) {
    tracker.value = sample;
  } else {
    tracker.value = tracker.alpha * sample + (1 - tracker.alpha) * tracker.value;
  }
  tracker.count++;
  return tracker.value;
}

// ─── Functional Welford ──────────────────────────────────────────────────────

export interface WelfordState {
  count: number;
  mean: number;
  m2: number;
}

export function createWelford(): WelfordState {
  return { count: 0, mean: 0, m2: 0 };
}

export function updateWelford(stats: WelfordState, value: number): void {
  stats.count++;
  const delta = value - stats.mean;
  stats.mean += delta / stats.count;
  const delta2 = value - stats.mean;
  stats.m2 += delta * delta2;
}

export function getVariance(stats: WelfordState): number {
  return stats.count < 2 ? 0 : stats.m2 / (stats.count - 1);
}

export function getStdDev(stats: WelfordState): number {
  return Math.sqrt(getVariance(stats));
}
