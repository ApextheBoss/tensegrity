/**
 * Circuit Breaker for Agent-to-Agent Communication
 * Prevents cascading failures when an agent becomes unresponsive.
 * Three states: CLOSED (normal), OPEN (blocking), HALF_OPEN (testing).
 */

type CircuitState = 'closed' | 'open' | 'half_open';

interface CircuitBreakerConfig {
  failureThreshold: number;    // failures before opening
  resetTimeoutMs: number;      // time before trying half-open
  halfOpenMaxAttempts: number;  // successful calls to close again
  monitorWindowMs: number;     // sliding window for failure counting
}

interface CircuitMetrics {
  state: CircuitState;
  failures: number;
  successes: number;
  lastFailureMs: number;
  lastStateChangeMs: number;
  totalRequests: number;
  totalFailures: number;
}

const DEFAULT_CONFIG: CircuitBreakerConfig = {
  failureThreshold: 3,
  resetTimeoutMs: 30000,      // 30 seconds
  halfOpenMaxAttempts: 2,
  monitorWindowMs: 60000,     // 1 minute window
};

export class CircuitBreaker {
  private state: CircuitState = 'closed';
  private failures: number = 0;
  private halfOpenSuccesses: number = 0;
  private lastFailureMs: number = 0;
  private lastStateChangeMs: number = Date.now();
  private totalRequests: number = 0;
  private totalFailures: number = 0;
  private failureTimestamps: number[] = [];
  private config: CircuitBreakerConfig;

  constructor(
    public readonly agentAddress: string,
    config?: Partial<CircuitBreakerConfig>
  ) {
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  /**
   * Execute a function through the circuit breaker.
   * Throws if circuit is open. Tracks success/failure for state transitions.
   */
  async execute<T>(fn: () => Promise<T>): Promise<T> {
    this.totalRequests++;

    if (this.state === 'open') {
      if (Date.now() - this.lastStateChangeMs >= this.config.resetTimeoutMs) {
        this.transitionTo('half_open');
      } else {
        throw new Error(`Circuit OPEN for agent ${this.agentAddress}. Retry after ${this.remainingCooldownMs()}ms`);
      }
    }

    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    }
  }

  private onSuccess(): void {
    if (this.state === 'half_open') {
      this.halfOpenSuccesses++;
      if (this.halfOpenSuccesses >= this.config.halfOpenMaxAttempts) {
        this.transitionTo('closed');
      }
    }
    // In closed state, do NOT clear failure timestamps.
    // Let the sliding monitorWindowMs handle expiry naturally.
    // This ensures bursty failure patterns (fail, success, fail, ...) are properly detected.
  }

  private onFailure(): void {
    this.totalFailures++;
    this.lastFailureMs = Date.now();
    this.failureTimestamps.push(Date.now());

    // Clean old failures outside monitoring window
    const cutoff = Date.now() - this.config.monitorWindowMs;
    this.failureTimestamps = this.failureTimestamps.filter(ts => ts > cutoff);
    this.failures = this.failureTimestamps.length;

    if (this.state === 'half_open') {
      this.transitionTo('open');
    } else if (this.failures >= this.config.failureThreshold) {
      this.transitionTo('open');
    }
  }

  private transitionTo(newState: CircuitState): void {
    this.state = newState;
    this.lastStateChangeMs = Date.now();

    if (newState === 'closed') {
      this.failures = 0;
      this.halfOpenSuccesses = 0;
      this.failureTimestamps = [];
    } else if (newState === 'half_open') {
      this.halfOpenSuccesses = 0;
    }
  }

  private remainingCooldownMs(): number {
    return Math.max(0, this.config.resetTimeoutMs - (Date.now() - this.lastStateChangeMs));
  }

  getMetrics(): CircuitMetrics {
    return {
      state: this.state,
      failures: this.failures,
      successes: this.halfOpenSuccesses,
      lastFailureMs: this.lastFailureMs,
      lastStateChangeMs: this.lastStateChangeMs,
      totalRequests: this.totalRequests,
      totalFailures: this.totalFailures,
    };
  }
}

/**
 * Registry of circuit breakers for multiple agents.
 * One breaker per agent address, created on demand.
 */
export class CircuitBreakerRegistry {
  private breakers = new Map<string, CircuitBreaker>();

  get(agentAddress: string, config?: Partial<CircuitBreakerConfig>): CircuitBreaker {
    if (!this.breakers.has(agentAddress)) {
      this.breakers.set(agentAddress, new CircuitBreaker(agentAddress, config));
    }
    return this.breakers.get(agentAddress)!;
  }

  getAll(): Map<string, CircuitMetrics> {
    const metrics = new Map<string, CircuitMetrics>();
    for (const [addr, breaker] of this.breakers) {
      metrics.set(addr, breaker.getMetrics());
    }
    return metrics;
  }

  getOpenCircuits(): string[] {
    return [...this.breakers.entries()]
      .filter(([_, b]) => b.getMetrics().state === 'open')
      .map(([addr]) => addr);
  }
}
