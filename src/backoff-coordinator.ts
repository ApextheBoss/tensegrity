/**
 * Backoff Coordinator for Multi-Agent Systems
 * 
 * Coordinates retry backoff across multiple agents to prevent thundering herd
 * and synchronized retry storms. Implements:
 * 
 * 1. **Slot-Based Coordination**: Agents claim retry slots to spread retries
 *    across time windows, preventing simultaneous retry bursts
 * 2. **Correlated Failure Detection**: Identifies when multiple agents fail
 *    against the same target, triggering coordinated backoff escalation
 * 3. **Harmonic Backoff**: Alternative to exponential that converges to a
 *    stable retry rate rather than growing unboundedly
 * 4. **Backoff Inheritance**: When agent A depends on agent B, A inherits
 *    B's backoff state to avoid retrying into a known-down dependency
 * 5. **Recovery Probing**: Coordinates who probes a recovering target to
 *    avoid thundering herd on recovery
 */

// ============================================================
// Types
// ============================================================

interface BackoffState {
  targetId: string;
  currentDelayMs: number;
  attemptCount: number;
  firstFailureAt: number;
  lastAttemptAt: number;
  lastFailureAt: number;
  strategy: 'exponential' | 'harmonic' | 'linear' | 'constant';
  jitterMs: number;
  maxDelayMs: number;
  cooldownUntil: number; // don't retry before this timestamp
}

interface RetrySlot {
  slotId: string;
  targetId: string;
  agentId: string;
  windowStart: number;
  windowEnd: number;
  claimed: boolean;
  executed: boolean;
  result: 'pending' | 'success' | 'failure';
}

interface CorrelatedFailure {
  targetId: string;
  failingAgents: Set<string>;
  firstDetectedAt: number;
  lastUpdatedAt: number;
  failureCount: number;
  escalationLevel: number; // 0=normal, 1=elevated, 2=critical, 3=blackout
  probeAgentId: string | null; // designated prober
  probeScheduledAt: number | null;
}

interface DependencyEdge {
  fromAgent: string;
  toAgent: string;
  targetId: string; // the shared dependency
  inheritBackoff: boolean;
}

interface BackoffEvent {
  type: 
    | 'backoff-started'
    | 'backoff-escalated'
    | 'backoff-reset'
    | 'slot-claimed'
    | 'slot-executed'
    | 'correlation-detected'
    | 'correlation-escalated'
    | 'probe-assigned'
    | 'probe-succeeded'
    | 'probe-failed'
    | 'inheritance-applied'
    | 'thundering-herd-prevented';
  timestamp: number;
  agentId: string;
  targetId: string;
  details: Record<string, unknown>;
}

interface BackoffCoordinatorConfig {
  // Slot coordination
  slotWindowMs: number;         // time window for retry slots
  slotsPerWindow: number;       // max concurrent retries per window
  slotJitterMs: number;         // jitter within a slot

  // Correlation detection
  correlationWindowMs: number;  // window to detect correlated failures
  correlationThreshold: number; // min agents failing to trigger correlation
  escalationDelayMs: number;    // delay between escalation levels

  // Backoff parameters
  baseDelayMs: number;
  maxDelayMs: number;
  multiplier: number;           // for exponential
  jitterFraction: number;       // 0-1, fraction of delay used as jitter
  defaultStrategy: 'exponential' | 'harmonic' | 'linear' | 'constant';

  // Recovery probing
  probeIntervalMs: number;
  probeTimeoutMs: number;
  probeSuccessThreshold: number; // consecutive successes to declare recovered

  // Inheritance
  inheritanceDecayFactor: number; // 0-1, how much of parent backoff to inherit

  // Limits
  maxTrackedTargets: number;
  maxEventHistory: number;
}

// ============================================================
// FNV-1a Hash (deterministic tie-breaking)
// ============================================================

function fnv1a(input: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = (hash * 0x01000193) >>> 0;
  }
  return hash;
}

// ============================================================
// Backoff Calculator
// ============================================================

class BackoffCalculator {
  static compute(
    attempt: number,
    strategy: BackoffState['strategy'],
    baseMs: number,
    maxMs: number,
    multiplier: number,
    jitterFraction: number
  ): { delayMs: number; jitterMs: number } {
    let raw: number;

    switch (strategy) {
      case 'exponential':
        raw = baseMs * Math.pow(multiplier, attempt);
        break;

      case 'harmonic':
        // Converges to maxMs rather than growing unboundedly
        // delay = maxMs * (1 - 1/(attempt+1))
        raw = maxMs * (1 - 1 / (attempt + 1));
        break;

      case 'linear':
        raw = baseMs * (attempt + 1);
        break;

      case 'constant':
        raw = baseMs;
        break;
    }

    const capped = Math.min(raw, maxMs);
    const jitterRange = capped * jitterFraction;
    // Deterministic-ish jitter using attempt number
    const jitter = (fnv1a(`jitter-${attempt}`) % 1000) / 1000 * jitterRange;

    return {
      delayMs: Math.floor(capped),
      jitterMs: Math.floor(jitter),
    };
  }
}

// ============================================================
// Slot Manager
// ============================================================

class SlotManager {
  private slots: Map<string, RetrySlot[]> = new Map(); // targetId -> slots
  private config: BackoffCoordinatorConfig;

  constructor(config: BackoffCoordinatorConfig) {
    this.config = config;
  }

  /**
   * Generate retry slots for a target within a time window.
   * Slots are evenly distributed across the window to prevent bursts.
   */
  generateSlots(targetId: string, windowStart: number): RetrySlot[] {
    const existing = this.slots.get(targetId);
    if (existing && existing.length > 0 && existing[0].windowStart === windowStart) {
      return existing;
    }

    const slotDuration = this.config.slotWindowMs / this.config.slotsPerWindow;
    const slots: RetrySlot[] = [];

    for (let i = 0; i < this.config.slotsPerWindow; i++) {
      const start = windowStart + i * slotDuration;
      slots.push({
        slotId: `${targetId}-${windowStart}-${i}`,
        targetId,
        agentId: '',
        windowStart: start,
        windowEnd: start + slotDuration,
        claimed: false,
        executed: false,
        result: 'pending',
      });
    }

    this.slots.set(targetId, slots);
    return slots;
  }

  /**
   * Claim the earliest available slot for an agent.
   * Uses FNV-1a hash for deterministic slot preference to reduce contention.
   */
  claimSlot(targetId: string, agentId: string, now: number): RetrySlot | null {
    const slots = this.slots.get(targetId);
    if (!slots) return null;

    // Hash-based preferred slot index
    const preferred = fnv1a(`${agentId}-${targetId}`) % slots.length;

    // Try preferred first, then scan forward
    for (let offset = 0; offset < slots.length; offset++) {
      const idx = (preferred + offset) % slots.length;
      const slot = slots[idx];

      if (!slot.claimed && slot.windowEnd > now) {
        slot.claimed = true;
        slot.agentId = agentId;
        return slot;
      }
    }

    return null; // all slots taken
  }

  markExecuted(slotId: string, result: 'success' | 'failure'): void {
    for (const slots of this.slots.values()) {
      const slot = slots.find(s => s.slotId === slotId);
      if (slot) {
        slot.executed = true;
        slot.result = result;
        return;
      }
    }
  }

  getActiveSlots(targetId: string): RetrySlot[] {
    return this.slots.get(targetId) || [];
  }

  cleanup(now: number): number {
    let cleaned = 0;
    for (const [targetId, slots] of this.slots.entries()) {
      const active = slots.filter(s => s.windowEnd > now || (s.claimed && !s.executed));
      if (active.length === 0) {
        this.slots.delete(targetId);
        cleaned += slots.length;
      } else {
        this.slots.set(targetId, active);
        cleaned += slots.length - active.length;
      }
    }
    return cleaned;
  }
}

// ============================================================
// Correlation Detector
// ============================================================

class CorrelationDetector {
  private correlations: Map<string, CorrelatedFailure> = new Map();
  private recentFailures: Array<{ targetId: string; agentId: string; at: number }> = [];
  private config: BackoffCoordinatorConfig;

  constructor(config: BackoffCoordinatorConfig) {
    this.config = config;
  }

  /**
   * Record a failure and check if it correlates with other agents' failures.
   * Returns the correlation if threshold is met.
   */
  recordFailure(targetId: string, agentId: string, now: number): CorrelatedFailure | null {
    // Add to recent failures
    this.recentFailures.push({ targetId, agentId, at: now });

    // Trim old failures
    const cutoff = now - this.config.correlationWindowMs;
    this.recentFailures = this.recentFailures.filter(f => f.at >= cutoff);

    // Count unique agents failing against this target in the window
    const failingAgents = new Set<string>();
    for (const f of this.recentFailures) {
      if (f.targetId === targetId) {
        failingAgents.add(f.agentId);
      }
    }

    // Check threshold
    if (failingAgents.size >= this.config.correlationThreshold) {
      let correlation = this.correlations.get(targetId);

      if (!correlation) {
        correlation = {
          targetId,
          failingAgents,
          firstDetectedAt: now,
          lastUpdatedAt: now,
          failureCount: 1,
          escalationLevel: 0,
          probeAgentId: null,
          probeScheduledAt: null,
        };
        this.correlations.set(targetId, correlation);
      } else {
        correlation.failingAgents = failingAgents;
        correlation.lastUpdatedAt = now;
        correlation.failureCount++;
      }

      return correlation;
    }

    return null;
  }

  /**
   * Escalate a correlated failure. Each level increases backoff more aggressively.
   * Level 0: normal backoff per-agent
   * Level 1: coordinated slots, 2x delay
   * Level 2: reduced probe frequency, 4x delay
   * Level 3: blackout — only designated prober attempts
   */
  escalate(targetId: string, now: number): CorrelatedFailure | null {
    const correlation = this.correlations.get(targetId);
    if (!correlation) return null;

    const timeSinceLastEscalation = now - correlation.lastUpdatedAt;
    if (timeSinceLastEscalation < this.config.escalationDelayMs) return correlation;

    if (correlation.escalationLevel < 3) {
      correlation.escalationLevel++;
      correlation.lastUpdatedAt = now;
    }

    return correlation;
  }

  /**
   * Assign a probe agent using deterministic selection.
   * Picks the agent with lowest FNV-1a hash to ensure consistency
   * without requiring consensus.
   */
  assignProber(targetId: string, now: number): string | null {
    const correlation = this.correlations.get(targetId);
    if (!correlation || correlation.failingAgents.size === 0) return null;

    // Deterministic: lowest hash wins probe duty
    let probeAgent: string | null = null;
    let lowestHash = Infinity;

    for (const agentId of correlation.failingAgents) {
      const hash = fnv1a(`probe-${targetId}-${agentId}-${Math.floor(now / this.config.probeIntervalMs)}`);
      if (hash < lowestHash) {
        lowestHash = hash;
        probeAgent = agentId;
      }
    }

    if (probeAgent) {
      correlation.probeAgentId = probeAgent;
      correlation.probeScheduledAt = now;
    }

    return probeAgent;
  }

  recordRecovery(targetId: string): void {
    this.correlations.delete(targetId);
  }

  getCorrelation(targetId: string): CorrelatedFailure | null {
    return this.correlations.get(targetId) || null;
  }

  getEscalationMultiplier(targetId: string): number {
    const correlation = this.correlations.get(targetId);
    if (!correlation) return 1;
    return Math.pow(2, correlation.escalationLevel);
  }

  isBlackout(targetId: string, agentId: string): boolean {
    const correlation = this.correlations.get(targetId);
    if (!correlation || correlation.escalationLevel < 3) return false;
    return agentId !== correlation.probeAgentId;
  }
}

// ============================================================
// Backoff Inheritance Manager
// ============================================================

class InheritanceManager {
  private dependencies: DependencyEdge[] = [];
  private config: BackoffCoordinatorConfig;

  constructor(config: BackoffCoordinatorConfig) {
    this.config = config;
  }

  addDependency(from: string, to: string, targetId: string): void {
    // Avoid duplicates
    const exists = this.dependencies.some(
      d => d.fromAgent === from && d.toAgent === to && d.targetId === targetId
    );
    if (!exists) {
      this.dependencies.push({
        fromAgent: from,
        toAgent: to,
        targetId,
        inheritBackoff: true,
      });
    }
  }

  removeDependency(from: string, to: string, targetId: string): void {
    this.dependencies = this.dependencies.filter(
      d => !(d.fromAgent === from && d.toAgent === to && d.targetId === targetId)
    );
  }

  /**
   * Compute inherited backoff delay for an agent.
   * Walks the dependency graph and applies decay factor at each hop.
   * Uses BFS to prevent cycles.
   */
  computeInheritedDelay(
    agentId: string,
    targetId: string,
    backoffStates: Map<string, BackoffState>
  ): number {
    const visited = new Set<string>();
    const queue: Array<{ agent: string; depth: number }> = [];
    let maxInherited = 0;

    // Find agents that agentId depends on for this target
    for (const dep of this.dependencies) {
      if (dep.fromAgent === agentId && dep.targetId === targetId && dep.inheritBackoff) {
        queue.push({ agent: dep.toAgent, depth: 1 });
      }
    }

    while (queue.length > 0) {
      const { agent, depth } = queue.shift()!;
      if (visited.has(agent)) continue;
      visited.add(agent);

      const state = backoffStates.get(`${agent}:${targetId}`);
      if (state && state.cooldownUntil > Date.now()) {
        const decayedDelay = state.currentDelayMs * Math.pow(this.config.inheritanceDecayFactor, depth);
        maxInherited = Math.max(maxInherited, decayedDelay);
      }

      // Transitive dependencies
      for (const dep of this.dependencies) {
        if (dep.fromAgent === agent && dep.targetId === targetId && dep.inheritBackoff) {
          queue.push({ agent: dep.toAgent, depth: depth + 1 });
        }
      }
    }

    return Math.floor(maxInherited);
  }

  getDependencies(agentId: string): DependencyEdge[] {
    return this.dependencies.filter(d => d.fromAgent === agentId);
  }
}

// ============================================================
// Backoff Coordinator (Main Orchestrator)
// ============================================================

class BackoffCoordinator {
  private config: BackoffCoordinatorConfig;
  private backoffStates: Map<string, BackoffState> = new Map(); // "agentId:targetId" -> state
  private slotManager: SlotManager;
  private correlationDetector: CorrelationDetector;
  private inheritanceManager: InheritanceManager;
  private events: BackoffEvent[] = [];
  private probeSuccessCounts: Map<string, number> = new Map(); // targetId -> consecutive successes

  constructor(config: BackoffCoordinatorConfig) {
    this.config = config;
    this.slotManager = new SlotManager(config);
    this.correlationDetector = new CorrelationDetector(config);
    this.inheritanceManager = new InheritanceManager(config);
  }

  /**
   * Determine when an agent should next retry a target.
   * Accounts for: own backoff, correlated escalation, slot coordination,
   * and inherited backoff from dependencies.
   */
  getNextRetryTime(agentId: string, targetId: string): {
    retryAt: number;
    reason: string;
    slot: RetrySlot | null;
    blocked: boolean;
  } {
    const now = Date.now();
    const key = `${agentId}:${targetId}`;
    const state = this.backoffStates.get(key);

    // No backoff state = first attempt, go immediately
    if (!state) {
      return { retryAt: now, reason: 'first-attempt', slot: null, blocked: false };
    }

    // Check blackout (escalation level 3 — only prober allowed)
    if (this.correlationDetector.isBlackout(targetId, agentId)) {
      this.emit({
        type: 'thundering-herd-prevented',
        timestamp: now,
        agentId,
        targetId,
        details: { reason: 'blackout', escalationLevel: 3 },
      });
      return { retryAt: Infinity, reason: 'blackout-not-prober', slot: null, blocked: true };
    }

    // Base delay from own backoff
    let delay = state.currentDelayMs + state.jitterMs;

    // Apply correlation escalation multiplier
    const escalationMult = this.correlationDetector.getEscalationMultiplier(targetId);
    delay *= escalationMult;

    // Apply inherited backoff
    const inheritedDelay = this.inheritanceManager.computeInheritedDelay(
      agentId,
      targetId,
      this.backoffStates
    );
    if (inheritedDelay > delay) {
      delay = inheritedDelay;
      this.emit({
        type: 'inheritance-applied',
        timestamp: now,
        agentId,
        targetId,
        details: { inheritedDelayMs: inheritedDelay, ownDelayMs: state.currentDelayMs },
      });
    }

    const retryAt = state.lastAttemptAt + delay;

    // Try to get a coordinated slot if correlation is active
    const correlation = this.correlationDetector.getCorrelation(targetId);
    let slot: RetrySlot | null = null;

    if (correlation && correlation.escalationLevel >= 1) {
      const windowStart = Math.ceil(retryAt / this.config.slotWindowMs) * this.config.slotWindowMs;
      this.slotManager.generateSlots(targetId, windowStart);
      slot = this.slotManager.claimSlot(targetId, agentId, now);

      if (slot) {
        this.emit({
          type: 'slot-claimed',
          timestamp: now,
          agentId,
          targetId,
          details: { slotId: slot.slotId, windowStart: slot.windowStart },
        });
        return { retryAt: slot.windowStart, reason: 'coordinated-slot', slot, blocked: false };
      }

      // No slots available — wait for next window
      return {
        retryAt: windowStart + this.config.slotWindowMs,
        reason: 'no-slots-available',
        slot: null,
        blocked: false,
      };
    }

    return { retryAt, reason: 'backoff-delay', slot: null, blocked: false };
  }

  /**
   * Record a failed attempt. Updates backoff state and checks for correlations.
   */
  recordFailure(agentId: string, targetId: string): {
    nextDelay: number;
    correlated: boolean;
    escalationLevel: number;
  } {
    const now = Date.now();
    const key = `${agentId}:${targetId}`;
    let state = this.backoffStates.get(key);

    if (!state) {
      state = {
        targetId,
        currentDelayMs: this.config.baseDelayMs,
        attemptCount: 0,
        firstFailureAt: now,
        lastAttemptAt: now,
        lastFailureAt: now,
        strategy: this.config.defaultStrategy,
        jitterMs: 0,
        maxDelayMs: this.config.maxDelayMs,
        cooldownUntil: 0,
      };
      this.backoffStates.set(key, state);
      this.emit({
        type: 'backoff-started',
        timestamp: now,
        agentId,
        targetId,
        details: { strategy: state.strategy },
      });
    }

    state.attemptCount++;
    state.lastAttemptAt = now;
    state.lastFailureAt = now;

    // Compute new delay
    const { delayMs, jitterMs } = BackoffCalculator.compute(
      state.attemptCount,
      state.strategy,
      this.config.baseDelayMs,
      this.config.maxDelayMs,
      this.config.multiplier,
      this.config.jitterFraction
    );

    state.currentDelayMs = delayMs;
    state.jitterMs = jitterMs;
    state.cooldownUntil = now + delayMs + jitterMs;

    this.emit({
      type: 'backoff-escalated',
      timestamp: now,
      agentId,
      targetId,
      details: { attempt: state.attemptCount, delayMs, jitterMs },
    });

    // Check for correlated failures
    const correlation = this.correlationDetector.recordFailure(targetId, agentId, now);
    let escalationLevel = 0;

    if (correlation) {
      if (correlation.failureCount === this.config.correlationThreshold) {
        this.emit({
          type: 'correlation-detected',
          timestamp: now,
          agentId,
          targetId,
          details: {
            failingAgents: Array.from(correlation.failingAgents),
            failureCount: correlation.failureCount,
          },
        });
      }

      // Try to escalate
      const escalated = this.correlationDetector.escalate(targetId, now);
      if (escalated) {
        escalationLevel = escalated.escalationLevel;

        if (escalated.escalationLevel >= 2 && !escalated.probeAgentId) {
          const prober = this.correlationDetector.assignProber(targetId, now);
          if (prober) {
            this.emit({
              type: 'probe-assigned',
              timestamp: now,
              agentId: prober,
              targetId,
              details: { escalationLevel: escalated.escalationLevel },
            });
          }
        }

        this.emit({
          type: 'correlation-escalated',
          timestamp: now,
          agentId,
          targetId,
          details: { level: escalated.escalationLevel },
        });
      }
    }

    return {
      nextDelay: state.currentDelayMs + state.jitterMs,
      correlated: !!correlation,
      escalationLevel,
    };
  }

  /**
   * Record a successful attempt. Resets backoff and updates correlation state.
   */
  recordSuccess(agentId: string, targetId: string): void {
    const now = Date.now();
    const key = `${agentId}:${targetId}`;

    // Reset backoff state
    this.backoffStates.delete(key);

    this.emit({
      type: 'backoff-reset',
      timestamp: now,
      agentId,
      targetId,
      details: {},
    });

    // Track consecutive probe successes for correlation recovery
    const correlation = this.correlationDetector.getCorrelation(targetId);
    if (correlation && correlation.probeAgentId === agentId) {
      const count = (this.probeSuccessCounts.get(targetId) || 0) + 1;
      this.probeSuccessCounts.set(targetId, count);

      this.emit({
        type: 'probe-succeeded',
        timestamp: now,
        agentId,
        targetId,
        details: { consecutiveSuccesses: count, threshold: this.config.probeSuccessThreshold },
      });

      if (count >= this.config.probeSuccessThreshold) {
        // Target recovered — clear all backoff for this target
        this.correlationDetector.recordRecovery(targetId);
        this.probeSuccessCounts.delete(targetId);

        // Clear all agents' backoff against this target
        for (const [k] of this.backoffStates) {
          if (k.endsWith(`:${targetId}`)) {
            this.backoffStates.delete(k);
          }
        }
      }
    }
  }

  /**
   * Register a dependency: agentA depends on agentB for targetId.
   * When B has active backoff against target, A inherits a decayed version.
   */
  addDependency(fromAgent: string, toAgent: string, targetId: string): void {
    this.inheritanceManager.addDependency(fromAgent, toAgent, targetId);
  }

  removeDependency(fromAgent: string, toAgent: string, targetId: string): void {
    this.inheritanceManager.removeDependency(fromAgent, toAgent, targetId);
  }

  /**
   * Override the backoff strategy for a specific agent-target pair.
   */
  setStrategy(
    agentId: string,
    targetId: string,
    strategy: BackoffState['strategy']
  ): void {
    const key = `${agentId}:${targetId}`;
    const state = this.backoffStates.get(key);
    if (state) {
      state.strategy = strategy;
    }
  }

  /**
   * Get current backoff state for an agent-target pair.
   */
  getState(agentId: string, targetId: string): BackoffState | null {
    return this.backoffStates.get(`${agentId}:${targetId}`) || null;
  }

  /**
   * Get all agents currently backing off against a target.
   */
  getAgentsBackingOff(targetId: string): Array<{ agentId: string; state: BackoffState }> {
    const results: Array<{ agentId: string; state: BackoffState }> = [];
    for (const [key, state] of this.backoffStates) {
      if (state.targetId === targetId) {
        const agentId = key.split(':')[0];
        results.push({ agentId, state });
      }
    }
    return results;
  }

  /**
   * Diagnostic: get overall system pressure for a target.
   */
  getTargetPressure(targetId: string): {
    backingOffCount: number;
    correlationLevel: number;
    averageDelay: number;
    isBlackout: boolean;
    probeAgent: string | null;
  } {
    const agents = this.getAgentsBackingOff(targetId);
    const correlation = this.correlationDetector.getCorrelation(targetId);

    const avgDelay = agents.length > 0
      ? agents.reduce((sum, a) => sum + a.state.currentDelayMs, 0) / agents.length
      : 0;

    return {
      backingOffCount: agents.length,
      correlationLevel: correlation?.escalationLevel ?? 0,
      averageDelay: Math.floor(avgDelay),
      isBlackout: (correlation?.escalationLevel ?? 0) >= 3,
      probeAgent: correlation?.probeAgentId ?? null,
    };
  }

  /**
   * Periodic cleanup of stale state.
   */
  cleanup(): { removedStates: number; removedSlots: number } {
    const now = Date.now();

    // Remove expired backoff states (no failure in 10x max delay)
    let removedStates = 0;
    for (const [key, state] of this.backoffStates) {
      if (now - state.lastFailureAt > this.config.maxDelayMs * 10) {
        this.backoffStates.delete(key);
        removedStates++;
      }
    }

    const removedSlots = this.slotManager.cleanup(now);

    // Trim event history
    if (this.events.length > this.config.maxEventHistory) {
      this.events = this.events.slice(-this.config.maxEventHistory);
    }

    return { removedStates, removedSlots };
  }

  getRecentEvents(limit: number = 50): BackoffEvent[] {
    return this.events.slice(-limit);
  }

  private emit(event: BackoffEvent): void {
    this.events.push(event);
    if (this.events.length > this.config.maxEventHistory * 1.5) {
      this.events = this.events.slice(-this.config.maxEventHistory);
    }
  }
}

// ============================================================
// Presets
// ============================================================

const PRESETS = {
  /** Fast retries for low-latency services */
  'fast-service': {
    slotWindowMs: 1000,
    slotsPerWindow: 5,
    slotJitterMs: 100,
    correlationWindowMs: 5000,
    correlationThreshold: 3,
    escalationDelayMs: 2000,
    baseDelayMs: 100,
    maxDelayMs: 10000,
    multiplier: 2,
    jitterFraction: 0.25,
    defaultStrategy: 'exponential' as const,
    probeIntervalMs: 2000,
    probeTimeoutMs: 1000,
    probeSuccessThreshold: 3,
    inheritanceDecayFactor: 0.7,
    maxTrackedTargets: 100,
    maxEventHistory: 500,
  },

  /** Standard backoff for typical agent-to-agent communication */
  'standard': {
    slotWindowMs: 5000,
    slotsPerWindow: 3,
    slotJitterMs: 500,
    correlationWindowMs: 30000,
    correlationThreshold: 3,
    escalationDelayMs: 10000,
    baseDelayMs: 1000,
    maxDelayMs: 60000,
    multiplier: 2,
    jitterFraction: 0.3,
    defaultStrategy: 'exponential' as const,
    probeIntervalMs: 15000,
    probeTimeoutMs: 5000,
    probeSuccessThreshold: 3,
    inheritanceDecayFactor: 0.5,
    maxTrackedTargets: 200,
    maxEventHistory: 1000,
  },

  /** Conservative backoff for external/paid APIs */
  'external-api': {
    slotWindowMs: 30000,
    slotsPerWindow: 2,
    slotJitterMs: 5000,
    correlationWindowMs: 120000,
    correlationThreshold: 2,
    escalationDelayMs: 60000,
    baseDelayMs: 5000,
    maxDelayMs: 300000,
    multiplier: 3,
    jitterFraction: 0.4,
    defaultStrategy: 'harmonic' as const,
    probeIntervalMs: 60000,
    probeTimeoutMs: 10000,
    probeSuccessThreshold: 5,
    inheritanceDecayFactor: 0.8,
    maxTrackedTargets: 50,
    maxEventHistory: 500,
  },
};

export {
  BackoffCoordinator,
  BackoffCalculator,
  SlotManager,
  CorrelationDetector,
  InheritanceManager,
  PRESETS,
  type BackoffCoordinatorConfig,
  type BackoffState,
  type RetrySlot,
  type CorrelatedFailure,
  type DependencyEdge,
  type BackoffEvent,
};
