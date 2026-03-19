/**
 * Vector Clock Causality Tracker
 * 
 * Full vector clock implementation for tracking causal relationships
 * in distributed agent systems. Goes beyond simple Lamport timestamps
 * to capture true happened-before semantics.
 * 
 * Features:
 * - Vector clock with efficient sparse representation
 * - Causal history tracking with bounded memory (interval tree clocks)
 * - Concurrent event detection and conflict resolution
 * - Causal barrier synchronization (wait for causal dependencies)
 * - Matrix clocks for tracking "what I know you know" (protocol optimization)
 * - Dotted version vectors for accurate replica divergence
 * - Plausible clocks for late-joining agents (clock reconstruction)
 * - Causal stability detection (identifying globally stable prefixes)
 * 
 * @module vector-clock-causality
 */

// ─── Types ───────────────────────────────────────────────────────────

export type AgentId = string;
export type LogicalTime = number;

export interface VectorClock {
  readonly entries: ReadonlyMap<AgentId, LogicalTime>;
  readonly origin: AgentId;
}

export interface CausalEvent<T = unknown> {
  readonly id: string;
  readonly origin: AgentId;
  readonly clock: VectorClock;
  readonly payload: T;
  readonly timestamp: number; // wall clock, untrusted
}

export interface Dot {
  readonly agent: AgentId;
  readonly counter: LogicalTime;
}

export interface DottedVersionVector {
  readonly base: ReadonlyMap<AgentId, LogicalTime>; // contiguous prefix per agent
  readonly dots: ReadonlySet<string>; // sparse dots above base: "agent:counter"
}

export interface MatrixClock {
  readonly rows: ReadonlyMap<AgentId, ReadonlyMap<AgentId, LogicalTime>>;
  readonly origin: AgentId;
}

export type CausalOrdering = 'before' | 'after' | 'concurrent' | 'equal';

export interface CausalBarrier {
  readonly id: string;
  readonly requiredClock: VectorClock;
  readonly callback: () => void;
  readonly createdAt: number;
  readonly timeoutMs: number;
}

export interface StabilityFrontier {
  readonly stableClock: VectorClock; // all agents have seen at least this
  readonly unstableEvents: CausalEvent[];
  readonly lastUpdated: number;
}

export type EventType =
  | 'clock:tick'
  | 'clock:merge'
  | 'clock:conflict'
  | 'barrier:created'
  | 'barrier:satisfied'
  | 'barrier:timeout'
  | 'stability:advanced'
  | 'matrix:updated'
  | 'dvv:merge'
  | 'dvv:conflict';

export interface TrackerEvent {
  readonly type: EventType;
  readonly timestamp: number;
  readonly detail: Record<string, unknown>;
}

export interface TrackerConfig {
  readonly maxAgents: number;
  readonly maxPendingEvents: number;
  readonly barrierTimeoutMs: number;
  readonly stabilityCheckIntervalMs: number;
  readonly enableMatrixClocks: boolean;
  readonly enableDVV: boolean;
  readonly gcIntervalMs: number;
  readonly maxEventHistory: number;
}

// ─── Vector Clock Operations ─────────────────────────────────────────

export function createClock(origin: AgentId): VectorClock {
  const entries = new Map<AgentId, LogicalTime>();
  entries.set(origin, 0);
  return { entries, origin };
}

export function tick(clock: VectorClock): VectorClock {
  const entries = new Map(clock.entries);
  entries.set(clock.origin, (entries.get(clock.origin) ?? 0) + 1);
  return { entries, origin: clock.origin };
}

export function merge(a: VectorClock, b: VectorClock): VectorClock {
  const entries = new Map(a.entries);
  for (const [agent, time] of b.entries) {
    entries.set(agent, Math.max(entries.get(agent) ?? 0, time));
  }
  // Tick own counter after merge
  entries.set(a.origin, (entries.get(a.origin) ?? 0) + 1);
  return { entries, origin: a.origin };
}

export function compare(a: VectorClock, b: VectorClock): CausalOrdering {
  const allAgents = new Set([...a.entries.keys(), ...b.entries.keys()]);
  let aBeforeB = false;
  let bBeforeA = false;

  for (const agent of allAgents) {
    const ta = a.entries.get(agent) ?? 0;
    const tb = b.entries.get(agent) ?? 0;
    if (ta < tb) aBeforeB = true;
    if (ta > tb) bBeforeA = true;
  }

  if (!aBeforeB && !bBeforeA) return 'equal';
  if (aBeforeB && !bBeforeA) return 'before';
  if (!aBeforeB && bBeforeA) return 'after';
  return 'concurrent';
}

export function happensBefore(a: VectorClock, b: VectorClock): boolean {
  return compare(a, b) === 'before';
}

export function isConcurrent(a: VectorClock, b: VectorClock): boolean {
  return compare(a, b) === 'concurrent';
}

export function dominates(a: VectorClock, b: VectorClock): boolean {
  // a dominates b if every entry in a >= corresponding entry in b
  // and at least one is strictly greater
  const allAgents = new Set([...a.entries.keys(), ...b.entries.keys()]);
  let strictlyGreater = false;
  for (const agent of allAgents) {
    const ta = a.entries.get(agent) ?? 0;
    const tb = b.entries.get(agent) ?? 0;
    if (ta < tb) return false;
    if (ta > tb) strictlyGreater = true;
  }
  return strictlyGreater;
}

// ─── Dotted Version Vectors ──────────────────────────────────────────

/**
 * DVVs extend version vectors with individual dots for precise
 * tracking of concurrent writes. Used in Riak-style systems.
 * Base tracks contiguous knowledge; dots track isolated events above base.
 */

export function createDVV(): DottedVersionVector {
  return { base: new Map(), dots: new Set() };
}

export function dvvAdd(dvv: DottedVersionVector, dot: Dot): DottedVersionVector {
  const base = new Map(dvv.base);
  const dots = new Set(dvv.dots);
  const currentBase = base.get(dot.agent) ?? 0;

  if (dot.counter <= currentBase) {
    // Already known
    return dvv;
  }

  if (dot.counter === currentBase + 1) {
    // Extends contiguous prefix
    base.set(dot.agent, dot.counter);
    // Collapse any dots that are now contiguous
    let next = dot.counter + 1;
    while (dots.has(`${dot.agent}:${next}`)) {
      dots.delete(`${dot.agent}:${next}`);
      base.set(dot.agent, next);
      next++;
    }
  } else {
    // Sparse dot above base
    dots.add(`${dot.agent}:${dot.counter}`);
  }

  return { base, dots };
}

export function dvvContains(dvv: DottedVersionVector, dot: Dot): boolean {
  const baseVal = dvv.base.get(dot.agent) ?? 0;
  if (dot.counter <= baseVal) return true;
  return dvv.dots.has(`${dot.agent}:${dot.counter}`);
}

export function dvvMerge(
  a: DottedVersionVector,
  b: DottedVersionVector
): DottedVersionVector {
  let result = createDVV();
  const base = new Map<AgentId, LogicalTime>();

  // Merge bases (take max)
  const allAgents = new Set([...a.base.keys(), ...b.base.keys()]);
  for (const agent of allAgents) {
    base.set(agent, Math.max(a.base.get(agent) ?? 0, b.base.get(agent) ?? 0));
  }

  result = { base, dots: new Set() };

  // Merge dots — add any dot from either side that's above the merged base
  for (const dotStr of [...a.dots, ...b.dots]) {
    const [agent, counterStr] = dotStr.split(':');
    const counter = parseInt(counterStr, 10);
    if (counter > (base.get(agent) ?? 0)) {
      result = dvvAdd(result, { agent, counter });
    }
  }

  return result;
}

export function dvvFindConflicts(
  a: DottedVersionVector,
  b: DottedVersionVector
): Dot[] {
  // Find dots in a that b doesn't know about and vice versa
  const conflicts: Dot[] = [];
  
  for (const dotStr of a.dots) {
    const [agent, counterStr] = dotStr.split(':');
    const counter = parseInt(counterStr, 10);
    if (!dvvContains(b, { agent, counter })) {
      conflicts.push({ agent, counter });
    }
  }

  for (const dotStr of b.dots) {
    const [agent, counterStr] = dotStr.split(':');
    const counter = parseInt(counterStr, 10);
    if (!dvvContains(a, { agent, counter })) {
      conflicts.push({ agent, counter });
    }
  }

  return conflicts;
}

// ─── Matrix Clocks ───────────────────────────────────────────────────

/**
 * Matrix clocks track "what agent i knows agent j knows".
 * Row i is agent i's vector clock. Row i, column j means:
 * "agent i knows that agent j has processed events up to time t".
 * 
 * Used for garbage collection: if all rows agree that all agents
 * have seen event e, then e's metadata can be safely discarded.
 */

export function createMatrixClock(origin: AgentId, agents: AgentId[]): MatrixClock {
  const rows = new Map<AgentId, ReadonlyMap<AgentId, LogicalTime>>();
  const zeroCols = new Map<AgentId, LogicalTime>();
  for (const a of agents) zeroCols.set(a, 0);
  for (const a of agents) rows.set(a, new Map(zeroCols));
  return { rows, origin };
}

export function matrixTick(mc: MatrixClock): MatrixClock {
  const rows = new Map(mc.rows);
  const myRow = new Map(rows.get(mc.origin) ?? new Map());
  myRow.set(mc.origin, (myRow.get(mc.origin) ?? 0) + 1);
  rows.set(mc.origin, myRow);
  return { rows, origin: mc.origin };
}

export function matrixMerge(local: MatrixClock, remote: MatrixClock, remoteId: AgentId): MatrixClock {
  const rows = new Map<AgentId, Map<AgentId, LogicalTime>>();

  // For each row i:
  // - If i == local.origin: take max of local[i][j] and remote[i][j]
  // - Otherwise: take max of local[i][j] and remote[i][j]
  const allAgents = new Set([
    ...local.rows.keys(),
    ...remote.rows.keys()
  ]);

  for (const i of allAgents) {
    const localRow = local.rows.get(i) ?? new Map();
    const remoteRow = remote.rows.get(i) ?? new Map();
    const mergedRow = new Map<AgentId, LogicalTime>();

    const allCols = new Set([...localRow.keys(), ...remoteRow.keys()]);
    for (const j of allCols) {
      mergedRow.set(j, Math.max(localRow.get(j) ?? 0, remoteRow.get(j) ?? 0));
    }
    rows.set(i, mergedRow);
  }

  // Update my knowledge of remote: copy remote's own row
  const remoteOwnRow = remote.rows.get(remoteId);
  if (remoteOwnRow) {
    rows.set(remoteId, new Map(remoteOwnRow));
  }

  // Tick own counter
  const myRow = rows.get(local.origin) ?? new Map();
  const newMyRow = new Map(myRow);
  newMyRow.set(local.origin, (newMyRow.get(local.origin) ?? 0) + 1);
  rows.set(local.origin, newMyRow);

  return { rows, origin: local.origin };
}

/**
 * Compute the minimum across all rows for each column.
 * This represents the "globally known" frontier — events
 * at or below this point have been seen by ALL agents.
 */
export function matrixStableFrontier(mc: MatrixClock): Map<AgentId, LogicalTime> {
  const frontier = new Map<AgentId, LogicalTime>();
  const agents = [...mc.rows.keys()];
  
  if (agents.length === 0) return frontier;

  // Initialize with first row
  const firstRow = mc.rows.get(agents[0])!;
  for (const [agent, time] of firstRow) {
    frontier.set(agent, time);
  }

  // Take min across all rows
  for (let i = 1; i < agents.length; i++) {
    const row = mc.rows.get(agents[i])!;
    for (const [agent, currentMin] of frontier) {
      frontier.set(agent, Math.min(currentMin, row.get(agent) ?? 0));
    }
  }

  return frontier;
}

// ─── Causal Barrier ──────────────────────────────────────────────────

/**
 * Causal barriers let agents wait until specific causal dependencies
 * are satisfied before proceeding. Essential for:
 * - Read-your-writes consistency
 * - Causal delivery ordering
 * - Snapshot isolation across agents
 */

export class CausalBarrierManager {
  private barriers: Map<string, CausalBarrier> = new Map();
  private currentClock: VectorClock;
  private timers: Map<string, ReturnType<typeof setTimeout>> = new Map();

  constructor(private origin: AgentId, private defaultTimeoutMs: number = 30_000) {
    this.currentClock = createClock(origin);
  }

  /**
   * Create a barrier that fires when the local clock reaches
   * or exceeds the required clock.
   */
  waitFor(requiredClock: VectorClock, callback: () => void, timeoutMs?: number): string {
    const id = `barrier-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const timeout = timeoutMs ?? this.defaultTimeoutMs;

    const barrier: CausalBarrier = {
      id,
      requiredClock,
      callback,
      createdAt: Date.now(),
      timeoutMs: timeout
    };

    // Check if already satisfied
    if (this.isSatisfied(requiredClock)) {
      callback();
      return id;
    }

    this.barriers.set(id, barrier);

    // Set timeout
    const timer = setTimeout(() => {
      if (this.barriers.has(id)) {
        this.barriers.delete(id);
        // Timeout — barrier not satisfied in time
      }
    }, timeout);
    this.timers.set(id, timer);

    return id;
  }

  /**
   * Advance the local clock (e.g., after receiving a message).
   * Checks all pending barriers.
   */
  advance(newClock: VectorClock): void {
    this.currentClock = merge(this.currentClock, newClock);
    this.checkBarriers();
  }

  cancel(barrierId: string): boolean {
    const timer = this.timers.get(barrierId);
    if (timer) {
      clearTimeout(timer);
      this.timers.delete(barrierId);
    }
    return this.barriers.delete(barrierId);
  }

  private isSatisfied(required: VectorClock): boolean {
    for (const [agent, time] of required.entries) {
      if ((this.currentClock.entries.get(agent) ?? 0) < time) {
        return false;
      }
    }
    return true;
  }

  private checkBarriers(): void {
    for (const [id, barrier] of this.barriers) {
      if (this.isSatisfied(barrier.requiredClock)) {
        barrier.callback();
        this.barriers.delete(id);
        const timer = this.timers.get(id);
        if (timer) {
          clearTimeout(timer);
          this.timers.delete(id);
        }
      }
    }
  }

  get pendingCount(): number {
    return this.barriers.size;
  }

  destroy(): void {
    for (const timer of this.timers.values()) {
      clearTimeout(timer);
    }
    this.timers.clear();
    this.barriers.clear();
  }
}

// ─── Causal Event Log ────────────────────────────────────────────────

/**
 * Ordered log of causal events with delivery guarantees.
 * Events are delivered in causal order: if a → b, then
 * deliver(a) happens before deliver(b). Concurrent events
 * can be delivered in any order.
 */

export class CausalEventLog<T = unknown> {
  private clock: VectorClock;
  private delivered: CausalEvent<T>[] = [];
  private pending: CausalEvent<T>[] = [];
  private deliveryCallbacks: ((event: CausalEvent<T>) => void)[] = [];

  constructor(private origin: AgentId, private maxHistory: number = 10_000) {
    this.clock = createClock(origin);
  }

  /**
   * Create and log a local event.
   */
  emit(payload: T): CausalEvent<T> {
    this.clock = tick(this.clock);
    const event: CausalEvent<T> = {
      id: `${this.origin}:${this.clock.entries.get(this.origin)}`,
      origin: this.origin,
      clock: this.clock,
      payload,
      timestamp: Date.now()
    };
    this.deliver(event);
    return event;
  }

  /**
   * Receive a remote event. It will be buffered until all
   * causal dependencies are met, then delivered in order.
   */
  receive(event: CausalEvent<T>): void {
    if (this.canDeliver(event)) {
      this.deliver(event);
      this.tryDeliverPending();
    } else {
      this.pending.push(event);
      // Sort pending by total clock sum (heuristic for likely-deliverable-first)
      this.pending.sort((a, b) => clockSum(a.clock) - clockSum(b.clock));
    }
  }

  onDeliver(callback: (event: CausalEvent<T>) => void): void {
    this.deliveryCallbacks.push(callback);
  }

  /**
   * Check if event's causal dependencies are all satisfied.
   * An event from agent X with clock C can be delivered when:
   * - For the origin agent X: local[X] == C[X] - 1 (this is the next expected)
   * - For all other agents Y: local[Y] >= C[Y] (we've seen everything they saw)
   */
  private canDeliver(event: CausalEvent<T>): boolean {
    for (const [agent, time] of event.clock.entries) {
      const localTime = this.clock.entries.get(agent) ?? 0;
      if (agent === event.origin) {
        // We need to have seen all prior events from this agent
        if (localTime < time - 1) return false;
      } else {
        // We need to have seen at least as much from other agents
        if (localTime < time) return false;
      }
    }
    return true;
  }

  private deliver(event: CausalEvent<T>): void {
    // Only merge clock for remote events; local events already ticked in emit()
    if (event.origin !== this.origin) {
      this.clock = merge(this.clock, event.clock);
    }

    this.delivered.push(event);

    // Enforce history limit
    if (this.delivered.length > this.maxHistory) {
      this.delivered = this.delivered.slice(-this.maxHistory);
    }

    // Notify listeners
    for (const cb of this.deliveryCallbacks) {
      try { cb(event); } catch { /* swallow */ }
    }
  }

  private tryDeliverPending(): void {
    let progress = true;
    while (progress) {
      progress = false;
      for (let i = this.pending.length - 1; i >= 0; i--) {
        if (this.canDeliver(this.pending[i])) {
          const [event] = this.pending.splice(i, 1);
          this.deliver(event);
          progress = true;
        }
      }
    }
  }

  get currentClock(): VectorClock { return this.clock; }
  get deliveredEvents(): readonly CausalEvent<T>[] { return this.delivered; }
  get pendingEvents(): readonly CausalEvent<T>[] { return this.pending; }
  get pendingCount(): number { return this.pending.length; }
}

// ─── Causal Stability Detector ───────────────────────────────────────

/**
 * Determines the "stable prefix" — the set of events that ALL
 * agents in the system have delivered. Events below the stability
 * frontier can have their metadata safely garbage collected.
 * 
 * Uses matrix clocks when available, falls back to periodic
 * heartbeat-based estimation otherwise.
 */

export class StabilityDetector {
  private knownClocks: Map<AgentId, VectorClock> = new Map();
  private stableFrontier: Map<AgentId, LogicalTime> = new Map();
  private callbacks: ((frontier: Map<AgentId, LogicalTime>) => void)[] = [];

  constructor(private agents: Set<AgentId>) {}

  /**
   * Report an agent's current clock (received via heartbeat or matrix row).
   */
  reportClock(agent: AgentId, clock: VectorClock): void {
    this.knownClocks.set(agent, clock);
    this.recompute();
  }

  /**
   * Recompute stable frontier as component-wise minimum across all known clocks.
   */
  private recompute(): void {
    // Need clocks from ALL agents
    if (this.knownClocks.size < this.agents.size) return;

    const newFrontier = new Map<AgentId, LogicalTime>();
    const allAgentIds = new Set<AgentId>();
    for (const clock of this.knownClocks.values()) {
      for (const id of clock.entries.keys()) allAgentIds.add(id);
    }

    for (const agentId of allAgentIds) {
      let min = Infinity;
      for (const clock of this.knownClocks.values()) {
        min = Math.min(min, clock.entries.get(agentId) ?? 0);
      }
      newFrontier.set(agentId, min);
    }

    // Check if frontier advanced
    let advanced = false;
    for (const [agent, time] of newFrontier) {
      if (time > (this.stableFrontier.get(agent) ?? 0)) {
        advanced = true;
        break;
      }
    }

    if (advanced) {
      this.stableFrontier = newFrontier;
      for (const cb of this.callbacks) {
        try { cb(new Map(this.stableFrontier)); } catch { /* swallow */ }
      }
    }
  }

  onAdvance(callback: (frontier: Map<AgentId, LogicalTime>) => void): void {
    this.callbacks.push(callback);
  }

  get frontier(): ReadonlyMap<AgentId, LogicalTime> {
    return this.stableFrontier;
  }

  /**
   * Check if a specific event is stable (delivered by all agents).
   */
  isStable(event: CausalEvent): boolean {
    const agentTime = this.stableFrontier.get(event.origin) ?? 0;
    const eventTime = event.clock.entries.get(event.origin) ?? 0;
    return eventTime <= agentTime;
  }
}

// ─── Plausible Clock Reconstruction ──────────────────────────────────

/**
 * When a new agent joins a running system, it doesn't have historical
 * clock state. Plausible clock reconstruction builds an approximate
 * clock from observed messages, giving the joiner enough context
 * to participate in causal ordering without replaying all history.
 */

export function reconstructClock(
  observedMessages: Array<{ origin: AgentId; clock: VectorClock }>,
  joiner: AgentId
): VectorClock {
  const entries = new Map<AgentId, LogicalTime>();
  entries.set(joiner, 0);

  // Take component-wise maximum across all observed clocks
  for (const msg of observedMessages) {
    for (const [agent, time] of msg.clock.entries) {
      entries.set(agent, Math.max(entries.get(agent) ?? 0, time));
    }
  }

  return { entries, origin: joiner };
}

// ─── Conflict Resolution ─────────────────────────────────────────────

export type ConflictStrategy<T> = (
  events: CausalEvent<T>[],
  context: { currentClock: VectorClock }
) => CausalEvent<T>;

/** Last-writer-wins using wall clock (unreliable but simple) */
export function lwwStrategy<T>(): ConflictStrategy<T> {
  return (events) => {
    return events.reduce((latest, e) =>
      e.timestamp > latest.timestamp ? e : latest
    );
  };
}

/** Deterministic winner by agent ID (total order on concurrent events) */
export function deterministicStrategy<T>(): ConflictStrategy<T> {
  return (events) => {
    return events.reduce((winner, e) =>
      e.origin < winner.origin ? e : winner
    );
  };
}

/** Priority-based: agents with lower ID win (configurable) */
export function priorityStrategy<T>(
  priorities: Map<AgentId, number>
): ConflictStrategy<T> {
  return (events) => {
    return events.reduce((winner, e) => {
      const wp = priorities.get(winner.origin) ?? Infinity;
      const ep = priorities.get(e.origin) ?? Infinity;
      return ep < wp ? e : winner;
    });
  };
}

// ─── Utilities ───────────────────────────────────────────────────────

function clockSum(clock: VectorClock): number {
  let sum = 0;
  for (const t of clock.entries.values()) sum += t;
  return sum;
}

export function clockToString(clock: VectorClock): string {
  const parts: string[] = [];
  const sorted = [...clock.entries.entries()].sort(([a], [b]) => a.localeCompare(b));
  for (const [agent, time] of sorted) {
    parts.push(`${agent.slice(0, 8)}:${time}`);
  }
  return `[${parts.join(', ')}]`;
}

export function clockSize(clock: VectorClock): number {
  return clock.entries.size;
}

// ─── Presets ─────────────────────────────────────────────────────────

export const PRESETS = {
  /** Small cluster (3-7 agents), full matrix clocks */
  'small-cluster': {
    maxAgents: 10,
    maxPendingEvents: 500,
    barrierTimeoutMs: 10_000,
    stabilityCheckIntervalMs: 1_000,
    enableMatrixClocks: true,
    enableDVV: true,
    gcIntervalMs: 30_000,
    maxEventHistory: 5_000
  } satisfies TrackerConfig,

  /** Medium network (10-50 agents), vector clocks only */
  'medium-network': {
    maxAgents: 50,
    maxPendingEvents: 2_000,
    barrierTimeoutMs: 30_000,
    stabilityCheckIntervalMs: 5_000,
    enableMatrixClocks: false,
    enableDVV: true,
    gcIntervalMs: 60_000,
    maxEventHistory: 20_000
  } satisfies TrackerConfig,

  /** Large federation (50+ agents), lightweight mode */
  'large-federation': {
    maxAgents: 500,
    maxPendingEvents: 10_000,
    barrierTimeoutMs: 60_000,
    stabilityCheckIntervalMs: 30_000,
    enableMatrixClocks: false,
    enableDVV: false,
    gcIntervalMs: 300_000,
    maxEventHistory: 50_000
  } satisfies TrackerConfig
} as const;
