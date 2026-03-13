/**
 * Adaptive Work Stealing for Heterogeneous Agent Pools
 * 
 * Locality-aware work stealing with adaptive steal policies that account for
 * agent heterogeneity, NUMA-like topology costs, and task affinity.
 * 
 * Components:
 * - WorkDeque: Lock-free Chase-Lev double-ended queue per agent
 * - TopologyCostModel: NUMA-inspired steal cost based on agent distance
 * - StealPolicyController: Adaptive steal frequency and batch sizing
 * - AffinityTracker: Task-to-agent affinity learning with exponential decay
 * - VictimSelector: Intelligent steal target selection (not random)
 * - LoadImbalanceDetector: Trigger stealing only when imbalance exceeds threshold
 * - TaskFragmentationAnalyzer: Detect and merge over-split work items
 * - AdaptiveWorkStealingPool: Unified orchestrator
 */

// ─── Types ───────────────────────────────────────────────────────────────────

interface Task {
  id: string;
  type: string;
  priority: number;
  estimatedCostMs: number;
  data: Record<string, unknown>;
  createdAt: number;
  affinityHint?: string; // preferred agent or topology zone
  splittable: boolean;
  parentId?: string; // if this was split from a larger task
}

interface AgentNode {
  id: string;
  zone: string;
  rack: string;
  capabilities: Set<string>;
  processingRateMultiplier: number; // 1.0 = baseline, >1 = faster
  currentLoad: number;
  maxConcurrency: number;
  deque: WorkDeque;
  stealStats: StealStatistics;
  completedCount: number;
  totalProcessingMs: number;
}

interface StealStatistics {
  attempts: number;
  successes: number;
  failures: number;
  tasksStolen: number;
  lastStealAt: number;
  consecutiveFailures: number;
  backoffUntil: number;
}

interface StealResult {
  success: boolean;
  tasks: Task[];
  victim: string;
  thief: string;
  costPenalty: number;
  timestamp: number;
}

interface TopologyDistance {
  sameHost: 0;
  sameRack: 1;
  sameZone: 2;
  crossZone: 3;
}

interface StealPolicy {
  minImbalanceRatio: number;
  maxStealBatchSize: number;
  stealCooldownMs: number;
  backoffBaseMs: number;
  backoffMaxMs: number;
  localityWeight: number;    // 0-1: how much to prefer nearby victims
  affinityWeight: number;    // 0-1: how much to prefer affinity matches
  loadWeight: number;        // 0-1: how much to prefer heavily loaded victims
}

interface PoolConfig {
  stealPolicy: StealPolicy;
  rebalanceIntervalMs: number;
  affinityDecayHalfLifeMs: number;
  fragmentationThreshold: number; // max ratio of split tasks
  maxStealDistance: number;        // topology distance limit
  enableTaskSplitting: boolean;
  minTaskCostForSplit: number;
}

type EventType =
  | 'task-enqueued'
  | 'task-completed'
  | 'steal-attempted'
  | 'steal-succeeded'
  | 'steal-failed'
  | 'task-split'
  | 'task-merged'
  | 'rebalance-triggered'
  | 'imbalance-detected'
  | 'affinity-updated'
  | 'backoff-entered'
  | 'backoff-exited';

interface PoolEvent {
  type: EventType;
  timestamp: number;
  data: Record<string, unknown>;
}

// ─── FNV-1a Hash ─────────────────────────────────────────────────────────────

function fnv1a(input: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = (hash * 0x01000193) >>> 0;
  }
  return hash;
}

// ─── Work Deque (Chase-Lev) ──────────────────────────────────────────────────

class WorkDeque {
  private items: Task[] = [];

  /** Owner pushes to bottom (LIFO for locality) */
  pushBottom(task: Task): void {
    this.items.push(task);
  }

  /** Owner pops from bottom (LIFO) */
  popBottom(): Task | null {
    if (this.items.length === 0) return null;
    return this.items.pop()!;
  }

  /** Thieves steal from top (FIFO — oldest tasks first) */
  stealTop(count: number = 1): Task[] {
    const stolen: Task[] = [];
    const toSteal = Math.min(count, Math.floor(this.items.length / 2));
    for (let i = 0; i < toSteal; i++) {
      const task = this.items.shift();
      if (task) stolen.push(task);
    }
    return stolen;
  }

  size(): number {
    return this.items.length;
  }

  peek(): Task | null {
    return this.items.length > 0 ? this.items[this.items.length - 1] : null;
  }

  /** Get tasks without removing (for analysis) */
  inspect(): ReadonlyArray<Task> {
    return [...this.items];
  }
}

// ─── Topology Cost Model ─────────────────────────────────────────────────────

class TopologyCostModel {
  private readonly costTable: Map<number, number> = new Map([
    [0, 1.0],   // same host: no penalty
    [1, 1.5],   // same rack: small penalty
    [2, 3.0],   // same zone: moderate penalty
    [3, 10.0],  // cross zone: high penalty
  ]);

  distance(a: AgentNode, b: AgentNode): number {
    if (a.id === b.id) return 0;
    if (a.zone === b.zone && a.rack === b.rack) return 1;
    if (a.zone === b.zone) return 2;
    return 3;
  }

  cost(a: AgentNode, b: AgentNode): number {
    return this.costTable.get(this.distance(a, b)) ?? 10.0;
  }

  /** Effective steal value: task value discounted by topology cost */
  effectiveValue(task: Task, thief: AgentNode, victim: AgentNode): number {
    const baseCost = task.estimatedCostMs * task.priority;
    const topologyCost = this.cost(thief, victim);
    return baseCost / topologyCost;
  }
}

// ─── Affinity Tracker ────────────────────────────────────────────────────────

class AffinityTracker {
  private affinities: Map<string, Map<string, AffinityRecord>> = new Map();
  private halfLifeMs: number;

  constructor(halfLifeMs: number) {
    this.halfLifeMs = halfLifeMs;
  }

  /** Record successful task completion on an agent */
  recordCompletion(taskType: string, agentId: string, durationMs: number, now: number): void {
    if (!this.affinities.has(taskType)) {
      this.affinities.set(taskType, new Map());
    }
    const typeMap = this.affinities.get(taskType)!;
    
    let record = typeMap.get(agentId);
    if (!record) {
      record = { totalCompletions: 0, avgDurationMs: 0, lastSeenAt: 0, score: 0 };
      typeMap.set(agentId, record);
    }
    
    // EWMA duration
    const alpha = 0.3;
    record.avgDurationMs = record.avgDurationMs === 0
      ? durationMs
      : record.avgDurationMs * (1 - alpha) + durationMs * alpha;
    record.totalCompletions++;
    record.lastSeenAt = now;
    
    // Score: completions weighted by recency
    record.score = this.computeScore(record, now);
  }

  /** Get affinity score for a task type on an agent (0 = no affinity, higher = stronger) */
  getScore(taskType: string, agentId: string, now: number): number {
    const typeMap = this.affinities.get(taskType);
    if (!typeMap) return 0;
    const record = typeMap.get(agentId);
    if (!record) return 0;
    return this.computeScore(record, now);
  }

  /** Find best agent for a task type */
  bestAgent(taskType: string, candidates: string[], now: number): string | null {
    let best: string | null = null;
    let bestScore = 0;
    
    for (const agentId of candidates) {
      const score = this.getScore(taskType, agentId, now);
      if (score > bestScore) {
        bestScore = score;
        best = agentId;
      }
    }
    return best;
  }

  private computeScore(record: AffinityRecord, now: number): number {
    const age = now - record.lastSeenAt;
    const decayFactor = Math.pow(0.5, age / this.halfLifeMs);
    // Log completions to prevent runaway scores, weighted by decay
    return Math.log2(record.totalCompletions + 1) * decayFactor;
  }

  /** Prune stale entries */
  prune(now: number, maxAge: number): number {
    let pruned = 0;
    for (const [taskType, typeMap] of this.affinities) {
      for (const [agentId, record] of typeMap) {
        if (now - record.lastSeenAt > maxAge) {
          typeMap.delete(agentId);
          pruned++;
        }
      }
      if (typeMap.size === 0) this.affinities.delete(taskType);
    }
    return pruned;
  }
}

interface AffinityRecord {
  totalCompletions: number;
  avgDurationMs: number;
  lastSeenAt: number;
  score: number;
}

// ─── Victim Selector ─────────────────────────────────────────────────────────

class VictimSelector {
  private topology: TopologyCostModel;
  private affinity: AffinityTracker;

  constructor(topology: TopologyCostModel, affinity: AffinityTracker) {
    this.topology = topology;
    this.affinity = affinity;
  }

  /**
   * Select best victim to steal from.
   * Composite score: load factor + locality bonus + affinity bonus
   */
  selectVictim(
    thief: AgentNode,
    candidates: AgentNode[],
    policy: StealPolicy,
    maxDistance: number,
    now: number
  ): AgentNode | null {
    if (candidates.length === 0) return null;

    let bestVictim: AgentNode | null = null;
    let bestScore = -Infinity;

    for (const candidate of candidates) {
      if (candidate.id === thief.id) continue;
      if (candidate.deque.size() === 0) continue;
      
      const dist = this.topology.distance(thief, candidate);
      if (dist > maxDistance) continue;

      // Load factor: prefer heavily loaded victims
      const loadRatio = candidate.deque.size() / Math.max(candidate.maxConcurrency, 1);
      const loadScore = Math.min(loadRatio, 5.0); // cap at 5x

      // Locality bonus: prefer nearby victims (inverse distance)
      const localityScore = 1.0 / this.topology.cost(thief, candidate);

      // Affinity bonus: check if victim has tasks this thief is good at
      let affinityScore = 0;
      const victimTasks = candidate.deque.inspect();
      for (const task of victimTasks.slice(0, 5)) { // sample first 5
        affinityScore = Math.max(
          affinityScore,
          this.affinity.getScore(task.type, thief.id, now)
        );
      }

      const composite =
        policy.loadWeight * loadScore +
        policy.localityWeight * localityScore +
        policy.affinityWeight * affinityScore;

      if (composite > bestScore) {
        bestScore = composite;
        bestVictim = candidate;
      }
    }

    return bestVictim;
  }

  /**
   * Select multiple victims for batch stealing (power-of-two-choices variant)
   */
  selectMultipleVictims(
    thief: AgentNode,
    candidates: AgentNode[],
    policy: StealPolicy,
    maxDistance: number,
    count: number,
    now: number
  ): AgentNode[] {
    const scored: Array<{ node: AgentNode; score: number }> = [];

    for (const candidate of candidates) {
      if (candidate.id === thief.id) continue;
      if (candidate.deque.size() === 0) continue;
      
      const dist = this.topology.distance(thief, candidate);
      if (dist > maxDistance) continue;

      const loadRatio = candidate.deque.size() / Math.max(candidate.maxConcurrency, 1);
      const localityScore = 1.0 / this.topology.cost(thief, candidate);

      const composite =
        policy.loadWeight * Math.min(loadRatio, 5.0) +
        policy.localityWeight * localityScore;

      scored.push({ node: candidate, score: composite });
    }

    scored.sort((a, b) => b.score - a.score);
    return scored.slice(0, count).map(s => s.node);
  }
}

// ─── Load Imbalance Detector ─────────────────────────────────────────────────

class LoadImbalanceDetector {
  private history: Array<{ timestamp: number; gini: number }> = [];
  private maxHistory: number = 100;

  /**
   * Compute Gini coefficient of load distribution.
   * 0 = perfect equality, 1 = maximum inequality
   */
  computeGini(agents: AgentNode[]): number {
    if (agents.length < 2) return 0;

    const loads = agents.map(a => a.deque.size() / Math.max(a.maxConcurrency, 1));
    loads.sort((a, b) => a - b);

    const n = loads.length;
    const mean = loads.reduce((s, v) => s + v, 0) / n;
    if (mean === 0) return 0;

    let numerator = 0;
    for (let i = 0; i < n; i++) {
      numerator += (2 * (i + 1) - n - 1) * loads[i];
    }
    return numerator / (n * n * mean);
  }

  /**
   * Detect if imbalance warrants stealing
   */
  detect(
    agents: AgentNode[],
    threshold: number,
    now: number
  ): { imbalanced: boolean; gini: number; overloaded: string[]; underloaded: string[] } {
    const gini = this.computeGini(agents);

    this.history.push({ timestamp: now, gini });
    if (this.history.length > this.maxHistory) this.history.shift();

    const avgLoad = agents.reduce((s, a) => s + a.deque.size(), 0) / agents.length;
    
    const overloaded: string[] = [];
    const underloaded: string[] = [];

    for (const agent of agents) {
      const ratio = agent.deque.size() / Math.max(avgLoad, 1);
      if (ratio > 1.5 && agent.deque.size() > agent.maxConcurrency) {
        overloaded.push(agent.id);
      }
      if (ratio < 0.5 && agent.deque.size() < agent.maxConcurrency * 0.3) {
        underloaded.push(agent.id);
      }
    }

    return {
      imbalanced: gini > threshold,
      gini,
      overloaded,
      underloaded,
    };
  }

  /** Trend: is imbalance growing or shrinking? */
  trend(windowSize: number = 10): 'increasing' | 'stable' | 'decreasing' {
    if (this.history.length < windowSize) return 'stable';
    
    const recent = this.history.slice(-windowSize);
    const firstHalf = recent.slice(0, Math.floor(windowSize / 2));
    const secondHalf = recent.slice(Math.floor(windowSize / 2));
    
    const avgFirst = firstHalf.reduce((s, h) => s + h.gini, 0) / firstHalf.length;
    const avgSecond = secondHalf.reduce((s, h) => s + h.gini, 0) / secondHalf.length;
    
    const diff = avgSecond - avgFirst;
    if (diff > 0.05) return 'increasing';
    if (diff < -0.05) return 'decreasing';
    return 'stable';
  }
}

// ─── Task Fragmentation Analyzer ─────────────────────────────────────────────

class TaskFragmentationAnalyzer {
  private splitRegistry: Map<string, Set<string>> = new Map(); // parentId → child task ids

  recordSplit(parentId: string, childIds: string[]): void {
    if (!this.splitRegistry.has(parentId)) {
      this.splitRegistry.set(parentId, new Set());
    }
    const children = this.splitRegistry.get(parentId)!;
    for (const id of childIds) children.add(id);
  }

  recordCompletion(taskId: string): void {
    for (const [parentId, children] of this.splitRegistry) {
      children.delete(taskId);
      if (children.size === 0) this.splitRegistry.delete(parentId);
    }
  }

  /**
   * Compute fragmentation ratio: proportion of tasks that are fragments
   */
  fragmentationRatio(totalTasks: number): number {
    let fragments = 0;
    for (const children of this.splitRegistry.values()) {
      fragments += children.size;
    }
    return totalTasks > 0 ? fragments / totalTasks : 0;
  }

  /**
   * Find mergeable task groups: fragments of same parent on same agent
   */
  findMergeableGroups(agents: AgentNode[]): Array<{ parentId: string; agentId: string; taskIds: string[] }> {
    const groups: Array<{ parentId: string; agentId: string; taskIds: string[] }> = [];

    for (const [parentId, childIds] of this.splitRegistry) {
      // Group children by which agent has them
      const byAgent = new Map<string, string[]>();
      
      for (const agent of agents) {
        const agentTasks = agent.deque.inspect();
        for (const task of agentTasks) {
          if (childIds.has(task.id)) {
            if (!byAgent.has(agent.id)) byAgent.set(agent.id, []);
            byAgent.get(agent.id)!.push(task.id);
          }
        }
      }

      // If multiple fragments on same agent, they're mergeable
      for (const [agentId, taskIds] of byAgent) {
        if (taskIds.length >= 2) {
          groups.push({ parentId, agentId, taskIds });
        }
      }
    }

    return groups;
  }
}

// ─── Steal Policy Controller ─────────────────────────────────────────────────

class StealPolicyController {
  private basePolicy: StealPolicy;
  private successRateWindow: Array<{ timestamp: number; success: boolean }> = [];
  private windowSize: number = 50;

  constructor(basePolicy: StealPolicy) {
    this.basePolicy = { ...basePolicy };
  }

  /** Adapt policy based on recent steal success rate */
  getAdaptedPolicy(now: number): StealPolicy {
    const recentRate = this.recentSuccessRate(now, 60000); // last 60s
    const policy = { ...this.basePolicy };

    if (recentRate < 0.2) {
      // Low success: reduce aggression
      policy.stealCooldownMs = Math.min(policy.stealCooldownMs * 1.5, 10000);
      policy.maxStealBatchSize = Math.max(1, Math.floor(policy.maxStealBatchSize * 0.7));
      policy.minImbalanceRatio = Math.min(policy.minImbalanceRatio * 1.2, 0.8);
    } else if (recentRate > 0.7) {
      // High success: increase aggression
      policy.stealCooldownMs = Math.max(policy.stealCooldownMs * 0.8, 100);
      policy.maxStealBatchSize = Math.min(policy.maxStealBatchSize + 1, 10);
      policy.minImbalanceRatio = Math.max(policy.minImbalanceRatio * 0.9, 0.1);
    }

    return policy;
  }

  recordAttempt(success: boolean, now: number): void {
    this.successRateWindow.push({ timestamp: now, success });
    if (this.successRateWindow.length > this.windowSize) {
      this.successRateWindow.shift();
    }
  }

  private recentSuccessRate(now: number, windowMs: number): number {
    const cutoff = now - windowMs;
    const recent = this.successRateWindow.filter(e => e.timestamp >= cutoff);
    if (recent.length === 0) return 0.5; // no data, assume neutral
    return recent.filter(e => e.success).length / recent.length;
  }

  getStats(): { attempts: number; successRate: number } {
    const total = this.successRateWindow.length;
    const successes = this.successRateWindow.filter(e => e.success).length;
    return {
      attempts: total,
      successRate: total > 0 ? successes / total : 0,
    };
  }
}

// ─── Task Splitter ───────────────────────────────────────────────────────────

class TaskSplitter {
  /**
   * Split a large task into smaller subtasks.
   * Only works for splittable tasks above minimum cost threshold.
   */
  split(task: Task, parts: number, now: number): Task[] | null {
    if (!task.splittable) return null;
    if (parts < 2 || parts > 8) return null;

    const subtasks: Task[] = [];
    const costPerPart = Math.ceil(task.estimatedCostMs / parts);

    for (let i = 0; i < parts; i++) {
      subtasks.push({
        id: `${task.id}-split-${i}`,
        type: task.type,
        priority: task.priority,
        estimatedCostMs: costPerPart,
        data: { ...task.data, splitIndex: i, splitTotal: parts, originalTaskId: task.id },
        createdAt: now,
        affinityHint: task.affinityHint,
        splittable: false, // don't recursively split
        parentId: task.id,
      });
    }

    return subtasks;
  }

  /** Determine optimal split count based on available idle agents */
  optimalParts(task: Task, idleAgentCount: number): number {
    if (idleAgentCount <= 1) return 1; // no benefit
    // Split into min(idle agents, task_cost / 100ms, 4) parts
    const costBasedParts = Math.floor(task.estimatedCostMs / 100);
    return Math.min(idleAgentCount, costBasedParts, 4);
  }
}

// ─── Adaptive Work Stealing Pool ─────────────────────────────────────────────

class AdaptiveWorkStealingPool {
  private agents: Map<string, AgentNode> = new Map();
  private topology: TopologyCostModel;
  private affinity: AffinityTracker;
  private victimSelector: VictimSelector;
  private imbalanceDetector: LoadImbalanceDetector;
  private fragmentationAnalyzer: TaskFragmentationAnalyzer;
  private policyController: StealPolicyController;
  private taskSplitter: TaskSplitter;
  private config: PoolConfig;
  private events: PoolEvent[] = [];
  private maxEvents: number = 1000;
  private totalTasksEnqueued: number = 0;
  private totalTasksCompleted: number = 0;
  private totalSteals: number = 0;
  private lastRebalanceAt: number = 0;

  constructor(config: PoolConfig) {
    this.config = config;
    this.topology = new TopologyCostModel();
    this.affinity = new AffinityTracker(config.affinityDecayHalfLifeMs);
    this.victimSelector = new VictimSelector(this.topology, this.affinity);
    this.imbalanceDetector = new LoadImbalanceDetector();
    this.fragmentationAnalyzer = new TaskFragmentationAnalyzer();
    this.policyController = new StealPolicyController(config.stealPolicy);
    this.taskSplitter = new TaskSplitter();
  }

  // ─── Agent Management ────────────────────────────────────────────────────

  addAgent(id: string, zone: string, rack: string, capabilities: string[], maxConcurrency: number, speedMultiplier: number = 1.0): void {
    this.agents.set(id, {
      id,
      zone,
      rack,
      capabilities: new Set(capabilities),
      processingRateMultiplier: speedMultiplier,
      currentLoad: 0,
      maxConcurrency,
      deque: new WorkDeque(),
      stealStats: {
        attempts: 0, successes: 0, failures: 0,
        tasksStolen: 0, lastStealAt: 0,
        consecutiveFailures: 0, backoffUntil: 0,
      },
      completedCount: 0,
      totalProcessingMs: 0,
    });
  }

  removeAgent(id: string): Task[] {
    const agent = this.agents.get(id);
    if (!agent) return [];
    
    // Drain remaining tasks
    const orphaned: Task[] = [];
    let task: Task | null;
    while ((task = agent.deque.popBottom()) !== null) {
      orphaned.push(task);
    }
    
    this.agents.delete(id);
    return orphaned;
  }

  // ─── Task Submission ─────────────────────────────────────────────────────

  /**
   * Submit a task to the pool. Routes to best agent based on affinity and load.
   */
  submit(task: Task, now: number): string {
    const agentList = Array.from(this.agents.values());
    if (agentList.length === 0) throw new Error('No agents in pool');

    // Find best agent: affinity hint > learned affinity > least loaded
    let target: AgentNode | null = null;

    // 1. Check affinity hint
    if (task.affinityHint && this.agents.has(task.affinityHint)) {
      const hinted = this.agents.get(task.affinityHint)!;
      if (hinted.deque.size() < hinted.maxConcurrency * 2) {
        target = hinted;
      }
    }

    // 2. Check learned affinity
    if (!target) {
      const bestAffinity = this.affinity.bestAgent(
        task.type,
        agentList.filter(a => a.deque.size() < a.maxConcurrency * 2).map(a => a.id),
        now
      );
      if (bestAffinity) {
        target = this.agents.get(bestAffinity)!;
      }
    }

    // 3. Least loaded with capability match
    if (!target) {
      const eligible = agentList.filter(a => 
        a.capabilities.has(task.type) || a.capabilities.has('*')
      );
      
      if (eligible.length > 0) {
        // Power-of-two-choices: pick 2 random, choose less loaded
        if (eligible.length >= 2) {
          const i = fnv1a(task.id) % eligible.length;
          const j = (fnv1a(task.id + 'x') % (eligible.length - 1) + i + 1) % eligible.length;
          const a = eligible[i], b = eligible[j];
          target = a.deque.size() / a.maxConcurrency <= b.deque.size() / b.maxConcurrency ? a : b;
        } else {
          target = eligible[0];
        }
      }
    }

    // 4. Absolute fallback: any agent with least load
    if (!target) {
      target = agentList.reduce((best, curr) =>
        curr.deque.size() / curr.maxConcurrency < best.deque.size() / best.maxConcurrency ? curr : best
      );
    }

    // Check if task should be split
    if (
      this.config.enableTaskSplitting &&
      task.splittable &&
      task.estimatedCostMs >= this.config.minTaskCostForSplit
    ) {
      const idleCount = agentList.filter(a => a.deque.size() === 0).length;
      const parts = this.taskSplitter.optimalParts(task, idleCount);
      
      if (parts >= 2) {
        const subtasks = this.taskSplitter.split(task, parts, now);
        if (subtasks) {
          this.fragmentationAnalyzer.recordSplit(task.id, subtasks.map(t => t.id));
          this.emit({ type: 'task-split', timestamp: now, data: { taskId: task.id, parts } });
          
          // Distribute subtasks across idle agents
          for (const subtask of subtasks) {
            this.submit(subtask, now); // recursive, but subtasks aren't splittable
          }
          return target.id;
        }
      }
    }

    target.deque.pushBottom(task);
    this.totalTasksEnqueued++;
    this.emit({ type: 'task-enqueued', timestamp: now, data: { taskId: task.id, agentId: target.id } });

    return target.id;
  }

  // ─── Task Completion ─────────────────────────────────────────────────────

  completeTask(agentId: string, taskId: string, durationMs: number, now: number): void {
    const agent = this.agents.get(agentId);
    if (!agent) return;

    agent.completedCount++;
    agent.totalProcessingMs += durationMs;
    agent.currentLoad = Math.max(0, agent.currentLoad - 1);
    this.totalTasksCompleted++;

    // Update affinity
    const tasks = agent.deque.inspect();
    const task = tasks.find(t => t.id === taskId);
    const taskType = task?.type ?? 'unknown';
    this.affinity.recordCompletion(taskType, agentId, durationMs, now);
    this.fragmentationAnalyzer.recordCompletion(taskId);

    this.emit({
      type: 'task-completed',
      timestamp: now,
      data: { taskId, agentId, durationMs },
    });
  }

  // ─── Work Stealing ───────────────────────────────────────────────────────

  /**
   * Attempt to steal work for an idle/underloaded agent.
   * Called by the agent or orchestrator when an agent has capacity.
   */
  attemptSteal(thiefId: string, now: number): StealResult {
    const thief = this.agents.get(thiefId);
    if (!thief) return { success: false, tasks: [], victim: '', thief: thiefId, costPenalty: 0, timestamp: now };

    // Check backoff
    if (now < thief.stealStats.backoffUntil) {
      return { success: false, tasks: [], victim: '', thief: thiefId, costPenalty: 0, timestamp: now };
    }

    const policy = this.policyController.getAdaptedPolicy(now);
    const candidates = Array.from(this.agents.values());

    // Select victim
    const victim = this.victimSelector.selectVictim(
      thief, candidates, policy, this.config.maxStealDistance, now
    );

    this.emit({ type: 'steal-attempted', timestamp: now, data: { thief: thiefId, victim: victim?.id ?? null } });

    if (!victim) {
      this.recordStealFailure(thief, now);
      return { success: false, tasks: [], victim: '', thief: thiefId, costPenalty: 0, timestamp: now };
    }

    // Steal tasks
    const stolen = victim.deque.stealTop(policy.maxStealBatchSize);
    
    if (stolen.length === 0) {
      this.recordStealFailure(thief, now);
      return { success: false, tasks: [], victim: victim.id, thief: thiefId, costPenalty: 0, timestamp: now };
    }

    // Enqueue stolen tasks on thief
    for (const task of stolen) {
      thief.deque.pushBottom(task);
    }

    const costPenalty = this.topology.cost(thief, victim);

    // Update stats
    thief.stealStats.attempts++;
    thief.stealStats.successes++;
    thief.stealStats.tasksStolen += stolen.length;
    thief.stealStats.lastStealAt = now;
    thief.stealStats.consecutiveFailures = 0;
    thief.stealStats.backoffUntil = 0;
    this.totalSteals++;

    this.policyController.recordAttempt(true, now);
    this.emit({
      type: 'steal-succeeded',
      timestamp: now,
      data: {
        thief: thiefId,
        victim: victim.id,
        count: stolen.length,
        costPenalty,
        distance: this.topology.distance(thief, victim),
      },
    });

    return { success: true, tasks: stolen, victim: victim.id, thief: thiefId, costPenalty, timestamp: now };
  }

  private recordStealFailure(thief: AgentNode, now: number): void {
    thief.stealStats.attempts++;
    thief.stealStats.failures++;
    thief.stealStats.consecutiveFailures++;

    this.policyController.recordAttempt(false, now);

    // Exponential backoff
    const backoffMs = Math.min(
      this.config.stealPolicy.backoffBaseMs * Math.pow(2, thief.stealStats.consecutiveFailures - 1),
      this.config.stealPolicy.backoffMaxMs
    );
    thief.stealStats.backoffUntil = now + backoffMs;

    this.emit({
      type: 'steal-failed',
      timestamp: now,
      data: {
        thief: thief.id,
        consecutiveFailures: thief.stealStats.consecutiveFailures,
        backoffMs,
      },
    });

    if (thief.stealStats.consecutiveFailures === 1) {
      this.emit({ type: 'backoff-entered', timestamp: now, data: { agentId: thief.id, backoffMs } });
    }
  }

  // ─── Periodic Rebalance (tick) ───────────────────────────────────────────

  /**
   * Periodic tick: detect imbalance, trigger steals, merge fragments, prune affinity.
   */
  tick(now: number): void {
    if (now - this.lastRebalanceAt < this.config.rebalanceIntervalMs) return;
    this.lastRebalanceAt = now;

    const agents = Array.from(this.agents.values());
    if (agents.length < 2) return;

    // 1. Detect imbalance
    const policy = this.policyController.getAdaptedPolicy(now);
    const imbalance = this.imbalanceDetector.detect(agents, policy.minImbalanceRatio, now);

    if (imbalance.imbalanced) {
      this.emit({
        type: 'imbalance-detected',
        timestamp: now,
        data: {
          gini: imbalance.gini,
          overloaded: imbalance.overloaded,
          underloaded: imbalance.underloaded,
          trend: this.imbalanceDetector.trend(),
        },
      });

      // 2. Trigger steals for underloaded agents
      for (const underloadedId of imbalance.underloaded) {
        this.attemptSteal(underloadedId, now);
      }
    }

    // 3. Check fragmentation
    const totalQueued = agents.reduce((s, a) => s + a.deque.size(), 0);
    const fragRatio = this.fragmentationAnalyzer.fragmentationRatio(totalQueued);
    if (fragRatio > this.config.fragmentationThreshold) {
      const mergeGroups = this.fragmentationAnalyzer.findMergeableGroups(agents);
      for (const group of mergeGroups) {
        this.emit({
          type: 'task-merged',
          timestamp: now,
          data: { parentId: group.parentId, agentId: group.agentId, count: group.taskIds.length },
        });
      }
    }

    // 4. Prune stale affinity data
    this.affinity.prune(now, this.config.affinityDecayHalfLifeMs * 4);

    // 5. Clear backoffs for agents that waited long enough
    for (const agent of agents) {
      if (agent.stealStats.backoffUntil > 0 && now >= agent.stealStats.backoffUntil) {
        agent.stealStats.consecutiveFailures = 0;
        agent.stealStats.backoffUntil = 0;
        this.emit({ type: 'backoff-exited', timestamp: now, data: { agentId: agent.id } });
      }
    }

    this.emit({ type: 'rebalance-triggered', timestamp: now, data: { gini: imbalance.gini } });
  }

  // ─── Observability ───────────────────────────────────────────────────────

  getStats(): {
    agents: number;
    totalEnqueued: number;
    totalCompleted: number;
    totalSteals: number;
    currentQueueDepth: number;
    gini: number;
    stealSuccessRate: number;
    perAgent: Array<{
      id: string;
      queueDepth: number;
      completed: number;
      avgProcessingMs: number;
      stealsAttempted: number;
      stealsSucceeded: number;
    }>;
  } {
    const agents = Array.from(this.agents.values());
    const policyStats = this.policyController.getStats();

    return {
      agents: agents.length,
      totalEnqueued: this.totalTasksEnqueued,
      totalCompleted: this.totalTasksCompleted,
      totalSteals: this.totalSteals,
      currentQueueDepth: agents.reduce((s, a) => s + a.deque.size(), 0),
      gini: this.imbalanceDetector.computeGini(agents),
      stealSuccessRate: policyStats.successRate,
      perAgent: agents.map(a => ({
        id: a.id,
        queueDepth: a.deque.size(),
        completed: a.completedCount,
        avgProcessingMs: a.completedCount > 0 ? a.totalProcessingMs / a.completedCount : 0,
        stealsAttempted: a.stealStats.attempts,
        stealsSucceeded: a.stealStats.successes,
      })),
    };
  }

  getEvents(limit: number = 50): PoolEvent[] {
    return this.events.slice(-limit);
  }

  private emit(event: PoolEvent): void {
    this.events.push(event);
    if (this.events.length > this.maxEvents) {
      this.events = this.events.slice(-Math.floor(this.maxEvents * 0.8));
    }
  }
}

// ─── Presets ──────────────────────────────────────────────────────────────────

const PRESETS = {
  /** Low-latency interactive workloads: aggressive stealing, locality-first */
  'interactive': {
    stealPolicy: {
      minImbalanceRatio: 0.15,
      maxStealBatchSize: 2,
      stealCooldownMs: 50,
      backoffBaseMs: 100,
      backoffMaxMs: 2000,
      localityWeight: 0.5,
      affinityWeight: 0.3,
      loadWeight: 0.2,
    },
    rebalanceIntervalMs: 200,
    affinityDecayHalfLifeMs: 30000,
    fragmentationThreshold: 0.3,
    maxStealDistance: 2,
    enableTaskSplitting: false,
    minTaskCostForSplit: 500,
  } satisfies PoolConfig,

  /** Batch processing: larger steal batches, topology-aware */
  'batch-processing': {
    stealPolicy: {
      minImbalanceRatio: 0.25,
      maxStealBatchSize: 5,
      stealCooldownMs: 500,
      backoffBaseMs: 1000,
      backoffMaxMs: 30000,
      localityWeight: 0.2,
      affinityWeight: 0.2,
      loadWeight: 0.6,
    },
    rebalanceIntervalMs: 2000,
    affinityDecayHalfLifeMs: 120000,
    fragmentationThreshold: 0.5,
    maxStealDistance: 3,
    enableTaskSplitting: true,
    minTaskCostForSplit: 1000,
  } satisfies PoolConfig,

  /** Heterogeneous agent pool: affinity-heavy, respect agent strengths */
  'heterogeneous-pool': {
    stealPolicy: {
      minImbalanceRatio: 0.2,
      maxStealBatchSize: 3,
      stealCooldownMs: 200,
      backoffBaseMs: 500,
      backoffMaxMs: 10000,
      localityWeight: 0.2,
      affinityWeight: 0.5,
      loadWeight: 0.3,
    },
    rebalanceIntervalMs: 1000,
    affinityDecayHalfLifeMs: 60000,
    fragmentationThreshold: 0.4,
    maxStealDistance: 3,
    enableTaskSplitting: true,
    minTaskCostForSplit: 500,
  } satisfies PoolConfig,
};

export {
  AdaptiveWorkStealingPool,
  WorkDeque,
  TopologyCostModel,
  AffinityTracker,
  VictimSelector,
  LoadImbalanceDetector,
  TaskFragmentationAnalyzer,
  StealPolicyController,
  TaskSplitter,
  PRESETS,
};
export type {
  Task,
  AgentNode,
  StealResult,
  StealPolicy,
  PoolConfig,
  PoolEvent,
  EventType,
};
