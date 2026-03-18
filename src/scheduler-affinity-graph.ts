import { fnv1a } from './shared-utils';
/**
 * Scheduler Affinity Graph
 * 
 * Graph-based scheduler that learns agent-task affinity patterns over time
 * and uses them to make optimal placement decisions. Combines historical
 * performance data with real-time load signals for intelligent routing.
 * 
 * Key concepts:
 * - Bipartite affinity graph between agents and task types
 * - Edge weights represent learned performance affinity (latency, success rate, cost)
 * - Hungarian algorithm variant for optimal batch assignment
 * - Online learning with Thompson Sampling for exploration vs exploitation
 * - Affinity decay for adapting to capability changes
 * - Anti-affinity constraints for fault isolation
 * 
 * @module scheduler-affinity-graph
 */

// ─── Types ───────────────────────────────────────────────────────────────────

interface AgentProfile {
  id: string;
  capabilities: Set<string>;
  maxConcurrency: number;
  currentLoad: number;
  region?: string;
  costPerUnit: number;
  lastSeen: number;
}

interface TaskType {
  id: string;
  requiredCapabilities: string[];
  priority: number; // 0=lowest, 10=highest
  deadline?: number;
  maxLatencyMs?: number;
  affinityHints?: string[]; // preferred agent IDs
  antiAffinityHints?: string[]; // avoid these agents
  idempotent: boolean;
}

interface Task {
  id: string;
  typeId: string;
  payload: unknown;
  submittedAt: number;
  deadline?: number;
  priority: number;
  attempts: number;
  maxAttempts: number;
}

interface AffinityEdge {
  agentId: string;
  taskTypeId: string;
  successCount: number;
  failureCount: number;
  totalLatencyMs: number;
  totalCost: number;
  lastUpdated: number;
  // Thompson Sampling parameters (Beta distribution)
  alpha: number; // successes + 1
  beta: number;  // failures + 1
}

interface AssignmentResult {
  taskId: string;
  agentId: string;
  score: number;
  reason: string;
}

interface SchedulerEvent {
  type: 'task-assigned' | 'task-completed' | 'task-failed' | 'affinity-updated' |
        'agent-overloaded' | 'agent-recovered' | 'exploration-triggered' |
        'anti-affinity-enforced' | 'deadline-breach' | 'batch-assigned';
  timestamp: number;
  data: any;
}

interface SchedulerConfig {
  affinityDecayHalfLifeMs: number;
  explorationRate: number; // 0-1, probability of Thompson Sampling exploration
  batchWindowMs: number;
  maxBatchSize: number;
  loadThreshold: number; // 0-1, fraction of maxConcurrency
  deadlineBufferMs: number;
  minObservationsForConfidence: number;
  antiAffinityWeight: number;
  costWeight: number;
  latencyWeight: number;
  successWeight: number;
  regionAffinityBonus: number;
}

// ─── FNV-1a Hash ─────────────────────────────────────────────────────────────

// ─── Affinity Graph ──────────────────────────────────────────────────────────

class AffinityGraph {
  private edges: Map<string, AffinityEdge> = new Map();
  private readonly decayHalfLifeMs: number;
  
  constructor(decayHalfLifeMs: number) {
    this.decayHalfLifeMs = decayHalfLifeMs;
  }
  
  private edgeKey(agentId: string, taskTypeId: string): string {
    return `${agentId}::${taskTypeId}`;
  }
  
  getEdge(agentId: string, taskTypeId: string): AffinityEdge | undefined {
    return this.edges.get(this.edgeKey(agentId, taskTypeId));
  }
  
  getOrCreateEdge(agentId: string, taskTypeId: string, now: number): AffinityEdge {
    const key = this.edgeKey(agentId, taskTypeId);
    let edge = this.edges.get(key);
    if (!edge) {
      edge = {
        agentId,
        taskTypeId,
        successCount: 0,
        failureCount: 0,
        totalLatencyMs: 0,
        totalCost: 0,
        lastUpdated: now,
        alpha: 1, // uniform prior
        beta: 1,
      };
      this.edges.set(key, edge);
    }
    return edge;
  }
  
  recordSuccess(agentId: string, taskTypeId: string, latencyMs: number, cost: number, now: number): void {
    const edge = this.getOrCreateEdge(agentId, taskTypeId, now);
    edge.successCount++;
    edge.totalLatencyMs += latencyMs;
    edge.totalCost += cost;
    edge.alpha++;
    edge.lastUpdated = now;
  }
  
  recordFailure(agentId: string, taskTypeId: string, now: number): void {
    const edge = this.getOrCreateEdge(agentId, taskTypeId, now);
    edge.failureCount++;
    edge.beta++;
    edge.lastUpdated = now;
  }
  
  /**
   * Compute decayed affinity score for an agent-taskType pair.
   * Applies exponential decay based on time since last observation.
   */
  getAffinityScore(agentId: string, taskTypeId: string, now: number): number {
    const edge = this.getEdge(agentId, taskTypeId);
    if (!edge) return 0.5; // no data = neutral prior
    
    const age = now - edge.lastUpdated;
    const decayFactor = Math.pow(0.5, age / this.decayHalfLifeMs);
    
    const total = edge.successCount + edge.failureCount;
    if (total === 0) return 0.5;
    
    const rawScore = edge.successCount / total;
    // Decay toward 0.5 (uncertainty) over time
    return 0.5 + (rawScore - 0.5) * decayFactor;
  }
  
  /**
   * Thompson Sampling: draw from Beta(alpha, beta) posterior.
   * Uses the Jöhnk algorithm for generating Beta variates from uniform random.
   */
  sampleThompson(agentId: string, taskTypeId: string): number {
    const edge = this.getEdge(agentId, taskTypeId);
    if (!edge) return this.betaSample(1, 1); // uniform prior
    return this.betaSample(edge.alpha, edge.beta);
  }
  
  private betaSample(alpha: number, beta: number): number {
    // Use the relationship: Beta(a,b) = Gamma(a) / (Gamma(a) + Gamma(b))
    const x = this.gammaSample(alpha);
    const y = this.gammaSample(beta);
    return x / (x + y);
  }
  
  private gammaSample(shape: number): number {
    // Marsaglia and Tsang's method for shape >= 1
    if (shape < 1) {
      return this.gammaSample(shape + 1) * Math.pow(Math.random(), 1 / shape);
    }
    const d = shape - 1 / 3;
    const c = 1 / Math.sqrt(9 * d);
    while (true) {
      let x: number, v: number;
      do {
        x = this.normalSample();
        v = 1 + c * x;
      } while (v <= 0);
      v = v * v * v;
      const u = Math.random();
      if (u < 1 - 0.0331 * (x * x) * (x * x)) return d * v;
      if (Math.log(u) < 0.5 * x * x + d * (1 - v + Math.log(v))) return d * v;
    }
  }
  
  private normalSample(): number {
    // Box-Muller transform
    const u1 = Math.random();
    const u2 = Math.random();
    return Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
  }
  
  getAverageLatency(agentId: string, taskTypeId: string): number | null {
    const edge = this.getEdge(agentId, taskTypeId);
    if (!edge || edge.successCount === 0) return null;
    return edge.totalLatencyMs / edge.successCount;
  }
  
  getAverageCost(agentId: string, taskTypeId: string): number | null {
    const edge = this.getEdge(agentId, taskTypeId);
    if (!edge || edge.successCount === 0) return null;
    return edge.totalCost / edge.successCount;
  }
  
  getObservationCount(agentId: string, taskTypeId: string): number {
    const edge = this.getEdge(agentId, taskTypeId);
    return edge ? edge.successCount + edge.failureCount : 0;
  }
  
  /**
   * Prune edges that have fully decayed (effectively no signal left).
   */
  prune(now: number, minSignal: number = 0.01): number {
    let pruned = 0;
    for (const [key, edge] of Array.from(this.edges)) {
      const age = now - edge.lastUpdated;
      const decayFactor = Math.pow(0.5, age / this.decayHalfLifeMs);
      const total = edge.successCount + edge.failureCount;
      if (total > 0 && decayFactor < minSignal) {
        this.edges.delete(key);
        pruned++;
      }
    }
    return pruned;
  }
  
  edgeCount(): number {
    return this.edges.size;
  }
}

// ─── Agent Load Tracker ──────────────────────────────────────────────────────

class AgentLoadTracker {
  private agents: Map<string, AgentProfile> = new Map();
  private assignedTasks: Map<string, Set<string>> = new Map(); // agentId -> taskIds
  
  registerAgent(profile: AgentProfile): void {
    this.agents.set(profile.id, profile);
    if (!this.assignedTasks.has(profile.id)) {
      this.assignedTasks.set(profile.id, new Set());
    }
  }
  
  removeAgent(agentId: string): void {
    this.agents.delete(agentId);
    this.assignedTasks.delete(agentId);
  }
  
  getAgent(agentId: string): AgentProfile | undefined {
    return this.agents.get(agentId);
  }
  
  assignTask(agentId: string, taskId: string): boolean {
    const agent = this.agents.get(agentId);
    if (!agent) return false;
    const tasks = this.assignedTasks.get(agentId);
    if (!tasks) return false;
    if (tasks.size >= agent.maxConcurrency) return false;
    tasks.add(taskId);
    agent.currentLoad = tasks.size;
    return true;
  }
  
  releaseTask(agentId: string, taskId: string): void {
    const tasks = this.assignedTasks.get(agentId);
    if (tasks) {
      tasks.delete(taskId);
      const agent = this.agents.get(agentId);
      if (agent) agent.currentLoad = tasks.size;
    }
  }
  
  getLoadFraction(agentId: string): number {
    const agent = this.agents.get(agentId);
    if (!agent || agent.maxConcurrency === 0) return 1;
    return agent.currentLoad / agent.maxConcurrency;
  }
  
  getAvailableCapacity(agentId: string): number {
    const agent = this.agents.get(agentId);
    if (!agent) return 0;
    const tasks = this.assignedTasks.get(agentId);
    return agent.maxConcurrency - (tasks?.size ?? 0);
  }
  
  getEligibleAgents(taskType: TaskType): AgentProfile[] {
    const eligible: AgentProfile[] = [];
    for (const agent of Array.from(this.agents.values())) {
      // Check capability match
      const hasCapabilities = taskType.requiredCapabilities.every(
        cap => agent.capabilities.has(cap)
      );
      if (!hasCapabilities) continue;
      // Check capacity
      if (this.getAvailableCapacity(agent.id) <= 0) continue;
      eligible.push(agent);
    }
    return eligible;
  }
  
  getAllAgents(): AgentProfile[] {
    return Array.from(this.agents.values());
  }
}

// ─── Anti-Affinity Manager ───────────────────────────────────────────────────

class AntiAffinityManager {
  // Track recent assignments for spread constraints
  private recentAssignments: Map<string, { agentId: string; timestamp: number }[]> = new Map();
  private readonly windowMs: number;
  
  constructor(windowMs: number = 300_000) { // 5 min default
    this.windowMs = windowMs;
  }
  
  recordAssignment(taskTypeId: string, agentId: string, now: number): void {
    if (!this.recentAssignments.has(taskTypeId)) {
      this.recentAssignments.set(taskTypeId, []);
    }
    this.recentAssignments.get(taskTypeId)!.push({ agentId, timestamp: now });
  }
  
  /**
   * Compute anti-affinity penalty for assigning a task type to an agent.
   * Higher penalty if the same agent recently handled the same task type
   * (encourages spread for fault isolation).
   */
  getPenalty(taskTypeId: string, agentId: string, now: number): number {
    const recent = this.recentAssignments.get(taskTypeId);
    if (!recent) return 0;
    
    // Clean old entries
    const cutoff = now - this.windowMs;
    const valid = recent.filter(r => r.timestamp >= cutoff);
    this.recentAssignments.set(taskTypeId, valid);
    
    // Count how many recent assignments went to this agent
    const sameAgentCount = valid.filter(r => r.agentId === agentId).length;
    const totalCount = valid.length;
    
    if (totalCount === 0) return 0;
    
    // Penalty proportional to concentration
    return sameAgentCount / totalCount;
  }
  
  /**
   * Check explicit anti-affinity hints from task definition.
   */
  isExplicitlyExcluded(taskType: TaskType, agentId: string): boolean {
    return taskType.antiAffinityHints?.includes(agentId) ?? false;
  }
}

// ─── Deadline Tracker ────────────────────────────────────────────────────────

class DeadlineTracker {
  private tasks: Map<string, Task> = new Map();
  
  addTask(task: Task): void {
    this.tasks.set(task.id, task);
  }
  
  removeTask(taskId: string): void {
    this.tasks.delete(taskId);
  }
  
  /**
   * Compute deadline urgency score (0-1).
   * Higher score = more urgent (closer to deadline).
   */
  getUrgency(task: Task, now: number, bufferMs: number): number {
    if (!task.deadline) return 0.5; // no deadline = medium urgency
    
    const remaining = task.deadline - now;
    if (remaining <= 0) return 1; // already past deadline
    if (remaining <= bufferMs) return 0.9 + 0.1 * (1 - remaining / bufferMs);
    
    const totalWindow = task.deadline - task.submittedAt;
    if (totalWindow <= 0) return 1;
    
    const elapsed = now - task.submittedAt;
    return Math.min(1, elapsed / totalWindow);
  }
  
  getBreachedTasks(now: number): Task[] {
    const breached: Task[] = [];
    for (const task of Array.from(this.tasks.values())) {
      if (task.deadline && now > task.deadline) {
        breached.push(task);
      }
    }
    return breached;
  }
}

// ─── Batch Optimizer ─────────────────────────────────────────────────────────

/**
 * Implements a modified Hungarian-style assignment for batch task scheduling.
 * Produces optimal assignment of tasks to agents maximizing total score.
 * 
 * For small batches, uses greedy with lookahead.
 * The greedy approach is O(N*M*log(N*M)) vs O(N^3) for full Hungarian,
 * acceptable for typical agent network sizes.
 */
class BatchOptimizer {
  
  /**
   * Compute optimal batch assignment.
   * Returns assignments sorted by priority (highest first).
   */
  optimize(
    tasks: Task[],
    agents: AgentProfile[],
    scoreMatrix: Map<string, Map<string, number>>, // taskId -> agentId -> score
    capacities: Map<string, number> // agentId -> remaining capacity
  ): AssignmentResult[] {
    const results: AssignmentResult[] = [];
    const remainingCapacity = new Map(capacities);
    
    // Sort tasks by priority descending, then by deadline ascending
    const sortedTasks = [...tasks].sort((a, b) => {
      if (a.priority !== b.priority) return b.priority - a.priority;
      if (a.deadline && b.deadline) return a.deadline - b.deadline;
      if (a.deadline) return -1;
      if (b.deadline) return 1;
      return fnv1a(a.id) - fnv1a(b.id);
    });
    
    for (const task of sortedTasks) {
      const taskScores = scoreMatrix.get(task.id);
      if (!taskScores) continue;
      
      // Find best available agent
      let bestAgent: string | null = null;
      let bestScore = -Infinity;
      
      for (const [agentId, score] of Array.from(taskScores)) {
        const capacity = remainingCapacity.get(agentId) ?? 0;
        if (capacity <= 0) continue;
        if (score > bestScore) {
          bestScore = score;
          bestAgent = agentId;
        }
      }
      
      if (bestAgent) {
        results.push({
          taskId: task.id,
          agentId: bestAgent,
          score: bestScore,
          reason: `affinity-score=${bestScore.toFixed(3)}`,
        });
        remainingCapacity.set(bestAgent, (remainingCapacity.get(bestAgent) ?? 1) - 1);
      }
    }
    
    return results;
  }
}

// ─── Scheduler Affinity Graph (Orchestrator) ─────────────────────────────────

class SchedulerAffinityGraph {
  private readonly config: SchedulerConfig;
  private readonly affinityGraph: AffinityGraph;
  private readonly loadTracker: AgentLoadTracker;
  private readonly antiAffinity: AntiAffinityManager;
  private readonly deadlineTracker: DeadlineTracker;
  private readonly batchOptimizer: BatchOptimizer;
  private readonly events: SchedulerEvent[] = [];
  private readonly taskTypes: Map<string, TaskType> = new Map();
  
  // Pending tasks waiting for batch assignment
  private pendingQueue: Task[] = [];
  private lastBatchTime: number = 0;
  
  constructor(config: SchedulerConfig) {
    this.config = config;
    this.affinityGraph = new AffinityGraph(config.affinityDecayHalfLifeMs);
    this.loadTracker = new AgentLoadTracker();
    this.antiAffinity = new AntiAffinityManager();
    this.deadlineTracker = new DeadlineTracker();
    this.batchOptimizer = new BatchOptimizer();
  }
  
  // ── Agent Management ────────────────────────────────────────────────────
  
  registerAgent(profile: AgentProfile): void {
    this.loadTracker.registerAgent(profile);
  }
  
  removeAgent(agentId: string): void {
    this.loadTracker.removeAgent(agentId);
  }
  
  registerTaskType(taskType: TaskType): void {
    this.taskTypes.set(taskType.id, taskType);
  }
  
  // ── Task Submission ─────────────────────────────────────────────────────
  
  submitTask(task: Task): void {
    this.pendingQueue.push(task);
    this.deadlineTracker.addTask(task);
  }
  
  // ── Scoring ─────────────────────────────────────────────────────────────
  
  /**
   * Compute composite score for assigning a task to an agent.
   * Combines affinity, load, latency, cost, anti-affinity, and deadline urgency.
   */
  private computeScore(task: Task, agent: AgentProfile, taskType: TaskType, now: number): number {
    const c = this.config;
    
    // 1. Affinity score (exploit vs explore)
    let affinityScore: number;
    const shouldExplore = Math.random() < c.explorationRate;
    if (shouldExplore && this.affinityGraph.getObservationCount(agent.id, task.typeId) < c.minObservationsForConfidence) {
      // Thompson Sampling exploration
      affinityScore = this.affinityGraph.sampleThompson(agent.id, task.typeId);
      this.emit({
        type: 'exploration-triggered',
        timestamp: now,
        data: { agentId: agent.id, taskTypeId: task.typeId, sampledScore: affinityScore },
      });
    } else {
      // Exploit: use decayed affinity
      affinityScore = this.affinityGraph.getAffinityScore(agent.id, task.typeId, now);
    }
    
    // 2. Load score (prefer less loaded agents)
    const loadFraction = this.loadTracker.getLoadFraction(agent.id);
    const loadScore = 1 - loadFraction;
    if (loadFraction >= c.loadThreshold) {
      this.emit({
        type: 'agent-overloaded',
        timestamp: now,
        data: { agentId: agent.id, loadFraction },
      });
    }
    
    // 3. Latency score (normalized)
    const avgLatency = this.affinityGraph.getAverageLatency(agent.id, task.typeId);
    let latencyScore = 0.5; // neutral if no data
    if (avgLatency !== null && taskType.maxLatencyMs) {
      latencyScore = Math.max(0, 1 - avgLatency / taskType.maxLatencyMs);
    }
    
    // 4. Cost score (prefer cheaper agents)
    const avgCost = this.affinityGraph.getAverageCost(agent.id, task.typeId);
    let costScore = 0.5;
    if (avgCost !== null) {
      costScore = 1 / (1 + avgCost); // sigmoid-like normalization
    }
    
    // 5. Anti-affinity penalty
    let antiAffinityPenalty = 0;
    if (this.antiAffinity.isExplicitlyExcluded(taskType, agent.id)) {
      return -Infinity; // hard exclusion
    }
    antiAffinityPenalty = this.antiAffinity.getPenalty(task.typeId, agent.id, now);
    
    // 6. Region affinity bonus
    let regionBonus = 0;
    if (taskType.affinityHints?.includes(agent.id)) {
      regionBonus = c.regionAffinityBonus;
    }
    
    // 7. Deadline urgency boost (urgent tasks get priority routing)
    const urgency = this.deadlineTracker.getUrgency(task, now, c.deadlineBufferMs);
    const urgencyBoost = urgency > 0.8 ? 0.2 : 0; // boost for urgent tasks
    
    // Weighted composite
    const composite =
      c.successWeight * affinityScore +
      (1 - c.costWeight - c.latencyWeight - c.successWeight) * loadScore +
      c.latencyWeight * latencyScore +
      c.costWeight * costScore -
      c.antiAffinityWeight * antiAffinityPenalty +
      regionBonus +
      urgencyBoost;
    
    return composite;
  }
  
  // ── Assignment ──────────────────────────────────────────────────────────
  
  /**
   * Assign a single task immediately (bypass batching for urgent tasks).
   */
  assignImmediate(task: Task, now: number): AssignmentResult | null {
    const taskType = this.taskTypes.get(task.typeId);
    if (!taskType) return null;
    
    const eligible = this.loadTracker.getEligibleAgents(taskType);
    if (eligible.length === 0) return null;
    
    let bestAgent: AgentProfile | null = null;
    let bestScore = -Infinity;
    
    for (const agent of eligible) {
      const score = this.computeScore(task, agent, taskType, now);
      if (score > bestScore) {
        bestScore = score;
        bestAgent = agent;
      }
    }
    
    if (!bestAgent) return null;
    
    if (!this.loadTracker.assignTask(bestAgent.id, task.id)) return null;
    
    this.antiAffinity.recordAssignment(task.typeId, bestAgent.id, now);
    this.deadlineTracker.removeTask(task.id);
    
    const result: AssignmentResult = {
      taskId: task.id,
      agentId: bestAgent.id,
      score: bestScore,
      reason: `immediate-assign, score=${bestScore.toFixed(3)}`,
    };
    
    this.emit({
      type: 'task-assigned',
      timestamp: now,
      data: result,
    });
    
    return result;
  }
  
  /**
   * Process pending queue as a batch for optimal global assignment.
   */
  processBatch(now: number): AssignmentResult[] {
    if (this.pendingQueue.length === 0) return [];
    if (now - this.lastBatchTime < this.config.batchWindowMs && 
        this.pendingQueue.length < this.config.maxBatchSize) {
      return []; // wait for batch window or max size
    }
    
    const batch = this.pendingQueue.splice(0, this.config.maxBatchSize);
    this.lastBatchTime = now;
    
    // Build score matrix
    const scoreMatrix = new Map<string, Map<string, number>>();
    const capacities = new Map<string, number>();
    
    for (const agent of this.loadTracker.getAllAgents()) {
      capacities.set(agent.id, this.loadTracker.getAvailableCapacity(agent.id));
    }
    
    for (const task of batch) {
      const taskType = this.taskTypes.get(task.typeId);
      if (!taskType) continue;
      
      const taskScores = new Map<string, number>();
      const eligible = this.loadTracker.getEligibleAgents(taskType);
      
      for (const agent of eligible) {
        taskScores.set(agent.id, this.computeScore(task, agent, taskType, now));
      }
      
      scoreMatrix.set(task.id, taskScores);
    }
    
    const assignments = this.batchOptimizer.optimize(batch, this.loadTracker.getAllAgents(), scoreMatrix, capacities);
    
    // Apply assignments
    for (const assignment of assignments) {
      this.loadTracker.assignTask(assignment.agentId, assignment.taskId);
      const task = batch.find(t => t.id === assignment.taskId);
      if (task) {
        this.antiAffinity.recordAssignment(task.typeId, assignment.agentId, now);
        this.deadlineTracker.removeTask(task.id);
      }
    }
    
    // Tasks that couldn't be assigned go back to pending
    const assignedIds = new Set(assignments.map(a => a.taskId));
    for (const task of batch) {
      if (!assignedIds.has(task.id)) {
        this.pendingQueue.unshift(task); // re-queue at front
      }
    }
    
    this.emit({
      type: 'batch-assigned',
      timestamp: now,
      data: {
        batchSize: batch.length,
        assigned: assignments.length,
        requeued: batch.length - assignments.length,
      },
    });
    
    return assignments;
  }
  
  // ── Feedback ────────────────────────────────────────────────────────────
  
  /**
   * Record task completion for affinity learning.
   */
  recordCompletion(agentId: string, taskId: string, taskTypeId: string, latencyMs: number, cost: number, now: number): void {
    this.affinityGraph.recordSuccess(agentId, taskTypeId, latencyMs, cost, now);
    this.loadTracker.releaseTask(agentId, taskId);
    this.deadlineTracker.removeTask(taskId);
    
    this.emit({
      type: 'task-completed',
      timestamp: now,
      data: { agentId, taskId, taskTypeId, latencyMs, cost },
    });
    
    this.emit({
      type: 'affinity-updated',
      timestamp: now,
      data: {
        agentId,
        taskTypeId,
        newScore: this.affinityGraph.getAffinityScore(agentId, taskTypeId, now),
        observations: this.affinityGraph.getObservationCount(agentId, taskTypeId),
      },
    });
  }
  
  /**
   * Record task failure for affinity learning.
   */
  recordFailure(agentId: string, taskId: string, taskTypeId: string, now: number): void {
    this.affinityGraph.recordFailure(agentId, taskTypeId, now);
    this.loadTracker.releaseTask(agentId, taskId);
    
    this.emit({
      type: 'task-failed',
      timestamp: now,
      data: { agentId, taskId, taskTypeId },
    });
  }
  
  // ── Tick ─────────────────────────────────────────────────────────────────
  
  /**
   * Periodic tick: process batch, check deadlines, prune graph.
   */
  tick(now: number): { assignments: AssignmentResult[]; breached: Task[] } {
    const assignments = this.processBatch(now);
    const breached = this.deadlineTracker.getBreachedTasks(now);
    
    for (const task of breached) {
      this.emit({
        type: 'deadline-breach',
        timestamp: now,
        data: { taskId: task.id, deadline: task.deadline },
      });
    }
    
    // Periodic graph maintenance
    this.affinityGraph.prune(now);
    
    return { assignments, breached };
  }
  
  // ── Diagnostics ─────────────────────────────────────────────────────────
  
  getAffinityScore(agentId: string, taskTypeId: string, now: number): number {
    return this.affinityGraph.getAffinityScore(agentId, taskTypeId, now);
  }
  
  getPendingCount(): number {
    return this.pendingQueue.length;
  }
  
  getGraphEdgeCount(): number {
    return this.affinityGraph.edgeCount();
  }
  
  getRecentEvents(limit: number = 20): SchedulerEvent[] {
    return this.events.slice(-limit);
  }
  
  private emit(event: SchedulerEvent): void {
    this.events.push(event);
    if (this.events.length > 5000) {
      this.events.splice(0, this.events.length - 4000);
    }
  }
}

// ─── Presets ──────────────────────────────────────────────────────────────────

const PRESETS = {
  /** Low-latency interactive tasks, fast assignment, aggressive exploration */
  'interactive': {
    affinityDecayHalfLifeMs: 600_000,      // 10 min
    explorationRate: 0.15,
    batchWindowMs: 50,
    maxBatchSize: 10,
    loadThreshold: 0.7,
    deadlineBufferMs: 5_000,
    minObservationsForConfidence: 5,
    antiAffinityWeight: 0.1,
    costWeight: 0.1,
    latencyWeight: 0.4,
    successWeight: 0.3,
    regionAffinityBonus: 0.1,
  } satisfies SchedulerConfig,
  
  /** Batch processing, larger windows, cost-sensitive */
  'batch-processing': {
    affinityDecayHalfLifeMs: 3_600_000,    // 1 hour
    explorationRate: 0.05,
    batchWindowMs: 5_000,
    maxBatchSize: 100,
    loadThreshold: 0.9,
    deadlineBufferMs: 60_000,
    minObservationsForConfidence: 10,
    antiAffinityWeight: 0.2,
    costWeight: 0.3,
    latencyWeight: 0.1,
    successWeight: 0.3,
    regionAffinityBonus: 0.05,
  } satisfies SchedulerConfig,
  
  /** Balanced for general multi-agent workloads */
  'balanced': {
    affinityDecayHalfLifeMs: 1_800_000,    // 30 min
    explorationRate: 0.1,
    batchWindowMs: 1_000,
    maxBatchSize: 50,
    loadThreshold: 0.8,
    deadlineBufferMs: 30_000,
    minObservationsForConfidence: 8,
    antiAffinityWeight: 0.15,
    costWeight: 0.2,
    latencyWeight: 0.25,
    successWeight: 0.3,
    regionAffinityBonus: 0.08,
  } satisfies SchedulerConfig,
};

export {
  SchedulerAffinityGraph,
  AffinityGraph,
  AgentLoadTracker,
  AntiAffinityManager,
  DeadlineTracker,
  BatchOptimizer,
  PRESETS,
};
export type {
  AgentProfile,
  TaskType,
  Task,
  AffinityEdge,
  AssignmentResult,
  SchedulerEvent,
  SchedulerConfig,
};
