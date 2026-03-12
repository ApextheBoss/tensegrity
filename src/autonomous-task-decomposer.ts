/**
 * Autonomous Task Decomposer for Agent Networks
 * 
 * Hierarchical task decomposition with dependency inference, complexity estimation,
 * and adaptive granularity control. Enables agents to break complex objectives into
 * executable sub-tasks with proper ordering and resource estimation.
 * 
 * Key capabilities:
 * - Recursive WBS (Work Breakdown Structure) generation with configurable depth limits
 * - Complexity estimation using weighted multi-factor scoring (Halstead-inspired metrics)
 * - Dependency inference via capability overlap analysis and output→input type matching
 * - Adaptive granularity: auto-splits tasks exceeding complexity threshold, merges trivial ones
 * - Skill-gap detection: identifies missing capabilities for a decomposition plan
 * - Time estimation using historical velocity with Bayesian updating
 * - Risk scoring per sub-task with propagation to parent nodes
 */

// --- Types ---

interface TaskSpec {
  id: string;
  title: string;
  description: string;
  requiredCapabilities: string[];
  inputTypes: string[];
  outputTypes: string[];
  constraints: TaskConstraint[];
  priority: number; // 0-100
  estimatedComplexity?: number;
  deadline?: number;
  metadata: Record<string, unknown>;
}

interface TaskConstraint {
  type: 'temporal' | 'resource' | 'ordering' | 'colocation' | 'isolation';
  params: Record<string, unknown>;
}

interface DecompositionNode {
  task: TaskSpec;
  children: DecompositionNode[];
  parent: string | null;
  depth: number;
  complexityScore: number;
  riskScore: number;
  estimatedDuration: number; // ms
  inferredDependencies: string[]; // task IDs this depends on
  assignmentHints: AgentHint[];
  status: 'pending' | 'ready' | 'assigned' | 'running' | 'completed' | 'failed';
}

interface AgentHint {
  agentId: string;
  capabilityMatch: number; // 0-1
  loadFactor: number; // current load
  historicalVelocity: number; // tasks/hour
}

interface DecompositionPlan {
  rootId: string;
  nodes: Map<string, DecompositionNode>;
  criticalPath: string[];
  totalComplexity: number;
  totalEstimatedDuration: number;
  skillGaps: SkillGap[];
  riskAssessment: RiskAssessment;
  createdAt: number;
}

interface SkillGap {
  capability: string;
  requiredBy: string[]; // task IDs
  availableAgents: number;
  severity: 'low' | 'medium' | 'high' | 'critical';
}

interface RiskAssessment {
  overallRisk: number; // 0-1
  highRiskTasks: string[];
  bottlenecks: string[];
  singlePointsOfFailure: string[];
}

interface VelocityRecord {
  agentId: string;
  capability: string;
  completionTimeMs: number;
  complexity: number;
  timestamp: number;
}

interface DecomposerEvent {
  type: 'task-decomposed' | 'dependency-inferred' | 'task-merged' | 'task-split' |
        'skill-gap-detected' | 'risk-escalated' | 'plan-created' | 'plan-updated' |
        'critical-path-changed' | 'granularity-adjusted';
  taskId?: string;
  planId?: string;
  details: Record<string, unknown>;
  timestamp: number;
}

type EventHandler = (event: DecomposerEvent) => void;

// --- FNV-1a Hash ---

function fnv1a(input: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = (hash * 0x01000193) >>> 0;
  }
  return hash;
}

// --- Complexity Estimator ---

class ComplexityEstimator {
  private weights: {
    capabilityCount: number;
    constraintCount: number;
    inputComplexity: number;
    outputComplexity: number;
    descriptionLength: number;
    priorityWeight: number;
  };

  constructor(weights?: Partial<ComplexityEstimator['weights']>) {
    this.weights = {
      capabilityCount: 3.0,
      constraintCount: 2.5,
      inputComplexity: 1.5,
      outputComplexity: 2.0,
      descriptionLength: 0.1,
      priorityWeight: 0.5,
      ...weights,
    };
  }

  estimate(task: TaskSpec): number {
    const capScore = task.requiredCapabilities.length * this.weights.capabilityCount;
    const constraintScore = task.constraints.length * this.weights.constraintCount;
    const inputScore = task.inputTypes.length * this.weights.inputComplexity;
    const outputScore = task.outputTypes.length * this.weights.outputComplexity;
    
    // Description complexity: word count as proxy for specification complexity
    const wordCount = task.description.split(/\s+/).filter(w => w.length > 0).length;
    const descScore = Math.log2(Math.max(wordCount, 1)) * this.weights.descriptionLength;
    
    // Priority amplifies complexity (high-priority tasks feel more complex due to pressure)
    const priorityAmplifier = 1.0 + (task.priority / 100) * this.weights.priorityWeight;
    
    const rawScore = (capScore + constraintScore + inputScore + outputScore + descScore) * priorityAmplifier;
    
    // Normalize to 0-100 range with sigmoid
    return 100 / (1 + Math.exp(-0.1 * (rawScore - 20)));
  }

  /**
   * Estimate if a task should be split based on complexity threshold
   */
  shouldSplit(task: TaskSpec, threshold: number): boolean {
    return this.estimate(task) > threshold;
  }

  /**
   * Estimate if adjacent tasks should be merged based on combined complexity
   */
  shouldMerge(tasks: TaskSpec[], maxMergedComplexity: number): boolean {
    if (tasks.length < 2) return false;
    
    // Merged complexity is sub-additive (shared context reduces overhead)
    const individualSum = tasks.reduce((sum, t) => sum + this.estimate(t), 0);
    const sharedCapabilities = new Set<string>();
    tasks.forEach(t => t.requiredCapabilities.forEach(c => sharedCapabilities.add(c)));
    
    // Discount for capability overlap
    const totalCaps = tasks.reduce((sum, t) => sum + t.requiredCapabilities.length, 0);
    const overlapRatio = totalCaps > 0 ? 1 - (sharedCapabilities.size / totalCaps) : 0;
    const mergedEstimate = individualSum * (0.7 + 0.3 * overlapRatio);
    
    return mergedEstimate <= maxMergedComplexity;
  }
}

// --- Dependency Inferrer ---

class DependencyInferrer {
  /**
   * Infer dependencies between tasks based on output→input type matching
   * Uses topological analysis to avoid cycles
   */
  inferDependencies(tasks: TaskSpec[]): Map<string, string[]> {
    const deps = new Map<string, string[]>();
    const taskMap = new Map(tasks.map(t => [t.id, t]));
    
    for (const task of tasks) {
      const taskDeps: string[] = [];
      
      for (const inputType of task.inputTypes) {
        // Find tasks whose outputs match this input
        for (const candidate of tasks) {
          if (candidate.id === task.id) continue;
          if (candidate.outputTypes.includes(inputType)) {
            taskDeps.push(candidate.id);
          }
        }
      }
      
      // Also check ordering constraints
      for (const constraint of task.constraints) {
        if (constraint.type === 'ordering' && typeof constraint.params['after'] === 'string') {
          const afterId = constraint.params['after'] as string;
          if (taskMap.has(afterId) && !taskDeps.includes(afterId)) {
            taskDeps.push(afterId);
          }
        }
      }
      
      deps.set(task.id, taskDeps);
    }
    
    // Remove cycles using DFS
    this.removeCycles(deps, tasks.map(t => t.id));
    
    return deps;
  }

  private removeCycles(deps: Map<string, string[]>, nodeIds: string[]): void {
    const visited = new Set<string>();
    const inStack = new Set<string>();
    
    const dfs = (nodeId: string): void => {
      if (inStack.has(nodeId)) return; // cycle detected, already being handled
      if (visited.has(nodeId)) return;
      
      visited.add(nodeId);
      inStack.add(nodeId);
      
      const nodeDeps = deps.get(nodeId) || [];
      const filtered: string[] = [];
      
      for (const dep of nodeDeps) {
        if (inStack.has(dep)) {
          // Back edge = cycle, remove this dependency
          continue;
        }
        filtered.push(dep);
        dfs(dep);
      }
      
      deps.set(nodeId, filtered);
      inStack.delete(nodeId);
    };
    
    for (const nodeId of nodeIds) {
      if (!visited.has(nodeId)) {
        dfs(nodeId);
      }
    }
  }

  /**
   * Compute transitive reduction: remove redundant edges
   * If A→B→C and A→C, remove A→C (it's implied)
   */
  transitiveReduction(deps: Map<string, string[]>): Map<string, string[]> {
    const reduced = new Map<string, string[]>();
    
    // For each node, BFS to find all transitive reachable nodes
    const reachable = new Map<string, Set<string>>();
    
    for (const [nodeId] of deps) {
      const reached = new Set<string>();
      const queue = [...(deps.get(nodeId) || [])];
      const visited = new Set<string>();
      
      while (queue.length > 0) {
        const current = queue.shift()!;
        if (visited.has(current)) continue;
        visited.add(current);
        
        for (const next of (deps.get(current) || [])) {
          reached.add(next);
          queue.push(next);
        }
      }
      
      reachable.set(nodeId, reached);
    }
    
    // Keep only edges that aren't transitively implied
    for (const [nodeId, nodeDeps] of deps) {
      const directOnly = nodeDeps.filter(dep => {
        const transitivelyReached = reachable.get(nodeId) || new Set();
        // Keep this edge only if removing it would make dep unreachable
        // i.e., dep is NOT reachable through other direct dependencies
        return !Array.from(nodeDeps)
          .filter(d => d !== dep)
          .some(d => {
            const dReachable = reachable.get(d) || new Set();
            return dReachable.has(dep) || d === dep;
          });
      });
      
      reduced.set(nodeId, directOnly);
    }
    
    return reduced;
  }
}

// --- Velocity Tracker (Bayesian) ---

class VelocityTracker {
  private records: VelocityRecord[] = [];
  private readonly maxRecords: number;
  private readonly priorMean: number; // ms per complexity unit
  private readonly priorVariance: number;

  constructor(maxRecords = 500, priorMeanMs = 60000, priorVariance = 900000000) {
    this.maxRecords = maxRecords;
    this.priorMean = priorMeanMs;
    this.priorVariance = priorVariance;
  }

  record(rec: VelocityRecord): void {
    this.records.push(rec);
    if (this.records.length > this.maxRecords) {
      this.records = this.records.slice(-this.maxRecords);
    }
  }

  /**
   * Bayesian estimate of time per complexity unit for a given agent+capability
   * Returns posterior mean with uncertainty bounds
   */
  estimateDuration(
    agentId: string,
    capability: string,
    complexity: number
  ): { mean: number; lower: number; upper: number } {
    // Filter relevant records
    const relevant = this.records.filter(r => {
      const agentMatch = r.agentId === agentId;
      const capMatch = r.capability === capability;
      return agentMatch || capMatch; // Use broader pool if specific is too small
    });

    if (relevant.length === 0) {
      // Pure prior
      const mean = this.priorMean * complexity;
      const stddev = Math.sqrt(this.priorVariance) * complexity;
      return { mean, lower: Math.max(0, mean - 2 * stddev), upper: mean + 2 * stddev };
    }

    // Compute sample statistics (time per complexity unit)
    const rates = relevant.map(r => r.completionTimeMs / Math.max(r.complexity, 1));
    const n = rates.length;
    const sampleMean = rates.reduce((a, b) => a + b, 0) / n;
    
    // Welford's online variance
    let m2 = 0;
    let mean = 0;
    for (let i = 0; i < rates.length; i++) {
      const delta = rates[i] - mean;
      mean += delta / (i + 1);
      const delta2 = rates[i] - mean;
      m2 += delta * delta2;
    }
    const sampleVariance = n > 1 ? m2 / (n - 1) : this.priorVariance;

    // Bayesian update: conjugate normal-normal
    const priorPrecision = 1 / this.priorVariance;
    const samplePrecision = n / sampleVariance;
    const posteriorPrecision = priorPrecision + samplePrecision;
    const posteriorMean = (priorPrecision * this.priorMean + samplePrecision * sampleMean) / posteriorPrecision;
    const posteriorVariance = 1 / posteriorPrecision;

    const totalMean = posteriorMean * complexity;
    const totalStddev = Math.sqrt(posteriorVariance) * complexity;

    return {
      mean: totalMean,
      lower: Math.max(0, totalMean - 2 * totalStddev),
      upper: totalMean + 2 * totalStddev,
    };
  }
}

// --- Risk Scorer ---

class RiskScorer {
  /**
   * Score risk for a decomposition node
   * Factors: complexity, dependency fan-in, capability scarcity, deadline pressure
   */
  scoreRisk(
    node: DecompositionNode,
    plan: DecompositionPlan,
    availableCapabilities: Map<string, number> // capability → agent count
  ): number {
    const factors: number[] = [];
    
    // Complexity risk: higher complexity = higher risk
    factors.push(Math.min(node.complexityScore / 80, 1.0));
    
    // Dependency risk: more incoming deps = more blocking potential
    const depCount = node.inferredDependencies.length;
    factors.push(Math.min(depCount / 5, 1.0));
    
    // Capability scarcity: fewer agents with required skills = higher risk
    let minAvailability = Infinity;
    for (const cap of node.task.requiredCapabilities) {
      const count = availableCapabilities.get(cap) || 0;
      minAvailability = Math.min(minAvailability, count);
    }
    factors.push(minAvailability === 0 ? 1.0 : Math.max(0, 1 - minAvailability / 5));
    
    // Deadline pressure: ratio of estimated duration to remaining time
    if (node.task.deadline) {
      const remaining = node.task.deadline - Date.now();
      if (remaining <= 0) {
        factors.push(1.0);
      } else {
        factors.push(Math.min(node.estimatedDuration / remaining, 1.0));
      }
    }
    
    // Weighted geometric mean for overall risk
    if (factors.length === 0) return 0;
    const logSum = factors.reduce((sum, f) => sum + Math.log(Math.max(f, 0.001)), 0);
    return Math.exp(logSum / factors.length);
  }

  /**
   * Find single points of failure: tasks that if failed, block >50% of remaining work
   */
  findSPOFs(plan: DecompositionPlan): string[] {
    const spofs: string[] = [];
    const totalTasks = plan.nodes.size;
    
    for (const [taskId] of plan.nodes) {
      // BFS: count how many tasks transitively depend on this one
      const dependents = new Set<string>();
      const queue = [taskId];
      
      while (queue.length > 0) {
        const current = queue.shift()!;
        for (const [otherId, otherNode] of plan.nodes) {
          if (dependents.has(otherId)) continue;
          if (otherNode.inferredDependencies.includes(current)) {
            dependents.add(otherId);
            queue.push(otherId);
          }
        }
      }
      
      if (dependents.size > totalTasks * 0.5) {
        spofs.push(taskId);
      }
    }
    
    return spofs;
  }
}

// --- Critical Path Calculator ---

class CriticalPathCalculator {
  /**
   * Forward/backward pass to find critical path through task DAG
   */
  calculate(plan: DecompositionPlan): { path: string[]; duration: number } {
    const nodes = plan.nodes;
    
    // Topological sort (Kahn's)
    const inDegree = new Map<string, number>();
    for (const [id, node] of nodes) {
      inDegree.set(id, node.inferredDependencies.length);
    }
    
    const queue: string[] = [];
    for (const [id, deg] of inDegree) {
      if (deg === 0) queue.push(id);
    }
    
    const topoOrder: string[] = [];
    while (queue.length > 0) {
      // Deterministic: sort by FNV-1a hash
      queue.sort((a, b) => fnv1a(a) - fnv1a(b));
      const current = queue.shift()!;
      topoOrder.push(current);
      
      for (const [id, node] of nodes) {
        if (node.inferredDependencies.includes(current)) {
          const newDeg = (inDegree.get(id) || 1) - 1;
          inDegree.set(id, newDeg);
          if (newDeg === 0) queue.push(id);
        }
      }
    }
    
    // Forward pass: earliest start/finish
    const earliestStart = new Map<string, number>();
    const earliestFinish = new Map<string, number>();
    
    for (const id of topoOrder) {
      const node = nodes.get(id)!;
      let es = 0;
      for (const depId of node.inferredDependencies) {
        es = Math.max(es, earliestFinish.get(depId) || 0);
      }
      earliestStart.set(id, es);
      earliestFinish.set(id, es + node.estimatedDuration);
    }
    
    // Find max finish time
    let maxFinish = 0;
    for (const [, ef] of earliestFinish) {
      maxFinish = Math.max(maxFinish, ef);
    }
    
    // Backward pass: latest start/finish
    const latestFinish = new Map<string, number>();
    const latestStart = new Map<string, number>();
    
    for (let i = topoOrder.length - 1; i >= 0; i--) {
      const id = topoOrder[i];
      const node = nodes.get(id)!;
      
      // Find minimum latest start of successors
      let lf = maxFinish;
      for (const [otherId, otherNode] of nodes) {
        if (otherNode.inferredDependencies.includes(id)) {
          lf = Math.min(lf, latestStart.get(otherId) || maxFinish);
        }
      }
      
      latestFinish.set(id, lf);
      latestStart.set(id, lf - node.estimatedDuration);
    }
    
    // Critical path: nodes where slack = 0
    const criticalPath: string[] = [];
    for (const id of topoOrder) {
      const es = earliestStart.get(id) || 0;
      const ls = latestStart.get(id) || 0;
      const slack = ls - es;
      if (Math.abs(slack) < 1) { // floating point tolerance
        criticalPath.push(id);
      }
    }
    
    return { path: criticalPath, duration: maxFinish };
  }
}

// --- Granularity Controller ---

class GranularityController {
  private readonly splitThreshold: number;
  private readonly mergeThreshold: number;
  private readonly maxDepth: number;
  private readonly estimator: ComplexityEstimator;

  constructor(
    splitThreshold = 70,
    mergeThreshold = 15,
    maxDepth = 5,
    estimator?: ComplexityEstimator
  ) {
    this.splitThreshold = splitThreshold;
    this.mergeThreshold = mergeThreshold;
    this.maxDepth = maxDepth;
    this.estimator = estimator || new ComplexityEstimator();
  }

  /**
   * Split a complex task into sub-tasks based on capability boundaries
   * Each required capability becomes a candidate split point
   */
  splitTask(task: TaskSpec, depth: number): TaskSpec[] {
    if (depth >= this.maxDepth) return [task];
    if (!this.estimator.shouldSplit(task, this.splitThreshold)) return [task];
    
    const caps = task.requiredCapabilities;
    if (caps.length <= 1) return [task]; // Can't split atomic capability
    
    // Split by capability groups
    const subTasks: TaskSpec[] = [];
    const midpoint = Math.ceil(caps.length / 2);
    
    const firstCaps = caps.slice(0, midpoint);
    const secondCaps = caps.slice(midpoint);
    
    subTasks.push({
      id: `${task.id}-sub-0`,
      title: `${task.title} (Phase 1)`,
      description: `First phase: ${firstCaps.join(', ')}`,
      requiredCapabilities: firstCaps,
      inputTypes: task.inputTypes,
      outputTypes: ['intermediate-result'],
      constraints: task.constraints.filter(c => c.type !== 'ordering'),
      priority: task.priority,
      deadline: task.deadline,
      metadata: { ...task.metadata, parentTask: task.id, phase: 0 },
    });
    
    subTasks.push({
      id: `${task.id}-sub-1`,
      title: `${task.title} (Phase 2)`,
      description: `Second phase: ${secondCaps.join(', ')}`,
      requiredCapabilities: secondCaps,
      inputTypes: ['intermediate-result'],
      outputTypes: task.outputTypes,
      constraints: [
        ...task.constraints.filter(c => c.type !== 'ordering'),
        { type: 'ordering', params: { after: `${task.id}-sub-0` } },
      ],
      priority: task.priority,
      deadline: task.deadline,
      metadata: { ...task.metadata, parentTask: task.id, phase: 1 },
    });
    
    return subTasks;
  }

  /**
   * Merge trivially small adjacent tasks that share capabilities
   */
  mergeTasks(tasks: TaskSpec[]): TaskSpec[] {
    if (tasks.length < 2) return tasks;
    
    const result: TaskSpec[] = [];
    let i = 0;
    
    while (i < tasks.length) {
      if (i + 1 < tasks.length && this.estimator.shouldMerge([tasks[i], tasks[i + 1]], this.mergeThreshold)) {
        // Merge pair
        const merged: TaskSpec = {
          id: `merged-${tasks[i].id}-${tasks[i + 1].id}`,
          title: `${tasks[i].title} + ${tasks[i + 1].title}`,
          description: `${tasks[i].description}\n---\n${tasks[i + 1].description}`,
          requiredCapabilities: [
            ...new Set([...tasks[i].requiredCapabilities, ...tasks[i + 1].requiredCapabilities]),
          ],
          inputTypes: [...new Set([...tasks[i].inputTypes, ...tasks[i + 1].inputTypes])],
          outputTypes: [...new Set([...tasks[i].outputTypes, ...tasks[i + 1].outputTypes])],
          constraints: [...tasks[i].constraints, ...tasks[i + 1].constraints],
          priority: Math.max(tasks[i].priority, tasks[i + 1].priority),
          deadline: tasks[i].deadline && tasks[i + 1].deadline
            ? Math.min(tasks[i].deadline!, tasks[i + 1].deadline!)
            : tasks[i].deadline || tasks[i + 1].deadline,
          metadata: { merged: [tasks[i].id, tasks[i + 1].id] },
        };
        result.push(merged);
        i += 2;
      } else {
        result.push(tasks[i]);
        i++;
      }
    }
    
    return result;
  }
}

// --- Skill Gap Analyzer ---

class SkillGapAnalyzer {
  analyze(
    tasks: TaskSpec[],
    availableCapabilities: Map<string, number>
  ): SkillGap[] {
    const gaps: SkillGap[] = [];
    const capDemand = new Map<string, string[]>(); // capability → task IDs
    
    for (const task of tasks) {
      for (const cap of task.requiredCapabilities) {
        if (!capDemand.has(cap)) capDemand.set(cap, []);
        capDemand.get(cap)!.push(task.id);
      }
    }
    
    for (const [cap, taskIds] of capDemand) {
      const available = availableCapabilities.get(cap) || 0;
      
      let severity: SkillGap['severity'];
      if (available === 0) {
        severity = 'critical';
      } else if (available < taskIds.length * 0.3) {
        severity = 'high';
      } else if (available < taskIds.length * 0.7) {
        severity = 'medium';
      } else {
        severity = 'low';
      }
      
      if (severity !== 'low') {
        gaps.push({ capability: cap, requiredBy: taskIds, availableAgents: available, severity });
      }
    }
    
    // Sort by severity
    const severityOrder: Record<string, number> = { critical: 0, high: 1, medium: 2, low: 3 };
    gaps.sort((a, b) => severityOrder[a.severity] - severityOrder[b.severity]);
    
    return gaps;
  }
}

// --- Task Decomposition Engine ---

class TaskDecompositionEngine {
  private readonly estimator: ComplexityEstimator;
  private readonly inferrer: DependencyInferrer;
  private readonly velocity: VelocityTracker;
  private readonly riskScorer: RiskScorer;
  private readonly criticalPath: CriticalPathCalculator;
  private readonly granularity: GranularityController;
  private readonly skillGapAnalyzer: SkillGapAnalyzer;
  private readonly handlers: EventHandler[] = [];
  private plans: Map<string, DecompositionPlan> = new Map();

  constructor(config?: {
    splitThreshold?: number;
    mergeThreshold?: number;
    maxDepth?: number;
    priorVelocityMs?: number;
  }) {
    this.estimator = new ComplexityEstimator();
    this.inferrer = new DependencyInferrer();
    this.velocity = new VelocityTracker(500, config?.priorVelocityMs || 60000);
    this.riskScorer = new RiskScorer();
    this.criticalPath = new CriticalPathCalculator();
    this.granularity = new GranularityController(
      config?.splitThreshold || 70,
      config?.mergeThreshold || 15,
      config?.maxDepth || 5,
      this.estimator
    );
    this.skillGapAnalyzer = new SkillGapAnalyzer();
  }

  on(handler: EventHandler): void {
    this.handlers.push(handler);
  }

  private emit(event: DecomposerEvent): void {
    for (const handler of this.handlers) {
      try { handler(event); } catch (_) { /* swallow */ }
    }
  }

  /**
   * Decompose a high-level objective into an executable plan
   */
  decompose(
    rootTask: TaskSpec,
    subTasks: TaskSpec[],
    availableCapabilities: Map<string, number>
  ): DecompositionPlan {
    const planId = `plan-${fnv1a(rootTask.id + Date.now().toString()).toString(16)}`;
    
    // Phase 1: Adaptive granularity — split complex, merge trivial
    let processedTasks = [...subTasks];
    
    // Split pass
    const splitTasks: TaskSpec[] = [];
    for (const task of processedTasks) {
      const splits = this.granularity.splitTask(task, 0);
      splitTasks.push(...splits);
      if (splits.length > 1) {
        this.emit({
          type: 'task-split',
          taskId: task.id,
          details: { splitInto: splits.map(s => s.id) },
          timestamp: Date.now(),
        });
      }
    }
    processedTasks = splitTasks;
    
    // Merge pass
    const preMergeCount = processedTasks.length;
    processedTasks = this.granularity.mergeTasks(processedTasks);
    if (processedTasks.length < preMergeCount) {
      this.emit({
        type: 'task-merged',
        details: { before: preMergeCount, after: processedTasks.length },
        timestamp: Date.now(),
      });
    }
    
    this.emit({
      type: 'granularity-adjusted',
      details: { original: subTasks.length, final: processedTasks.length },
      timestamp: Date.now(),
    });
    
    // Phase 2: Infer dependencies
    const rawDeps = this.inferrer.inferDependencies(processedTasks);
    const reducedDeps = this.inferrer.transitiveReduction(rawDeps);
    
    for (const [taskId, deps] of reducedDeps) {
      if (deps.length > 0) {
        this.emit({
          type: 'dependency-inferred',
          taskId,
          details: { dependencies: deps },
          timestamp: Date.now(),
        });
      }
    }
    
    // Phase 3: Build decomposition nodes
    const nodes = new Map<string, DecompositionNode>();
    
    for (const task of processedTasks) {
      const complexity = this.estimator.estimate(task);
      const deps = reducedDeps.get(task.id) || [];
      
      // Estimate duration using primary capability
      const primaryCap = task.requiredCapabilities[0] || 'general';
      const duration = this.velocity.estimateDuration('any', primaryCap, complexity);
      
      const node: DecompositionNode = {
        task,
        children: [],
        parent: rootTask.id,
        depth: 1,
        complexityScore: complexity,
        riskScore: 0, // calculated after plan creation
        estimatedDuration: duration.mean,
        inferredDependencies: deps,
        assignmentHints: [],
        status: deps.length === 0 ? 'ready' : 'pending',
      };
      
      nodes.set(task.id, node);
    }
    
    // Phase 4: Create plan
    const plan: DecompositionPlan = {
      rootId: rootTask.id,
      nodes,
      criticalPath: [],
      totalComplexity: 0,
      totalEstimatedDuration: 0,
      skillGaps: [],
      riskAssessment: {
        overallRisk: 0,
        highRiskTasks: [],
        bottlenecks: [],
        singlePointsOfFailure: [],
      },
      createdAt: Date.now(),
    };
    
    // Phase 5: Calculate critical path
    const cpResult = this.criticalPath.calculate(plan);
    plan.criticalPath = cpResult.path;
    plan.totalEstimatedDuration = cpResult.duration;
    plan.totalComplexity = Array.from(nodes.values())
      .reduce((sum, n) => sum + n.complexityScore, 0);
    
    // Phase 6: Score risks
    for (const [taskId, node] of nodes) {
      node.riskScore = this.riskScorer.scoreRisk(node, plan, availableCapabilities);
      if (node.riskScore > 0.7) {
        plan.riskAssessment.highRiskTasks.push(taskId);
      }
    }
    
    // Find SPOFs and bottlenecks
    plan.riskAssessment.singlePointsOfFailure = this.riskScorer.findSPOFs(plan);
    plan.riskAssessment.bottlenecks = plan.criticalPath.filter(id => {
      const node = nodes.get(id);
      return node && node.inferredDependencies.length >= 2;
    });
    plan.riskAssessment.overallRisk = plan.riskAssessment.highRiskTasks.length / Math.max(nodes.size, 1);
    
    // Phase 7: Skill gaps
    plan.skillGaps = this.skillGapAnalyzer.analyze(processedTasks, availableCapabilities);
    for (const gap of plan.skillGaps) {
      this.emit({
        type: 'skill-gap-detected',
        details: { capability: gap.capability, severity: gap.severity, requiredBy: gap.requiredBy.length },
        timestamp: Date.now(),
      });
    }
    
    this.plans.set(planId, plan);
    
    this.emit({
      type: 'plan-created',
      planId,
      details: {
        taskCount: nodes.size,
        criticalPathLength: plan.criticalPath.length,
        totalComplexity: plan.totalComplexity,
        estimatedDuration: plan.totalEstimatedDuration,
        overallRisk: plan.riskAssessment.overallRisk,
        skillGaps: plan.skillGaps.length,
      },
      timestamp: Date.now(),
    });
    
    return plan;
  }

  /**
   * Record task completion for velocity tracking
   */
  recordCompletion(agentId: string, capability: string, complexity: number, durationMs: number): void {
    this.velocity.record({
      agentId,
      capability,
      completionTimeMs: durationMs,
      complexity,
      timestamp: Date.now(),
    });
  }

  /**
   * Re-evaluate a plan after changes (task completion, failure, new info)
   */
  replan(planId: string, availableCapabilities: Map<string, number>): DecompositionPlan | null {
    const plan = this.plans.get(planId);
    if (!plan) return null;
    
    // Recalculate critical path
    const cpResult = this.criticalPath.calculate(plan);
    const oldCP = plan.criticalPath;
    plan.criticalPath = cpResult.path;
    plan.totalEstimatedDuration = cpResult.duration;
    
    if (JSON.stringify(oldCP) !== JSON.stringify(plan.criticalPath)) {
      this.emit({
        type: 'critical-path-changed',
        planId,
        details: { oldPath: oldCP, newPath: plan.criticalPath },
        timestamp: Date.now(),
      });
    }
    
    // Recalculate risks
    for (const [taskId, node] of plan.nodes) {
      if (node.status === 'completed' || node.status === 'running') continue;
      node.riskScore = this.riskScorer.scoreRisk(node, plan, availableCapabilities);
    }
    
    // Update ready status
    for (const [, node] of plan.nodes) {
      if (node.status !== 'pending') continue;
      const allDepsComplete = node.inferredDependencies.every(depId => {
        const depNode = plan.nodes.get(depId);
        return depNode && depNode.status === 'completed';
      });
      if (allDepsComplete) {
        node.status = 'ready';
      }
    }
    
    this.emit({
      type: 'plan-updated',
      planId,
      details: { activeTasks: Array.from(plan.nodes.values()).filter(n => n.status === 'ready').length },
      timestamp: Date.now(),
    });
    
    return plan;
  }

  getPlan(planId: string): DecompositionPlan | undefined {
    return this.plans.get(planId);
  }
}

// --- Presets ---

function createSmallTeamDecomposer(): TaskDecompositionEngine {
  return new TaskDecompositionEngine({
    splitThreshold: 60,
    mergeThreshold: 20,
    maxDepth: 3,
    priorVelocityMs: 30000,
  });
}

function createLargeProjectDecomposer(): TaskDecompositionEngine {
  return new TaskDecompositionEngine({
    splitThreshold: 50,
    mergeThreshold: 10,
    maxDepth: 6,
    priorVelocityMs: 120000,
  });
}

function createSprintDecomposer(): TaskDecompositionEngine {
  return new TaskDecompositionEngine({
    splitThreshold: 80,
    mergeThreshold: 25,
    maxDepth: 3,
    priorVelocityMs: 15000,
  });
}

export {
  TaskDecompositionEngine,
  ComplexityEstimator,
  DependencyInferrer,
  VelocityTracker,
  RiskScorer,
  CriticalPathCalculator,
  GranularityController,
  SkillGapAnalyzer,
  createSmallTeamDecomposer,
  createLargeProjectDecomposer,
  createSprintDecomposer,
};
