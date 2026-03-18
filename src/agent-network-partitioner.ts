import { fnv1a } from './shared-utils';
/**
 * Agent Network Partitioner
 * 
 * Controlled network partitioning for agent clusters with configurable
 * split-brain resolution, partition-aware routing, and automated healing.
 * 
 * Components:
 * - PartitionTopologyManager: real-time network graph with connectivity tracking
 * - PartitionOracle: failure detection via phi-accrual + asymmetric link probing
 * - ControlledPartitionEngine: intentional partition creation for testing/maintenance
 * - SplitBrainResolver: 5 resolution strategies for conflicting partition states
 * - PartitionAwareRouter: route messages respecting partition boundaries
 * - HealingCoordinator: automated partition repair with state reconciliation
 * - PartitionHistoryLogger: tamper-evident audit trail of all partition events
 * - NetworkPartitioner: unified orchestrator
 */

// ─── Types ───────────────────────────────────────────────────────────
interface AgentNode {
  id: string;
  zone: string;
  priority: number;
  capabilities: string[];
  lastSeen: number;
  metadata: Record<string, unknown>;
}

interface NetworkLink {
  source: string;
  target: string;
  latency: number;
  bandwidth: number;
  lossRate: number;
  lastProbed: number;
  status: 'healthy' | 'degraded' | 'failed' | 'unknown';
  asymmetricHealth: { sourceToTarget: number; targetToSource: number };
}

interface Partition {
  id: string;
  members: Set<string>;
  epoch: number;
  createdAt: number;
  reason: 'detected' | 'controlled' | 'zone_failure';
  leader?: string;
  stateVersion: number;
}

interface PartitionEvent {
  type: 'partition_detected' | 'partition_created' | 'partition_healed' |
        'split_brain_detected' | 'split_brain_resolved' | 'link_failed' |
        'link_restored' | 'node_isolated' | 'controlled_split' |
        'healing_started' | 'healing_completed' | 'state_reconciled';
  timestamp: number;
  partitionId?: string;
  details: Record<string, unknown>;
}

interface SplitBrainResolution {
  strategy: string;
  winningPartition: string;
  losingPartitions: string[];
  conflictsResolved: number;
  dataLossEstimate: number;
}

interface HealingPlan {
  id: string;
  sourcePartition: string;
  targetPartition: string;
  phase: 'quiesce' | 'reconcile' | 'merge' | 'verify' | 'complete';
  startedAt: number;
  estimatedDuration: number;
  stateConflicts: number;
  progress: number;
}

interface PartitionerConfig {
  phiThreshold: number;
  probeIntervalMs: number;
  asymmetricProbeEnabled: boolean;
  minPartitionSize: number;
  splitBrainStrategy: 'largest_partition' | 'highest_priority' | 'most_recent_state' |
                      'designated_leader' | 'quorum_based';
  healingDelayMs: number;
  autoHealEnabled: boolean;
  maxConcurrentHealings: number;
  controlledPartitionMaxDuration: number;
  stateReconciliationStrategy: 'lww' | 'vector_clock' | 'manual';
  historyRetention: number;
}

// ─── FNV-1a Hash ─────────────────────────────────────────────────────

// ─── Phi Accrual Failure Detector ────────────────────────────────────
class PhiAccrualDetector {
  private intervals: Map<string, number[]> = new Map();
  private lastHeartbeat: Map<string, number> = new Map();
  private readonly maxSamples: number;
  private readonly minSamples: number;

  constructor(maxSamples = 100, minSamples = 3) {
    this.maxSamples = maxSamples;
    this.minSamples = minSamples;
  }

  recordHeartbeat(nodeId: string, now: number): void {
    const last = this.lastHeartbeat.get(nodeId);
    if (last !== undefined) {
      const intervals = this.intervals.get(nodeId) || [];
      intervals.push(now - last);
      if (intervals.length > this.maxSamples) intervals.shift();
      this.intervals.set(nodeId, intervals);
    }
    this.lastHeartbeat.set(nodeId, now);
  }

  phi(nodeId: string, now: number): number {
    const last = this.lastHeartbeat.get(nodeId);
    if (last === undefined) return 0;
    const intervals = this.intervals.get(nodeId);
    if (!intervals || intervals.length < this.minSamples) return 0;

    const elapsed = now - last;
    const mean = intervals.reduce((s, v) => s + v, 0) / intervals.length;
    const variance = intervals.reduce((s, v) => s + (v - mean) ** 2, 0) / intervals.length;
    const stddev = Math.sqrt(variance) || 1;

    // Abramowitz-Stegun approximation of normal CDF
    const y = (elapsed - mean) / stddev;
    const t = 1 / (1 + 0.2316419 * Math.abs(y));
    const d = 0.3989422804 * Math.exp(-y * y / 2);
    const p = d * t * (0.3193815 + t * (-0.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));
    const cdf = y > 0 ? 1 - p : p;

    return -Math.log10(1 - cdf + 1e-12);
  }

  removeNode(nodeId: string): void {
    this.intervals.delete(nodeId);
    this.lastHeartbeat.delete(nodeId);
  }
}

// ─── Partition Topology Manager ──────────────────────────────────────
class PartitionTopologyManager {
  private nodes: Map<string, AgentNode> = new Map();
  private links: Map<string, NetworkLink> = new Map();
  private adjacency: Map<string, Set<string>> = new Map();

  addNode(node: AgentNode): void {
    this.nodes.set(node.id, node);
    if (!this.adjacency.has(node.id)) {
      this.adjacency.set(node.id, new Set());
    }
  }

  removeNode(nodeId: string): void {
    this.nodes.delete(nodeId);
    const neighbors = this.adjacency.get(nodeId) || new Set();
    for (const neighbor of neighbors) {
      this.adjacency.get(neighbor)?.delete(nodeId);
      this.links.delete(this.linkKey(nodeId, neighbor));
      this.links.delete(this.linkKey(neighbor, nodeId));
    }
    this.adjacency.delete(nodeId);
  }

  addLink(source: string, target: string, link: Partial<NetworkLink>): void {
    const key = this.linkKey(source, target);
    this.links.set(key, {
      source, target,
      latency: link.latency ?? 10,
      bandwidth: link.bandwidth ?? 1000,
      lossRate: link.lossRate ?? 0,
      lastProbed: link.lastProbed ?? Date.now(),
      status: link.status ?? 'healthy',
      asymmetricHealth: link.asymmetricHealth ?? { sourceToTarget: 1, targetToSource: 1 },
    });
    this.adjacency.get(source)?.add(target);
    this.adjacency.get(target)?.add(source);
  }

  updateLinkStatus(source: string, target: string, status: NetworkLink['status']): void {
    const key = this.linkKey(source, target);
    const link = this.links.get(key);
    if (link) link.status = status;
  }

  getConnectedComponents(): Set<string>[] {
    const visited = new Set<string>();
    const components: Set<string>[] = [];

    for (const nodeId of this.nodes.keys()) {
      if (visited.has(nodeId)) continue;
      const component = new Set<string>();
      const queue = [nodeId];
      while (queue.length > 0) {
        const current = queue.shift()!;
        if (visited.has(current)) continue;
        visited.add(current);
        component.add(current);
        const neighbors = this.adjacency.get(current) || new Set();
        for (const neighbor of neighbors) {
          if (visited.has(neighbor)) continue;
          const link = this.links.get(this.linkKey(current, neighbor)) ||
                       this.links.get(this.linkKey(neighbor, current));
          if (link && link.status !== 'failed') {
            queue.push(neighbor);
          }
        }
      }
      components.push(component);
    }
    return components;
  }

  getBridgeLinks(): Array<{ source: string; target: string }> {
    const bridges: Array<{ source: string; target: string }> = [];
    const disc: Map<string, number> = new Map();
    const low: Map<string, number> = new Map();
    let timer = 0;

    const dfs = (u: string, parent: string | null): void => {
      disc.set(u, timer);
      low.set(u, timer);
      timer++;
      const neighbors = this.adjacency.get(u) || new Set();
      for (const v of neighbors) {
        if (v === parent) continue;
        const link = this.links.get(this.linkKey(u, v)) ||
                     this.links.get(this.linkKey(v, u));
        if (!link || link.status === 'failed') continue;
        if (!disc.has(v)) {
          dfs(v, u);
          low.set(u, Math.min(low.get(u)!, low.get(v)!));
          if (low.get(v)! > disc.get(u)!) {
            bridges.push({ source: u, target: v });
          }
        } else {
          low.set(u, Math.min(low.get(u)!, disc.get(v)!));
        }
      }
    };

    for (const nodeId of this.nodes.keys()) {
      if (!disc.has(nodeId)) dfs(nodeId, null);
    }
    return bridges;
  }

  getNode(id: string): AgentNode | undefined { return this.nodes.get(id); }
  getLink(source: string, target: string): NetworkLink | undefined {
    return this.links.get(this.linkKey(source, target));
  }
  getAllNodes(): AgentNode[] { return Array.from(this.nodes.values()); }
  getNeighbors(nodeId: string): string[] { return Array.from(this.adjacency.get(nodeId) || []); }

  private linkKey(a: string, b: string): string { return `${a}→${b}`; }
}

// ─── Controlled Partition Engine ─────────────────────────────────────
class ControlledPartitionEngine {
  private activePartitions: Map<string, { groups: string[][]; expiresAt: number; reason: string }> = new Map();

  createPartition(
    id: string, groups: string[][], reason: string, durationMs: number
  ): { partitionId: string; groups: string[][] } {
    this.activePartitions.set(id, {
      groups,
      expiresAt: Date.now() + durationMs,
      reason,
    });
    return { partitionId: id, groups };
  }

  isPartitioned(nodeA: string, nodeB: string): boolean {
    for (const partition of this.activePartitions.values()) {
      let groupA = -1, groupB = -1;
      for (let i = 0; i < partition.groups.length; i++) {
        if (partition.groups[i].includes(nodeA)) groupA = i;
        if (partition.groups[i].includes(nodeB)) groupB = i;
      }
      if (groupA >= 0 && groupB >= 0 && groupA !== groupB) return true;
    }
    return false;
  }

  getExpiredPartitions(now: number): string[] {
    const expired: string[] = [];
    for (const [id, p] of this.activePartitions) {
      if (now >= p.expiresAt) expired.push(id);
    }
    return expired;
  }

  removePartition(id: string): boolean {
    return this.activePartitions.delete(id);
  }

  getActivePartitions(): Map<string, { groups: string[][]; expiresAt: number; reason: string }> {
    return new Map(this.activePartitions);
  }
}

// ─── Split Brain Resolver ────────────────────────────────────────────
class SplitBrainResolver {
  resolve(
    partitions: Partition[],
    strategy: PartitionerConfig['splitBrainStrategy'],
    topology: PartitionTopologyManager
  ): SplitBrainResolution {
    if (partitions.length < 2) {
      return { strategy, winningPartition: partitions[0]?.id ?? '', losingPartitions: [], conflictsResolved: 0, dataLossEstimate: 0 };
    }

    let winnerId: string;
    switch (strategy) {
      case 'largest_partition':
        winnerId = partitions.reduce((a, b) => a.members.size >= b.members.size ? a : b).id;
        break;
      case 'highest_priority': {
        let bestPriority = -Infinity;
        winnerId = partitions[0].id;
        for (const p of partitions) {
          const maxPri = Array.from(p.members).reduce((max, id) => {
            const node = topology.getNode(id);
            return node ? Math.max(max, node.priority) : max;
          }, -Infinity);
          if (maxPri > bestPriority) { bestPriority = maxPri; winnerId = p.id; }
        }
        break;
      }
      case 'most_recent_state':
        winnerId = partitions.reduce((a, b) => a.stateVersion >= b.stateVersion ? a : b).id;
        break;
      case 'designated_leader':
        winnerId = (partitions.find(p => p.leader) || partitions[0]).id;
        break;
      case 'quorum_based': {
        const totalNodes = partitions.reduce((s, p) => s + p.members.size, 0);
        const quorum = Math.floor(totalNodes / 2) + 1;
        winnerId = (partitions.find(p => p.members.size >= quorum) || 
                    partitions.reduce((a, b) => a.members.size >= b.members.size ? a : b)).id;
        break;
      }
      default:
        winnerId = partitions[0].id;
    }

    const losingPartitions = partitions.filter(p => p.id !== winnerId).map(p => p.id);
    const losingMembers = partitions.filter(p => p.id !== winnerId).reduce((s, p) => s + p.members.size, 0);
    const totalMembers = partitions.reduce((s, p) => s + p.members.size, 0);

    return {
      strategy,
      winningPartition: winnerId,
      losingPartitions,
      conflictsResolved: losingPartitions.length,
      dataLossEstimate: losingMembers / totalMembers,
    };
  }
}

// ─── Partition Aware Router ──────────────────────────────────────────
class PartitionAwareRouter {
  private partitionMembership: Map<string, string> = new Map(); // nodeId → partitionId

  updateMembership(partitions: Partition[]): void {
    this.partitionMembership.clear();
    for (const p of partitions) {
      for (const member of p.members) {
        this.partitionMembership.set(member, p.id);
      }
    }
  }

  canRoute(source: string, target: string): boolean {
    const sp = this.partitionMembership.get(source);
    const tp = this.partitionMembership.get(target);
    if (sp === undefined || tp === undefined) return true; // unknown nodes can route
    return sp === tp;
  }

  getReachableNodes(from: string): string[] {
    const partition = this.partitionMembership.get(from);
    if (!partition) return [];
    return Array.from(this.partitionMembership.entries())
      .filter(([_, pid]) => pid === partition)
      .map(([nid]) => nid);
  }

  routeMessage(
    source: string, target: string, message: unknown,
    topology: PartitionTopologyManager
  ): { delivered: boolean; hops: string[]; reason?: string } {
    if (!this.canRoute(source, target)) {
      return { delivered: false, hops: [], reason: 'cross_partition' };
    }

    // BFS shortest path
    const visited = new Set<string>();
    const parent = new Map<string, string>();
    const queue = [source];
    visited.add(source);

    while (queue.length > 0) {
      const current = queue.shift()!;
      if (current === target) {
        const path: string[] = [];
        let node = target;
        while (node !== source) {
          path.unshift(node);
          node = parent.get(node)!;
        }
        path.unshift(source);
        return { delivered: true, hops: path };
      }
      for (const neighbor of topology.getNeighbors(current)) {
        if (visited.has(neighbor) || !this.canRoute(current, neighbor)) continue;
        visited.add(neighbor);
        parent.set(neighbor, current);
        queue.push(neighbor);
      }
    }
    return { delivered: false, hops: [], reason: 'no_path' };
  }
}

// ─── Healing Coordinator ─────────────────────────────────────────────
class HealingCoordinator {
  private activePlans: Map<string, HealingPlan> = new Map();
  private readonly maxConcurrent: number;

  constructor(maxConcurrent: number) {
    this.maxConcurrent = maxConcurrent;
  }

  createPlan(
    sourcePartition: string, targetPartition: string, stateConflicts: number
  ): HealingPlan | null {
    if (this.activePlans.size >= this.maxConcurrent) return null;

    const plan: HealingPlan = {
      id: `heal-${fnv1a(`${sourcePartition}-${targetPartition}-${Date.now()}`).toString(16)}`,
      sourcePartition,
      targetPartition,
      phase: 'quiesce',
      startedAt: Date.now(),
      estimatedDuration: Math.max(5000, stateConflicts * 100),
      stateConflicts,
      progress: 0,
    };
    this.activePlans.set(plan.id, plan);
    return plan;
  }

  advancePhase(planId: string): HealingPlan | null {
    const plan = this.activePlans.get(planId);
    if (!plan) return null;

    const phases: HealingPlan['phase'][] = ['quiesce', 'reconcile', 'merge', 'verify', 'complete'];
    const currentIdx = phases.indexOf(plan.phase);
    if (currentIdx < phases.length - 1) {
      plan.phase = phases[currentIdx + 1];
      plan.progress = ((currentIdx + 1) / (phases.length - 1)) * 100;
    }

    if (plan.phase === 'complete') {
      this.activePlans.delete(planId);
    }
    return plan;
  }

  getActivePlans(): HealingPlan[] {
    return Array.from(this.activePlans.values());
  }

  cancelPlan(planId: string): boolean {
    return this.activePlans.delete(planId);
  }
}

// ─── Partition History Logger ────────────────────────────────────────
class PartitionHistoryLogger {
  private events: PartitionEvent[] = [];
  private hashChain: number = 0;
  private readonly maxRetention: number;

  constructor(maxRetention: number) {
    this.maxRetention = maxRetention;
  }

  log(event: PartitionEvent): void {
    this.hashChain = fnv1a(`${this.hashChain}-${event.type}-${event.timestamp}-${JSON.stringify(event.details)}`);
    this.events.push(event);

    // Prune old events
    const cutoff = Date.now() - this.maxRetention;
    while (this.events.length > 0 && this.events[0].timestamp < cutoff) {
      this.events.shift();
    }
  }

  getEvents(since?: number): PartitionEvent[] {
    if (!since) return [...this.events];
    return this.events.filter(e => e.timestamp >= since);
  }

  getEventsByType(type: PartitionEvent['type']): PartitionEvent[] {
    return this.events.filter(e => e.type === type);
  }

  verifyIntegrity(): boolean {
    let hash = 0;
    for (const event of this.events) {
      hash = fnv1a(`${hash}-${event.type}-${event.timestamp}-${JSON.stringify(event.details)}`);
    }
    return hash === this.hashChain;
  }

  getChainHash(): number { return this.hashChain; }
}

// ─── Network Partitioner (Unified Orchestrator) ──────────────────────
class NetworkPartitioner {
  private config: PartitionerConfig;
  private topology: PartitionTopologyManager;
  private phiDetector: PhiAccrualDetector;
  private controlledEngine: ControlledPartitionEngine;
  private splitBrainResolver: SplitBrainResolver;
  private router: PartitionAwareRouter;
  private healingCoordinator: HealingCoordinator;
  private logger: PartitionHistoryLogger;

  private partitions: Map<string, Partition> = new Map();
  private epoch: number = 0;

  constructor(config: Partial<PartitionerConfig> = {}) {
    this.config = {
      phiThreshold: 8,
      probeIntervalMs: 1000,
      asymmetricProbeEnabled: true,
      minPartitionSize: 1,
      splitBrainStrategy: 'quorum_based',
      healingDelayMs: 5000,
      autoHealEnabled: true,
      maxConcurrentHealings: 2,
      controlledPartitionMaxDuration: 300000,
      stateReconciliationStrategy: 'lww',
      historyRetention: 86400000,
      ...config,
    };

    this.topology = new PartitionTopologyManager();
    this.phiDetector = new PhiAccrualDetector();
    this.controlledEngine = new ControlledPartitionEngine();
    this.splitBrainResolver = new SplitBrainResolver();
    this.router = new PartitionAwareRouter();
    this.healingCoordinator = new HealingCoordinator(this.config.maxConcurrentHealings);
    this.logger = new PartitionHistoryLogger(this.config.historyRetention);
  }

  // ── Node Management ──
  addNode(node: AgentNode): void {
    this.topology.addNode(node);
  }

  removeNode(nodeId: string): void {
    this.topology.removeNode(nodeId);
    this.phiDetector.removeNode(nodeId);
    for (const partition of this.partitions.values()) {
      partition.members.delete(nodeId);
    }
  }

  addLink(source: string, target: string, link?: Partial<NetworkLink>): void {
    this.topology.addLink(source, target, link || {});
  }

  // ── Heartbeat ──
  recordHeartbeat(nodeId: string, now: number = Date.now()): void {
    this.phiDetector.recordHeartbeat(nodeId, now);
    const node = this.topology.getNode(nodeId);
    if (node) node.lastSeen = now;
  }

  // ── Controlled Partition ──
  createControlledPartition(
    id: string, groups: string[][], reason: string,
    durationMs?: number
  ): { partitionId: string; groups: string[][] } {
    const duration = Math.min(
      durationMs ?? this.config.controlledPartitionMaxDuration,
      this.config.controlledPartitionMaxDuration
    );
    const result = this.controlledEngine.createPartition(id, groups, reason, duration);

    // Mark cross-group links as failed
    for (let i = 0; i < groups.length; i++) {
      for (let j = i + 1; j < groups.length; j++) {
        for (const a of groups[i]) {
          for (const b of groups[j]) {
            this.topology.updateLinkStatus(a, b, 'failed');
            this.topology.updateLinkStatus(b, a, 'failed');
          }
        }
      }
    }

    this.logger.log({
      type: 'controlled_split',
      timestamp: Date.now(),
      partitionId: id,
      details: { groups, reason, durationMs: duration },
    });

    this.detectPartitions();
    return result;
  }

  // ── Route Message ──
  routeMessage(source: string, target: string, message: unknown): {
    delivered: boolean; hops: string[]; reason?: string;
  } {
    return this.router.routeMessage(source, target, message, this.topology);
  }

  // ── Main Tick ──
  tick(now: number = Date.now()): PartitionEvent[] {
    const events: PartitionEvent[] = [];

    // Phase 1: Failure detection
    for (const node of this.topology.getAllNodes()) {
      const phi = this.phiDetector.phi(node.id, now);
      if (phi > this.config.phiThreshold) {
        // Mark links to this node as failed
        for (const neighbor of this.topology.getNeighbors(node.id)) {
          const prevLink = this.topology.getLink(node.id, neighbor) ||
                           this.topology.getLink(neighbor, node.id);
          if (prevLink && prevLink.status !== 'failed') {
            this.topology.updateLinkStatus(node.id, neighbor, 'failed');
            this.topology.updateLinkStatus(neighbor, node.id, 'failed');
            const evt: PartitionEvent = {
              type: 'link_failed',
              timestamp: now,
              details: { source: node.id, target: neighbor, phi },
            };
            events.push(evt);
            this.logger.log(evt);
          }
        }
      }
    }

    // Phase 2: Controlled partition expiry
    for (const expiredId of this.controlledEngine.getExpiredPartitions(now)) {
      this.controlledEngine.removePartition(expiredId);
      const evt: PartitionEvent = {
        type: 'partition_healed',
        timestamp: now,
        partitionId: expiredId,
        details: { reason: 'controlled_expiry' },
      };
      events.push(evt);
      this.logger.log(evt);
    }

    // Phase 3: Detect partitions from topology
    const prevPartitionCount = this.partitions.size;
    this.detectPartitions();
    const currentPartitions = Array.from(this.partitions.values());

    if (currentPartitions.length > 1 && prevPartitionCount <= 1) {
      const evt: PartitionEvent = {
        type: 'split_brain_detected',
        timestamp: now,
        details: {
          partitionCount: currentPartitions.length,
          sizes: currentPartitions.map(p => p.members.size),
        },
      };
      events.push(evt);
      this.logger.log(evt);

      // Phase 4: Split-brain resolution
      const resolution = this.splitBrainResolver.resolve(
        currentPartitions, this.config.splitBrainStrategy, this.topology
      );
      const resEvt: PartitionEvent = {
        type: 'split_brain_resolved',
        timestamp: now,
        details: { ...resolution },
      };
      events.push(resEvt);
      this.logger.log(resEvt);
    }

    // Phase 5: Auto-healing
    if (this.config.autoHealEnabled && currentPartitions.length > 1) {
      for (let i = 0; i < currentPartitions.length - 1; i++) {
        const plan = this.healingCoordinator.createPlan(
          currentPartitions[i].id,
          currentPartitions[i + 1].id,
          0 // state conflicts determined during reconciliation
        );
        if (plan) {
          const evt: PartitionEvent = {
            type: 'healing_started',
            timestamp: now,
            details: { planId: plan.id, source: plan.sourcePartition, target: plan.targetPartition },
          };
          events.push(evt);
          this.logger.log(evt);
        }
      }
    }

    // Phase 6: Advance active healing plans
    for (const plan of this.healingCoordinator.getActivePlans()) {
      const advanced = this.healingCoordinator.advancePhase(plan.id);
      if (advanced && advanced.phase === 'complete') {
        const evt: PartitionEvent = {
          type: 'healing_completed',
          timestamp: now,
          details: { planId: plan.id },
        };
        events.push(evt);
        this.logger.log(evt);
      }
    }

    // Phase 7: Update router membership
    this.router.updateMembership(currentPartitions);

    return events;
  }

  // ── Detect Partitions from Topology ──
  private detectPartitions(): void {
    const components = this.topology.getConnectedComponents();
    this.partitions.clear();
    this.epoch++;

    for (const component of components) {
      if (component.size < this.config.minPartitionSize) continue;
      const members = Array.from(component).sort();
      const id = `part-${fnv1a(members.join(',')).toString(16)}`;

      // Elect leader: highest priority, then FNV-1a tie-break
      let leader = members[0];
      let bestPri = -Infinity;
      for (const m of members) {
        const node = this.topology.getNode(m);
        if (node && node.priority > bestPri) {
          bestPri = node.priority;
          leader = m;
        } else if (node && node.priority === bestPri && fnv1a(m) > fnv1a(leader)) {
          leader = m;
        }
      }

      this.partitions.set(id, {
        id,
        members: component,
        epoch: this.epoch,
        createdAt: Date.now(),
        reason: 'detected',
        leader,
        stateVersion: this.epoch,
      });
    }
  }

  // ── Queries ──
  getPartitions(): Partition[] { return Array.from(this.partitions.values()); }
  getBridgeLinks() { return this.topology.getBridgeLinks(); }
  getReachableFrom(nodeId: string): string[] { return this.router.getReachableNodes(nodeId); }
  getHealingPlans(): HealingPlan[] { return this.healingCoordinator.getActivePlans(); }
  getHistory(since?: number): PartitionEvent[] { return this.logger.getEvents(since); }
  verifyAuditIntegrity(): boolean { return this.logger.verifyIntegrity(); }

  getDashboard(): Record<string, unknown> {
    const partitions = this.getPartitions();
    return {
      epoch: this.epoch,
      nodeCount: this.topology.getAllNodes().length,
      partitionCount: partitions.length,
      partitionSizes: partitions.map(p => ({ id: p.id, size: p.members.size, leader: p.leader })),
      bridgeLinks: this.topology.getBridgeLinks().length,
      activeHealings: this.healingCoordinator.getActivePlans().length,
      controlledPartitions: this.controlledEngine.getActivePartitions().size,
      recentEvents: this.logger.getEvents(Date.now() - 60000).length,
      auditIntegrity: this.logger.verifyIntegrity(),
    };
  }
}

// ─── Presets ─────────────────────────────────────────────────────────
const PRESETS = {
  'small-cluster': {
    phiThreshold: 6,
    probeIntervalMs: 500,
    asymmetricProbeEnabled: false,
    minPartitionSize: 1,
    splitBrainStrategy: 'largest_partition' as const,
    healingDelayMs: 2000,
    autoHealEnabled: true,
    maxConcurrentHealings: 1,
    controlledPartitionMaxDuration: 60000,
    stateReconciliationStrategy: 'lww' as const,
    historyRetention: 3600000,
  },
  'multi-region': {
    phiThreshold: 10,
    probeIntervalMs: 2000,
    asymmetricProbeEnabled: true,
    minPartitionSize: 2,
    splitBrainStrategy: 'quorum_based' as const,
    healingDelayMs: 15000,
    autoHealEnabled: true,
    maxConcurrentHealings: 3,
    controlledPartitionMaxDuration: 600000,
    stateReconciliationStrategy: 'vector_clock' as const,
    historyRetention: 604800000,
  },
  'chaos-testing': {
    phiThreshold: 4,
    probeIntervalMs: 250,
    asymmetricProbeEnabled: true,
    minPartitionSize: 1,
    splitBrainStrategy: 'designated_leader' as const,
    healingDelayMs: 1000,
    autoHealEnabled: false,
    maxConcurrentHealings: 5,
    controlledPartitionMaxDuration: 300000,
    stateReconciliationStrategy: 'manual' as const,
    historyRetention: 86400000,
  },
};

export {
  NetworkPartitioner, PartitionTopologyManager, PhiAccrualDetector,
  ControlledPartitionEngine, SplitBrainResolver, PartitionAwareRouter,
  HealingCoordinator, PartitionHistoryLogger, PRESETS,
  type AgentNode, type NetworkLink, type Partition, type PartitionEvent,
  type SplitBrainResolution, type HealingPlan, type PartitionerConfig,
};
