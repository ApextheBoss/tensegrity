/**
 * Contract Upgrade Proxy for Agent Protocol Evolution
 * 
 * Transparent proxy pattern enabling live protocol upgrades without breaking
 * existing agent integrations. Implements storage-slot separation, migration
 * pipelines, and rollback safety for evolving agent coordination contracts.
 * 
 * Key patterns:
 * - Transparent proxy with admin/user call routing
 * - Diamond pattern for modular facet upgrades  
 * - Storage slot collision prevention with EIP-1967 style layout
 * - Migration pipeline with pre/post hooks and invariant checks
 * - Timelock governance for upgrade proposals
 * - Emergency rollback with state snapshot verification
 */

// ─── Storage Layout Manager ────────────────────────────────────────────────
// EIP-1967 inspired deterministic storage slots to prevent collision

interface StorageSlot {
  name: string;
  slot: string; // hex slot position
  type: 'address' | 'uint256' | 'bytes32' | 'bool' | 'mapping' | 'array';
  version: number; // version when introduced
  deprecated?: number; // version when deprecated
}

interface StorageLayout {
  version: number;
  slots: StorageSlot[];
  mappings: Map<string, { keySlot: string; valueType: string }>;
}

class StorageLayoutManager {
  private layouts: Map<number, StorageLayout> = new Map();
  private currentVersion: number = 0;

  registerLayout(layout: StorageLayout): void {
    // Validate no slot collisions with previous versions
    const conflicts = this.detectCollisions(layout);
    if (conflicts.length > 0) {
      throw new Error(
        `Storage slot collisions detected: ${conflicts.map(c => 
          `${c.name} collides with ${c.collidesWith} at slot ${c.slot}`
        ).join(', ')}`
      );
    }
    this.layouts.set(layout.version, layout);
    if (layout.version > this.currentVersion) {
      this.currentVersion = layout.version;
    }
  }

  private detectCollisions(
    newLayout: StorageLayout
  ): Array<{ name: string; slot: string; collidesWith: string }> {
    const conflicts: Array<{ name: string; slot: string; collidesWith: string }> = [];
    const occupiedSlots = new Map<string, string>();

    // Collect all slots from previous versions (non-deprecated)
    for (const [ver, layout] of this.layouts) {
      if (ver >= newLayout.version) continue;
      for (const slot of layout.slots) {
        if (slot.deprecated && slot.deprecated <= newLayout.version) continue;
        occupiedSlots.set(slot.slot, slot.name);
      }
    }

    // Check new slots against occupied
    for (const slot of newLayout.slots) {
      const existing = occupiedSlots.get(slot.slot);
      if (existing && existing !== slot.name) {
        conflicts.push({ name: slot.name, slot: slot.slot, collidesWith: existing });
      }
    }

    return conflicts;
  }

  computeSlot(namespace: string, name: string): string {
    // Deterministic slot computation: keccak256(namespace.name) - 1
    // Using FNV-1a as proxy for keccak256 in this simulation
    let hash = 0x811c9dc5;
    const input = `${namespace}.${name}`;
    for (let i = 0; i < input.length; i++) {
      hash ^= input.charCodeAt(i);
      hash = (hash * 0x01000193) >>> 0;
    }
    return '0x' + hash.toString(16).padStart(64, '0');
  }

  getMigrationPath(fromVersion: number, toVersion: number): StorageSlot[] {
    const newSlots: StorageSlot[] = [];
    const deprecatedSlots: StorageSlot[] = [];

    for (let v = fromVersion + 1; v <= toVersion; v++) {
      const layout = this.layouts.get(v);
      if (!layout) continue;
      for (const slot of layout.slots) {
        if (slot.version === v) newSlots.push(slot);
        if (slot.deprecated === v) deprecatedSlots.push(slot);
      }
    }

    return [...newSlots, ...deprecatedSlots];
  }

  getLayout(version: number): StorageLayout | undefined {
    return this.layouts.get(version);
  }

  getCurrentVersion(): number {
    return this.currentVersion;
  }
}

// ─── Implementation Registry ───────────────────────────────────────────────
// Tracks all deployed implementation versions

interface ImplementationRecord {
  version: number;
  address: string; // simulated contract address
  deployedAt: number;
  codeHash: string;
  abi: string[];
  deprecated: boolean;
  deprecatedAt?: number;
}

class ImplementationRegistry {
  private implementations: Map<number, ImplementationRecord> = new Map();
  private addressIndex: Map<string, number> = new Map();

  register(record: ImplementationRecord): void {
    if (this.implementations.has(record.version)) {
      throw new Error(`Implementation version ${record.version} already registered`);
    }
    this.implementations.set(record.version, { ...record });
    this.addressIndex.set(record.address, record.version);
  }

  get(version: number): ImplementationRecord | undefined {
    return this.implementations.get(version);
  }

  getByAddress(address: string): ImplementationRecord | undefined {
    const version = this.addressIndex.get(address);
    return version !== undefined ? this.implementations.get(version) : undefined;
  }

  deprecate(version: number, timestamp: number): void {
    const impl = this.implementations.get(version);
    if (impl) {
      impl.deprecated = true;
      impl.deprecatedAt = timestamp;
    }
  }

  getLatestActive(): ImplementationRecord | undefined {
    let latest: ImplementationRecord | undefined;
    for (const impl of this.implementations.values()) {
      if (!impl.deprecated && (!latest || impl.version > latest.version)) {
        latest = impl;
      }
    }
    return latest;
  }

  getAll(): ImplementationRecord[] {
    return Array.from(this.implementations.values())
      .sort((a, b) => b.version - a.version);
  }
}

// ─── Facet Manager (Diamond Pattern) ───────────────────────────────────────
// Modular function routing for granular upgrades

interface Facet {
  name: string;
  version: number;
  address: string;
  selectors: string[]; // function selectors this facet handles
  dependencies: string[]; // other facet names this depends on
}

type FacetAction = 'add' | 'replace' | 'remove';

interface FacetCut {
  action: FacetAction;
  facetName: string;
  facetAddress: string;
  selectors: string[];
}

class FacetManager {
  private facets: Map<string, Facet> = new Map();
  private selectorToFacet: Map<string, string> = new Map();
  private cutHistory: Array<{ cuts: FacetCut[]; timestamp: number; version: number }> = [];
  private currentVersion: number = 0;

  executeCut(cuts: FacetCut[], timestamp: number): {
    applied: FacetCut[];
    conflicts: Array<{ selector: string; existingFacet: string; newFacet: string }>;
  } {
    const conflicts: Array<{ selector: string; existingFacet: string; newFacet: string }> = [];
    const applied: FacetCut[] = [];

    // Validate all cuts before applying
    for (const cut of cuts) {
      if (cut.action === 'add') {
        for (const selector of cut.selectors) {
          const existing = this.selectorToFacet.get(selector);
          if (existing && existing !== cut.facetName) {
            conflicts.push({
              selector,
              existingFacet: existing,
              newFacet: cut.facetName,
            });
          }
        }
      }
    }

    if (conflicts.length > 0) return { applied, conflicts };

    // Apply cuts
    for (const cut of cuts) {
      switch (cut.action) {
        case 'add':
        case 'replace': {
          const existing = this.facets.get(cut.facetName);
          const facet: Facet = {
            name: cut.facetName,
            version: (existing?.version || 0) + 1,
            address: cut.facetAddress,
            selectors: cut.selectors,
            dependencies: existing?.dependencies || [],
          };

          // Remove old selector mappings
          if (existing) {
            for (const sel of existing.selectors) {
              this.selectorToFacet.delete(sel);
            }
          }

          // Add new mappings
          for (const sel of cut.selectors) {
            this.selectorToFacet.set(sel, cut.facetName);
          }

          this.facets.set(cut.facetName, facet);
          applied.push(cut);
          break;
        }
        case 'remove': {
          const facet = this.facets.get(cut.facetName);
          if (facet) {
            // Check no other facets depend on this one
            const dependents = this.getDependents(cut.facetName);
            if (dependents.length === 0) {
              for (const sel of facet.selectors) {
                this.selectorToFacet.delete(sel);
              }
              this.facets.delete(cut.facetName);
              applied.push(cut);
            }
          }
          break;
        }
      }
    }

    this.currentVersion++;
    this.cutHistory.push({ cuts: applied, timestamp, version: this.currentVersion });
    return { applied, conflicts };
  }

  private getDependents(facetName: string): string[] {
    const dependents: string[] = [];
    for (const [name, facet] of this.facets) {
      if (facet.dependencies.includes(facetName)) {
        dependents.push(name);
      }
    }
    return dependents;
  }

  routeSelector(selector: string): Facet | undefined {
    const facetName = this.selectorToFacet.get(selector);
    return facetName ? this.facets.get(facetName) : undefined;
  }

  getFacet(name: string): Facet | undefined {
    return this.facets.get(name);
  }

  getAllFacets(): Facet[] {
    return Array.from(this.facets.values());
  }

  getCutHistory(): Array<{ cuts: FacetCut[]; timestamp: number; version: number }> {
    return [...this.cutHistory];
  }
}

// ─── Migration Pipeline ────────────────────────────────────────────────────
// Structured state migration between implementation versions

interface MigrationStep {
  name: string;
  fromVersion: number;
  toVersion: number;
  preCheck: () => { ok: boolean; reason?: string };
  migrate: (state: Map<string, unknown>) => Map<string, unknown>;
  postCheck: (state: Map<string, unknown>) => { ok: boolean; reason?: string };
  rollback: (state: Map<string, unknown>) => Map<string, unknown>;
}

interface MigrationResult {
  success: boolean;
  fromVersion: number;
  toVersion: number;
  stepsCompleted: number;
  stepsTotal: number;
  failedStep?: string;
  failureReason?: string;
  rolledBack: boolean;
  duration: number;
  stateSnapshot: Map<string, unknown>;
}

class MigrationPipeline {
  private steps: MigrationStep[] = [];
  private completedMigrations: MigrationResult[] = [];

  registerStep(step: MigrationStep): void {
    // Insert in order
    const idx = this.steps.findIndex(
      s => s.fromVersion > step.fromVersion ||
        (s.fromVersion === step.fromVersion && s.toVersion > step.toVersion)
    );
    if (idx === -1) {
      this.steps.push(step);
    } else {
      this.steps.splice(idx, 0, step);
    }
  }

  getPath(fromVersion: number, toVersion: number): MigrationStep[] {
    // Find migration path using BFS
    const graph = new Map<number, Array<{ step: MigrationStep; to: number }>>();
    for (const step of this.steps) {
      if (!graph.has(step.fromVersion)) graph.set(step.fromVersion, []);
      graph.get(step.fromVersion)!.push({ step, to: step.toVersion });
    }

    const queue: Array<{ version: number; path: MigrationStep[] }> = [
      { version: fromVersion, path: [] },
    ];
    const visited = new Set<number>();

    while (queue.length > 0) {
      const { version, path } = queue.shift()!;
      if (version === toVersion) return path;
      if (visited.has(version)) continue;
      visited.add(version);

      const edges = graph.get(version) || [];
      for (const { step, to } of edges) {
        if (!visited.has(to)) {
          queue.push({ version: to, path: [...path, step] });
        }
      }
    }

    return []; // No path found
  }

  async execute(
    fromVersion: number,
    toVersion: number,
    initialState: Map<string, unknown>
  ): Promise<MigrationResult> {
    const startTime = Date.now();
    const path = this.getPath(fromVersion, toVersion);

    if (path.length === 0) {
      return {
        success: false,
        fromVersion,
        toVersion,
        stepsCompleted: 0,
        stepsTotal: 0,
        failureReason: `No migration path from v${fromVersion} to v${toVersion}`,
        rolledBack: false,
        duration: Date.now() - startTime,
        stateSnapshot: new Map(initialState),
      };
    }

    let state = new Map(initialState);
    const snapshots: Map<string, unknown>[] = [new Map(state)];
    let stepsCompleted = 0;

    for (const step of path) {
      // Pre-check
      const preResult = step.preCheck();
      if (!preResult.ok) {
        // Rollback completed steps
        const rolledBackState = this.rollbackSteps(path, stepsCompleted, snapshots);
        const result: MigrationResult = {
          success: false,
          fromVersion,
          toVersion,
          stepsCompleted,
          stepsTotal: path.length,
          failedStep: step.name,
          failureReason: `Pre-check failed: ${preResult.reason}`,
          rolledBack: true,
          duration: Date.now() - startTime,
          stateSnapshot: rolledBackState,
        };
        this.completedMigrations.push(result);
        return result;
      }

      // Execute migration
      try {
        state = step.migrate(state);
      } catch (err) {
        const rolledBackState = this.rollbackSteps(path, stepsCompleted, snapshots);
        const result: MigrationResult = {
          success: false,
          fromVersion,
          toVersion,
          stepsCompleted,
          stepsTotal: path.length,
          failedStep: step.name,
          failureReason: `Migration error: ${(err as Error).message}`,
          rolledBack: true,
          duration: Date.now() - startTime,
          stateSnapshot: rolledBackState,
        };
        this.completedMigrations.push(result);
        return result;
      }

      // Post-check
      const postResult = step.postCheck(state);
      if (!postResult.ok) {
        const rolledBackState = this.rollbackSteps(path, stepsCompleted, snapshots);
        const result: MigrationResult = {
          success: false,
          fromVersion,
          toVersion,
          stepsCompleted,
          stepsTotal: path.length,
          failedStep: step.name,
          failureReason: `Post-check failed: ${postResult.reason}`,
          rolledBack: true,
          duration: Date.now() - startTime,
          stateSnapshot: rolledBackState,
        };
        this.completedMigrations.push(result);
        return result;
      }

      stepsCompleted++;
      snapshots.push(new Map(state));
    }

    const result: MigrationResult = {
      success: true,
      fromVersion,
      toVersion,
      stepsCompleted,
      stepsTotal: path.length,
      rolledBack: false,
      duration: Date.now() - startTime,
      stateSnapshot: state,
    };
    this.completedMigrations.push(result);
    return result;
  }

  private rollbackSteps(
    path: MigrationStep[],
    completedCount: number,
    snapshots: Map<string, unknown>[]
  ): Map<string, unknown> {
    // Reverse-order rollback
    let state = snapshots[completedCount] || new Map();
    for (let i = completedCount - 1; i >= 0; i--) {
      try {
        state = path[i].rollback(state);
      } catch {
        // If rollback fails, return the snapshot from before this step
        state = snapshots[i];
      }
    }
    return state;
  }

  getHistory(): MigrationResult[] {
    return [...this.completedMigrations];
  }
}

// ─── Timelock Governor ─────────────────────────────────────────────────────
// Time-delayed upgrade proposals with voting

interface UpgradeProposal {
  id: string;
  proposer: string;
  description: string;
  targetVersion: number;
  implementationAddress: string;
  facetCuts?: FacetCut[];
  createdAt: number;
  executionDelay: number; // ms before execution allowed
  executableAfter: number;
  expiresAt: number;
  status: 'pending' | 'approved' | 'executed' | 'cancelled' | 'expired';
  votes: Map<string, { support: boolean; weight: number; timestamp: number }>;
  quorum: number; // minimum total weight needed
  approvalThreshold: number; // fraction of votes that must be 'support'
}

class TimelockGovernor {
  private proposals: Map<string, UpgradeProposal> = new Map();
  private minDelay: number;
  private maxDelay: number;
  private proposalExpiry: number;
  private defaultQuorum: number;
  private defaultThreshold: number;

  constructor(config: {
    minDelay: number;
    maxDelay: number;
    proposalExpiry: number;
    defaultQuorum: number;
    defaultThreshold: number;
  }) {
    this.minDelay = config.minDelay;
    this.maxDelay = config.maxDelay;
    this.proposalExpiry = config.proposalExpiry;
    this.defaultQuorum = config.defaultQuorum;
    this.defaultThreshold = config.defaultThreshold;
  }

  propose(params: {
    proposer: string;
    description: string;
    targetVersion: number;
    implementationAddress: string;
    facetCuts?: FacetCut[];
    delay?: number;
    quorum?: number;
    threshold?: number;
  }, now: number): UpgradeProposal {
    const delay = Math.max(
      this.minDelay,
      Math.min(this.maxDelay, params.delay || this.minDelay)
    );

    // FNV-1a proposal ID
    let hash = 0x811c9dc5;
    const input = `${params.proposer}:${params.targetVersion}:${now}`;
    for (let i = 0; i < input.length; i++) {
      hash ^= input.charCodeAt(i);
      hash = (hash * 0x01000193) >>> 0;
    }

    const proposal: UpgradeProposal = {
      id: 'prop-' + hash.toString(16),
      proposer: params.proposer,
      description: params.description,
      targetVersion: params.targetVersion,
      implementationAddress: params.implementationAddress,
      facetCuts: params.facetCuts,
      createdAt: now,
      executionDelay: delay,
      executableAfter: now + delay,
      expiresAt: now + delay + this.proposalExpiry,
      status: 'pending',
      votes: new Map(),
      quorum: params.quorum || this.defaultQuorum,
      approvalThreshold: params.threshold || this.defaultThreshold,
    };

    this.proposals.set(proposal.id, proposal);
    return proposal;
  }

  vote(proposalId: string, voter: string, support: boolean, weight: number, now: number): boolean {
    const proposal = this.proposals.get(proposalId);
    if (!proposal || proposal.status !== 'pending') return false;
    if (now >= proposal.executableAfter) return false; // voting period ended

    proposal.votes.set(voter, { support, weight, timestamp: now });

    // Check if quorum reached and threshold met
    let totalWeight = 0;
    let supportWeight = 0;
    for (const vote of proposal.votes.values()) {
      totalWeight += vote.weight;
      if (vote.support) supportWeight += vote.weight;
    }

    if (totalWeight >= proposal.quorum) {
      const ratio = supportWeight / totalWeight;
      if (ratio >= proposal.approvalThreshold) {
        proposal.status = 'approved';
      }
    }

    return true;
  }

  canExecute(proposalId: string, now: number): { executable: boolean; reason?: string } {
    const proposal = this.proposals.get(proposalId);
    if (!proposal) return { executable: false, reason: 'Proposal not found' };
    if (proposal.status === 'executed') return { executable: false, reason: 'Already executed' };
    if (proposal.status === 'cancelled') return { executable: false, reason: 'Cancelled' };
    if (proposal.status !== 'approved') return { executable: false, reason: 'Not yet approved' };
    if (now < proposal.executableAfter) {
      return { executable: false, reason: `Timelock active, executable after ${proposal.executableAfter}` };
    }
    if (now > proposal.expiresAt) {
      proposal.status = 'expired';
      return { executable: false, reason: 'Proposal expired' };
    }
    return { executable: true };
  }

  markExecuted(proposalId: string): void {
    const proposal = this.proposals.get(proposalId);
    if (proposal) proposal.status = 'executed';
  }

  cancel(proposalId: string, canceller: string): boolean {
    const proposal = this.proposals.get(proposalId);
    if (!proposal || proposal.status !== 'pending') return false;
    if (canceller !== proposal.proposer) return false;
    proposal.status = 'cancelled';
    return true;
  }

  getProposal(id: string): UpgradeProposal | undefined {
    return this.proposals.get(id);
  }

  getActiveProposals(): UpgradeProposal[] {
    return Array.from(this.proposals.values())
      .filter(p => p.status === 'pending' || p.status === 'approved');
  }
}

// ─── State Snapshot Manager ────────────────────────────────────────────────
// Pre-upgrade state capture for rollback safety

interface StateSnapshot {
  id: string;
  version: number;
  timestamp: number;
  state: Map<string, unknown>;
  stateHash: string;
  parentId?: string;
  metadata: Record<string, string>;
}

class StateSnapshotManager {
  private snapshots: Map<string, StateSnapshot> = new Map();
  private versionToSnapshot: Map<number, string> = new Map();
  private maxSnapshots: number;

  constructor(maxSnapshots: number = 10) {
    this.maxSnapshots = maxSnapshots;
  }

  capture(
    version: number,
    state: Map<string, unknown>,
    metadata: Record<string, string> = {},
    now: number
  ): StateSnapshot {
    const stateHash = this.computeHash(state);
    const parentId = this.versionToSnapshot.get(version - 1);

    // FNV-1a snapshot ID
    let hash = 0x811c9dc5;
    const input = `snap:${version}:${now}:${stateHash}`;
    for (let i = 0; i < input.length; i++) {
      hash ^= input.charCodeAt(i);
      hash = (hash * 0x01000193) >>> 0;
    }

    const snapshot: StateSnapshot = {
      id: 'snap-' + hash.toString(16),
      version,
      timestamp: now,
      state: new Map(state),
      stateHash,
      parentId,
      metadata,
    };

    this.snapshots.set(snapshot.id, snapshot);
    this.versionToSnapshot.set(version, snapshot.id);

    // Evict oldest if over limit
    if (this.snapshots.size > this.maxSnapshots) {
      const oldest = Array.from(this.snapshots.values())
        .sort((a, b) => a.timestamp - b.timestamp)[0];
      this.snapshots.delete(oldest.id);
    }

    return snapshot;
  }

  private computeHash(state: Map<string, unknown>): string {
    let hash = 0x811c9dc5;
    const sorted = Array.from(state.entries()).sort(([a], [b]) => a.localeCompare(b));
    for (const [key, value] of sorted) {
      const str = `${key}:${JSON.stringify(value)}`;
      for (let i = 0; i < str.length; i++) {
        hash ^= str.charCodeAt(i);
        hash = (hash * 0x01000193) >>> 0;
      }
    }
    return hash.toString(16);
  }

  getSnapshot(id: string): StateSnapshot | undefined {
    return this.snapshots.get(id);
  }

  getByVersion(version: number): StateSnapshot | undefined {
    const id = this.versionToSnapshot.get(version);
    return id ? this.snapshots.get(id) : undefined;
  }

  verify(snapshotId: string): boolean {
    const snapshot = this.snapshots.get(snapshotId);
    if (!snapshot) return false;
    const currentHash = this.computeHash(snapshot.state);
    return currentHash === snapshot.stateHash;
  }

  getChain(fromVersion: number): StateSnapshot[] {
    const chain: StateSnapshot[] = [];
    let currentId = this.versionToSnapshot.get(fromVersion);
    while (currentId) {
      const snap = this.snapshots.get(currentId);
      if (!snap) break;
      chain.unshift(snap);
      currentId = snap.parentId;
    }
    return chain;
  }
}

// ─── Compatibility Checker ─────────────────────────────────────────────────
// Validates upgrade compatibility between versions

interface CompatibilityReport {
  compatible: boolean;
  breakingChanges: Array<{
    type: 'selector-removed' | 'selector-changed' | 'storage-collision' | 'dependency-missing';
    description: string;
    severity: 'error' | 'warning';
  }>;
  newFeatures: string[];
  deprecations: string[];
  migrationRequired: boolean;
}

class CompatibilityChecker {
  checkFacetCompatibility(
    currentFacets: Facet[],
    proposedCuts: FacetCut[]
  ): CompatibilityReport {
    const report: CompatibilityReport = {
      compatible: true,
      breakingChanges: [],
      newFeatures: [],
      deprecations: [],
      migrationRequired: false,
    };

    const currentSelectors = new Map<string, string>();
    for (const facet of currentFacets) {
      for (const sel of facet.selectors) {
        currentSelectors.set(sel, facet.name);
      }
    }

    for (const cut of proposedCuts) {
      switch (cut.action) {
        case 'remove': {
          // Removing selectors is a breaking change
          const facet = currentFacets.find(f => f.name === cut.facetName);
          if (facet) {
            for (const sel of facet.selectors) {
              report.breakingChanges.push({
                type: 'selector-removed',
                description: `Selector ${sel} from facet ${cut.facetName} will be removed`,
                severity: 'error',
              });
            }
            report.deprecations.push(cut.facetName);
          }
          break;
        }
        case 'add': {
          for (const sel of cut.selectors) {
            if (currentSelectors.has(sel)) {
              report.breakingChanges.push({
                type: 'selector-changed',
                description: `Selector ${sel} already exists in ${currentSelectors.get(sel)}`,
                severity: 'error',
              });
            } else {
              report.newFeatures.push(`${cut.facetName}:${sel}`);
            }
          }
          break;
        }
        case 'replace': {
          report.migrationRequired = true;
          report.newFeatures.push(`${cut.facetName} (upgraded)`);
          break;
        }
      }
    }

    report.compatible = report.breakingChanges.filter(c => c.severity === 'error').length === 0;
    return report;
  }

  checkStorageCompatibility(
    currentLayout: StorageLayout,
    proposedLayout: StorageLayout
  ): CompatibilityReport {
    const report: CompatibilityReport = {
      compatible: true,
      breakingChanges: [],
      newFeatures: [],
      deprecations: [],
      migrationRequired: false,
    };

    const currentSlots = new Map(currentLayout.slots.map(s => [s.slot, s]));

    for (const slot of proposedLayout.slots) {
      const existing = currentSlots.get(slot.slot);
      if (existing && existing.name !== slot.name) {
        report.breakingChanges.push({
          type: 'storage-collision',
          description: `Slot ${slot.slot}: ${slot.name} collides with existing ${existing.name}`,
          severity: 'error',
        });
      } else if (existing && existing.type !== slot.type) {
        report.breakingChanges.push({
          type: 'storage-collision',
          description: `Slot ${slot.slot}: type changed from ${existing.type} to ${slot.type}`,
          severity: 'error',
        });
        report.migrationRequired = true;
      } else if (!existing) {
        report.newFeatures.push(`${slot.name} (${slot.type})`);
      }
    }

    // Check for removed slots
    for (const [slotPos, slot] of currentSlots) {
      if (!proposedLayout.slots.find(s => s.slot === slotPos)) {
        report.deprecations.push(slot.name);
      }
    }

    report.compatible = report.breakingChanges.filter(c => c.severity === 'error').length === 0;
    return report;
  }
}

// ─── Emergency Rollback Controller ─────────────────────────────────────────
// Fast rollback to previous version when upgrade causes issues

interface RollbackTrigger {
  type: 'manual' | 'health-check' | 'error-rate' | 'timeout';
  threshold?: number;
  description: string;
}

interface RollbackPlan {
  currentVersion: number;
  targetVersion: number;
  snapshotId: string;
  facetRestoration: FacetCut[];
  estimatedDowntime: number;
  triggers: RollbackTrigger[];
}

class EmergencyRollbackController {
  private plans: Map<number, RollbackPlan> = new Map();
  private errorRates: Map<number, { errors: number; total: number; window: number[] }> = new Map();
  private healthCheckResults: Map<number, { passed: number; failed: number }> = new Map();
  private rollbackCooldown: number;
  private lastRollback: number = 0;

  constructor(cooldownMs: number = 300000) {
    this.rollbackCooldown = cooldownMs;
  }

  registerPlan(plan: RollbackPlan): void {
    this.plans.set(plan.currentVersion, plan);
  }

  recordError(version: number, now: number): void {
    if (!this.errorRates.has(version)) {
      this.errorRates.set(version, { errors: 0, total: 0, window: [] });
    }
    const rates = this.errorRates.get(version)!;
    rates.errors++;
    rates.total++;
    rates.window.push(now);

    // Sliding window: keep last 60 seconds
    const cutoff = now - 60000;
    rates.window = rates.window.filter(t => t > cutoff);
  }

  recordSuccess(version: number): void {
    if (!this.errorRates.has(version)) {
      this.errorRates.set(version, { errors: 0, total: 0, window: [] });
    }
    this.errorRates.get(version)!.total++;
  }

  recordHealthCheck(version: number, passed: boolean): void {
    if (!this.healthCheckResults.has(version)) {
      this.healthCheckResults.set(version, { passed: 0, failed: 0 });
    }
    const results = this.healthCheckResults.get(version)!;
    if (passed) results.passed++;
    else results.failed++;
  }

  shouldRollback(version: number, now: number): {
    rollback: boolean;
    trigger?: RollbackTrigger;
  } {
    if (now - this.lastRollback < this.rollbackCooldown) {
      return { rollback: false };
    }

    const plan = this.plans.get(version);
    if (!plan) return { rollback: false };

    for (const trigger of plan.triggers) {
      switch (trigger.type) {
        case 'error-rate': {
          const rates = this.errorRates.get(version);
          if (rates && rates.total > 10) {
            const errorRate = rates.errors / rates.total;
            if (errorRate > (trigger.threshold || 0.1)) {
              return { rollback: true, trigger };
            }
          }
          break;
        }
        case 'health-check': {
          const results = this.healthCheckResults.get(version);
          if (results && results.failed > (trigger.threshold || 3)) {
            return { rollback: true, trigger };
          }
          break;
        }
      }
    }

    return { rollback: false };
  }

  getPlan(version: number): RollbackPlan | undefined {
    return this.plans.get(version);
  }

  markRollback(now: number): void {
    this.lastRollback = now;
  }
}

// ─── Contract Upgrade Proxy (Unified Orchestrator) ─────────────────────────

type ProxyEvent =
  | { type: 'implementation-registered'; version: number; address: string }
  | { type: 'proposal-created'; proposalId: string; targetVersion: number }
  | { type: 'proposal-approved'; proposalId: string }
  | { type: 'upgrade-started'; fromVersion: number; toVersion: number }
  | { type: 'upgrade-completed'; fromVersion: number; toVersion: number; duration: number }
  | { type: 'upgrade-failed'; fromVersion: number; toVersion: number; reason: string }
  | { type: 'facet-cut-applied'; cuts: number }
  | { type: 'rollback-triggered'; fromVersion: number; toVersion: number; trigger: string }
  | { type: 'rollback-completed'; version: number }
  | { type: 'migration-step'; step: string; status: 'started' | 'completed' | 'failed' }
  | { type: 'snapshot-captured'; version: number; snapshotId: string }
  | { type: 'compatibility-check'; compatible: boolean; breakingChanges: number };

interface ProxyConfig {
  timelockDelay: number;
  proposalExpiry: number;
  quorum: number;
  approvalThreshold: number;
  rollbackCooldown: number;
  maxSnapshots: number;
  autoRollbackErrorThreshold: number;
  autoRollbackHealthFailures: number;
}

class ContractUpgradeProxy {
  private storageManager: StorageLayoutManager;
  private implRegistry: ImplementationRegistry;
  private facetManager: FacetManager;
  private migrationPipeline: MigrationPipeline;
  private governor: TimelockGovernor;
  private snapshots: StateSnapshotManager;
  private compatChecker: CompatibilityChecker;
  private rollbackController: EmergencyRollbackController;
  private currentVersion: number = 0;
  private currentState: Map<string, unknown> = new Map();
  private listeners: Array<(event: ProxyEvent) => void> = [];
  private config: ProxyConfig;

  constructor(config: Partial<ProxyConfig> = {}) {
    this.config = {
      timelockDelay: config.timelockDelay ?? 86400000, // 24h
      proposalExpiry: config.proposalExpiry ?? 604800000, // 7d
      quorum: config.quorum ?? 100,
      approvalThreshold: config.approvalThreshold ?? 0.67,
      rollbackCooldown: config.rollbackCooldown ?? 300000,
      maxSnapshots: config.maxSnapshots ?? 10,
      autoRollbackErrorThreshold: config.autoRollbackErrorThreshold ?? 0.1,
      autoRollbackHealthFailures: config.autoRollbackHealthFailures ?? 3,
    };

    this.storageManager = new StorageLayoutManager();
    this.implRegistry = new ImplementationRegistry();
    this.facetManager = new FacetManager();
    this.migrationPipeline = new MigrationPipeline();
    this.governor = new TimelockGovernor({
      minDelay: this.config.timelockDelay,
      maxDelay: this.config.timelockDelay * 4,
      proposalExpiry: this.config.proposalExpiry,
      defaultQuorum: this.config.quorum,
      defaultThreshold: this.config.approvalThreshold,
    });
    this.snapshots = new StateSnapshotManager(this.config.maxSnapshots);
    this.compatChecker = new CompatibilityChecker();
    this.rollbackController = new EmergencyRollbackController(this.config.rollbackCooldown);
  }

  on(listener: (event: ProxyEvent) => void): () => void {
    this.listeners.push(listener);
    return () => {
      this.listeners = this.listeners.filter(l => l !== listener);
    };
  }

  private emit(event: ProxyEvent): void {
    for (const listener of this.listeners) {
      try { listener(event); } catch {}
    }
  }

  // Register a new implementation version
  registerImplementation(
    version: number,
    address: string,
    codeHash: string,
    abi: string[],
    storageLayout: StorageLayout,
    now: number
  ): void {
    this.implRegistry.register({
      version,
      address,
      deployedAt: now,
      codeHash,
      abi,
      deprecated: false,
    });
    this.storageManager.registerLayout(storageLayout);
    this.emit({ type: 'implementation-registered', version, address });
  }

  // Register migration steps
  registerMigration(step: MigrationStep): void {
    this.migrationPipeline.registerStep(step);
  }

  // Propose an upgrade (goes through timelock)
  proposeUpgrade(
    proposer: string,
    targetVersion: number,
    description: string,
    facetCuts?: FacetCut[],
    now: number = Date.now()
  ): { proposal: UpgradeProposal; compatibility: CompatibilityReport } {
    const impl = this.implRegistry.get(targetVersion);
    if (!impl) throw new Error(`Implementation v${targetVersion} not registered`);

    // Check compatibility
    let compatibility: CompatibilityReport;
    if (facetCuts) {
      compatibility = this.compatChecker.checkFacetCompatibility(
        this.facetManager.getAllFacets(),
        facetCuts
      );
    } else {
      const currentLayout = this.storageManager.getLayout(this.currentVersion);
      const targetLayout = this.storageManager.getLayout(targetVersion);
      compatibility = currentLayout && targetLayout
        ? this.compatChecker.checkStorageCompatibility(currentLayout, targetLayout)
        : { compatible: true, breakingChanges: [], newFeatures: [], deprecations: [], migrationRequired: false };
    }

    this.emit({
      type: 'compatibility-check',
      compatible: compatibility.compatible,
      breakingChanges: compatibility.breakingChanges.length,
    });

    const proposal = this.governor.propose({
      proposer,
      description,
      targetVersion,
      implementationAddress: impl.address,
      facetCuts,
    }, now);

    this.emit({ type: 'proposal-created', proposalId: proposal.id, targetVersion });
    return { proposal, compatibility };
  }

  // Vote on a proposal
  voteOnProposal(proposalId: string, voter: string, support: boolean, weight: number, now: number): boolean {
    const result = this.governor.vote(proposalId, voter, support, weight, now);
    const proposal = this.governor.getProposal(proposalId);
    if (proposal?.status === 'approved') {
      this.emit({ type: 'proposal-approved', proposalId });
    }
    return result;
  }

  // Execute an approved upgrade
  async executeUpgrade(proposalId: string, now: number): Promise<{
    success: boolean;
    migration?: MigrationResult;
    facetResult?: { applied: FacetCut[]; conflicts: Array<{ selector: string; existingFacet: string; newFacet: string }> };
    reason?: string;
  }> {
    const canExec = this.governor.canExecute(proposalId, now);
    if (!canExec.executable) {
      return { success: false, reason: canExec.reason };
    }

    const proposal = this.governor.getProposal(proposalId)!;
    const fromVersion = this.currentVersion;
    const toVersion = proposal.targetVersion;

    this.emit({ type: 'upgrade-started', fromVersion, toVersion });

    // Capture pre-upgrade snapshot
    const snapshot = this.snapshots.capture(fromVersion, this.currentState, {
      upgradeProposal: proposalId,
      reason: 'pre-upgrade-snapshot',
    }, now);
    this.emit({ type: 'snapshot-captured', version: fromVersion, snapshotId: snapshot.id });

    // Register rollback plan
    this.rollbackController.registerPlan({
      currentVersion: toVersion,
      targetVersion: fromVersion,
      snapshotId: snapshot.id,
      facetRestoration: [], // Would restore previous facet state
      estimatedDowntime: 5000,
      triggers: [
        { type: 'error-rate', threshold: this.config.autoRollbackErrorThreshold, description: 'High error rate' },
        { type: 'health-check', threshold: this.config.autoRollbackHealthFailures, description: 'Health check failures' },
      ],
    });

    // Run migration if needed
    let migrationResult: MigrationResult | undefined;
    const migrationPath = this.migrationPipeline.getPath(fromVersion, toVersion);
    if (migrationPath.length > 0) {
      migrationResult = await this.migrationPipeline.execute(fromVersion, toVersion, this.currentState);
      if (!migrationResult.success) {
        this.emit({
          type: 'upgrade-failed',
          fromVersion,
          toVersion,
          reason: migrationResult.failureReason || 'Migration failed',
        });
        return { success: false, migration: migrationResult, reason: migrationResult.failureReason };
      }
      this.currentState = migrationResult.stateSnapshot;
    }

    // Apply facet cuts if any
    let facetResult: { applied: FacetCut[]; conflicts: Array<{ selector: string; existingFacet: string; newFacet: string }> } | undefined;
    if (proposal.facetCuts && proposal.facetCuts.length > 0) {
      facetResult = this.facetManager.executeCut(proposal.facetCuts, now);
      if (facetResult.conflicts.length > 0) {
        this.emit({
          type: 'upgrade-failed',
          fromVersion,
          toVersion,
          reason: `Facet conflicts: ${facetResult.conflicts.length}`,
        });
        return { success: false, facetResult, reason: 'Facet selector conflicts' };
      }
      this.emit({ type: 'facet-cut-applied', cuts: facetResult.applied.length });
    }

    // Update version
    this.currentVersion = toVersion;
    this.implRegistry.deprecate(fromVersion, now);
    this.governor.markExecuted(proposalId);

    // Post-upgrade snapshot
    this.snapshots.capture(toVersion, this.currentState, {
      upgradeProposal: proposalId,
      reason: 'post-upgrade-snapshot',
    }, now);

    const duration = Date.now() - now;
    this.emit({ type: 'upgrade-completed', fromVersion, toVersion, duration });

    return { success: true, migration: migrationResult, facetResult };
  }

  // Emergency rollback
  async emergencyRollback(now: number): Promise<{
    success: boolean;
    fromVersion: number;
    toVersion: number;
    reason?: string;
  }> {
    const plan = this.rollbackController.getPlan(this.currentVersion);
    if (!plan) {
      return { success: false, fromVersion: this.currentVersion, toVersion: 0, reason: 'No rollback plan' };
    }

    const snapshot = this.snapshots.getSnapshot(plan.snapshotId);
    if (!snapshot) {
      return { success: false, fromVersion: this.currentVersion, toVersion: plan.targetVersion, reason: 'Snapshot not found' };
    }

    // Verify snapshot integrity
    if (!this.snapshots.verify(plan.snapshotId)) {
      return { success: false, fromVersion: this.currentVersion, toVersion: plan.targetVersion, reason: 'Snapshot integrity check failed' };
    }

    const fromVersion = this.currentVersion;
    this.emit({ type: 'rollback-triggered', fromVersion, toVersion: plan.targetVersion, trigger: 'emergency' });

    // Restore state
    this.currentState = new Map(snapshot.state);
    this.currentVersion = plan.targetVersion;
    this.rollbackController.markRollback(now);

    this.emit({ type: 'rollback-completed', version: plan.targetVersion });

    return { success: true, fromVersion, toVersion: plan.targetVersion };
  }

  // Auto-rollback check (call periodically)
  async checkAutoRollback(now: number): Promise<boolean> {
    const check = this.rollbackController.shouldRollback(this.currentVersion, now);
    if (check.rollback && check.trigger) {
      this.emit({
        type: 'rollback-triggered',
        fromVersion: this.currentVersion,
        toVersion: this.currentVersion - 1,
        trigger: check.trigger.description,
      });
      const result = await this.emergencyRollback(now);
      return result.success;
    }
    return false;
  }

  // Record operation results for auto-rollback detection
  recordOperation(success: boolean, now: number): void {
    if (success) {
      this.rollbackController.recordSuccess(this.currentVersion);
    } else {
      this.rollbackController.recordError(this.currentVersion, now);
    }
  }

  recordHealthCheck(passed: boolean): void {
    this.rollbackController.recordHealthCheck(this.currentVersion, passed);
  }

  // Route a function call through the proxy
  routeCall(selector: string): {
    facet?: Facet;
    implementation?: ImplementationRecord;
  } {
    const facet = this.facetManager.routeSelector(selector);
    const impl = this.implRegistry.get(this.currentVersion);
    return { facet, implementation: impl };
  }

  // Getters
  getCurrentVersion(): number { return this.currentVersion; }
  getState(): Map<string, unknown> { return new Map(this.currentState); }
  getActiveProposals(): UpgradeProposal[] { return this.governor.getActiveProposals(); }
  getFacets(): Facet[] { return this.facetManager.getAllFacets(); }
  getImplementations(): ImplementationRecord[] { return this.implRegistry.getAll(); }
  getMigrationHistory(): MigrationResult[] { return this.migrationPipeline.getHistory(); }
}

// ─── Presets ────────────────────────────────────────────────────────────────

function createAgentProtocolProxy(): ContractUpgradeProxy {
  return new ContractUpgradeProxy({
    timelockDelay: 3600000, // 1h for agent protocols (faster iteration)
    proposalExpiry: 86400000, // 1d
    quorum: 50,
    approvalThreshold: 0.6,
    rollbackCooldown: 60000,
    maxSnapshots: 5,
    autoRollbackErrorThreshold: 0.15,
    autoRollbackHealthFailures: 5,
  });
}

function createGovernanceProxy(): ContractUpgradeProxy {
  return new ContractUpgradeProxy({
    timelockDelay: 172800000, // 48h for governance (safety)
    proposalExpiry: 1209600000, // 14d
    quorum: 200,
    approvalThreshold: 0.75,
    rollbackCooldown: 600000,
    maxSnapshots: 20,
    autoRollbackErrorThreshold: 0.05,
    autoRollbackHealthFailures: 2,
  });
}

function createRapidIterationProxy(): ContractUpgradeProxy {
  return new ContractUpgradeProxy({
    timelockDelay: 600000, // 10m for dev/staging
    proposalExpiry: 3600000, // 1h
    quorum: 10,
    approvalThreshold: 0.5,
    rollbackCooldown: 30000,
    maxSnapshots: 3,
    autoRollbackErrorThreshold: 0.2,
    autoRollbackHealthFailures: 10,
  });
}

export {
  StorageLayoutManager,
  ImplementationRegistry,
  FacetManager,
  MigrationPipeline,
  TimelockGovernor,
  StateSnapshotManager,
  CompatibilityChecker,
  EmergencyRollbackController,
  ContractUpgradeProxy,
  createAgentProtocolProxy,
  createGovernanceProxy,
  createRapidIterationProxy,
};

export type {
  StorageSlot,
  StorageLayout,
  ImplementationRecord,
  Facet,
  FacetAction,
  FacetCut,
  MigrationStep,
  MigrationResult,
  UpgradeProposal,
  StateSnapshot,
  CompatibilityReport,
  RollbackTrigger,
  RollbackPlan,
  ProxyEvent,
  ProxyConfig,
};
