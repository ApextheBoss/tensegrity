import { describe, it, expect, vi } from 'vitest';
import {
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
} from '../contract-upgrade-proxy.js';
import type {
  StorageLayout,
  FacetCut,
  MigrationStep,
} from '../contract-upgrade-proxy.js';

// ─── StorageLayoutManager ──────────────────────────────────────────────────

describe('StorageLayoutManager', () => {
  it('registers layouts and tracks current version', () => {
    const mgr = new StorageLayoutManager();
    mgr.registerLayout({ version: 1, slots: [], mappings: new Map() });
    mgr.registerLayout({ version: 2, slots: [], mappings: new Map() });
    expect(mgr.getCurrentVersion()).toBe(2);
  });

  it('detects slot collisions between versions', () => {
    const mgr = new StorageLayoutManager();
    mgr.registerLayout({
      version: 1,
      slots: [{ name: 'owner', slot: '0x00', type: 'address', version: 1 }],
      mappings: new Map(),
    });
    expect(() =>
      mgr.registerLayout({
        version: 2,
        slots: [{ name: 'admin', slot: '0x00', type: 'address', version: 2 }],
        mappings: new Map(),
      })
    ).toThrow('Storage slot collisions detected');
  });

  it('allows same-name slot across versions (not a collision)', () => {
    const mgr = new StorageLayoutManager();
    mgr.registerLayout({
      version: 1,
      slots: [{ name: 'owner', slot: '0x00', type: 'address', version: 1 }],
      mappings: new Map(),
    });
    // Same name, same slot = OK
    mgr.registerLayout({
      version: 2,
      slots: [{ name: 'owner', slot: '0x00', type: 'address', version: 1 }],
      mappings: new Map(),
    });
    expect(mgr.getCurrentVersion()).toBe(2);
  });

  it('ignores deprecated slots in collision detection', () => {
    const mgr = new StorageLayoutManager();
    mgr.registerLayout({
      version: 1,
      slots: [{ name: 'old', slot: '0x01', type: 'uint256', version: 1, deprecated: 2 }],
      mappings: new Map(),
    });
    // Slot 0x01 was deprecated at version 2, so reusing it is fine
    mgr.registerLayout({
      version: 2,
      slots: [{ name: 'new', slot: '0x01', type: 'bytes32', version: 2 }],
      mappings: new Map(),
    });
    expect(mgr.getCurrentVersion()).toBe(2);
  });

  it('computes deterministic slots', () => {
    const mgr = new StorageLayoutManager();
    const slot1 = mgr.computeSlot('eip1967', 'implementation');
    const slot2 = mgr.computeSlot('eip1967', 'implementation');
    expect(slot1).toBe(slot2);
    expect(slot1.startsWith('0x')).toBe(true);
  });

  it('gets migration path between versions', () => {
    const mgr = new StorageLayoutManager();
    mgr.registerLayout({
      version: 1,
      slots: [{ name: 'a', slot: '0x00', type: 'uint256', version: 1 }],
      mappings: new Map(),
    });
    mgr.registerLayout({
      version: 2,
      slots: [
        { name: 'a', slot: '0x00', type: 'uint256', version: 1 },
        { name: 'b', slot: '0x01', type: 'address', version: 2 },
      ],
      mappings: new Map(),
    });
    const path = mgr.getMigrationPath(1, 2);
    expect(path.length).toBe(1);
    expect(path[0].name).toBe('b');
  });

  it('getLayout returns undefined for unregistered version', () => {
    const mgr = new StorageLayoutManager();
    expect(mgr.getLayout(99)).toBeUndefined();
  });
});

// ─── ImplementationRegistry ────────────────────────────────────────────────

describe('ImplementationRegistry', () => {
  it('registers and retrieves implementations', () => {
    const reg = new ImplementationRegistry();
    reg.register({ version: 1, address: '0xAAA', deployedAt: 1000, codeHash: 'abc', abi: ['fn1'], deprecated: false });
    expect(reg.get(1)?.address).toBe('0xAAA');
    expect(reg.getByAddress('0xAAA')?.version).toBe(1);
  });

  it('throws on duplicate version', () => {
    const reg = new ImplementationRegistry();
    reg.register({ version: 1, address: '0xAAA', deployedAt: 1000, codeHash: 'abc', abi: [], deprecated: false });
    expect(() =>
      reg.register({ version: 1, address: '0xBBB', deployedAt: 2000, codeHash: 'def', abi: [], deprecated: false })
    ).toThrow('already registered');
  });

  it('deprecates implementations', () => {
    const reg = new ImplementationRegistry();
    reg.register({ version: 1, address: '0xAAA', deployedAt: 1000, codeHash: 'abc', abi: [], deprecated: false });
    reg.register({ version: 2, address: '0xBBB', deployedAt: 2000, codeHash: 'def', abi: [], deprecated: false });
    reg.deprecate(1, 3000);
    expect(reg.get(1)?.deprecated).toBe(true);
    expect(reg.getLatestActive()?.version).toBe(2);
  });

  it('getAll returns sorted by version descending', () => {
    const reg = new ImplementationRegistry();
    reg.register({ version: 1, address: '0xA', deployedAt: 1000, codeHash: 'a', abi: [], deprecated: false });
    reg.register({ version: 3, address: '0xC', deployedAt: 3000, codeHash: 'c', abi: [], deprecated: false });
    reg.register({ version: 2, address: '0xB', deployedAt: 2000, codeHash: 'b', abi: [], deprecated: false });
    const all = reg.getAll();
    expect(all.map(i => i.version)).toEqual([3, 2, 1]);
  });
});

// ─── FacetManager ──────────────────────────────────────────────────────────

describe('FacetManager', () => {
  it('adds facets and routes selectors', () => {
    const fm = new FacetManager();
    fm.executeCut([{
      action: 'add', facetName: 'Auth', facetAddress: '0xA1', selectors: ['login', 'logout'],
    }], 1000);
    expect(fm.routeSelector('login')?.name).toBe('Auth');
    expect(fm.routeSelector('unknown')).toBeUndefined();
  });

  it('detects selector conflicts on add', () => {
    const fm = new FacetManager();
    fm.executeCut([{ action: 'add', facetName: 'A', facetAddress: '0xA', selectors: ['fn1'] }], 1000);
    const result = fm.executeCut([{ action: 'add', facetName: 'B', facetAddress: '0xB', selectors: ['fn1'] }], 2000);
    expect(result.conflicts.length).toBe(1);
    expect(result.applied.length).toBe(0);
  });

  it('replaces facet selectors', () => {
    const fm = new FacetManager();
    fm.executeCut([{ action: 'add', facetName: 'A', facetAddress: '0xA', selectors: ['fn1', 'fn2'] }], 1000);
    fm.executeCut([{ action: 'replace', facetName: 'A', facetAddress: '0xA2', selectors: ['fn1', 'fn3'] }], 2000);
    expect(fm.routeSelector('fn1')?.address).toBe('0xA2');
    expect(fm.routeSelector('fn2')).toBeUndefined(); // old selector removed
    expect(fm.routeSelector('fn3')?.name).toBe('A');
  });

  it('removes facets without dependents', () => {
    const fm = new FacetManager();
    fm.executeCut([{ action: 'add', facetName: 'A', facetAddress: '0xA', selectors: ['fn1'] }], 1000);
    const result = fm.executeCut([{ action: 'remove', facetName: 'A', facetAddress: '', selectors: [] }], 2000);
    expect(result.applied.length).toBe(1);
    expect(fm.routeSelector('fn1')).toBeUndefined();
  });

  it('blocks removal of facet with dependents', () => {
    const fm = new FacetManager();
    fm.executeCut([{ action: 'add', facetName: 'Base', facetAddress: '0xB', selectors: ['base'] }], 1000);
    // Manually add dependency by replacing with a facet that has dependencies
    const facet = fm.getFacet('Base')!;
    // Add a dependent facet
    fm.executeCut([{ action: 'add', facetName: 'Child', facetAddress: '0xC', selectors: ['child'] }], 2000);
    const child = fm.getFacet('Child')!;
    child.dependencies.push('Base');

    const result = fm.executeCut([{ action: 'remove', facetName: 'Base', facetAddress: '', selectors: [] }], 3000);
    // Base should NOT be removed because Child depends on it
    expect(fm.getFacet('Base')).toBeDefined();
  });

  it('tracks cut history', () => {
    const fm = new FacetManager();
    fm.executeCut([{ action: 'add', facetName: 'A', facetAddress: '0xA', selectors: ['fn1'] }], 1000);
    fm.executeCut([{ action: 'add', facetName: 'B', facetAddress: '0xB', selectors: ['fn2'] }], 2000);
    expect(fm.getCutHistory().length).toBe(2);
  });
});

// ─── MigrationPipeline ────────────────────────────────────────────────────

describe('MigrationPipeline', () => {
  function makeStep(from: number, to: number, opts: Partial<MigrationStep> = {}): MigrationStep {
    return {
      name: `v${from}→v${to}`,
      fromVersion: from,
      toVersion: to,
      preCheck: () => ({ ok: true }),
      migrate: (state) => { state.set(`migrated_${to}`, true); return state; },
      postCheck: () => ({ ok: true }),
      rollback: (state) => { state.delete(`migrated_${to}`); return state; },
      ...opts,
    };
  }

  it('finds migration path via BFS', () => {
    const pipeline = new MigrationPipeline();
    pipeline.registerStep(makeStep(1, 2));
    pipeline.registerStep(makeStep(2, 3));
    const path = pipeline.getPath(1, 3);
    expect(path.length).toBe(2);
  });

  it('returns empty path when no route exists', () => {
    const pipeline = new MigrationPipeline();
    pipeline.registerStep(makeStep(1, 2));
    expect(pipeline.getPath(1, 5)).toEqual([]);
  });

  it('executes migration successfully', async () => {
    const pipeline = new MigrationPipeline();
    pipeline.registerStep(makeStep(1, 2));
    pipeline.registerStep(makeStep(2, 3));
    const result = await pipeline.execute(1, 3, new Map([['initial', true]]));
    expect(result.success).toBe(true);
    expect(result.stepsCompleted).toBe(2);
    expect(result.stateSnapshot.get('migrated_2')).toBe(true);
    expect(result.stateSnapshot.get('migrated_3')).toBe(true);
  });

  it('rolls back on pre-check failure', async () => {
    const pipeline = new MigrationPipeline();
    pipeline.registerStep(makeStep(1, 2));
    pipeline.registerStep(makeStep(2, 3, {
      preCheck: () => ({ ok: false, reason: 'not ready' }),
    }));
    const result = await pipeline.execute(1, 3, new Map());
    expect(result.success).toBe(false);
    expect(result.failedStep).toBe('v2→v3');
    expect(result.rolledBack).toBe(true);
    expect(result.stepsCompleted).toBe(1);
  });

  it('rolls back on migration error', async () => {
    const pipeline = new MigrationPipeline();
    pipeline.registerStep(makeStep(1, 2));
    pipeline.registerStep(makeStep(2, 3, {
      migrate: () => { throw new Error('boom'); },
    }));
    const result = await pipeline.execute(1, 3, new Map());
    expect(result.success).toBe(false);
    expect(result.failureReason).toContain('boom');
    expect(result.rolledBack).toBe(true);
  });

  it('rolls back on post-check failure', async () => {
    const pipeline = new MigrationPipeline();
    pipeline.registerStep(makeStep(1, 2));
    pipeline.registerStep(makeStep(2, 3, {
      postCheck: () => ({ ok: false, reason: 'invariant violated' }),
    }));
    const result = await pipeline.execute(1, 3, new Map());
    expect(result.success).toBe(false);
    expect(result.rolledBack).toBe(true);
  });

  it('returns failure when no path exists', async () => {
    const pipeline = new MigrationPipeline();
    const result = await pipeline.execute(1, 5, new Map());
    expect(result.success).toBe(false);
    expect(result.failureReason).toContain('No migration path');
  });

  it('tracks migration history', async () => {
    const pipeline = new MigrationPipeline();
    pipeline.registerStep(makeStep(1, 2));
    await pipeline.execute(1, 2, new Map());
    expect(pipeline.getHistory().length).toBe(1);
  });
});

// ─── TimelockGovernor ──────────────────────────────────────────────────────

describe('TimelockGovernor', () => {
  const defaultConfig = {
    minDelay: 1000,
    maxDelay: 10000,
    proposalExpiry: 50000,
    defaultQuorum: 100,
    defaultThreshold: 0.6,
  };

  it('creates proposals with timelock', () => {
    const gov = new TimelockGovernor(defaultConfig);
    const proposal = gov.propose({
      proposer: 'alice',
      description: 'Upgrade to v2',
      targetVersion: 2,
      implementationAddress: '0xBBB',
    }, 1000);
    expect(proposal.status).toBe('pending');
    expect(proposal.executableAfter).toBe(2000);
  });

  it('approves proposal when quorum+threshold met', () => {
    const gov = new TimelockGovernor(defaultConfig);
    const proposal = gov.propose({
      proposer: 'alice',
      description: 'test',
      targetVersion: 2,
      implementationAddress: '0xBBB',
    }, 1000);
    gov.vote(proposal.id, 'voter1', true, 70, 1500);
    gov.vote(proposal.id, 'voter2', true, 40, 1500);
    const p = gov.getProposal(proposal.id)!;
    expect(p.status).toBe('approved');
  });

  it('does not approve when threshold not met', () => {
    const gov = new TimelockGovernor(defaultConfig);
    const proposal = gov.propose({
      proposer: 'alice',
      description: 'test',
      targetVersion: 2,
      implementationAddress: '0xBBB',
    }, 1000);
    gov.vote(proposal.id, 'v1', true, 30, 1500);
    gov.vote(proposal.id, 'v2', false, 40, 1500);
    gov.vote(proposal.id, 'v3', false, 40, 1500);
    expect(gov.getProposal(proposal.id)!.status).toBe('pending');
  });

  it('canExecute respects timelock', () => {
    const gov = new TimelockGovernor(defaultConfig);
    const proposal = gov.propose({
      proposer: 'alice',
      description: 'test',
      targetVersion: 2,
      implementationAddress: '0xBBB',
    }, 1000);
    gov.vote(proposal.id, 'v1', true, 100, 1500);
    expect(gov.canExecute(proposal.id, 1500).executable).toBe(false); // before timelock
    expect(gov.canExecute(proposal.id, 2000).executable).toBe(true); // after timelock
  });

  it('expires proposals after expiry period', () => {
    const gov = new TimelockGovernor(defaultConfig);
    const proposal = gov.propose({
      proposer: 'alice',
      description: 'test',
      targetVersion: 2,
      implementationAddress: '0xBBB',
    }, 1000);
    gov.vote(proposal.id, 'v1', true, 100, 1500);
    // executableAfter=2000, expiresAt=2000+50000=52000
    expect(gov.canExecute(proposal.id, 60000).executable).toBe(false);
    expect(gov.getProposal(proposal.id)!.status).toBe('expired');
  });

  it('cancels proposal by proposer', () => {
    const gov = new TimelockGovernor(defaultConfig);
    const proposal = gov.propose({
      proposer: 'alice',
      description: 'test',
      targetVersion: 2,
      implementationAddress: '0xBBB',
    }, 1000);
    expect(gov.cancel(proposal.id, 'bob')).toBe(false); // not proposer
    expect(gov.cancel(proposal.id, 'alice')).toBe(true);
    expect(gov.getProposal(proposal.id)!.status).toBe('cancelled');
  });

  it('rejects votes after voting period', () => {
    const gov = new TimelockGovernor(defaultConfig);
    const proposal = gov.propose({
      proposer: 'alice',
      description: 'test',
      targetVersion: 2,
      implementationAddress: '0xBBB',
    }, 1000);
    // executableAfter=2000, voting period ends at 2000
    expect(gov.vote(proposal.id, 'v1', true, 100, 2000)).toBe(false);
  });

  it('getActiveProposals filters correctly', () => {
    const gov = new TimelockGovernor(defaultConfig);
    gov.propose({ proposer: 'a', description: 't', targetVersion: 2, implementationAddress: '0x1' }, 1000);
    const p2 = gov.propose({ proposer: 'b', description: 't', targetVersion: 3, implementationAddress: '0x2' }, 2000);
    gov.cancel(p2.id, 'b');
    expect(gov.getActiveProposals().length).toBe(1);
  });
});

// ─── StateSnapshotManager ──────────────────────────────────────────────────

describe('StateSnapshotManager', () => {
  it('captures and retrieves snapshots', () => {
    const mgr = new StateSnapshotManager(5);
    const state = new Map<string, unknown>([['key', 'value']]);
    const snap = mgr.capture(1, state, { reason: 'test' }, 1000);
    expect(mgr.getSnapshot(snap.id)).toBeDefined();
    expect(mgr.getByVersion(1)?.state.get('key')).toBe('value');
  });

  it('verifies snapshot integrity', () => {
    const mgr = new StateSnapshotManager();
    const state = new Map<string, unknown>([['a', 1]]);
    const snap = mgr.capture(1, state, {}, 1000);
    expect(mgr.verify(snap.id)).toBe(true);
  });

  it('evicts oldest snapshots when over limit', () => {
    const mgr = new StateSnapshotManager(3);
    const ids: string[] = [];
    for (let i = 1; i <= 4; i++) {
      const snap = mgr.capture(i, new Map([['v', i]]), {}, i * 1000);
      ids.push(snap.id);
    }
    expect(mgr.getSnapshot(ids[0])).toBeUndefined(); // evicted
    expect(mgr.getSnapshot(ids[3])).toBeDefined();
  });

  it('builds snapshot chain via parentId', () => {
    const mgr = new StateSnapshotManager();
    mgr.capture(1, new Map(), {}, 1000);
    mgr.capture(2, new Map(), {}, 2000);
    mgr.capture(3, new Map(), {}, 3000);
    const chain = mgr.getChain(3);
    expect(chain.length).toBe(3);
    expect(chain[0].version).toBe(1);
    expect(chain[2].version).toBe(3);
  });

  it('verify returns false for unknown snapshot', () => {
    const mgr = new StateSnapshotManager();
    expect(mgr.verify('nonexistent')).toBe(false);
  });
});

// ─── CompatibilityChecker ──────────────────────────────────────────────────

describe('CompatibilityChecker', () => {
  it('detects breaking changes from facet removal', () => {
    const checker = new CompatibilityChecker();
    const facets = [{ name: 'Auth', version: 1, address: '0xA', selectors: ['login'], dependencies: [] }];
    const cuts: FacetCut[] = [{ action: 'remove', facetName: 'Auth', facetAddress: '', selectors: [] }];
    const report = checker.checkFacetCompatibility(facets, cuts);
    expect(report.compatible).toBe(false);
    expect(report.breakingChanges[0].type).toBe('selector-removed');
  });

  it('detects selector conflicts on add', () => {
    const checker = new CompatibilityChecker();
    const facets = [{ name: 'A', version: 1, address: '0xA', selectors: ['fn1'], dependencies: [] }];
    const cuts: FacetCut[] = [{ action: 'add', facetName: 'B', facetAddress: '0xB', selectors: ['fn1'] }];
    const report = checker.checkFacetCompatibility(facets, cuts);
    expect(report.compatible).toBe(false);
  });

  it('reports new features on add without conflicts', () => {
    const checker = new CompatibilityChecker();
    const cuts: FacetCut[] = [{ action: 'add', facetName: 'B', facetAddress: '0xB', selectors: ['fn2'] }];
    const report = checker.checkFacetCompatibility([], cuts);
    expect(report.compatible).toBe(true);
    expect(report.newFeatures.length).toBe(1);
  });

  it('marks replace as migration required', () => {
    const checker = new CompatibilityChecker();
    const cuts: FacetCut[] = [{ action: 'replace', facetName: 'A', facetAddress: '0xA2', selectors: ['fn1'] }];
    const report = checker.checkFacetCompatibility([], cuts);
    expect(report.migrationRequired).toBe(true);
  });

  it('checks storage compatibility', () => {
    const checker = new CompatibilityChecker();
    const current: StorageLayout = {
      version: 1,
      slots: [{ name: 'owner', slot: '0x00', type: 'address', version: 1 }],
      mappings: new Map(),
    };
    const proposed: StorageLayout = {
      version: 2,
      slots: [
        { name: 'owner', slot: '0x00', type: 'address', version: 1 },
        { name: 'balance', slot: '0x01', type: 'uint256', version: 2 },
      ],
      mappings: new Map(),
    };
    const report = checker.checkStorageCompatibility(current, proposed);
    expect(report.compatible).toBe(true);
    expect(report.newFeatures).toContain('balance (uint256)');
  });

  it('detects storage type changes as breaking', () => {
    const checker = new CompatibilityChecker();
    const current: StorageLayout = {
      version: 1,
      slots: [{ name: 'data', slot: '0x00', type: 'uint256', version: 1 }],
      mappings: new Map(),
    };
    const proposed: StorageLayout = {
      version: 2,
      slots: [{ name: 'data', slot: '0x00', type: 'bytes32', version: 2 }],
      mappings: new Map(),
    };
    const report = checker.checkStorageCompatibility(current, proposed);
    expect(report.compatible).toBe(false);
    expect(report.migrationRequired).toBe(true);
  });
});

// ─── EmergencyRollbackController ───────────────────────────────────────────

describe('EmergencyRollbackController', () => {
  it('triggers rollback on high error rate', () => {
    const ctrl = new EmergencyRollbackController(0);
    ctrl.registerPlan({
      currentVersion: 2,
      targetVersion: 1,
      snapshotId: 'snap1',
      facetRestoration: [],
      estimatedDowntime: 1000,
      triggers: [{ type: 'error-rate', threshold: 0.1, description: 'High errors' }],
    });
    // Record >10 operations with >10% error rate
    for (let i = 0; i < 10; i++) ctrl.recordSuccess(2);
    for (let i = 0; i < 5; i++) ctrl.recordError(2, 1000);
    const check = ctrl.shouldRollback(2, 1000);
    expect(check.rollback).toBe(true);
  });

  it('triggers rollback on health check failures', () => {
    const ctrl = new EmergencyRollbackController(0);
    ctrl.registerPlan({
      currentVersion: 2,
      targetVersion: 1,
      snapshotId: 'snap1',
      facetRestoration: [],
      estimatedDowntime: 1000,
      triggers: [{ type: 'health-check', threshold: 3, description: 'Health failures' }],
    });
    for (let i = 0; i < 4; i++) ctrl.recordHealthCheck(2, false);
    expect(ctrl.shouldRollback(2, 1000).rollback).toBe(true);
  });

  it('respects cooldown period', () => {
    const ctrl = new EmergencyRollbackController(5000);
    ctrl.registerPlan({
      currentVersion: 2,
      targetVersion: 1,
      snapshotId: 'snap1',
      facetRestoration: [],
      estimatedDowntime: 1000,
      triggers: [{ type: 'health-check', threshold: 1, description: 'test' }],
    });
    ctrl.recordHealthCheck(2, false);
    ctrl.recordHealthCheck(2, false);
    ctrl.markRollback(1000);
    expect(ctrl.shouldRollback(2, 2000).rollback).toBe(false); // within cooldown
    expect(ctrl.shouldRollback(2, 7000).rollback).toBe(true); // after cooldown
  });

  it('returns false when no plan exists', () => {
    const ctrl = new EmergencyRollbackController(0);
    expect(ctrl.shouldRollback(99, 1000).rollback).toBe(false);
  });
});

// ─── ContractUpgradeProxy (Full Orchestrator) ──────────────────────────────

describe('ContractUpgradeProxy', () => {
  function setupProxy() {
    const proxy = new ContractUpgradeProxy({
      timelockDelay: 1000,
      proposalExpiry: 50000,
      quorum: 10,
      approvalThreshold: 0.5,
      rollbackCooldown: 0,
      maxSnapshots: 5,
      autoRollbackErrorThreshold: 0.1,
      autoRollbackHealthFailures: 3,
    });

    // Register v1
    proxy.registerImplementation(1, '0xV1', 'hash1', ['fn1'], {
      version: 1, slots: [], mappings: new Map(),
    }, 1000);

    // Register v2
    proxy.registerImplementation(2, '0xV2', 'hash2', ['fn1', 'fn2'], {
      version: 2, slots: [], mappings: new Map(),
    }, 2000);

    return proxy;
  }

  it('registers implementations and emits events', () => {
    const events: any[] = [];
    const proxy = new ContractUpgradeProxy({ timelockDelay: 1000, proposalExpiry: 50000, quorum: 10, approvalThreshold: 0.5 });
    proxy.on(e => events.push(e));
    proxy.registerImplementation(1, '0xV1', 'h', [], { version: 1, slots: [], mappings: new Map() }, 1000);
    expect(events[0].type).toBe('implementation-registered');
  });

  it('proposes and executes upgrade end-to-end', async () => {
    const proxy = setupProxy();
    const events: any[] = [];
    proxy.on(e => events.push(e));

    // Propose
    const { proposal } = proxy.proposeUpgrade('alice', 2, 'Upgrade to v2', undefined, 3000);
    expect(proposal.status).toBe('pending');

    // Vote
    proxy.voteOnProposal(proposal.id, 'voter1', true, 10, 3500);

    // Execute after timelock
    const result = await proxy.executeUpgrade(proposal.id, 4000);
    expect(result.success).toBe(true);
    expect(proxy.getCurrentVersion()).toBe(2);
  });

  it('rejects execution before timelock', async () => {
    const proxy = setupProxy();
    const { proposal } = proxy.proposeUpgrade('alice', 2, 'test', undefined, 3000);
    proxy.voteOnProposal(proposal.id, 'v1', true, 10, 3500);
    const result = await proxy.executeUpgrade(proposal.id, 3500); // before timelock
    expect(result.success).toBe(false);
    expect(result.reason).toContain('Timelock');
  });

  it('executes upgrade with facet cuts', async () => {
    const proxy = setupProxy();
    const cuts: FacetCut[] = [{ action: 'add', facetName: 'Auth', facetAddress: '0xAuth', selectors: ['login'] }];
    const { proposal } = proxy.proposeUpgrade('alice', 2, 'test', cuts, 3000);
    proxy.voteOnProposal(proposal.id, 'v1', true, 10, 3500);
    const result = await proxy.executeUpgrade(proposal.id, 4000);
    expect(result.success).toBe(true);
    expect(result.facetResult?.applied.length).toBe(1);
    expect(proxy.getFacets().length).toBe(1);
  });

  it('executes upgrade with migration steps', async () => {
    const proxy = setupProxy();
    proxy.registerMigration({
      name: 'v0→v2',
      fromVersion: 0,
      toVersion: 2,
      preCheck: () => ({ ok: true }),
      migrate: (state) => { state.set('migrated', true); return state; },
      postCheck: () => ({ ok: true }),
      rollback: (state) => { state.delete('migrated'); return state; },
    });
    const { proposal } = proxy.proposeUpgrade('alice', 2, 'test', undefined, 3000);
    proxy.voteOnProposal(proposal.id, 'v1', true, 10, 3500);
    const result = await proxy.executeUpgrade(proposal.id, 4000);
    expect(result.success).toBe(true);
    expect(result.migration?.success).toBe(true);
    expect(proxy.getState().get('migrated')).toBe(true);
  });

  it('emergency rollback restores state', async () => {
    const proxy = setupProxy();
    const { proposal } = proxy.proposeUpgrade('alice', 2, 'test', undefined, 3000);
    proxy.voteOnProposal(proposal.id, 'v1', true, 10, 3500);
    await proxy.executeUpgrade(proposal.id, 4000);
    expect(proxy.getCurrentVersion()).toBe(2);

    const rollback = await proxy.emergencyRollback(5000);
    expect(rollback.success).toBe(true);
    expect(rollback.toVersion).toBe(0);
    expect(proxy.getCurrentVersion()).toBe(0);
  });

  it('auto-rollback triggers on error rate', async () => {
    const proxy = setupProxy();
    const { proposal } = proxy.proposeUpgrade('alice', 2, 'test', undefined, 3000);
    proxy.voteOnProposal(proposal.id, 'v1', true, 10, 3500);
    await proxy.executeUpgrade(proposal.id, 4000);

    // Record many errors
    for (let i = 0; i < 10; i++) proxy.recordOperation(true, 5000);
    for (let i = 0; i < 5; i++) proxy.recordOperation(false, 5000);

    const rolled = await proxy.checkAutoRollback(5000);
    expect(rolled).toBe(true);
    expect(proxy.getCurrentVersion()).toBe(0);
  });

  it('routes calls through facets and implementations', async () => {
    const proxy = setupProxy();
    const cuts: FacetCut[] = [{ action: 'add', facetName: 'A', facetAddress: '0xA', selectors: ['fn1'] }];
    const { proposal } = proxy.proposeUpgrade('alice', 2, 'test', cuts, 3000);
    proxy.voteOnProposal(proposal.id, 'v1', true, 10, 3500);
    await proxy.executeUpgrade(proposal.id, 4000);

    const route = proxy.routeCall('fn1');
    expect(route.facet?.name).toBe('A');
    expect(route.implementation?.version).toBe(2);
  });

  it('unsubscribes event listener', () => {
    const proxy = new ContractUpgradeProxy();
    const events: any[] = [];
    const unsub = proxy.on(e => events.push(e));
    proxy.registerImplementation(1, '0x1', 'h', [], { version: 1, slots: [], mappings: new Map() }, 1000);
    expect(events.length).toBe(1);
    unsub();
    proxy.registerImplementation(2, '0x2', 'h', [], { version: 2, slots: [], mappings: new Map() }, 2000);
    expect(events.length).toBe(1); // no new events
  });

  it('throws when proposing unregistered version', () => {
    const proxy = new ContractUpgradeProxy();
    expect(() => proxy.proposeUpgrade('alice', 99, 'test')).toThrow('not registered');
  });

  it('getters return correct data', async () => {
    const proxy = setupProxy();
    const { proposal } = proxy.proposeUpgrade('alice', 2, 'test', undefined, 3000);
    expect(proxy.getActiveProposals().length).toBe(1);
    expect(proxy.getImplementations().length).toBe(2);
    expect(proxy.getMigrationHistory().length).toBe(0);
  });
});

// ─── Presets ───────────────────────────────────────────────────────────────

describe('Presets', () => {
  it('createAgentProtocolProxy returns configured proxy', () => {
    const proxy = createAgentProtocolProxy();
    expect(proxy.getCurrentVersion()).toBe(0);
  });

  it('createGovernanceProxy returns configured proxy', () => {
    const proxy = createGovernanceProxy();
    expect(proxy.getCurrentVersion()).toBe(0);
  });

  it('createRapidIterationProxy returns configured proxy', () => {
    const proxy = createRapidIterationProxy();
    expect(proxy.getCurrentVersion()).toBe(0);
  });
});
