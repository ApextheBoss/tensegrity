/**
 * Lease-Based Consensus Protocol
 * 
 * Lightweight consensus using time-bounded leases instead of
 * traditional Paxos/Raft rounds. Optimized for read-heavy workloads
 * where leader stability reduces coordination overhead.
 * 
 * Components:
 * - LeaseGrantor: Issues time-bounded authority leases with renewal
 * - LeaseValidator: Clock-skew-aware lease validity checking
 * - ReadDelegator: Zero-RTT reads during valid lease periods
 * - WriteForwarder: Consistent write forwarding to lease holder
 * - LeaseTransferProtocol: Graceful and forced lease migration
 * - FailureDetector: Phi-accrual based holder liveness
 * - SplitBrainGuard: Fencing tokens prevent stale lease holders
 * - LeaseConflictResolver: Handles overlapping lease claims
 * - LeaseConsensusProtocol: Unified orchestrator
 */

// ─── Utilities ───────────────────────────────────────────────────────────

function fnv1a(str: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

class EWMATracker {
  private value: number | null = null;
  constructor(private alpha: number = 0.2) {}
  update(v: number): void {
    this.value = this.value === null ? v : this.alpha * v + (1 - this.alpha) * this.value;
  }
  get(): number | null { return this.value; }
  reset(): void { this.value = null; }
}

class WelfordStats {
  private n = 0;
  private mean_ = 0;
  private m2 = 0;
  update(x: number): void {
    this.n++;
    const d = x - this.mean_;
    this.mean_ += d / this.n;
    this.m2 += d * (x - this.mean_);
  }
  count(): number { return this.n; }
  mean(): number { return this.n > 0 ? this.mean_ : 0; }
  variance(): number { return this.n > 1 ? this.m2 / (this.n - 1) : 0; }
  stddev(): number { return Math.sqrt(this.variance()); }
}

// ─── Types ───────────────────────────────────────────────────────────────

interface LeaseConfig {
  leaseDurationMs: number;
  renewalWindowMs: number;
  maxClockSkewMs: number;
  gracePeriodMs: number;
  maxConsecutiveRenewals: number;
  fencingTokenEnabled: boolean;
  readDelegationEnabled: boolean;
  phiThreshold: number;
  heartbeatIntervalMs: number;
  transferTimeoutMs: number;
}

interface Lease {
  id: string;
  resourceId: string;
  holderId: string;
  grantedAt: number;
  expiresAt: number;
  fencingToken: number;
  epoch: number;
  renewalCount: number;
  state: 'active' | 'expiring' | 'expired' | 'transferring' | 'revoked';
  metadata: Record<string, unknown>;
}

interface LeaseRequest {
  resourceId: string;
  requesterId: string;
  priority: number;
  metadata?: Record<string, unknown>;
}

interface LeaseTransfer {
  leaseId: string;
  fromAgent: string;
  toAgent: string;
  initiatedAt: number;
  completedAt: number | null;
  state: 'pending' | 'accepted' | 'rejected' | 'timeout' | 'completed';
  fencingToken: number;
}

interface HeartbeatRecord {
  agentId: string;
  timestamps: number[];
  maxHistory: number;
}

interface LeaseEvent {
  type: 'lease_granted' | 'lease_renewed' | 'lease_expired' | 'lease_revoked' |
        'lease_transfer_initiated' | 'lease_transfer_completed' | 'lease_transfer_failed' |
        'read_delegated' | 'write_forwarded' | 'split_brain_detected' |
        'failure_detected' | 'conflict_resolved';
  timestamp: number;
  data: Record<string, unknown>;
}

// ─── LeaseGrantor ────────────────────────────────────────────────────────

class LeaseGrantor {
  private leases = new Map<string, Lease>();
  private resourceLeases = new Map<string, string>(); // resourceId -> leaseId
  private nextFencingToken = 1;
  private nextEpoch = 1;

  constructor(private config: LeaseConfig) {}

  grant(request: LeaseRequest, now: number): Lease | null {
    const existing = this.resourceLeases.get(request.resourceId);
    if (existing) {
      const lease = this.leases.get(existing);
      if (lease && lease.state === 'active' && lease.expiresAt > now + this.config.maxClockSkewMs) {
        return null; // Resource already leased
      }
      // Expired or expiring — can grant new
      if (lease) {
        lease.state = 'expired';
      }
    }

    const leaseId = `lease-${fnv1a(`${request.resourceId}-${request.requesterId}-${now}`).toString(16)}`;
    const lease: Lease = {
      id: leaseId,
      resourceId: request.resourceId,
      holderId: request.requesterId,
      grantedAt: now,
      expiresAt: now + this.config.leaseDurationMs,
      fencingToken: this.nextFencingToken++,
      epoch: this.nextEpoch++,
      renewalCount: 0,
      state: 'active',
      metadata: request.metadata || {},
    };

    this.leases.set(leaseId, lease);
    this.resourceLeases.set(request.resourceId, leaseId);
    return lease;
  }

  renew(leaseId: string, now: number): Lease | null {
    const lease = this.leases.get(leaseId);
    if (!lease || lease.state !== 'active') return null;

    const timeToExpiry = lease.expiresAt - now;
    if (timeToExpiry > this.config.renewalWindowMs) return null; // Too early

    if (lease.renewalCount >= this.config.maxConsecutiveRenewals) {
      // Force re-election after max renewals
      lease.state = 'expiring';
      return null;
    }

    lease.expiresAt = now + this.config.leaseDurationMs;
    lease.renewalCount++;
    lease.fencingToken = this.nextFencingToken++;
    return lease;
  }

  revoke(leaseId: string): boolean {
    const lease = this.leases.get(leaseId);
    if (!lease || lease.state === 'revoked' || lease.state === 'expired') return false;
    lease.state = 'revoked';
    return true;
  }

  getActiveLease(resourceId: string, now: number): Lease | null {
    const leaseId = this.resourceLeases.get(resourceId);
    if (!leaseId) return null;
    const lease = this.leases.get(leaseId);
    if (!lease || lease.state !== 'active') return null;
    if (lease.expiresAt <= now) {
      lease.state = 'expired';
      return null;
    }
    return lease;
  }

  getLease(leaseId: string): Lease | undefined {
    return this.leases.get(leaseId);
  }

  processExpiries(now: number): Lease[] {
    const expired: Lease[] = [];
    for (const lease of Array.from(this.leases.values())) {
      if (lease.state === 'active' && lease.expiresAt <= now) {
        lease.state = 'expired';
        expired.push(lease);
      } else if (lease.state === 'active') {
        const timeToExpiry = lease.expiresAt - now;
        if (timeToExpiry <= this.config.gracePeriodMs) {
          lease.state = 'expiring';
        }
      }
    }
    return expired;
  }

  getStats(): { total: number; active: number; expired: number; revoked: number } {
    let active = 0, expired = 0, revoked = 0;
    for (const l of Array.from(this.leases.values())) {
      if (l.state === 'active' || l.state === 'expiring') active++;
      else if (l.state === 'expired') expired++;
      else if (l.state === 'revoked') revoked++;
    }
    return { total: this.leases.size, active, expired, revoked };
  }
}

// ─── LeaseValidator ──────────────────────────────────────────────────────

class LeaseValidator {
  private clockOffsets = new Map<string, EWMATracker>();

  constructor(private config: LeaseConfig) {}

  isValid(lease: Lease, now: number): boolean {
    if (lease.state !== 'active' && lease.state !== 'expiring') return false;
    const adjustedNow = now + this.config.maxClockSkewMs;
    return lease.expiresAt > adjustedNow;
  }

  isValidForReads(lease: Lease, now: number): boolean {
    if (!this.config.readDelegationEnabled) return false;
    if (lease.state !== 'active') return false;
    // Conservative: reads need more margin than writes
    const readMargin = this.config.maxClockSkewMs * 2;
    return lease.expiresAt > now + readMargin;
  }

  validateFencingToken(lease: Lease, presentedToken: number): boolean {
    if (!this.config.fencingTokenEnabled) return true;
    return presentedToken >= lease.fencingToken;
  }

  updateClockOffset(agentId: string, localTime: number, remoteTime: number): void {
    let tracker = this.clockOffsets.get(agentId);
    if (!tracker) {
      tracker = new EWMATracker(0.1);
      this.clockOffsets.set(agentId, tracker);
    }
    tracker.update(remoteTime - localTime);
  }

  getEstimatedSkew(agentId: string): number {
    const tracker = this.clockOffsets.get(agentId);
    return tracker?.get() ?? 0;
  }

  getRemainingValidity(lease: Lease, now: number): number {
    return Math.max(0, lease.expiresAt - now - this.config.maxClockSkewMs);
  }
}

// ─── ReadDelegator ───────────────────────────────────────────────────────

interface ReadResult {
  success: boolean;
  data: unknown;
  fromCache: boolean;
  leaseValid: boolean;
  fencingToken: number;
}

class ReadDelegator {
  private readCache = new Map<string, { data: unknown; fencingToken: number; cachedAt: number }>();
  private stats = { delegated: 0, cached: 0, forwarded: 0, rejected: 0 };

  constructor(
    private config: LeaseConfig,
    private validator: LeaseValidator,
  ) {}

  handleRead(
    resourceId: string,
    lease: Lease | null,
    localAgentId: string,
    now: number,
    dataProvider: () => unknown,
  ): ReadResult {
    if (!lease) {
      this.stats.rejected++;
      return { success: false, data: null, fromCache: false, leaseValid: false, fencingToken: 0 };
    }

    // If we are the lease holder, serve directly
    if (lease.holderId === localAgentId && this.validator.isValid(lease, now)) {
      const data = dataProvider();
      this.readCache.set(resourceId, { data, fencingToken: lease.fencingToken, cachedAt: now });
      this.stats.delegated++;
      return { success: true, data, fromCache: false, leaseValid: true, fencingToken: lease.fencingToken };
    }

    // If read delegation is enabled and lease is valid for reads
    if (this.validator.isValidForReads(lease, now)) {
      const cached = this.readCache.get(resourceId);
      if (cached && cached.fencingToken === lease.fencingToken) {
        const age = now - cached.cachedAt;
        if (age < this.config.leaseDurationMs / 4) {
          this.stats.cached++;
          return { success: true, data: cached.data, fromCache: true, leaseValid: true, fencingToken: cached.fencingToken };
        }
      }
      // Forward to lease holder
      this.stats.forwarded++;
      return { success: false, data: null, fromCache: false, leaseValid: true, fencingToken: lease.fencingToken };
    }

    this.stats.rejected++;
    return { success: false, data: null, fromCache: false, leaseValid: false, fencingToken: 0 };
  }

  invalidateCache(resourceId: string): void {
    this.readCache.delete(resourceId);
  }

  invalidateAll(): void {
    this.readCache.clear();
  }

  getStats(): typeof this.stats {
    return { ...this.stats };
  }
}

// ─── WriteForwarder ──────────────────────────────────────────────────────

interface WriteResult {
  success: boolean;
  fencingToken: number;
  forwardedTo: string | null;
  error?: string;
}

class WriteForwarder {
  private pendingWrites = new Map<string, { resourceId: string; data: unknown; submittedAt: number; fencingToken: number }>();
  private stats = { local: 0, forwarded: 0, rejected: 0, stale: 0 };
  private writeLog: Array<{ resourceId: string; agentId: string; fencingToken: number; at: number }> = [];

  constructor(private config: LeaseConfig) {}

  handleWrite(
    resourceId: string,
    lease: Lease | null,
    localAgentId: string,
    fencingToken: number,
    now: number,
  ): WriteResult {
    if (!lease || (lease.state !== 'active' && lease.state !== 'expiring')) {
      this.stats.rejected++;
      return { success: false, fencingToken: 0, forwardedTo: null, error: 'no_valid_lease' };
    }

    if (lease.expiresAt <= now) {
      this.stats.rejected++;
      return { success: false, fencingToken: 0, forwardedTo: null, error: 'lease_expired' };
    }

    // Fencing token validation
    if (this.config.fencingTokenEnabled && fencingToken < lease.fencingToken) {
      this.stats.stale++;
      return { success: false, fencingToken: lease.fencingToken, forwardedTo: null, error: 'stale_fencing_token' };
    }

    if (lease.holderId === localAgentId) {
      // We are the lease holder — execute locally
      this.stats.local++;
      this.writeLog.push({ resourceId, agentId: localAgentId, fencingToken: lease.fencingToken, at: now });
      if (this.writeLog.length > 1000) this.writeLog.splice(0, 500);
      return { success: true, fencingToken: lease.fencingToken, forwardedTo: null };
    }

    // Forward to lease holder
    this.stats.forwarded++;
    return { success: true, fencingToken: lease.fencingToken, forwardedTo: lease.holderId };
  }

  getRecentWrites(resourceId: string, limit: number = 10): typeof this.writeLog {
    return this.writeLog.filter(w => w.resourceId === resourceId).slice(-limit);
  }

  getStats(): typeof this.stats {
    return { ...this.stats };
  }
}

// ─── LeaseTransferProtocol ───────────────────────────────────────────────

class LeaseTransferProtocol {
  private transfers = new Map<string, LeaseTransfer>();
  private transferHistory: LeaseTransfer[] = [];

  constructor(private config: LeaseConfig) {}

  initiateTransfer(lease: Lease, toAgent: string, now: number): LeaseTransfer | null {
    if (lease.state !== 'active' && lease.state !== 'expiring') return null;

    // Check no pending transfer for this lease
    for (const t of Array.from(this.transfers.values())) {
      if (t.leaseId === lease.id && t.state === 'pending') return null;
    }

    const transferId = `xfer-${fnv1a(`${lease.id}-${toAgent}-${now}`).toString(16)}`;
    const transfer: LeaseTransfer = {
      leaseId: lease.id,
      fromAgent: lease.holderId,
      toAgent,
      initiatedAt: now,
      completedAt: null,
      state: 'pending',
      fencingToken: lease.fencingToken,
    };

    lease.state = 'transferring';
    this.transfers.set(transferId, transfer);
    return transfer;
  }

  acceptTransfer(transferId: string, now: number): boolean {
    const transfer = this.transfers.get(transferId);
    if (!transfer || transfer.state !== 'pending') return false;
    transfer.state = 'accepted';
    return true;
  }

  completeTransfer(transferId: string, grantor: LeaseGrantor, now: number): Lease | null {
    const transfer = this.transfers.get(transferId);
    if (!transfer || transfer.state !== 'accepted') return null;

    // Revoke old lease
    grantor.revoke(transfer.leaseId);

    // Grant new lease to target
    const newLease = grantor.grant({
      resourceId: grantor.getLease(transfer.leaseId)?.resourceId || '',
      requesterId: transfer.toAgent,
      priority: 0,
    }, now);

    if (newLease) {
      transfer.state = 'completed';
      transfer.completedAt = now;
      this.archiveTransfer(transferId);
      return newLease;
    }

    transfer.state = 'rejected';
    this.archiveTransfer(transferId);
    return null;
  }

  rejectTransfer(transferId: string): boolean {
    const transfer = this.transfers.get(transferId);
    if (!transfer || transfer.state !== 'pending') return false;
    transfer.state = 'rejected';
    this.archiveTransfer(transferId);
    return true;
  }

  processTimeouts(now: number): LeaseTransfer[] {
    const timedOut: LeaseTransfer[] = [];
    for (const [id, transfer] of Array.from(this.transfers)) {
      if (transfer.state === 'pending' && now - transfer.initiatedAt > this.config.transferTimeoutMs) {
        transfer.state = 'timeout';
        timedOut.push(transfer);
        this.archiveTransfer(id);
      }
    }
    return timedOut;
  }

  private archiveTransfer(id: string): void {
    const transfer = this.transfers.get(id);
    if (transfer) {
      this.transferHistory.push(transfer);
      this.transfers.delete(id);
      if (this.transferHistory.length > 200) {
        this.transferHistory.splice(0, 100);
      }
    }
  }

  getPendingTransfers(): LeaseTransfer[] {
    return Array.from(this.transfers.values()).filter(t => t.state === 'pending');
  }

  getTransferStats(): { total: number; completed: number; rejected: number; timedOut: number } {
    let completed = 0, rejected = 0, timedOut = 0;
    for (const t of Array.from(this.transferHistory)) {
      if (t.state === 'completed') completed++;
      else if (t.state === 'rejected') rejected++;
      else if (t.state === 'timeout') timedOut++;
    }
    return { total: this.transferHistory.length, completed, rejected, timedOut };
  }
}

// ─── FailureDetector ─────────────────────────────────────────────────────

class FailureDetector {
  private heartbeats = new Map<string, HeartbeatRecord>();
  private suspicionLevels = new Map<string, number>();
  private latencyStats = new Map<string, WelfordStats>();

  constructor(private config: LeaseConfig) {}

  recordHeartbeat(agentId: string, now: number): void {
    let record = this.heartbeats.get(agentId);
    if (!record) {
      record = { agentId, timestamps: [], maxHistory: 100 };
      this.heartbeats.set(agentId, record);
    }
    record.timestamps.push(now);
    if (record.timestamps.length > record.maxHistory) {
      record.timestamps.shift();
    }

    // Update latency stats
    if (record.timestamps.length >= 2) {
      const interval = record.timestamps[record.timestamps.length - 1] - record.timestamps[record.timestamps.length - 2];
      let stats = this.latencyStats.get(agentId);
      if (!stats) {
        stats = new WelfordStats();
        this.latencyStats.set(agentId, stats);
      }
      stats.update(interval);
    }

    this.suspicionLevels.set(agentId, 0);
  }

  /**
   * Phi-accrual failure detection.
   * Returns suspicion level (phi). Higher = more suspicious.
   * phi > threshold indicates likely failure.
   */
  computePhi(agentId: string, now: number): number {
    const record = this.heartbeats.get(agentId);
    if (!record || record.timestamps.length < 2) return 0;

    const lastHeartbeat = record.timestamps[record.timestamps.length - 1];
    const timeSinceLast = now - lastHeartbeat;

    const stats = this.latencyStats.get(agentId);
    if (!stats || stats.count() < 2) return 0;

    const mean = stats.mean();
    const std = Math.max(stats.stddev(), 1); // Prevent division by zero

    // Phi = -log10(1 - F(timeSinceLast))
    // where F is the CDF of the normal distribution
    // Approximate using error function
    const y = (timeSinceLast - mean) / (std * Math.SQRT2);
    const t = 1 / (1 + 0.3275911 * Math.abs(y));
    const erf = 1 - (0.254829592 * t - 0.284496736 * t * t + 1.421413741 * t * t * t
      - 1.453152027 * t * t * t * t + 1.061405429 * t * t * t * t * t) * Math.exp(-y * y);
    const cdf = 0.5 * (1 + (y >= 0 ? erf : -erf));

    const phi = cdf >= 1 ? 16 : -Math.log10(1 - cdf);
    this.suspicionLevels.set(agentId, phi);
    return phi;
  }

  isSuspected(agentId: string, now: number): boolean {
    return this.computePhi(agentId, now) > this.config.phiThreshold;
  }

  getAgentHealth(agentId: string, now: number): {
    phi: number;
    suspected: boolean;
    lastHeartbeat: number | null;
    avgInterval: number | null;
  } {
    const phi = this.computePhi(agentId, now);
    const record = this.heartbeats.get(agentId);
    const stats = this.latencyStats.get(agentId);
    return {
      phi,
      suspected: phi > this.config.phiThreshold,
      lastHeartbeat: record?.timestamps[record.timestamps.length - 1] ?? null,
      avgInterval: stats?.mean() ?? null,
    };
  }

  getFailedAgents(now: number): string[] {
    const failed: string[] = [];
    for (const agentId of Array.from(this.heartbeats.keys())) {
      if (this.isSuspected(agentId, now)) {
        failed.push(agentId);
      }
    }
    return failed;
  }

  removeAgent(agentId: string): void {
    this.heartbeats.delete(agentId);
    this.suspicionLevels.delete(agentId);
    this.latencyStats.delete(agentId);
  }
}

// ─── SplitBrainGuard ─────────────────────────────────────────────────────

interface FencingRecord {
  resourceId: string;
  token: number;
  holderId: string;
  observedAt: number;
}

class SplitBrainGuard {
  private fencingHistory = new Map<string, FencingRecord[]>();
  private detections: Array<{ resourceId: string; claimants: string[]; tokens: number[]; at: number }> = [];

  constructor(private maxHistoryPerResource: number = 50) {}

  recordFencingToken(resourceId: string, token: number, holderId: string, now: number): void {
    let history = this.fencingHistory.get(resourceId);
    if (!history) {
      history = [];
      this.fencingHistory.set(resourceId, history);
    }
    history.push({ resourceId, token, holderId, observedAt: now });
    if (history.length > this.maxHistoryPerResource) {
      history.splice(0, history.length - this.maxHistoryPerResource);
    }
  }

  /**
   * Detect split-brain: multiple agents claiming the same resource
   * with overlapping validity windows
   */
  detectSplitBrain(resourceId: string, windowMs: number, now: number): {
    detected: boolean;
    claimants: string[];
    latestToken: number;
  } {
    const history = this.fencingHistory.get(resourceId);
    if (!history) return { detected: false, claimants: [], latestToken: 0 };

    const recentCutoff = now - windowMs;
    const recentRecords = history.filter(r => r.observedAt >= recentCutoff);

    // Group by holder
    const holderTokens = new Map<string, number>();
    let maxToken = 0;
    for (const record of recentRecords) {
      const existing = holderTokens.get(record.holderId) ?? 0;
      holderTokens.set(record.holderId, Math.max(existing, record.token));
      maxToken = Math.max(maxToken, record.token);
    }

    const claimants = Array.from(holderTokens.keys());
    const detected = claimants.length > 1;

    if (detected) {
      this.detections.push({
        resourceId,
        claimants,
        tokens: Array.from(holderTokens.values()),
        at: now,
      });
      if (this.detections.length > 100) {
        this.detections.splice(0, 50);
      }
    }

    return { detected, claimants, latestToken: maxToken };
  }

  /**
   * Validate that a write operation uses a current fencing token
   */
  validateWrite(resourceId: string, presentedToken: number): boolean {
    const history = this.fencingHistory.get(resourceId);
    if (!history || history.length === 0) return true;
    const latest = history[history.length - 1];
    return presentedToken >= latest.token;
  }

  getDetectionHistory(limit: number = 20): typeof this.detections {
    return this.detections.slice(-limit);
  }
}

// ─── LeaseConflictResolver ───────────────────────────────────────────────

type ConflictStrategy = 'highest_token' | 'earliest_grant' | 'priority_based' | 'epoch_based';

interface ConflictResolution {
  winnerId: string;
  loserId: string;
  strategy: ConflictStrategy;
  reason: string;
}

class LeaseConflictResolver {
  private resolutionHistory: ConflictResolution[] = [];
  private agentPriorities = new Map<string, number>();

  constructor(private defaultStrategy: ConflictStrategy = 'highest_token') {}

  setAgentPriority(agentId: string, priority: number): void {
    this.agentPriorities.set(agentId, priority);
  }

  resolve(leaseA: Lease, leaseB: Lease): ConflictResolution {
    let winner: Lease;
    let loser: Lease;
    let reason: string;

    switch (this.defaultStrategy) {
      case 'highest_token':
        if (leaseA.fencingToken >= leaseB.fencingToken) {
          winner = leaseA; loser = leaseB;
        } else {
          winner = leaseB; loser = leaseA;
        }
        reason = `Fencing token ${winner.fencingToken} > ${loser.fencingToken}`;
        break;

      case 'earliest_grant':
        if (leaseA.grantedAt <= leaseB.grantedAt) {
          winner = leaseA; loser = leaseB;
        } else {
          winner = leaseB; loser = leaseA;
        }
        reason = `Granted at ${winner.grantedAt} <= ${loser.grantedAt}`;
        break;

      case 'priority_based': {
        const prioA = this.agentPriorities.get(leaseA.holderId) ?? 0;
        const prioB = this.agentPriorities.get(leaseB.holderId) ?? 0;
        if (prioA >= prioB) {
          winner = leaseA; loser = leaseB;
        } else {
          winner = leaseB; loser = leaseA;
        }
        const wp = this.agentPriorities.get(winner.holderId) ?? 0;
        const lp = this.agentPriorities.get(loser.holderId) ?? 0;
        reason = `Priority ${wp} >= ${lp}`;
        break;
      }

      case 'epoch_based':
        if (leaseA.epoch >= leaseB.epoch) {
          winner = leaseA; loser = leaseB;
        } else {
          winner = leaseB; loser = leaseA;
        }
        reason = `Epoch ${winner.epoch} >= ${loser.epoch}`;
        break;

      default:
        winner = leaseA.fencingToken >= leaseB.fencingToken ? leaseA : leaseB;
        loser = winner === leaseA ? leaseB : leaseA;
        reason = 'Default: highest fencing token';
    }

    const resolution: ConflictResolution = {
      winnerId: winner.holderId,
      loserId: loser.holderId,
      strategy: this.defaultStrategy,
      reason,
    };

    this.resolutionHistory.push(resolution);
    if (this.resolutionHistory.length > 200) {
      this.resolutionHistory.splice(0, 100);
    }

    return resolution;
  }

  getResolutionHistory(limit: number = 20): ConflictResolution[] {
    return this.resolutionHistory.slice(-limit);
  }

  getResolutionStats(): Record<ConflictStrategy, number> {
    const stats: Record<string, number> = {};
    for (const r of Array.from(this.resolutionHistory)) {
      stats[r.strategy] = (stats[r.strategy] || 0) + 1;
    }
    return stats as Record<ConflictStrategy, number>;
  }
}

// ─── WaitQueue ───────────────────────────────────────────────────────────

interface WaitEntry {
  request: LeaseRequest;
  enqueuedAt: number;
  timeoutAt: number;
}

class LeaseWaitQueue {
  private queues = new Map<string, WaitEntry[]>();

  enqueue(request: LeaseRequest, now: number, timeoutMs: number): void {
    let queue = this.queues.get(request.resourceId);
    if (!queue) {
      queue = [];
      this.queues.set(request.resourceId, queue);
    }
    queue.push({ request, enqueuedAt: now, timeoutAt: now + timeoutMs });
    // Sort by priority descending, then FIFO
    queue.sort((a, b) => {
      if (b.request.priority !== a.request.priority) return b.request.priority - a.request.priority;
      return a.enqueuedAt - b.enqueuedAt;
    });
  }

  dequeueNext(resourceId: string): WaitEntry | null {
    const queue = this.queues.get(resourceId);
    if (!queue || queue.length === 0) return null;
    return queue.shift() || null;
  }

  processTimeouts(now: number): WaitEntry[] {
    const timedOut: WaitEntry[] = [];
    for (const [resourceId, queue] of Array.from(this.queues)) {
      const remaining: WaitEntry[] = [];
      for (const entry of queue) {
        if (entry.timeoutAt <= now) {
          timedOut.push(entry);
        } else {
          remaining.push(entry);
        }
      }
      if (remaining.length === 0) {
        this.queues.delete(resourceId);
      } else {
        this.queues.set(resourceId, remaining);
      }
    }
    return timedOut;
  }

  getQueueLength(resourceId: string): number {
    return this.queues.get(resourceId)?.length ?? 0;
  }

  getPosition(resourceId: string, requesterId: string): number {
    const queue = this.queues.get(resourceId);
    if (!queue) return -1;
    return queue.findIndex(e => e.request.requesterId === requesterId);
  }
}

// ─── LeaseConsensusProtocol ──────────────────────────────────────────────

interface ProtocolPreset {
  name: string;
  description: string;
  config: LeaseConfig;
  conflictStrategy: ConflictStrategy;
}

const PRESETS: Record<string, ProtocolPreset> = {
  'fast-reads': {
    name: 'Fast Reads',
    description: 'Optimized for read-heavy workloads with aggressive caching',
    config: {
      leaseDurationMs: 10000,
      renewalWindowMs: 3000,
      maxClockSkewMs: 500,
      gracePeriodMs: 2000,
      maxConsecutiveRenewals: 20,
      fencingTokenEnabled: true,
      readDelegationEnabled: true,
      phiThreshold: 8,
      heartbeatIntervalMs: 1000,
      transferTimeoutMs: 5000,
    },
    conflictStrategy: 'highest_token',
  },
  'strong-consistency': {
    name: 'Strong Consistency',
    description: 'Conservative leases with strict fencing for write-heavy workloads',
    config: {
      leaseDurationMs: 5000,
      renewalWindowMs: 2000,
      maxClockSkewMs: 200,
      gracePeriodMs: 1000,
      maxConsecutiveRenewals: 5,
      fencingTokenEnabled: true,
      readDelegationEnabled: false,
      phiThreshold: 6,
      heartbeatIntervalMs: 500,
      transferTimeoutMs: 3000,
    },
    conflictStrategy: 'epoch_based',
  },
  'high-availability': {
    name: 'High Availability',
    description: 'Long leases with fast failover for stable environments',
    config: {
      leaseDurationMs: 30000,
      renewalWindowMs: 10000,
      maxClockSkewMs: 1000,
      gracePeriodMs: 5000,
      maxConsecutiveRenewals: 50,
      fencingTokenEnabled: true,
      readDelegationEnabled: true,
      phiThreshold: 10,
      heartbeatIntervalMs: 2000,
      transferTimeoutMs: 10000,
    },
    conflictStrategy: 'priority_based',
  },
};

class LeaseConsensusProtocol {
  private grantor: LeaseGrantor;
  private validator: LeaseValidator;
  private readDelegator: ReadDelegator;
  private writeForwarder: WriteForwarder;
  private transferProtocol: LeaseTransferProtocol;
  private failureDetector: FailureDetector;
  private splitBrainGuard: SplitBrainGuard;
  private conflictResolver: LeaseConflictResolver;
  private waitQueue: LeaseWaitQueue;
  private events: LeaseEvent[] = [];
  private config: LeaseConfig;
  private localAgentId: string;
  private tickCount = 0;

  constructor(localAgentId: string, preset: string = 'fast-reads') {
    const p = PRESETS[preset] || PRESETS['fast-reads'];
    this.config = { ...p.config };
    this.localAgentId = localAgentId;

    this.grantor = new LeaseGrantor(this.config);
    this.validator = new LeaseValidator(this.config);
    this.readDelegator = new ReadDelegator(this.config, this.validator);
    this.writeForwarder = new WriteForwarder(this.config);
    this.transferProtocol = new LeaseTransferProtocol(this.config);
    this.failureDetector = new FailureDetector(this.config);
    this.splitBrainGuard = new SplitBrainGuard();
    this.conflictResolver = new LeaseConflictResolver(p.conflictStrategy);
    this.waitQueue = new LeaseWaitQueue();
  }

  // ─── Core Operations ────────────────────────────────────────────────

  acquireLease(resourceId: string, priority: number = 0, now: number = Date.now()): Lease | null {
    const request: LeaseRequest = { resourceId, requesterId: this.localAgentId, priority };
    const lease = this.grantor.grant(request, now);

    if (lease) {
      this.splitBrainGuard.recordFencingToken(resourceId, lease.fencingToken, this.localAgentId, now);
      this.emit({ type: 'lease_granted', timestamp: now, data: { leaseId: lease.id, resourceId, holder: this.localAgentId } });
      return lease;
    }

    // Enqueue if resource is taken
    this.waitQueue.enqueue(request, now, this.config.leaseDurationMs * 2);
    return null;
  }

  renewLease(leaseId: string, now: number = Date.now()): Lease | null {
    const renewed = this.grantor.renew(leaseId, now);
    if (renewed) {
      this.splitBrainGuard.recordFencingToken(renewed.resourceId, renewed.fencingToken, renewed.holderId, now);
      this.emit({ type: 'lease_renewed', timestamp: now, data: { leaseId, fencingToken: renewed.fencingToken } });
    }
    return renewed;
  }

  read(resourceId: string, dataProvider: () => unknown, now: number = Date.now()): ReadResult {
    const lease = this.grantor.getActiveLease(resourceId, now);
    const result = this.readDelegator.handleRead(resourceId, lease, this.localAgentId, now, dataProvider);
    if (result.success) {
      this.emit({ type: 'read_delegated', timestamp: now, data: { resourceId, fromCache: result.fromCache } });
    }
    return result;
  }

  write(resourceId: string, fencingToken: number, now: number = Date.now()): WriteResult {
    const lease = this.grantor.getActiveLease(resourceId, now);
    const result = this.writeForwarder.handleWrite(resourceId, lease, this.localAgentId, fencingToken, now);
    if (result.success) {
      this.readDelegator.invalidateCache(resourceId);
      this.emit({ type: 'write_forwarded', timestamp: now, data: { resourceId, forwardedTo: result.forwardedTo } });
    }
    return result;
  }

  transferLease(leaseId: string, toAgent: string, now: number = Date.now()): boolean {
    const lease = this.grantor.getLease(leaseId);
    if (!lease) return false;

    const transfer = this.transferProtocol.initiateTransfer(lease, toAgent, now);
    if (transfer) {
      this.emit({ type: 'lease_transfer_initiated', timestamp: now, data: { leaseId, from: lease.holderId, to: toAgent } });
      return true;
    }
    return false;
  }

  reportHeartbeat(agentId: string, now: number = Date.now()): void {
    this.failureDetector.recordHeartbeat(agentId, now);
  }

  // ─── Tick ───────────────────────────────────────────────────────────

  tick(now: number = Date.now()): void {
    this.tickCount++;

    // Phase 1: Process lease expiries
    const expired = this.grantor.processExpiries(now);
    for (const lease of expired) {
      this.readDelegator.invalidateCache(lease.resourceId);
      this.emit({ type: 'lease_expired', timestamp: now, data: { leaseId: lease.id, resourceId: lease.resourceId } });

      // Process wait queue
      const next = this.waitQueue.dequeueNext(lease.resourceId);
      if (next) {
        const newLease = this.grantor.grant(next.request, now);
        if (newLease) {
          this.splitBrainGuard.recordFencingToken(lease.resourceId, newLease.fencingToken, next.request.requesterId, now);
          this.emit({ type: 'lease_granted', timestamp: now, data: { leaseId: newLease.id, resourceId: lease.resourceId, holder: next.request.requesterId, fromQueue: true } });
        }
      }
    }

    // Phase 2: Detect failures
    const failed = this.failureDetector.getFailedAgents(now);
    for (const agentId of failed) {
      this.emit({ type: 'failure_detected', timestamp: now, data: { agentId } });
    }

    // Phase 3: Process transfer timeouts
    const timedOutTransfers = this.transferProtocol.processTimeouts(now);
    for (const transfer of timedOutTransfers) {
      this.emit({ type: 'lease_transfer_failed', timestamp: now, data: { leaseId: transfer.leaseId, reason: 'timeout' } });
      // Restore lease state
      const lease = this.grantor.getLease(transfer.leaseId);
      if (lease && lease.state === 'transferring') {
        lease.state = lease.expiresAt > now ? 'active' : 'expired';
      }
    }

    // Phase 4: Split-brain detection (every 5 ticks)
    if (this.tickCount % 5 === 0) {
      const leaseStats = this.grantor.getStats();
      // Check resources for split-brain
      // (In practice, this would check specific resources under contention)
    }

    // Phase 5: Wait queue timeouts
    const timedOutWaits = this.waitQueue.processTimeouts(now);

    // Phase 6: Prune events
    if (this.events.length > 2000) {
      this.events.splice(0, 1000);
    }
  }

  // ─── Queries ────────────────────────────────────────────────────────

  getLeaseInfo(resourceId: string, now: number = Date.now()): {
    lease: Lease | null;
    valid: boolean;
    remaining: number;
    queueLength: number;
  } {
    const lease = this.grantor.getActiveLease(resourceId, now);
    return {
      lease,
      valid: lease ? this.validator.isValid(lease, now) : false,
      remaining: lease ? this.validator.getRemainingValidity(lease, now) : 0,
      queueLength: this.waitQueue.getQueueLength(resourceId),
    };
  }

  getAgentHealth(agentId: string, now: number = Date.now()) {
    return this.failureDetector.getAgentHealth(agentId, now);
  }

  dashboard(now: number = Date.now()): Record<string, unknown> {
    return {
      leases: this.grantor.getStats(),
      reads: this.readDelegator.getStats(),
      writes: this.writeForwarder.getStats(),
      transfers: this.transferProtocol.getTransferStats(),
      conflicts: this.conflictResolver.getResolutionStats(),
      splitBrainDetections: this.splitBrainGuard.getDetectionHistory(5),
      failedAgents: this.failureDetector.getFailedAgents(now),
      tickCount: this.tickCount,
      recentEvents: this.events.slice(-10),
    };
  }

  // ─── Internal ───────────────────────────────────────────────────────

  private emit(event: LeaseEvent): void {
    this.events.push(event);
  }

  getEvents(since?: number): LeaseEvent[] {
    if (since === undefined) return [...this.events];
    return this.events.filter(e => e.timestamp >= since);
  }

  static getPresets(): Record<string, { name: string; description: string }> {
    const result: Record<string, { name: string; description: string }> = {};
    for (const [key, preset] of Object.entries(PRESETS)) {
      result[key] = { name: preset.name, description: preset.description };
    }
    return result;
  }
}

export {
  LeaseConsensusProtocol,
  LeaseGrantor,
  LeaseValidator,
  ReadDelegator,
  WriteForwarder,
  LeaseTransferProtocol,
  FailureDetector,
  SplitBrainGuard,
  LeaseConflictResolver,
  LeaseWaitQueue,
  PRESETS,
};
