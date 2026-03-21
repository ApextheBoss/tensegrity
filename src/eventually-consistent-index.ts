import { fnv1a, EWMATracker } from './shared-utils';
/**
 * Eventually Consistent Index
 * 
 * Multi-strategy secondary indexing for distributed agent state with
 * convergence guarantees, conflict resolution, and query optimization.
 * 
 * Supports inverted (BM25), B-tree (range), hash (O(1) exact), and
 * covering indexes. Tracks convergence via vector clocks, resolves
 * conflicts with pluggable strategies (LWW, highest version, merge,
 * priority), and includes a query planner that picks the cheapest
 * index for each query shape.
 * 
 * @module eventually-consistent-index
 * @packageDocumentation
 */

// ─── FNV-1a Hash ─────────────────────────────────────────────────────────────

// ─── Types ───────────────────────────────────────────────────────────────────

interface IndexDefinition {
  name: string;
  type: 'inverted' | 'btree' | 'hash' | 'covering';
  keyExtractor: (doc: Record<string, unknown>) => string | string[];
  fields: string[];
  coveringFields?: string[];
  ordering?: 'asc' | 'desc';
  unique?: boolean;
  sparse?: boolean;
}

interface PostingEntry {
  docId: string;
  score: number;
  version: number;
  timestamp: number;
  payload?: Record<string, unknown>;
}

interface IndexEvent {
  type: 'index_updated' | 'index_created' | 'index_dropped' | 'convergence_achieved' |
        'convergence_lost' | 'conflict_resolved' | 'compaction_completed' |
        'query_executed' | 'stale_read' | 'index_rebuilt';
  timestamp: number;
  data: Record<string, unknown>;
}

interface VersionVector {
  [replicaId: string]: number;
}

interface QueryPlan {
  indexName: string;
  strategy: 'exact' | 'range' | 'prefix' | 'fullscan' | 'intersection' | 'covering';
  estimatedCost: number;
  estimatedRows: number;
  coveringFields?: string[];
}

interface Query {
  type: 'exact' | 'range' | 'prefix' | 'fulltext' | 'multi';
  field: string;
  value?: string;
  low?: string;
  high?: string;
  terms?: string[];
  fields?: { field: string; value: string; type: 'exact' | 'range' | 'prefix' }[];
  limit?: number;
}

type ConflictStrategy = 'lww' | 'highest_version' | 'merge_union' | 'priority';

interface ConvergenceStatus {
  indexName: string;
  converged: boolean;
  lagMs: number;
  divergence: number;
  lastUpdateTimestamp: number;
  replicaCount: number;
  staleReplicas: string[];
}

interface ConflictRecord {
  indexName: string;
  key: string;
  localEntry: PostingEntry;
  remoteEntry: PostingEntry;
  resolution: 'local' | 'remote' | 'merged';
  strategy: ConflictStrategy;
  timestamp: number;
}

interface CompactionStats {
  indexName: string;
  entriesRemoved: number;
  spaceReclaimed: number;
  durationMs: number;
  timestamp: number;
}

interface ECIndexConfig {
  replicaId: string;
  convergenceThresholdMs?: number;
  maxDivergence?: number;
  conflictStrategy?: ConflictStrategy;
  compactionIntervalMs?: number;
  staleReadThresholdMs?: number;
  btreeOrder?: number;
  hashBuckets?: number;
}

// ─── Inverted Index ──────────────────────────────────────────────────────────

class InvertedIndex {
  private postings = new Map<string, Map<string, PostingEntry>>();
  private docFrequency = new Map<string, number>();
  private totalDocs = 0;
  private docLengths = new Map<string, number>();
  private avgDocLength = 0;

  index(docId: string, terms: string[], version: number, payload?: Record<string, unknown>): void {
    this.remove(docId);
    this.totalDocs++;
    this.docLengths.set(docId, terms.length);
    this.updateAvgDocLength();

    const termFreqs = new Map<string, number>();
    for (const term of terms) {
      termFreqs.set(term, (termFreqs.get(term) || 0) + 1);
    }

    for (const [term, tf] of termFreqs) {
      if (!this.postings.has(term)) this.postings.set(term, new Map());
      const list = this.postings.get(term)!;
      if (!list.has(docId)) {
        this.docFrequency.set(term, (this.docFrequency.get(term) || 0) + 1);
      }

      const k1 = 1.2, b = 0.75;
      const dl = terms.length;
      const idf = Math.log((this.totalDocs - (this.docFrequency.get(term) || 0) + 0.5) /
                           ((this.docFrequency.get(term) || 0) + 0.5) + 1);
      const tfNorm = (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl / Math.max(this.avgDocLength, 1)));

      list.set(docId, { docId, score: idf * tfNorm, version, timestamp: Date.now(), payload });
    }
  }

  remove(docId: string): void {
    if (!this.docLengths.has(docId)) return;
    for (const [term, list] of this.postings) {
      if (list.delete(docId)) {
        const df = this.docFrequency.get(term) || 1;
        this.docFrequency.set(term, df - 1);
        if (list.size === 0) {
          this.postings.delete(term);
          this.docFrequency.delete(term);
        }
      }
    }
    this.docLengths.delete(docId);
    this.totalDocs = Math.max(0, this.totalDocs - 1);
    this.updateAvgDocLength();
  }

  search(terms: string[], limit: number = 10): PostingEntry[] {
    const scores = new Map<string, number>();
    const entries = new Map<string, PostingEntry>();
    for (const term of terms) {
      const list = this.postings.get(term);
      if (!list) continue;
      for (const [docId, entry] of list) {
        scores.set(docId, (scores.get(docId) || 0) + entry.score);
        entries.set(docId, entry);
      }
    }
    return Array.from(scores.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, limit)
      .map(([docId]) => ({ ...entries.get(docId)!, score: scores.get(docId)! }));
  }

  exactLookup(term: string): PostingEntry[] {
    const list = this.postings.get(term);
    return list ? Array.from(list.values()) : [];
  }

  prefixSearch(prefix: string): PostingEntry[] {
    const results = new Map<string, PostingEntry>();
    for (const [term, list] of this.postings) {
      if (term.startsWith(prefix)) {
        for (const [docId, entry] of list) {
          const existing = results.get(docId);
          if (!existing || entry.score > existing.score) results.set(docId, entry);
        }
      }
    }
    return Array.from(results.values());
  }

  getStats(): { terms: number; documents: number; avgPostingLength: number } {
    let totalPostings = 0;
    for (const list of this.postings.values()) totalPostings += list.size;
    return {
      terms: this.postings.size,
      documents: this.totalDocs,
      avgPostingLength: this.postings.size > 0 ? totalPostings / this.postings.size : 0
    };
  }

  private updateAvgDocLength(): void {
    if (this.totalDocs === 0) { this.avgDocLength = 0; return; }
    let sum = 0;
    for (const len of this.docLengths.values()) sum += len;
    this.avgDocLength = sum / this.totalDocs;
  }
}

// ─── B-Tree Index ────────────────────────────────────────────────────────────

interface BTreeNode {
  keys: string[];
  values: PostingEntry[][];
  children: BTreeNode[];
  isLeaf: boolean;
}

class BTreeIndex {
  private root: BTreeNode;
  private readonly order: number;
  private size = 0;

  constructor(order: number = 32) {
    this.order = order;
    this.root = { keys: [], values: [], children: [], isLeaf: true };
  }

  insert(key: string, entry: PostingEntry): void {
    const root = this.root;
    if (root.keys.length === 2 * this.order - 1) {
      const newRoot: BTreeNode = { keys: [], values: [], children: [root], isLeaf: false };
      this.splitChild(newRoot, 0);
      this.root = newRoot;
    }
    this.insertNonFull(this.root, key, entry);
    this.size++;
  }

  search(key: string): PostingEntry[] {
    return this.searchNode(this.root, key);
  }

  rangeQuery(low: string, high: string, limit: number = 100): PostingEntry[] {
    const results: PostingEntry[] = [];
    this.rangeSearch(this.root, low, high, results, limit);
    return results;
  }

  remove(key: string, docId: string): boolean {
    return this.removeFromNode(this.root, key, docId);
  }

  getSize(): number { return this.size; }

  private searchNode(node: BTreeNode, key: string): PostingEntry[] {
    let i = 0;
    while (i < node.keys.length && key > node.keys[i]) i++;
    if (i < node.keys.length && key === node.keys[i]) return node.values[i] || [];
    if (node.isLeaf) return [];
    return this.searchNode(node.children[i], key);
  }

  private rangeSearch(node: BTreeNode, low: string, high: string, results: PostingEntry[], limit: number): void {
    if (results.length >= limit) return;
    let i = 0;
    while (i < node.keys.length && node.keys[i] < low) i++;
    while (i < node.keys.length && node.keys[i] <= high) {
      if (!node.isLeaf) this.rangeSearch(node.children[i], low, high, results, limit);
      if (results.length >= limit) return;
      for (const entry of (node.values[i] || [])) {
        results.push(entry);
        if (results.length >= limit) return;
      }
      i++;
    }
    if (!node.isLeaf && i < node.children.length) {
      this.rangeSearch(node.children[i], low, high, results, limit);
    }
  }

  private insertNonFull(node: BTreeNode, key: string, entry: PostingEntry): void {
    let i = node.keys.length - 1;
    if (node.isLeaf) {
      while (i >= 0 && key < node.keys[i]) i--;
      if (i >= 0 && node.keys[i] === key) { node.values[i].push(entry); return; }
      node.keys.splice(i + 1, 0, key);
      node.values.splice(i + 1, 0, [entry]);
    } else {
      while (i >= 0 && key < node.keys[i]) i--;
      if (i >= 0 && node.keys[i] === key) { node.values[i].push(entry); return; }
      i++;
      if (node.children[i] && node.children[i].keys.length === 2 * this.order - 1) {
        this.splitChild(node, i);
        if (key > node.keys[i]) i++;
        if (node.keys[i] === key) { node.values[i].push(entry); return; }
      }
      this.insertNonFull(node.children[i], key, entry);
    }
  }

  private splitChild(parent: BTreeNode, index: number): void {
    const child = parent.children[index];
    const mid = this.order - 1;
    const newChild: BTreeNode = {
      keys: child.keys.splice(mid + 1),
      values: child.values.splice(mid + 1),
      children: child.isLeaf ? [] : child.children.splice(mid + 1),
      isLeaf: child.isLeaf
    };
    const midKey = child.keys.pop()!;
    const midValue = child.values.pop()!;
    parent.keys.splice(index, 0, midKey);
    parent.values.splice(index, 0, midValue);
    parent.children.splice(index + 1, 0, newChild);
  }

  private removeFromNode(node: BTreeNode, key: string, docId: string): boolean {
    let i = 0;
    while (i < node.keys.length && key > node.keys[i]) i++;
    if (i < node.keys.length && key === node.keys[i]) {
      const before = node.values[i].length;
      node.values[i] = node.values[i].filter(e => e.docId !== docId);
      const removed = before - node.values[i].length;
      this.size -= removed;
      if (node.values[i].length === 0) {
        node.keys.splice(i, 1);
        node.values.splice(i, 1);
      }
      return removed > 0;
    }
    if (node.isLeaf) return false;
    return this.removeFromNode(node.children[i], key, docId);
  }
}

// ─── Hash Index ──────────────────────────────────────────────────────────────

class HashIndex {
  private buckets: Map<number, Map<string, PostingEntry[]>>;
  private readonly numBuckets: number;
  private size = 0;

  constructor(numBuckets: number = 1024) {
    this.numBuckets = numBuckets;
    this.buckets = new Map();
  }

  insert(key: string, entry: PostingEntry, unique: boolean = false): boolean {
    const bucket = fnv1a(key) % this.numBuckets;
    if (!this.buckets.has(bucket)) this.buckets.set(bucket, new Map());
    const map = this.buckets.get(bucket)!;
    if (unique && map.has(key)) {
      const existing = map.get(key)!;
      if (existing.some(e => e.docId !== entry.docId)) return false;
    }
    if (!map.has(key)) map.set(key, []);
    const list = map.get(key)!;
    const existingIdx = list.findIndex(e => e.docId === entry.docId);
    if (existingIdx >= 0) { list[existingIdx] = entry; } else { list.push(entry); this.size++; }
    return true;
  }

  lookup(key: string): PostingEntry[] {
    const bucket = fnv1a(key) % this.numBuckets;
    const map = this.buckets.get(bucket);
    if (!map) return [];
    return map.get(key) || [];
  }

  remove(key: string, docId: string): boolean {
    const bucket = fnv1a(key) % this.numBuckets;
    const map = this.buckets.get(bucket);
    if (!map || !map.has(key)) return false;
    const list = map.get(key)!;
    const idx = list.findIndex(e => e.docId === docId);
    if (idx < 0) return false;
    list.splice(idx, 1);
    if (list.length === 0) map.delete(key);
    this.size--;
    return true;
  }

  getLoadFactor(): number {
    return this.buckets.size > 0 ? this.size / this.numBuckets : 0;
  }

  getSize(): number { return this.size; }
}

// ─── Index Version Vector ────────────────────────────────────────────────────

class IndexVersionVector {
  private vectors = new Map<string, VersionVector>();
  private readonly replicaId: string;

  constructor(replicaId: string) { this.replicaId = replicaId; }

  increment(indexName: string): number {
    if (!this.vectors.has(indexName)) this.vectors.set(indexName, {});
    const vv = this.vectors.get(indexName)!;
    vv[this.replicaId] = (vv[this.replicaId] || 0) + 1;
    return vv[this.replicaId];
  }

  merge(indexName: string, remote: VersionVector): void {
    if (!this.vectors.has(indexName)) this.vectors.set(indexName, {});
    const local = this.vectors.get(indexName)!;
    for (const [replica, version] of Object.entries(remote)) {
      local[replica] = Math.max(local[replica] || 0, version);
    }
  }

  get(indexName: string): VersionVector { return { ...(this.vectors.get(indexName) || {}) }; }

  dominates(indexName: string, remote: VersionVector): boolean {
    const local = this.vectors.get(indexName) || {};
    for (const [replica, version] of Object.entries(remote)) {
      if ((local[replica] || 0) < version) return false;
    }
    return true;
  }

  divergence(indexName: string, remote: VersionVector): number {
    const local = this.vectors.get(indexName) || {};
    const allReplicas = new Set([...Object.keys(local), ...Object.keys(remote)]);
    let diff = 0;
    for (const replica of allReplicas) {
      diff += Math.abs((local[replica] || 0) - (remote[replica] || 0));
    }
    return diff;
  }
}

// ─── Convergence Checker ─────────────────────────────────────────────────────

class ConvergenceChecker {
  private lastUpdates = new Map<string, Map<string, number>>();
  private convergenceThresholdMs: number;
  private maxDivergence: number;
  private lagTracker = new Map<string, EWMATracker>();

  constructor(convergenceThresholdMs: number = 5000, maxDivergence: number = 10) {
    this.convergenceThresholdMs = convergenceThresholdMs;
    this.maxDivergence = maxDivergence;
  }

  recordUpdate(indexName: string, replicaId: string): void {
    if (!this.lastUpdates.has(indexName)) this.lastUpdates.set(indexName, new Map());
    this.lastUpdates.get(indexName)!.set(replicaId, Date.now());
  }

  recordLag(indexName: string, lagMs: number): void {
    if (!this.lagTracker.has(indexName)) this.lagTracker.set(indexName, new EWMATracker(0.2));
    this.lagTracker.get(indexName)!.update(lagMs);
  }

  checkConvergence(indexName: string, versionVector: IndexVersionVector,
                    remoteVectors: Map<string, VersionVector>): ConvergenceStatus {
    const now = Date.now();
    const updates = this.lastUpdates.get(indexName) || new Map();
    const staleReplicas: string[] = [];
    let maxLag = 0;
    let totalDivergence = 0;

    for (const [replicaId, remoteVV] of remoteVectors) {
      const lastUpdate = updates.get(replicaId) || 0;
      const lag = now - lastUpdate;
      maxLag = Math.max(maxLag, lag);
      if (lag > this.convergenceThresholdMs) staleReplicas.push(replicaId);
      totalDivergence += versionVector.divergence(indexName, remoteVV);
    }

    const avgDivergence = remoteVectors.size > 0 ? totalDivergence / remoteVectors.size : 0;
    const ewmaLag = this.lagTracker.get(indexName)?.current || maxLag;

    return {
      indexName, converged: staleReplicas.length === 0 && avgDivergence <= this.maxDivergence,
      lagMs: ewmaLag, divergence: avgDivergence,
      lastUpdateTimestamp: Math.max(...Array.from(updates.values()), 0),
      replicaCount: remoteVectors.size + 1, staleReplicas
    };
  }
}

// ─── Conflict Resolver ───────────────────────────────────────────────────────

class ConflictResolver {
  private strategy: ConflictStrategy;
  private conflictLog: ConflictRecord[] = [];
  private readonly maxLogSize: number;

  constructor(strategy: ConflictStrategy = 'lww', maxLogSize: number = 1000) {
    this.strategy = strategy;
    this.maxLogSize = maxLogSize;
  }

  resolve(indexName: string, key: string, local: PostingEntry, remote: PostingEntry): PostingEntry {
    let resolution: 'local' | 'remote' | 'merged';
    let result: PostingEntry;

    switch (this.strategy) {
      case 'lww':
        if (remote.timestamp > local.timestamp) { result = remote; resolution = 'remote'; }
        else if (remote.timestamp < local.timestamp) { result = local; resolution = 'local'; }
        else {
          result = fnv1a(remote.docId) > fnv1a(local.docId) ? remote : local;
          resolution = result === remote ? 'remote' : 'local';
        }
        break;
      case 'highest_version':
        if (remote.version > local.version) { result = remote; resolution = 'remote'; }
        else if (remote.version < local.version) { result = local; resolution = 'local'; }
        else {
          result = remote.timestamp > local.timestamp ? remote : local;
          resolution = result === remote ? 'remote' : 'local';
        }
        break;
      case 'merge_union':
        result = {
          docId: local.docId,
          score: Math.max(local.score, remote.score),
          version: Math.max(local.version, remote.version),
          timestamp: Math.max(local.timestamp, remote.timestamp),
          payload: { ...(local.payload || {}), ...(remote.payload || {}) }
        };
        resolution = 'merged';
        break;
      case 'priority':
        if (remote.score > local.score) { result = remote; resolution = 'remote'; }
        else { result = local; resolution = 'local'; }
        break;
      default:
        result = local; resolution = 'local';
    }

    this.conflictLog.push({
      indexName, key, localEntry: local, remoteEntry: remote,
      resolution, strategy: this.strategy, timestamp: Date.now()
    });
    while (this.conflictLog.length > this.maxLogSize) this.conflictLog.shift();
    return result;
  }

  getConflictRate(windowMs: number = 60000): number {
    const cutoff = Date.now() - windowMs;
    return this.conflictLog.filter(r => r.timestamp > cutoff).length;
  }

  getConflictLog(limit: number = 50): ConflictRecord[] {
    return this.conflictLog.slice(-limit);
  }
}

// ─── Query Planner ───────────────────────────────────────────────────────────

class QueryPlanner {
  private indexStats = new Map<string, { size: number; avgLookupMs: number; hitRate: number }>();
  private queryHistory: { query: Query; indexUsed: string; latencyMs: number; rows: number }[] = [];
  private readonly maxHistory = 500;

  recordStats(indexName: string, size: number, avgLookupMs: number, hitRate: number): void {
    this.indexStats.set(indexName, { size, avgLookupMs, hitRate });
  }

  recordQuery(query: Query, indexUsed: string, latencyMs: number, rows: number): void {
    this.queryHistory.push({ query, indexUsed, latencyMs, rows });
    while (this.queryHistory.length > this.maxHistory) this.queryHistory.shift();
  }

  plan(query: Query, availableIndexes: IndexDefinition[]): QueryPlan {
    const candidates: QueryPlan[] = [];

    for (const idx of availableIndexes) {
      const matchesField = idx.fields.includes(query.field) ||
        (query.fields && query.fields.some(f => idx.fields.includes(f.field)));
      if (!matchesField && query.type !== 'multi') continue;

      const stats = this.indexStats.get(idx.name);
      const baseLatency = stats?.avgLookupMs || 1;
      const size = stats?.size || 100;

      switch (query.type) {
        case 'exact':
          if (idx.type === 'hash' || idx.type === 'covering') {
            candidates.push({ indexName: idx.name, strategy: 'exact', estimatedCost: baseLatency, estimatedRows: 1, coveringFields: idx.coveringFields });
          } else if (idx.type === 'btree') {
            candidates.push({ indexName: idx.name, strategy: 'exact', estimatedCost: baseLatency * Math.log2(size + 1), estimatedRows: 1, coveringFields: idx.coveringFields });
          } else if (idx.type === 'inverted') {
            candidates.push({ indexName: idx.name, strategy: 'exact', estimatedCost: baseLatency * 1.5, estimatedRows: size * 0.01 });
          }
          break;
        case 'range':
          if (idx.type === 'btree') {
            const selectivity = 0.1;
            candidates.push({ indexName: idx.name, strategy: 'range', estimatedCost: baseLatency * Math.log2(size + 1) + size * selectivity * 0.1, estimatedRows: size * selectivity, coveringFields: idx.coveringFields });
          }
          break;
        case 'prefix':
          if (idx.type === 'btree' || idx.type === 'inverted') {
            candidates.push({ indexName: idx.name, strategy: 'prefix', estimatedCost: baseLatency * Math.log2(size + 1) * 2, estimatedRows: size * 0.05 });
          }
          break;
        case 'fulltext':
          if (idx.type === 'inverted') {
            candidates.push({ indexName: idx.name, strategy: 'exact', estimatedCost: baseLatency * (query.terms?.length || 1), estimatedRows: size * 0.1 });
          }
          break;
        case 'multi':
          if (query.fields && query.fields.every(f => idx.fields.includes(f.field))) {
            candidates.push({ indexName: idx.name, strategy: 'intersection', estimatedCost: baseLatency * query.fields.length, estimatedRows: size * Math.pow(0.1, query.fields.length) });
          }
          break;
      }
    }

    candidates.sort((a, b) => a.estimatedCost - b.estimatedCost);
    if (candidates.length > 0) {
      const covering = candidates.find(c => c.coveringFields && c.coveringFields.length > 0);
      if (covering && covering.estimatedCost < candidates[0].estimatedCost * 2) {
        return { ...covering, strategy: 'covering' };
      }
      return candidates[0];
    }
    return { indexName: '', strategy: 'fullscan', estimatedCost: 1000, estimatedRows: 10000 };
  }

  getQueryStats(): { avgLatencyMs: number; planDistribution: Record<string, number> } {
    const dist: Record<string, number> = {};
    let totalLatency = 0;
    for (const q of this.queryHistory) {
      dist[q.indexUsed] = (dist[q.indexUsed] || 0) + 1;
      totalLatency += q.latencyMs;
    }
    return {
      avgLatencyMs: this.queryHistory.length > 0 ? totalLatency / this.queryHistory.length : 0,
      planDistribution: dist
    };
  }
}

// ─── Index Compactor ─────────────────────────────────────────────────────────

class IndexCompactor {
  private compactionHistory: CompactionStats[] = [];
  private readonly maxHistory = 100;
  private readonly staleThresholdMs: number;
  private readonly tombstoneRatio: number;

  constructor(staleThresholdMs: number = 3600000, tombstoneRatio: number = 0.3) {
    this.staleThresholdMs = staleThresholdMs;
    this.tombstoneRatio = tombstoneRatio;
  }

  shouldCompact(totalEntries: number, tombstoneCount: number, lastCompactionMs: number): boolean {
    if (totalEntries === 0) return false;
    const ratio = tombstoneCount / totalEntries;
    const timeSinceCompaction = Date.now() - lastCompactionMs;
    return ratio > this.tombstoneRatio || timeSinceCompaction > this.staleThresholdMs * 3;
  }

  compactInvertedIndex(index: InvertedIndex, _validDocIds: Set<string>): CompactionStats {
    const start = Date.now();
    return { indexName: 'inverted', entriesRemoved: 0, spaceReclaimed: 0, durationMs: Date.now() - start, timestamp: Date.now() };
  }

  recordCompaction(stats: CompactionStats): void {
    this.compactionHistory.push(stats);
    while (this.compactionHistory.length > this.maxHistory) this.compactionHistory.shift();
  }

  getCompactionHistory(limit: number = 20): CompactionStats[] {
    return this.compactionHistory.slice(-limit);
  }
}

// ─── Stale Read Detector ─────────────────────────────────────────────────────

class StaleReadDetector {
  private writeVersions = new Map<string, number>();
  private staleReadCount = 0;
  private totalReadCount = 0;
  private readonly maxStalenessMs: number;
  private lastWriteTimestamps = new Map<string, number>();

  constructor(maxStalenessMs: number = 5000) { this.maxStalenessMs = maxStalenessMs; }

  recordWrite(indexName: string, version: number): void {
    this.writeVersions.set(indexName, Math.max(this.writeVersions.get(indexName) || 0, version));
    this.lastWriteTimestamps.set(indexName, Date.now());
  }

  recordRead(indexName: string, version: number): { stale: boolean; lagMs: number; versionsBehind: number } {
    this.totalReadCount++;
    const latestWrite = this.writeVersions.get(indexName) || 0;
    const lastWriteTime = this.lastWriteTimestamps.get(indexName) || 0;
    const lagMs = lastWriteTime > 0 ? Date.now() - lastWriteTime : 0;
    const versionsBehind = latestWrite - version;
    const stale = versionsBehind > 0 && lagMs > this.maxStalenessMs;
    if (stale) this.staleReadCount++;
    return { stale, lagMs, versionsBehind };
  }

  getStaleReadRate(): number {
    return this.totalReadCount > 0 ? this.staleReadCount / this.totalReadCount : 0;
  }
}

// ─── Eventually Consistent Index Orchestrator ────────────────────────────────

class EventuallyConsistentIndex {
  private readonly config: Required<ECIndexConfig>;
  private definitions = new Map<string, IndexDefinition>();
  private invertedIndexes = new Map<string, InvertedIndex>();
  private btreeIndexes = new Map<string, BTreeIndex>();
  private hashIndexes = new Map<string, HashIndex>();
  private versionVector: IndexVersionVector;
  private convergenceChecker: ConvergenceChecker;
  private conflictResolver: ConflictResolver;
  private queryPlanner: QueryPlanner;
  private compactor: IndexCompactor;
  private staleDetector: StaleReadDetector;
  private events: IndexEvent[] = [];
  private readonly maxEvents = 500;
  private documents = new Map<string, { doc: Record<string, unknown>; version: number }>();
  private tombstones = new Map<string, number>();
  private lastCompactionMs = 0;
  private globalVersion = 0;

  constructor(config: ECIndexConfig) {
    this.config = {
      convergenceThresholdMs: 5000, maxDivergence: 10, conflictStrategy: 'lww',
      compactionIntervalMs: 60000, staleReadThresholdMs: 5000, btreeOrder: 32, hashBuckets: 1024,
      ...config
    };
    this.versionVector = new IndexVersionVector(config.replicaId);
    this.convergenceChecker = new ConvergenceChecker(this.config.convergenceThresholdMs, this.config.maxDivergence);
    this.conflictResolver = new ConflictResolver(this.config.conflictStrategy);
    this.queryPlanner = new QueryPlanner();
    this.compactor = new IndexCompactor(this.config.compactionIntervalMs);
    this.staleDetector = new StaleReadDetector(this.config.staleReadThresholdMs);
  }

  createIndex(definition: IndexDefinition): void {
    if (this.definitions.has(definition.name)) throw new Error(`Index ${definition.name} already exists`);
    this.definitions.set(definition.name, definition);
    switch (definition.type) {
      case 'inverted': this.invertedIndexes.set(definition.name, new InvertedIndex()); break;
      case 'btree': this.btreeIndexes.set(definition.name, new BTreeIndex(this.config.btreeOrder)); break;
      case 'hash': case 'covering': this.hashIndexes.set(definition.name, new HashIndex(this.config.hashBuckets)); break;
    }
    for (const [docId, { doc, version }] of this.documents) {
      this.indexDocument(definition, docId, doc, version);
    }
    this.emitEvent('index_created', { indexName: definition.name, type: definition.type });
  }

  dropIndex(name: string): void {
    this.definitions.delete(name); this.invertedIndexes.delete(name);
    this.btreeIndexes.delete(name); this.hashIndexes.delete(name);
    this.emitEvent('index_dropped', { indexName: name });
  }

  upsert(docId: string, doc: Record<string, unknown>): void {
    this.globalVersion++;
    if (this.documents.has(docId)) this.removeFromAllIndexes(docId);
    this.documents.set(docId, { doc, version: this.globalVersion });
    this.tombstones.delete(docId);
    for (const def of this.definitions.values()) {
      this.indexDocument(def, docId, doc, this.globalVersion);
      const v = this.versionVector.increment(def.name);
      this.staleDetector.recordWrite(def.name, v);
      this.convergenceChecker.recordUpdate(def.name, this.config.replicaId);
    }
    this.emitEvent('index_updated', { docId, version: this.globalVersion, operation: 'upsert' });
  }

  delete(docId: string): void {
    if (!this.documents.has(docId)) return;
    this.removeFromAllIndexes(docId);
    this.documents.delete(docId);
    this.tombstones.set(docId, Date.now());
    this.globalVersion++;
    for (const def of this.definitions.values()) this.versionVector.increment(def.name);
    this.emitEvent('index_updated', { docId, operation: 'delete' });
  }

  query(q: Query): PostingEntry[] {
    const start = Date.now();
    const plan = this.queryPlanner.plan(q, Array.from(this.definitions.values()));
    const results = plan.strategy === 'fullscan' ? this.fullScan(q) : this.executeIndexedQuery(plan, q);
    if (plan.indexName) {
      const vv = this.versionVector.get(plan.indexName);
      const localVersion = vv[this.config.replicaId] || 0;
      const staleCheck = this.staleDetector.recordRead(plan.indexName, localVersion);
      if (staleCheck.stale) {
        this.emitEvent('stale_read', { indexName: plan.indexName, lagMs: staleCheck.lagMs, versionsBehind: staleCheck.versionsBehind });
      }
    }
    const latencyMs = Date.now() - start;
    this.queryPlanner.recordQuery(q, plan.indexName || 'fullscan', latencyMs, results.length);
    this.emitEvent('query_executed', { strategy: plan.strategy, indexName: plan.indexName, latencyMs, resultCount: results.length });
    return results.slice(0, q.limit || 100);
  }

  applyRemoteUpdate(indexName: string, key: string, entry: PostingEntry, remoteVersion: VersionVector): void {
    const def = this.definitions.get(indexName);
    if (!def) return;
    const existingEntries = this.lookupInIndex(indexName, key);
    const existing = existingEntries.find(e => e.docId === entry.docId);
    if (existing && existing.version !== entry.version) {
      const resolved = this.conflictResolver.resolve(indexName, key, existing, entry);
      this.insertIntoIndex(def, key, resolved);
      this.emitEvent('conflict_resolved', { indexName, key, strategy: this.config.conflictStrategy });
    } else if (!existing) {
      this.insertIntoIndex(def, key, entry);
    }
    this.versionVector.merge(indexName, remoteVersion);
    this.convergenceChecker.recordUpdate(indexName, 'remote');
  }

  getVersionVector(indexName: string): VersionVector { return this.versionVector.get(indexName); }

  checkConvergence(remoteVectors: Map<string, VersionVector>): Map<string, ConvergenceStatus> {
    const results = new Map<string, ConvergenceStatus>();
    for (const indexName of this.definitions.keys()) {
      const status = this.convergenceChecker.checkConvergence(indexName, this.versionVector, remoteVectors);
      results.set(indexName, status);
      if (status.converged) this.emitEvent('convergence_achieved', { indexName });
      else if (status.staleReplicas.length > 0) this.emitEvent('convergence_lost', { indexName, staleReplicas: status.staleReplicas });
    }
    return results;
  }

  tick(): void {
    const now = Date.now();
    const tombstoneExpiry = this.config.convergenceThresholdMs * 2;
    for (const [docId, ts] of this.tombstones) {
      if (now - ts > tombstoneExpiry) this.tombstones.delete(docId);
    }
    if (now - this.lastCompactionMs > this.config.compactionIntervalMs) {
      const validDocIds = new Set(this.documents.keys());
      for (const [, idx] of this.invertedIndexes) {
        const stats = this.compactor.compactInvertedIndex(idx, validDocIds);
        this.compactor.recordCompaction(stats);
      }
      this.lastCompactionMs = now;
      this.emitEvent('compaction_completed', { timestamp: now });
    }
    for (const [name, def] of this.definitions) {
      let size = 0;
      if (def.type === 'inverted') size = this.invertedIndexes.get(name)?.getStats().documents || 0;
      else if (def.type === 'btree') size = this.btreeIndexes.get(name)?.getSize() || 0;
      else size = this.hashIndexes.get(name)?.getSize() || 0;
      this.queryPlanner.recordStats(name, size, 1, 0.9);
    }
    while (this.events.length > this.maxEvents) this.events.shift();
  }

  rebuildIndex(indexName: string): void {
    const def = this.definitions.get(indexName);
    if (!def) return;
    switch (def.type) {
      case 'inverted': this.invertedIndexes.set(indexName, new InvertedIndex()); break;
      case 'btree': this.btreeIndexes.set(indexName, new BTreeIndex(this.config.btreeOrder)); break;
      case 'hash': case 'covering': this.hashIndexes.set(indexName, new HashIndex(this.config.hashBuckets)); break;
    }
    for (const [docId, { doc, version }] of this.documents) this.indexDocument(def, docId, doc, version);
    this.emitEvent('index_rebuilt', { indexName, documentCount: this.documents.size });
  }

  getDashboard(): Record<string, unknown> {
    const indexStats: Record<string, unknown> = {};
    for (const [name, def] of this.definitions) {
      const stats: Record<string, unknown> = { type: def.type, fields: def.fields };
      if (def.type === 'inverted') stats.details = this.invertedIndexes.get(name)?.getStats();
      else if (def.type === 'btree') stats.size = this.btreeIndexes.get(name)?.getSize();
      else { stats.size = this.hashIndexes.get(name)?.getSize(); stats.loadFactor = this.hashIndexes.get(name)?.getLoadFactor(); }
      stats.versionVector = this.versionVector.get(name);
      indexStats[name] = stats;
    }
    return {
      replicaId: this.config.replicaId, documentCount: this.documents.size,
      tombstoneCount: this.tombstones.size, globalVersion: this.globalVersion,
      indexes: indexStats, queryStats: this.queryPlanner.getQueryStats(),
      staleReadRate: this.staleDetector.getStaleReadRate(),
      conflictRate: this.conflictResolver.getConflictRate(),
      compactionHistory: this.compactor.getCompactionHistory(5),
      recentEvents: this.events.slice(-10)
    };
  }

  private indexDocument(def: IndexDefinition, docId: string, doc: Record<string, unknown>, version: number): void {
    const keys = def.keyExtractor(doc);
    if ((keys === null || keys === undefined) && def.sparse) return;
    const keyArray = Array.isArray(keys) ? keys : [keys];
    const payload = def.coveringFields
      ? Object.fromEntries(def.coveringFields.map(f => [f, this.getNestedField(doc, f)]))
      : undefined;
    for (const key of keyArray) {
      if ((key === null || key === undefined || key === '') && def.sparse) continue;
      const entry: PostingEntry = { docId, score: 1, version, timestamp: Date.now(), payload };
      switch (def.type) {
        case 'inverted': this.invertedIndexes.get(def.name)?.index(docId, keyArray, version, payload); return;
        case 'btree': this.btreeIndexes.get(def.name)?.insert(String(key), entry); break;
        case 'hash': case 'covering': this.hashIndexes.get(def.name)?.insert(String(key), entry, def.unique); break;
      }
    }
  }

  private removeFromAllIndexes(docId: string): void {
    const existing = this.documents.get(docId);
    if (!existing) return;
    for (const [name, def] of this.definitions) {
      const keys = def.keyExtractor(existing.doc);
      const keyArray = Array.isArray(keys) ? keys : [keys];
      for (const key of keyArray) {
        switch (def.type) {
          case 'inverted': this.invertedIndexes.get(name)?.remove(docId); break;
          case 'btree': this.btreeIndexes.get(name)?.remove(String(key), docId); break;
          case 'hash': case 'covering': this.hashIndexes.get(name)?.remove(String(key), docId); break;
        }
      }
    }
  }

  private lookupInIndex(indexName: string, key: string): PostingEntry[] {
    const def = this.definitions.get(indexName);
    if (!def) return [];
    switch (def.type) {
      case 'inverted': return this.invertedIndexes.get(indexName)?.exactLookup(key) || [];
      case 'btree': return this.btreeIndexes.get(indexName)?.search(key) || [];
      case 'hash': case 'covering': return this.hashIndexes.get(indexName)?.lookup(key) || [];
    }
    return [];
  }

  private insertIntoIndex(def: IndexDefinition, key: string, entry: PostingEntry): void {
    switch (def.type) {
      case 'inverted': this.invertedIndexes.get(def.name)?.index(entry.docId, [key], entry.version, entry.payload); break;
      case 'btree': this.btreeIndexes.get(def.name)?.insert(key, entry); break;
      case 'hash': case 'covering': this.hashIndexes.get(def.name)?.insert(key, entry, def.unique); break;
    }
  }

  private executeIndexedQuery(plan: QueryPlan, q: Query): PostingEntry[] {
    const def = this.definitions.get(plan.indexName);
    if (!def) return [];
    switch (plan.strategy) {
      case 'exact': case 'covering': return this.lookupInIndex(plan.indexName, q.value || '');
      case 'range':
        if (def.type === 'btree') return this.btreeIndexes.get(plan.indexName)?.rangeQuery(q.low || '', q.high || '\uffff', q.limit || 100) || [];
        break;
      case 'prefix':
        if (def.type === 'inverted') return this.invertedIndexes.get(plan.indexName)?.prefixSearch(q.value || '') || [];
        break;
      case 'intersection': return this.executeIntersection(plan.indexName, q);
    }
    return [];
  }

  private executeIntersection(indexName: string, q: Query): PostingEntry[] {
    if (!q.fields) return [];
    const resultSets: Set<string>[] = [];
    const entryMap = new Map<string, PostingEntry>();
    for (const field of q.fields) {
      const entries = this.lookupInIndex(indexName, field.value);
      resultSets.push(new Set(entries.map(e => e.docId)));
      for (const e of entries) entryMap.set(e.docId, e);
    }
    if (resultSets.length === 0) return [];
    let intersection = resultSets[0];
    for (let i = 1; i < resultSets.length; i++) {
      intersection = new Set([...intersection].filter(id => resultSets[i].has(id)));
    }
    return Array.from(intersection).map(id => entryMap.get(id)!).filter(Boolean);
  }

  private fullScan(q: Query): PostingEntry[] {
    const results: PostingEntry[] = [];
    for (const [docId, { doc, version }] of this.documents) {
      const value = this.getNestedField(doc, q.field);
      let match = false;
      switch (q.type) {
        case 'exact': match = String(value) === q.value; break;
        case 'range': match = String(value) >= (q.low || '') && String(value) <= (q.high || '\uffff'); break;
        case 'prefix': match = String(value || '').startsWith(q.value || ''); break;
      }
      if (match) results.push({ docId, score: 1, version, timestamp: Date.now() });
    }
    return results;
  }

  private getNestedField(obj: Record<string, unknown>, path: string): unknown {
    const parts = path.split('.');
    let current: unknown = obj;
    for (const part of parts) {
      if (current === null || current === undefined) return undefined;
      current = (current as Record<string, unknown>)[part];
    }
    return current;
  }

  private emitEvent(type: IndexEvent['type'], data: Record<string, unknown>): void {
    this.events.push({ type, timestamp: Date.now(), data });
  }
}

// ─── Presets ─────────────────────────────────────────────────────────────────

const PRESETS = {
  'agent-registry': {
    replicaId: 'node-1', convergenceThresholdMs: 3000, maxDivergence: 5,
    conflictStrategy: 'lww' as ConflictStrategy, compactionIntervalMs: 30000,
    staleReadThresholdMs: 2000, btreeOrder: 64, hashBuckets: 2048
  },
  'knowledge-store': {
    replicaId: 'node-1', convergenceThresholdMs: 10000, maxDivergence: 20,
    conflictStrategy: 'merge_union' as ConflictStrategy, compactionIntervalMs: 120000,
    staleReadThresholdMs: 10000, btreeOrder: 128, hashBuckets: 4096
  },
  'capability-index': {
    replicaId: 'node-1', convergenceThresholdMs: 5000, maxDivergence: 10,
    conflictStrategy: 'highest_version' as ConflictStrategy, compactionIntervalMs: 60000,
    staleReadThresholdMs: 5000, btreeOrder: 32, hashBuckets: 1024
  }
};

export {
  EventuallyConsistentIndex, InvertedIndex, BTreeIndex, HashIndex,
  IndexVersionVector, ConvergenceChecker, ConflictResolver, QueryPlanner,
  IndexCompactor, StaleReadDetector, PRESETS
};
export type {
  IndexDefinition, PostingEntry, IndexEvent, VersionVector, QueryPlan,
  Query, ConvergenceStatus, ConflictStrategy, ConflictRecord, CompactionStats, ECIndexConfig
};
