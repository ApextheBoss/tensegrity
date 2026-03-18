/**
 * Reputation-Weighted Task Router
 * Routes tasks to agents based on domain-specific reputation scores.
 * Part of the Agent Coordination Toolkit.
 */

export interface Agent {
  address: string;
  name: string;
  reputationByDomain: Record<string, number>;
  availability: boolean;
  lastActiveMs: number;
}

export interface Task {
  id: string;
  domain: string;
  complexity: 'tier1' | 'tier2' | 'tier3';
  requiredMinReputation: number;
  deadline: number;
}

export interface RouteResult {
  agent: Agent;
  score: number;
  reason: string;
}

const DECAY_RATE = 0.05; // 5% per 30 days of inactivity
const RECENCY_WEIGHT = 0.3;
const REPUTATION_WEIGHT = 0.5;
const AVAILABILITY_WEIGHT = 0.2;

/**
 * Calculate time-decayed reputation for an agent in a specific domain.
 * Reputation decays logarithmically based on time since last activity.
 */
export function decayedReputation(agent: Agent, domain: string, nowMs: number): number {
  const baseRep = agent.reputationByDomain[domain] ?? 0;
  const daysSinceActive = (nowMs - agent.lastActiveMs) / (1000 * 60 * 60 * 24);
  
  if (daysSinceActive <= 1) return baseRep;
  
  const decayFactor = Math.exp(-DECAY_RATE * Math.log(daysSinceActive));
  return baseRep * decayFactor;
}

/**
 * Score an agent for a specific task using weighted multi-factor evaluation.
 * Returns a normalized score between 0 and 1.
 */
function scoreAgent(agent: Agent, task: Task, nowMs: number): number {
  const rep = decayedReputation(agent, task.domain, nowMs);
  if (rep < task.requiredMinReputation) return 0;

  const repScore = Math.min(rep / 1000, 1); // normalize to 0-1
  const recencyDays = (nowMs - agent.lastActiveMs) / (1000 * 60 * 60 * 24);
  const recencyScore = Math.exp(-0.1 * recencyDays); // exponential recency decay
  const availScore = agent.availability ? 1 : 0;

  return (
    REPUTATION_WEIGHT * repScore +
    RECENCY_WEIGHT * recencyScore +
    AVAILABILITY_WEIGHT * availScore
  );
}

/**
 * Route a task to the best available agent.
 * Filters by minimum reputation, scores remaining candidates,
 * and returns the top match with explanation.
 */
export function routeTask(agents: Agent[], task: Task): RouteResult | null {
  const nowMs = Date.now();
  
  const scored = agents
    .filter(a => a.availability)
    .map(a => ({
      agent: a,
      score: scoreAgent(a, task, nowMs),
      reason: ''
    }))
    .filter(r => r.score > 0)
    .sort((a, b) => b.score - a.score);

  if (scored.length === 0) return null;

  const best = scored[0];
  best.reason = `Selected: rep=${decayedReputation(best.agent, task.domain, nowMs).toFixed(0)}, ` +
    `recency=${((nowMs - best.agent.lastActiveMs) / 3600000).toFixed(1)}h ago, ` +
    `score=${best.score.toFixed(3)}`;

  return best;
}

/**
 * Route a task with fallback: if no agent meets the reputation threshold,
 * relax requirements by 50% and try again. Useful for cold-start networks.
 */
export function routeTaskWithFallback(agents: Agent[], task: Task): RouteResult | null {
  const result = routeTask(agents, task);
  if (result) return result;

  // Fallback: relax reputation requirement
  const relaxedTask = { ...task, requiredMinReputation: task.requiredMinReputation * 0.5 };
  const fallback = routeTask(agents, relaxedTask);
  if (fallback) {
    fallback.reason = `[FALLBACK] ${fallback.reason}`;
  }
  return fallback;
}
