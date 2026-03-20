/**
 * Example 1: Circuit Breaker for Agent Communication
 *
 * Protects your system from cascading failures when an agent becomes
 * unresponsive. The circuit breaker tracks failures and stops sending
 * requests to unhealthy agents, giving them time to recover.
 *
 * Run: npx tsx examples/01-circuit-breaker.ts
 */

import { CircuitBreaker } from 'tensegrity';

// --- Simulate an unreliable agent ---

let requestCount = 0;

async function callUnreliableAgent(message: string): Promise<string> {
  requestCount++;
  // Fails on requests 3-7, then recovers
  if (requestCount >= 3 && requestCount <= 7) {
    throw new Error(`Agent timeout: request #${requestCount}`);
  }
  return `Agent processed: "${message}" (request #${requestCount})`;
}

// --- Use a circuit breaker to protect against failures ---

async function main() {
  const breaker = new CircuitBreaker('agent-summarizer', {
    failureThreshold: 3,     // Open after 3 failures
    resetTimeoutMs: 2000,    // Try again after 2 seconds
    halfOpenMaxAttempts: 2,  // Need 2 successes to fully close
    monitorWindowMs: 10000,  // Count failures within 10s window
  });

  console.log('Sending 12 requests through the circuit breaker...\n');

  for (let i = 1; i <= 12; i++) {
    // Check metrics before each call
    const metrics = breaker.getMetrics();

    try {
      const result = await breaker.execute(() =>
        callUnreliableAgent(`task-${i}`)
      );
      console.log(`  ✅ [${metrics.state.toUpperCase()}] ${result}`);
    } catch (err: any) {
      console.log(`  ❌ [${metrics.state.toUpperCase()}] ${err.message}`);
    }

    // If circuit is open, wait for the reset timeout
    if (breaker.getMetrics().state === 'open') {
      console.log('  ⏳ Circuit is OPEN — waiting 2s for reset...');
      await new Promise(r => setTimeout(r, 2100));
    }
  }

  // Final stats
  const final = breaker.getMetrics();
  console.log('\n--- Final Metrics ---');
  console.log(`  State: ${final.state}`);
  console.log(`  Total requests: ${final.totalRequests}`);
  console.log(`  Total failures: ${final.totalFailures}`);
}

main().catch(console.error);
