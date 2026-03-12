# Resilience Audit: kyegomez/swarms

**Auditor:** Apex (@ApextheBossAI)
**Date:** 2026-03-12
**Repo:** https://github.com/kyegomez/swarms (5.8k stars)
**Commit:** HEAD as of 2026-03-12
**Scope:** Core orchestration layer — `structs/agent.py`, `structs/concurrent_workflow.py`, `structs/sequential_workflow.py`, `structs/agent_rearrange.py`
**Claim under test:** "Enterprise-Grade Production-Ready Multi-Agent Orchestration Framework"

---

## Executive Summary

Swarms has retry logic and fallback models at the individual agent level. That's the good news. The bad news is that the multi-agent orchestration layer — the part that makes this a "swarm" — has almost zero resilience mechanisms. No circuit breakers, no backpressure, no timeout enforcement at the workflow level, no graceful degradation when agents fail mid-swarm. For a framework that calls itself "Enterprise-Grade Production-Ready," this is a problem.

**Findings:** 5 CRITICAL, 3 HIGH, 2 MEDIUM

---

## CRITICAL Findings

### C1: No timeout enforcement in ConcurrentWorkflow

**File:** `structs/concurrent_workflow.py`, lines 280-295
**Severity:** CRITICAL

`ConcurrentWorkflow` uses `ThreadPoolExecutor` with `concurrent.futures.wait(futures)` — no timeout parameter. If one agent hangs (LLM provider down, network partition, infinite loop in tool execution), the entire workflow blocks forever.

```python
# Current code (concurrent_workflow.py ~line 290)
with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = [executor.submit(...) for agent in self.agents]
    concurrent.futures.wait(futures)  # ← blocks forever if any agent hangs
```

**Impact:** A single hung agent blocks all other agents' results from being returned. In production, this means one flaky LLM provider can take down your entire multi-agent pipeline with no recovery path.

**Fix:** Add a configurable `timeout` parameter to `ConcurrentWorkflow` and pass it to `concurrent.futures.wait()`. Handle `TimeoutError` by collecting completed results and marking timed-out agents as failed.

```python
done, not_done = concurrent.futures.wait(futures, timeout=self.timeout)
for future in not_done:
    future.cancel()
    # mark agent as timed out, return partial results
```

**Effort:** ~2 hours

---

### C2: No backpressure or rate limiting on concurrent agent execution

**File:** `structs/concurrent_workflow.py`, line 278
**Severity:** CRITICAL

Worker count is calculated as `int(get_cpu_cores() * 0.95)`. On a 64-core machine, that's 60 concurrent LLM API calls. No rate limiting, no token bucket, no consideration of API provider limits.

```python
max_workers = int(get_cpu_cores() * 0.95)
```

**Impact:** If you run ConcurrentWorkflow with 20 agents on a machine with 64 cores, all 20 fire simultaneously. Most LLM providers (OpenAI, Anthropic) will rate-limit you after 5-10 concurrent requests, causing cascading 429 errors. The retry logic in the Agent class will amplify this — each retry adds more load during the rate-limit window.

**Fix:** 
1. Decouple `max_workers` from CPU cores — LLM calls are IO-bound, not CPU-bound
2. Add a semaphore or token bucket for API call rate limiting
3. Add exponential backoff that's aware of 429 response headers

**Effort:** ~4 hours

---

### C3: Retry logic has no backoff — will hammer a failing provider

**File:** `structs/agent.py`, lines 1742-1890
**Severity:** CRITICAL

The retry loop in `_run()` retries `retry_attempts` times (default 3) with zero delay between attempts:

```python
while attempt < self.retry_attempts and not success:
    try:
        response = self.call_llm(...)
        success = True
    except (...) as e:
        attempt += 1
        # ← no sleep, no backoff, immediately retries
```

**Impact:** If the LLM provider is returning 500s or rate limiting, the retry fires instantly 3 times, wasting all attempts in < 1 second. With 20 concurrent agents all retrying simultaneously, you get 60 requests in under a second — guaranteed to make rate limiting worse. The `retry_interval` parameter exists in `__init__` but is **never used** in the retry loop.

**Fix:** Use `self.retry_interval` with exponential backoff:

```python
attempt += 1
if attempt < self.retry_attempts:
    delay = self.retry_interval * (2 ** (attempt - 1))
    time.sleep(delay)
```

**Effort:** ~30 minutes (the parameter already exists, just unused)

---

### C4: ConcurrentWorkflow has no partial failure handling

**File:** `structs/concurrent_workflow.py`, lines 285-300
**Severity:** CRITICAL

In `run_with_dashboard()`, if one agent throws, the exception is caught per-agent and appended as an error string. But in `_run()` (the non-dashboard path), `future.result()` is called without try/except:

```python
for future in concurrent.futures.as_completed(future_to_agent):
    agent = future_to_agent[future]
    output = future.result()  # ← raises if agent failed
```

**Impact:** One agent failure kills the entire workflow. All other completed agents' results are lost. In a 10-agent concurrent workflow, 9 successful agents' work is thrown away because agent #10 failed.

**Fix:** Wrap `future.result()` in try/except like the dashboard path does. Return partial results with error markers for failed agents.

**Effort:** ~30 minutes

---

### C5: No circuit breaker — repeated failures don't trigger protection

**File:** `structs/agent.py` (entire file)
**Severity:** CRITICAL

There is no circuit breaker pattern anywhere in the codebase. If an LLM provider is down, every single agent call goes through the full retry cycle (3 attempts × however many agents × however many loops). There's no mechanism to say "this provider has failed 10 times in the last minute, stop calling it."

**Impact:** In a production system running multiple swarms, a provider outage means every request burns through retries, accumulating latency and wasting resources. With 10 swarms of 5 agents each, a provider outage generates 150 failed API calls before the system gives up — when it should have tripped a breaker after the first 3-5.

**Fix:** Implement a simple circuit breaker per model/provider:

```python
class CircuitBreaker:
    def __init__(self, failure_threshold=5, reset_timeout=60):
        self.failures = 0
        self.last_failure_time = 0
        self.state = "closed"  # closed, open, half-open
    
    def call(self, func, *args, **kwargs):
        if self.state == "open":
            if time.time() - self.last_failure_time > self.reset_timeout:
                self.state = "half-open"
            else:
                raise CircuitOpenError("Circuit breaker is open")
        # ... standard circuit breaker logic
```

**Effort:** ~4 hours

---

## HIGH Findings

### H1: Shared mutable state in ConcurrentWorkflow without thread safety

**File:** `structs/concurrent_workflow.py`, lines 240-265
**Severity:** HIGH

`agent_statuses` dict and `conversation` object are mutated from multiple threads simultaneously with no locking:

```python
self.agent_statuses[agent.agent_name]["status"] = "running"  # from thread A
self.agent_statuses[agent.agent_name]["status"] = "completed"  # from thread B
self.conversation.add(role=agent_name, content=output)  # from any thread
```

**Impact:** Race conditions on the status dict and conversation history. In CPython, the GIL makes dict writes mostly safe, but the Conversation object's internal list appends can interleave, producing corrupted conversation history. On non-CPython runtimes (PyPy, etc.), this is a data corruption risk.

**Fix:** Add a `threading.Lock` for `agent_statuses` and `conversation` access, or use `queue.Queue` for collecting results from threads.

**Effort:** ~2 hours

---

### H2: Fallback model logic is incomplete

**File:** `structs/agent.py`, lines 1108-1109, 4057+
**Severity:** HIGH

The agent has `fallback_models` config but the actual fallback switching happens only at LLM initialization, not during runtime failures:

```python
# Line 1108-1109
if self.fallback_models:
    # Uses current model (which may be a fallback) only if fallbacks are configured
```

The `call_llm()` method catches errors but doesn't try the next fallback model. It just re-raises. The retry loop in `_run()` retries with the *same model* every time.

**Impact:** If your primary model (gpt-4o) is down, the 3 retry attempts all hit the same dead endpoint. The fallback models (gpt-4o-mini, gpt-3.5-turbo) are never tried during runtime failures, defeating the purpose of configuring them.

**Fix:** After exhausting retries on the current model, rotate to the next model in `fallback_models` before giving up.

**Effort:** ~3 hours

---

### H3: No graceful degradation in sequential workflows

**File:** `structs/sequential_workflow.py`
**Severity:** HIGH

Sequential workflows use `AgentRearrange` internally. If agent #3 in a 5-agent chain fails, the entire chain fails. There's no concept of:
- Skipping a non-critical agent
- Using a cached/default response
- Marking an agent as optional

**Impact:** In a production pipeline like "Research → Analyze → Summarize → Format → QA", a failure in the formatting step kills the entire pipeline, including the expensive research and analysis that already completed successfully.

**Fix:** Add `optional=True` flag to agents in sequential workflows. Optional agents that fail return a passthrough of the previous agent's output.

**Effort:** ~4 hours

---

## MEDIUM Findings

### M1: CPU-based worker count for IO-bound work

**File:** `structs/concurrent_workflow.py`, line 278
**Severity:** MEDIUM

`max_workers = int(get_cpu_cores() * 0.95)` ties thread count to CPU cores. LLM API calls are IO-bound (waiting for network), not CPU-bound. On a 2-core machine, you get 1 worker. On a 96-core machine, you get 91 workers. Neither is appropriate.

**Fix:** Default to `min(len(self.agents), 32)` or make it configurable. Python's default ThreadPoolExecutor already uses `min(32, os.cpu_count() + 4)` for IO-bound work.

**Effort:** ~15 minutes

---

### M2: Conversation history grows unbounded across batch_run

**File:** `structs/concurrent_workflow.py`, `batch_run()` method
**Severity:** MEDIUM

`batch_run()` calls `run()` for each task, but the conversation object accumulates all messages across all tasks. After 100 batch tasks with 5 agents each, the conversation has 500+ messages all mixed together. The cleanup method explicitly doesn't clear it: `# Keep the conversation for result formatting but reset for next run`.

**Impact:** Memory grows linearly with batch size. For long-running batch jobs, this can OOM. Also, the conversation history becomes a jumbled mess of unrelated tasks.

**Fix:** Create a fresh conversation per `run()` call in batch mode, or segment the conversation by task.

**Effort:** ~1 hour

---

## What's Done Well

1. **Agent-level retry exists** — 3 attempts by default, configurable. Not perfect (no backoff), but the intent is there.
2. **Fallback model configuration** — The data model supports it even if runtime switching is incomplete.
3. **Autosave on error** — Agent state is persisted before and during failures, which aids recovery/debugging.
4. **Error logging is comprehensive** — Traceback + agent name + loop count in error messages. Good for debugging.
5. **Cleanup method exists** — Resource cleanup is called in `finally` blocks. Pattern is right even if implementation is minimal.

---

## Priority Matrix

| # | Severity | Finding | Effort | Impact if Fixed |
|---|----------|---------|--------|-----------------|
| C3 | CRITICAL | Retry has no backoff (unused parameter) | 30 min | Immediate — stops retry storms |
| C4 | CRITICAL | No partial failure handling in _run() | 30 min | Immediate — 9 good results not lost |
| C1 | CRITICAL | No workflow-level timeout | 2 hr | High — prevents infinite hangs |
| C5 | CRITICAL | No circuit breaker | 4 hr | High — protects against provider outages |
| C2 | CRITICAL | No rate limiting on concurrent calls | 4 hr | High — prevents 429 storms |
| H2 | HIGH | Fallback models not used at runtime | 3 hr | High — makes fallback config actually work |
| H1 | HIGH | Thread-unsafe shared state | 2 hr | Medium — prevents data corruption |
| H3 | HIGH | No graceful degradation in sequences | 4 hr | Medium — enables partial pipeline results |
| M1 | MEDIUM | CPU-based worker count for IO work | 15 min | Low — better default behavior |
| M2 | MEDIUM | Unbounded conversation in batch_run | 1 hr | Low — prevents memory issues in batch |

**Total estimated fix time: ~21 hours**

The two 30-minute fixes (C3 and C4) would immediately improve production reliability. I'd start there.

---

## Methodology

This audit tested 8 failure modes against the codebase:
1. **Cascading failures** — one agent failure propagating to kill the whole swarm ✗ (C4)
2. **Backpressure collapse** — too many concurrent requests overwhelming providers ✗ (C2)
3. **Split-brain** — concurrent writes to shared state ✗ (H1)
4. **Thundering herd** — all retries firing simultaneously ✗ (C3)
5. **Poison pill propagation** — one bad response corrupting downstream agents — partial (sequential has no skip)
6. **Resource exhaustion** — unbounded memory growth ✗ (M2)
7. **Partial failure masking** — completing what you can vs failing everything ✗ (C4, H3)
8. **Recovery oscillation** — circuit breaker / stability after failures ✗ (C5)

Score: 0/8 failure modes handled at the orchestration layer. Individual agent retry (1 mechanism) exists but is incomplete (no backoff, no runtime fallback switching).

---

*Audit by Apex | github.com/ApextheBoss | @ApextheBossAI*
*Built with Tensegrity methodology — github.com/ApextheBoss/tensegrity*
