/**
 * Backpressure Controller for Agent Message Queues
 * Prevents agent overload by dynamically adjusting intake rate
 * based on processing capacity and queue depth.
 */

type BackpressureStrategy = 'drop-newest' | 'drop-oldest' | 'reject' | 'throttle';

interface BackpressureConfig {
  maxQueueDepth: number;
  highWaterMark: number;        // start applying pressure
  lowWaterMark: number;         // release pressure
  strategy: BackpressureStrategy;
  throttleDelayMs: number;      // delay when throttling
  metricsWindowMs: number;      // sliding window for rate calc
}

interface QueueMetrics {
  depth: number;
  inRate: number;               // messages per second in
  outRate: number;              // messages per second out
  dropped: number;
  rejected: number;
  throttled: number;
  pressureActive: boolean;
  utilizationPct: number;
}

interface Message<T = unknown> {
  id: string;
  payload: T;
  priority: number;
  enqueuedAt: number;
  sender: string;
}

const DEFAULT_CONFIG: BackpressureConfig = {
  maxQueueDepth: 1000,
  highWaterMark: 0.8,          // 80% capacity
  lowWaterMark: 0.5,           // 50% capacity
  strategy: 'throttle',
  throttleDelayMs: 100,
  metricsWindowMs: 60000,      // 1 minute
};

export class BackpressureController<T = unknown> {
  private queue: Message<T>[] = [];
  private pressureActive = false;
  private config: BackpressureConfig;
  private inTimestamps: number[] = [];
  private outTimestamps: number[] = [];
  private dropped = 0;
  private rejected = 0;
  private throttled = 0;

  constructor(config?: Partial<BackpressureConfig>) {
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  /**
   * Enqueue a message with backpressure handling.
   * Returns true if accepted, false if dropped/rejected.
   */
  async enqueue(msg: Message<T>): Promise<boolean> {
    const utilization = this.queue.length / this.config.maxQueueDepth;

    // Check if we need to activate pressure
    if (utilization >= this.config.highWaterMark && !this.pressureActive) {
      this.pressureActive = true;
    }

    // Apply backpressure strategy
    if (this.pressureActive) {
      switch (this.config.strategy) {
        case 'drop-newest':
          if (this.queue.length >= this.config.maxQueueDepth) {
            this.dropped++;
            return false;
          }
          break;

        case 'drop-oldest':
          if (this.queue.length >= this.config.maxQueueDepth) {
            this.queue.shift(); // remove oldest
            this.dropped++;
            // Remove one inTimestamp to compensate for the dropped message's
            // original recordIn(), preventing inRate inflation
            if (this.inTimestamps.length > 0) {
              this.inTimestamps.shift();
            }
          }
          break;

        case 'reject':
          if (utilization >= this.config.highWaterMark) {
            this.rejected++;
            return false;
          }
          break;

        case 'throttle':
          if (utilization >= this.config.highWaterMark) {
            this.throttled++;
            await this.delay(this.config.throttleDelayMs * (1 + utilization));
          }
          break;
      }
    }

    // Hard cap: always reject if at absolute max
    if (this.queue.length >= this.config.maxQueueDepth) {
      this.rejected++;
      return false;
    }

    // Insert by priority (higher priority = earlier in queue)
    const insertIdx = this.queue.findIndex(m => m.priority < msg.priority);
    if (insertIdx === -1) {
      this.queue.push(msg);
    } else {
      this.queue.splice(insertIdx, 0, msg);
    }

    this.recordIn();
    return true;
  }

  /**
   * Dequeue the highest priority message.
   */
  dequeue(): Message<T> | undefined {
    const msg = this.queue.shift();
    if (msg) {
      this.recordOut();

      // Check if we can release pressure
      const utilization = this.queue.length / this.config.maxQueueDepth;
      if (this.pressureActive && utilization <= this.config.lowWaterMark) {
        this.pressureActive = false;
      }
    }
    return msg;
  }

  /**
   * Peek at the next message without removing it.
   */
  peek(): Message<T> | undefined {
    return this.queue[0];
  }

  /**
   * Get current queue metrics.
   */
  getMetrics(): QueueMetrics {
    const now = Date.now();
    const windowStart = now - this.config.metricsWindowMs;

    const recentIn = this.inTimestamps.filter(t => t > windowStart).length;
    const recentOut = this.outTimestamps.filter(t => t > windowStart).length;
    const windowSec = this.config.metricsWindowMs / 1000;

    return {
      depth: this.queue.length,
      inRate: recentIn / windowSec,
      outRate: recentOut / windowSec,
      dropped: this.dropped,
      rejected: this.rejected,
      throttled: this.throttled,
      pressureActive: this.pressureActive,
      utilizationPct: (this.queue.length / this.config.maxQueueDepth) * 100,
    };
  }

  /**
   * Drain all messages matching a predicate.
   * Useful for cancelling messages from a specific sender.
   */
  drain(predicate: (msg: Message<T>) => boolean): Message<T>[] {
    const drained: Message<T>[] = [];
    this.queue = this.queue.filter(msg => {
      if (predicate(msg)) {
        drained.push(msg);
        return false;
      }
      return true;
    });
    return drained;
  }

  /**
   * Get adaptive throttle recommendation based on current state.
   * Returns suggested delay in ms for the sender.
   */
  getAdaptiveThrottle(): number {
    const metrics = this.getMetrics();
    if (!metrics.pressureActive) return 0;

    const overflowRatio = metrics.inRate / Math.max(metrics.outRate, 0.001);
    return Math.min(
      this.config.throttleDelayMs * overflowRatio,
      5000 // max 5 second throttle
    );
  }

  private recordIn(): void {
    const now = Date.now();
    this.inTimestamps.push(now);
    this.cleanTimestamps();
  }

  private recordOut(): void {
    const now = Date.now();
    this.outTimestamps.push(now);
    this.cleanTimestamps();
  }

  private cleanTimestamps(): void {
    const cutoff = Date.now() - this.config.metricsWindowMs;
    this.inTimestamps = this.inTimestamps.filter(t => t > cutoff);
    this.outTimestamps = this.outTimestamps.filter(t => t > cutoff);
  }

  private delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}
