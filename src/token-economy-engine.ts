import { fnv1a } from './shared-utils';
/**
 * Token Economy Engine for Agent Networks
 * 
 * Implements a complete token-based micro-economy for agent coordination:
 * - Token minting with supply schedules (fixed, inflationary, deflationary, bonding curve)
 * - Staking with slashing conditions and reward distribution
 * - Payment channels for high-frequency off-chain transfers
 * - Automated Market Maker (AMM) for capability pricing
 * - Revenue sharing with contribution-weighted splits
 * - Anti-inflation mechanisms (burn, lock, decay)
 * 
 * Designed for agent networks where work, reputation, and resources
 * need to be quantified, exchanged, and governed without central authority.
 */

// ============================================================
// Types
// ============================================================

interface TokenConfig {
  symbol: string;
  decimals: number;
  initialSupply: number;
  maxSupply: number | null;          // null = unlimited
  supplySchedule: SupplySchedule;
  burnRate: number;                   // fraction burned per transfer (0-1)
  transferFee: number;                // fraction collected as fee (0-1)
  stakingEnabled: boolean;
  paymentChannelsEnabled: boolean;
  ammEnabled: boolean;
}

type SupplySchedule =
  | { kind: 'fixed' }
  | { kind: 'inflationary'; annualRate: number; decayFactor: number }
  | { kind: 'deflationary'; burnPerBlock: number }
  | { kind: 'bonding-curve'; reserveRatio: number; basePrice: number };

interface Account {
  address: string;
  balance: number;
  staked: number;
  locked: number;                     // locked in payment channels
  nonce: number;
  createdAt: number;
  lastActiveAt: number;
}

interface StakePosition {
  id: string;
  staker: string;
  amount: number;
  startEpoch: number;
  lockDuration: number;               // epochs
  unlockEpoch: number;
  slashable: boolean;
  slashConditions: SlashCondition[];
  rewardsAccrued: number;
  rewardsPerTokenPaid: number;        // snapshot for proportional distribution
  status: 'active' | 'unbonding' | 'withdrawn' | 'slashed';
}

interface SlashCondition {
  kind: 'downtime' | 'equivocation' | 'task-failure' | 'sla-breach' | 'custom';
  severity: number;                   // 0-1 fraction of stake to slash
  description: string;
}

interface PaymentChannel {
  id: string;
  sender: string;
  receiver: string;
  deposit: number;
  senderBalance: number;
  receiverBalance: number;
  nonce: number;
  expiresAt: number;
  status: 'open' | 'closing' | 'closed' | 'disputed';
  lastUpdate: number;
  disputeWindow: number;              // ticks after close-request before finalize
}

interface ChannelUpdate {
  channelId: string;
  senderBalance: number;
  receiverBalance: number;
  nonce: number;
  senderSig: string;                  // FNV-1a hash as signature proxy
  receiverSig: string;
}

interface AMMPool {
  tokenA: string;                     // "WORK" token
  tokenB: string;                     // "REP" token or capability slug
  reserveA: number;
  reserveB: number;
  k: number;                          // constant product invariant
  feeRate: number;
  totalLPShares: number;
  lpHolders: Map<string, number>;
  swapCount: number;
  volumeA: number;
  volumeB: number;
}

interface RevenuePool {
  id: string;
  totalRevenue: number;
  distributedRevenue: number;
  contributors: Map<string, ContributorShare>;
  distributionStrategy: DistributionStrategy;
  vestingSchedule: VestingSchedule | null;
  lastDistribution: number;
}

interface ContributorShare {
  address: string;
  weight: number;                     // contribution weight (normalized to sum=1)
  earned: number;
  claimed: number;
  vestingStart: number;
  vestingCliff: number;               // epochs before any claim
}

type DistributionStrategy =
  | { kind: 'proportional' }
  | { kind: 'quadratic' }            // square root of weight for more equal distribution
  | { kind: 'tiered'; tiers: Array<{ threshold: number; rate: number }> };

interface VestingSchedule {
  cliffEpochs: number;
  totalEpochs: number;
  linearAfterCliff: boolean;
}

interface TokenEvent {
  type: 'mint' | 'burn' | 'transfer' | 'stake' | 'unstake' | 'slash' |
        'channel-open' | 'channel-update' | 'channel-close' | 'channel-dispute' |
        'swap' | 'add-liquidity' | 'remove-liquidity' |
        'revenue-deposit' | 'revenue-distribute' | 'revenue-claim' |
        'epoch-advance' | 'supply-adjustment';
  timestamp: number;
  data: Record<string, unknown>;
}

// ============================================================
// FNV-1a Hash (deterministic signatures / tie-breaking)
// ============================================================

function signUpdate(channelId: string, nonce: number, address: string): string {
  return fnv1a(`${channelId}:${nonce}:${address}`).toString(16);
}

// ============================================================
// Supply Schedule Calculator
// ============================================================

class SupplyController {
  private config: TokenConfig;
  private currentSupply: number;
  private currentEpoch: number = 0;
  private totalBurned: number = 0;

  constructor(config: TokenConfig) {
    this.config = config;
    this.currentSupply = config.initialSupply;
  }

  /**
   * Calculate tokens to mint this epoch based on schedule.
   * Respects maxSupply cap.
   */
  epochMintAmount(): number {
    const schedule = this.config.supplySchedule;
    let mint = 0;

    switch (schedule.kind) {
      case 'fixed':
        return 0; // no new supply

      case 'inflationary': {
        // Annual rate decays over time: rate * decayFactor^epoch
        const effectiveRate = schedule.annualRate * Math.pow(schedule.decayFactor, this.currentEpoch);
        // Assume 365 epochs/year for simplicity
        mint = this.currentSupply * (effectiveRate / 365);
        break;
      }

      case 'deflationary':
        // Net negative: burn per block, no mint
        return 0;

      case 'bonding-curve': {
        // Price = basePrice * (supply / initialSupply) ^ (1/reserveRatio)
        // Minting only happens on purchase, not per-epoch
        return 0;
      }
    }

    // Cap at maxSupply
    if (this.config.maxSupply !== null) {
      const headroom = this.config.maxSupply - this.currentSupply;
      mint = Math.min(mint, Math.max(0, headroom));
    }

    return Math.floor(mint * 1000) / 1000; // 3 decimal precision
  }

  /**
   * Bonding curve: price for buying `amount` tokens.
   * Uses integral of price curve for exact cost.
   */
  bondingCurvePrice(amount: number): number {
    const schedule = this.config.supplySchedule;
    if (schedule.kind !== 'bonding-curve') return amount; // 1:1 fallback

    const { reserveRatio, basePrice } = schedule;
    const s0 = this.currentSupply;
    const s1 = s0 + amount;
    const init = this.config.initialSupply;
    const exp = 1 / reserveRatio;

    // Integral of basePrice * (s/init)^exp from s0 to s1
    const expPlus1 = exp + 1;
    const coefficient = basePrice / (init ** exp * expPlus1);
    return coefficient * (s1 ** expPlus1 - s0 ** expPlus1);
  }

  /**
   * Apply deflationary burn per epoch.
   */
  epochBurnAmount(): number {
    const schedule = this.config.supplySchedule;
    if (schedule.kind !== 'deflationary') return 0;
    return Math.min(schedule.burnPerBlock, this.currentSupply);
  }

  advanceEpoch(): { minted: number; burned: number } {
    const minted = this.epochMintAmount();
    const burned = this.epochBurnAmount();
    this.currentSupply += minted - burned;
    this.totalBurned += burned;
    this.currentEpoch++;
    return { minted, burned };
  }

  mint(amount: number): boolean {
    if (this.config.maxSupply !== null && this.currentSupply + amount > this.config.maxSupply) {
      return false;
    }
    this.currentSupply += amount;
    return true;
  }

  burn(amount: number): boolean {
    if (amount > this.currentSupply) return false;
    this.currentSupply -= amount;
    this.totalBurned += amount;
    return true;
  }

  getSupply(): number { return this.currentSupply; }
  getBurned(): number { return this.totalBurned; }
  getEpoch(): number { return this.currentEpoch; }
}

// ============================================================
// Staking Manager
// ============================================================

class StakingManager {
  private positions: Map<string, StakePosition> = new Map();
  private totalStaked: number = 0;
  private rewardPerTokenStored: number = 0;
  private lastRewardEpoch: number = 0;
  private rewardRate: number;          // tokens per epoch distributed to stakers
  private positionCounter: number = 0;
  private unbondingPeriod: number;     // epochs to wait after unstake request

  constructor(rewardRate: number, unbondingPeriod: number = 7) {
    this.rewardRate = rewardRate;
    this.unbondingPeriod = unbondingPeriod;
  }

  /**
   * Update global reward accumulator.
   * rewardPerToken = cumulative reward per unit staked.
   */
  private updateRewards(currentEpoch: number): void {
    if (this.totalStaked === 0) {
      this.lastRewardEpoch = currentEpoch;
      return;
    }
    const elapsed = currentEpoch - this.lastRewardEpoch;
    this.rewardPerTokenStored += (elapsed * this.rewardRate) / this.totalStaked;
    this.lastRewardEpoch = currentEpoch;
  }

  /**
   * Calculate pending rewards for a position.
   */
  private pendingRewards(position: StakePosition): number {
    return position.amount * (this.rewardPerTokenStored - position.rewardsPerTokenPaid)
      + position.rewardsAccrued;
  }

  stake(
    staker: string,
    amount: number,
    currentEpoch: number,
    lockDuration: number = 0,
    slashConditions: SlashCondition[] = []
  ): StakePosition | null {
    if (amount <= 0) return null;

    this.updateRewards(currentEpoch);

    const id = `stake-${++this.positionCounter}`;
    const position: StakePosition = {
      id,
      staker,
      amount,
      startEpoch: currentEpoch,
      lockDuration,
      unlockEpoch: currentEpoch + lockDuration,
      slashable: slashConditions.length > 0,
      slashConditions,
      rewardsAccrued: 0,
      rewardsPerTokenPaid: this.rewardPerTokenStored,
      status: 'active',
    };

    this.positions.set(id, position);
    this.totalStaked += amount;
    return position;
  }

  /**
   * Request unstake. Enters unbonding period.
   */
  requestUnstake(positionId: string, currentEpoch: number): boolean {
    const pos = this.positions.get(positionId);
    if (!pos || pos.status !== 'active') return false;
    if (currentEpoch < pos.unlockEpoch) return false; // still locked

    this.updateRewards(currentEpoch);
    pos.rewardsAccrued = this.pendingRewards(pos);
    pos.rewardsPerTokenPaid = this.rewardPerTokenStored;
    pos.status = 'unbonding';
    pos.unlockEpoch = currentEpoch + this.unbondingPeriod;
    return true;
  }

  /**
   * Finalize withdrawal after unbonding.
   * Returns { principal, rewards }.
   */
  withdraw(positionId: string, currentEpoch: number): { principal: number; rewards: number } | null {
    const pos = this.positions.get(positionId);
    if (!pos || pos.status !== 'unbonding') return null;
    if (currentEpoch < pos.unlockEpoch) return null; // still unbonding

    this.updateRewards(currentEpoch);
    const rewards = this.pendingRewards(pos);
    const principal = pos.amount;

    this.totalStaked -= pos.amount;
    pos.status = 'withdrawn';
    pos.rewardsAccrued = 0;

    return { principal, rewards };
  }

  /**
   * Slash a staker for violating conditions.
   * Returns amount slashed.
   */
  slash(positionId: string, conditionKind: string, currentEpoch: number): number {
    const pos = this.positions.get(positionId);
    if (!pos || !pos.slashable) return 0;
    if (pos.status === 'withdrawn' || pos.status === 'slashed') return 0;

    this.updateRewards(currentEpoch);

    const condition = pos.slashConditions.find(c => c.kind === conditionKind);
    if (!condition) return 0;

    const slashAmount = pos.amount * condition.severity;
    pos.amount -= slashAmount;
    this.totalStaked -= slashAmount;

    if (pos.amount <= 0) {
      pos.status = 'slashed';
      pos.amount = 0;
    }

    return slashAmount;
  }

  /**
   * Claim accrued rewards without unstaking.
   */
  claimRewards(positionId: string, currentEpoch: number): number {
    const pos = this.positions.get(positionId);
    if (!pos || pos.status === 'withdrawn' || pos.status === 'slashed') return 0;

    this.updateRewards(currentEpoch);
    const rewards = this.pendingRewards(pos);
    pos.rewardsAccrued = 0;
    pos.rewardsPerTokenPaid = this.rewardPerTokenStored;

    return rewards;
  }

  getTotalStaked(): number { return this.totalStaked; }
  getPosition(id: string): StakePosition | undefined { return this.positions.get(id); }

  getStakerPositions(staker: string): StakePosition[] {
    const result: StakePosition[] = [];
    for (const pos of this.positions.values()) {
      if (pos.staker === staker) result.push(pos);
    }
    return result;
  }
}

// ============================================================
// Payment Channel Manager
// ============================================================

class PaymentChannelManager {
  private channels: Map<string, PaymentChannel> = new Map();
  private channelCounter: number = 0;
  private defaultDisputeWindow: number;

  constructor(disputeWindow: number = 10) {
    this.defaultDisputeWindow = disputeWindow;
  }

  /**
   * Open a payment channel with a deposit from sender.
   */
  open(sender: string, receiver: string, deposit: number, expiresAt: number): PaymentChannel | null {
    if (deposit <= 0) return null;

    const id = `chan-${++this.channelCounter}`;
    const channel: PaymentChannel = {
      id,
      sender,
      receiver,
      deposit,
      senderBalance: deposit,
      receiverBalance: 0,
      nonce: 0,
      expiresAt,
      status: 'open',
      lastUpdate: Date.now(),
      disputeWindow: this.defaultDisputeWindow,
    };

    this.channels.set(id, channel);
    return channel;
  }

  /**
   * Update channel balances (off-chain, just store latest state).
   * Both parties must sign (verified via FNV-1a signature proxy).
   */
  update(update: ChannelUpdate): boolean {
    const channel = this.channels.get(update.channelId);
    if (!channel || channel.status !== 'open') return false;
    if (update.nonce <= channel.nonce) return false; // replay protection

    // Verify total conservation
    const total = update.senderBalance + update.receiverBalance;
    if (Math.abs(total - channel.deposit) > 0.001) return false;

    // Verify signatures
    const expectedSenderSig = signUpdate(update.channelId, update.nonce, channel.sender);
    const expectedReceiverSig = signUpdate(update.channelId, update.nonce, channel.receiver);
    if (update.senderSig !== expectedSenderSig || update.receiverSig !== expectedReceiverSig) {
      return false;
    }

    channel.senderBalance = update.senderBalance;
    channel.receiverBalance = update.receiverBalance;
    channel.nonce = update.nonce;
    channel.lastUpdate = Date.now();

    return true;
  }

  /**
   * Initiate cooperative close. Immediate if both agree.
   */
  cooperativeClose(channelId: string): { senderPayout: number; receiverPayout: number } | null {
    const channel = this.channels.get(channelId);
    if (!channel || channel.status !== 'open') return null;

    channel.status = 'closed';
    return {
      senderPayout: channel.senderBalance,
      receiverPayout: channel.receiverBalance,
    };
  }

  /**
   * Unilateral close request. Starts dispute window.
   */
  requestClose(channelId: string, currentTick: number): boolean {
    const channel = this.channels.get(channelId);
    if (!channel || channel.status !== 'open') return false;

    channel.status = 'closing';
    channel.expiresAt = currentTick + channel.disputeWindow;
    return true;
  }

  /**
   * Dispute with a higher-nonce state during dispute window.
   */
  dispute(channelId: string, update: ChannelUpdate): boolean {
    const channel = this.channels.get(channelId);
    if (!channel || channel.status !== 'closing') return false;
    if (update.nonce <= channel.nonce) return false;

    // Apply the newer state
    const total = update.senderBalance + update.receiverBalance;
    if (Math.abs(total - channel.deposit) > 0.001) return false;

    channel.senderBalance = update.senderBalance;
    channel.receiverBalance = update.receiverBalance;
    channel.nonce = update.nonce;
    channel.status = 'disputed';
    channel.lastUpdate = Date.now();

    return true;
  }

  /**
   * Finalize close after dispute window expires.
   */
  finalize(channelId: string, currentTick: number): { senderPayout: number; receiverPayout: number } | null {
    const channel = this.channels.get(channelId);
    if (!channel) return null;
    if (channel.status !== 'closing' && channel.status !== 'disputed') return null;
    if (currentTick < channel.expiresAt) return null; // still in dispute window

    channel.status = 'closed';
    return {
      senderPayout: channel.senderBalance,
      receiverPayout: channel.receiverBalance,
    };
  }

  /**
   * Force-close expired channels.
   */
  expireChannels(currentTick: number): string[] {
    const expired: string[] = [];
    for (const [id, channel] of this.channels) {
      if (channel.status === 'open' && currentTick >= channel.expiresAt) {
        channel.status = 'closed';
        expired.push(id);
      }
    }
    return expired;
  }

  getChannel(id: string): PaymentChannel | undefined { return this.channels.get(id); }
}

// ============================================================
// Automated Market Maker (Constant Product)
// ============================================================

class ConstantProductAMM {
  private pools: Map<string, AMMPool> = new Map();

  /**
   * Create a new liquidity pool.
   */
  createPool(
    tokenA: string,
    tokenB: string,
    initialA: number,
    initialB: number,
    feeRate: number = 0.003, // 0.3% default like Uniswap
    provider: string
  ): AMMPool | null {
    if (initialA <= 0 || initialB <= 0) return null;

    const poolId = `${tokenA}-${tokenB}`;
    if (this.pools.has(poolId)) return null;

    const initialShares = Math.sqrt(initialA * initialB);
    const lpHolders = new Map<string, number>();
    lpHolders.set(provider, initialShares);

    const pool: AMMPool = {
      tokenA,
      tokenB,
      reserveA: initialA,
      reserveB: initialB,
      k: initialA * initialB,
      feeRate,
      totalLPShares: initialShares,
      lpHolders,
      swapCount: 0,
      volumeA: 0,
      volumeB: 0,
    };

    this.pools.set(poolId, pool);
    return pool;
  }

  /**
   * Swap tokenA for tokenB (or vice versa).
   * Returns amount out after fee.
   */
  swap(
    poolId: string,
    inputToken: 'A' | 'B',
    inputAmount: number
  ): { outputAmount: number; priceImpact: number; fee: number } | null {
    const pool = this.pools.get(poolId);
    if (!pool || inputAmount <= 0) return null;

    const fee = inputAmount * pool.feeRate;
    const amountAfterFee = inputAmount - fee;

    let outputAmount: number;
    let reserveIn: number;
    let reserveOut: number;

    if (inputToken === 'A') {
      reserveIn = pool.reserveA;
      reserveOut = pool.reserveB;
    } else {
      reserveIn = pool.reserveB;
      reserveOut = pool.reserveA;
    }

    // Constant product: (reserveIn + amountAfterFee) * (reserveOut - outputAmount) = k
    outputAmount = reserveOut - (pool.k / (reserveIn + amountAfterFee));
    if (outputAmount <= 0 || outputAmount >= reserveOut) return null;

    // Price impact: how much worse than spot price
    const spotPrice = reserveOut / reserveIn;
    const executionPrice = outputAmount / inputAmount;
    const priceImpact = 1 - (executionPrice / spotPrice);

    // Update reserves
    if (inputToken === 'A') {
      pool.reserveA += inputAmount; // fee stays in pool as LP reward
      pool.reserveB -= outputAmount;
      pool.volumeA += inputAmount;
    } else {
      pool.reserveB += inputAmount;
      pool.reserveA -= outputAmount;
      pool.volumeB += inputAmount;
    }
    pool.k = pool.reserveA * pool.reserveB; // recalc with fees included
    pool.swapCount++;

    return { outputAmount, priceImpact, fee };
  }

  /**
   * Add liquidity proportional to current reserves.
   * Returns LP shares minted.
   */
  addLiquidity(
    poolId: string,
    amountA: number,
    amountB: number,
    provider: string
  ): { shares: number; usedA: number; usedB: number } | null {
    const pool = this.pools.get(poolId);
    if (!pool || amountA <= 0 || amountB <= 0) return null;

    // Match ratio
    const ratioA = amountA / pool.reserveA;
    const ratioB = amountB / pool.reserveB;
    const ratio = Math.min(ratioA, ratioB);

    const usedA = pool.reserveA * ratio;
    const usedB = pool.reserveB * ratio;
    const shares = pool.totalLPShares * ratio;

    pool.reserveA += usedA;
    pool.reserveB += usedB;
    pool.k = pool.reserveA * pool.reserveB;
    pool.totalLPShares += shares;

    const current = pool.lpHolders.get(provider) || 0;
    pool.lpHolders.set(provider, current + shares);

    return { shares, usedA, usedB };
  }

  /**
   * Remove liquidity by burning LP shares.
   * Returns tokens withdrawn.
   */
  removeLiquidity(
    poolId: string,
    shares: number,
    provider: string
  ): { amountA: number; amountB: number } | null {
    const pool = this.pools.get(poolId);
    if (!pool) return null;

    const held = pool.lpHolders.get(provider) || 0;
    if (shares > held || shares <= 0) return null;

    const fraction = shares / pool.totalLPShares;
    const amountA = pool.reserveA * fraction;
    const amountB = pool.reserveB * fraction;

    pool.reserveA -= amountA;
    pool.reserveB -= amountB;
    pool.k = pool.reserveA * pool.reserveB;
    pool.totalLPShares -= shares;
    pool.lpHolders.set(provider, held - shares);

    return { amountA, amountB };
  }

  /**
   * Get current spot price of tokenB in terms of tokenA.
   */
  spotPrice(poolId: string): number | null {
    const pool = this.pools.get(poolId);
    if (!pool || pool.reserveA === 0) return null;
    return pool.reserveB / pool.reserveA;
  }

  getPool(poolId: string): AMMPool | undefined { return this.pools.get(poolId); }
}

// ============================================================
// Revenue Sharing Manager
// ============================================================

class RevenueManager {
  private pools: Map<string, RevenuePool> = new Map();
  private poolCounter: number = 0;

  createPool(
    contributors: Array<{ address: string; weight: number }>,
    strategy: DistributionStrategy,
    vesting: VestingSchedule | null = null
  ): RevenuePool {
    const id = `rev-${++this.poolCounter}`;

    // Normalize weights
    const totalWeight = contributors.reduce((s, c) => s + c.weight, 0);
    const contribMap = new Map<string, ContributorShare>();
    for (const c of contributors) {
      contribMap.set(c.address, {
        address: c.address,
        weight: c.weight / totalWeight,
        earned: 0,
        claimed: 0,
        vestingStart: 0,
        vestingCliff: vesting?.cliffEpochs || 0,
      });
    }

    const pool: RevenuePool = {
      id,
      totalRevenue: 0,
      distributedRevenue: 0,
      contributors: contribMap,
      distributionStrategy: strategy,
      vestingSchedule: vesting,
      lastDistribution: 0,
    };

    this.pools.set(id, pool);
    return pool;
  }

  /**
   * Deposit revenue into the pool.
   */
  deposit(poolId: string, amount: number): boolean {
    const pool = this.pools.get(poolId);
    if (!pool || amount <= 0) return false;
    pool.totalRevenue += amount;
    return true;
  }

  /**
   * Distribute undistributed revenue to contributors.
   */
  distribute(poolId: string, currentEpoch: number): Map<string, number> {
    const pool = this.pools.get(poolId);
    if (!pool) return new Map();

    const undistributed = pool.totalRevenue - pool.distributedRevenue;
    if (undistributed <= 0) return new Map();

    const distributions = new Map<string, number>();
    const strategy = pool.distributionStrategy;

    for (const [addr, share] of pool.contributors) {
      let amount: number;

      switch (strategy.kind) {
        case 'proportional':
          amount = undistributed * share.weight;
          break;

        case 'quadratic': {
          // sqrt(weight) normalized
          const sqrtWeights: number[] = [];
          let sqrtTotal = 0;
          for (const [, s] of pool.contributors) {
            const sq = Math.sqrt(s.weight);
            sqrtWeights.push(sq);
            sqrtTotal += sq;
          }
          const myIdx = [...pool.contributors.keys()].indexOf(addr);
          amount = undistributed * (sqrtWeights[myIdx] / sqrtTotal);
          break;
        }

        case 'tiered': {
          // Find applicable tier based on cumulative earned
          const totalEarned = share.earned;
          let rate = strategy.tiers[0]?.rate || 1;
          for (const tier of strategy.tiers) {
            if (totalEarned >= tier.threshold) {
              rate = tier.rate;
            }
          }
          amount = undistributed * share.weight * rate;
          break;
        }
      }

      share.earned += amount;
      if (share.vestingStart === 0) share.vestingStart = currentEpoch;
      distributions.set(addr, amount);
    }

    pool.distributedRevenue = pool.totalRevenue;
    pool.lastDistribution = currentEpoch;

    return distributions;
  }

  /**
   * Claim vested revenue. Respects cliff and linear vesting.
   */
  claim(poolId: string, address: string, currentEpoch: number): number {
    const pool = this.pools.get(poolId);
    if (!pool) return 0;

    const share = pool.contributors.get(address);
    if (!share) return 0;

    const unclaimed = share.earned - share.claimed;
    if (unclaimed <= 0) return 0;

    // Apply vesting if configured
    if (pool.vestingSchedule) {
      const vs = pool.vestingSchedule;
      const elapsed = currentEpoch - share.vestingStart;

      if (elapsed < vs.cliffEpochs) return 0; // before cliff

      if (vs.linearAfterCliff) {
        const vestingElapsed = elapsed - vs.cliffEpochs;
        const vestingDuration = vs.totalEpochs - vs.cliffEpochs;
        const vestedFraction = Math.min(1, vestingElapsed / vestingDuration);
        const totalClaimable = share.earned * vestedFraction;
        const claimable = totalClaimable - share.claimed;
        if (claimable <= 0) return 0;
        share.claimed += claimable;
        return claimable;
      }
    }

    // No vesting or fully vested: claim all
    share.claimed = share.earned;
    return unclaimed;
  }

  getPool(poolId: string): RevenuePool | undefined { return this.pools.get(poolId); }
}

// ============================================================
// Token Economy Engine (Unified Orchestrator)
// ============================================================

class TokenEconomyEngine {
  private config: TokenConfig;
  private accounts: Map<string, Account> = new Map();
  private supplyController: SupplyController;
  private stakingManager: StakingManager;
  private channelManager: PaymentChannelManager;
  private amm: ConstantProductAMM;
  private revenueManager: RevenueManager;
  private feeCollector: string = 'treasury';
  private feeBalance: number = 0;
  private events: TokenEvent[] = [];
  private currentEpoch: number = 0;
  private currentTick: number = 0;

  constructor(config: TokenConfig) {
    this.config = config;
    this.supplyController = new SupplyController(config);
    this.stakingManager = new StakingManager(
      config.initialSupply * 0.0001, // 0.01% per epoch as staking reward
      7 // 7 epoch unbonding
    );
    this.channelManager = new PaymentChannelManager(10);
    this.amm = new ConstantProductAMM();
    this.revenueManager = new RevenueManager();

    // Initialize treasury account with initial supply
    this.accounts.set(this.feeCollector, {
      address: this.feeCollector,
      balance: config.initialSupply,
      staked: 0,
      locked: 0,
      nonce: 0,
      createdAt: Date.now(),
      lastActiveAt: Date.now(),
    });
  }

  // -- Account Management --

  private getOrCreateAccount(address: string): Account {
    let acc = this.accounts.get(address);
    if (!acc) {
      acc = {
        address,
        balance: 0,
        staked: 0,
        locked: 0,
        nonce: 0,
        createdAt: Date.now(),
        lastActiveAt: Date.now(),
      };
      this.accounts.set(address, acc);
    }
    return acc;
  }

  // -- Token Operations --

  mint(to: string, amount: number): boolean {
    if (!this.supplyController.mint(amount)) return false;
    const acc = this.getOrCreateAccount(to);
    acc.balance += amount;
    this.emit('mint', { to, amount });
    return true;
  }

  burn(from: string, amount: number): boolean {
    const acc = this.accounts.get(from);
    if (!acc || acc.balance < amount) return false;
    if (!this.supplyController.burn(amount)) return false;
    acc.balance -= amount;
    this.emit('burn', { from, amount });
    return true;
  }

  transfer(from: string, to: string, amount: number): boolean {
    const sender = this.accounts.get(from);
    if (!sender || sender.balance < amount || amount <= 0) return false;

    const burnAmount = amount * this.config.burnRate;
    const feeAmount = amount * this.config.transferFee;
    const netAmount = amount - burnAmount - feeAmount;

    sender.balance -= amount;
    sender.nonce++;
    sender.lastActiveAt = Date.now();

    const receiver = this.getOrCreateAccount(to);
    receiver.balance += netAmount;

    if (burnAmount > 0) this.supplyController.burn(burnAmount);
    if (feeAmount > 0) this.feeBalance += feeAmount;

    this.emit('transfer', { from, to, amount, netAmount, burnAmount, feeAmount });
    return true;
  }

  // -- Staking --

  stake(staker: string, amount: number, lockDuration: number = 0, slashConditions: SlashCondition[] = []): StakePosition | null {
    if (!this.config.stakingEnabled) return null;
    const acc = this.accounts.get(staker);
    if (!acc || acc.balance < amount) return null;

    const position = this.stakingManager.stake(staker, amount, this.currentEpoch, lockDuration, slashConditions);
    if (!position) return null;

    acc.balance -= amount;
    acc.staked += amount;
    this.emit('stake', { staker, amount, positionId: position.id });
    return position;
  }

  unstake(positionId: string): boolean {
    if (!this.stakingManager.requestUnstake(positionId, this.currentEpoch)) return false;
    this.emit('unstake', { positionId, epoch: this.currentEpoch });
    return true;
  }

  withdrawStake(positionId: string): { principal: number; rewards: number } | null {
    const result = this.stakingManager.withdraw(positionId, this.currentEpoch);
    if (!result) return null;

    const pos = this.stakingManager.getPosition(positionId);
    if (!pos) return null;

    const acc = this.getOrCreateAccount(pos.staker);
    acc.balance += result.principal + result.rewards;
    acc.staked -= result.principal;

    // Mint rewards
    this.supplyController.mint(result.rewards);
    return result;
  }

  slashStake(positionId: string, conditionKind: string): number {
    const slashed = this.stakingManager.slash(positionId, conditionKind, this.currentEpoch);
    if (slashed > 0) {
      const pos = this.stakingManager.getPosition(positionId);
      if (pos) {
        const acc = this.accounts.get(pos.staker);
        if (acc) acc.staked -= slashed;
      }
      // Slashed tokens go to treasury
      this.feeBalance += slashed;
      this.emit('slash', { positionId, conditionKind, amount: slashed });
    }
    return slashed;
  }

  // -- Payment Channels --

  openChannel(sender: string, receiver: string, deposit: number, ttlTicks: number = 100): PaymentChannel | null {
    if (!this.config.paymentChannelsEnabled) return null;
    const acc = this.accounts.get(sender);
    if (!acc || acc.balance < deposit) return null;

    const channel = this.channelManager.open(sender, receiver, deposit, this.currentTick + ttlTicks);
    if (!channel) return null;

    acc.balance -= deposit;
    acc.locked += deposit;
    this.emit('channel-open', { channelId: channel.id, sender, receiver, deposit });
    return channel;
  }

  updateChannel(update: ChannelUpdate): boolean {
    const result = this.channelManager.update(update);
    if (result) this.emit('channel-update', { channelId: update.channelId, nonce: update.nonce });
    return result;
  }

  closeChannel(channelId: string, cooperative: boolean = true): { senderPayout: number; receiverPayout: number } | null {
    let result: { senderPayout: number; receiverPayout: number } | null;

    if (cooperative) {
      result = this.channelManager.cooperativeClose(channelId);
    } else {
      this.channelManager.requestClose(channelId, this.currentTick);
      return null; // must wait for dispute window
    }

    if (result) {
      const channel = this.channelManager.getChannel(channelId);
      if (channel) {
        this.settleChannel(channel, result);
      }
    }
    return result;
  }

  private settleChannel(channel: PaymentChannel, payouts: { senderPayout: number; receiverPayout: number }): void {
    const senderAcc = this.getOrCreateAccount(channel.sender);
    const receiverAcc = this.getOrCreateAccount(channel.receiver);

    senderAcc.balance += payouts.senderPayout;
    senderAcc.locked -= channel.deposit;
    receiverAcc.balance += payouts.receiverPayout;

    this.emit('channel-close', {
      channelId: channel.id,
      senderPayout: payouts.senderPayout,
      receiverPayout: payouts.receiverPayout,
    });
  }

  // -- AMM --

  createLiquidityPool(
    tokenA: string,
    tokenB: string,
    amountA: number,
    amountB: number,
    provider: string,
    feeRate: number = 0.003
  ): AMMPool | null {
    if (!this.config.ammEnabled) return null;
    return this.amm.createPool(tokenA, tokenB, amountA, amountB, feeRate, provider);
  }

  swap(poolId: string, inputToken: 'A' | 'B', inputAmount: number): { outputAmount: number; priceImpact: number; fee: number } | null {
    const result = this.amm.swap(poolId, inputToken, inputAmount);
    if (result) this.emit('swap', { poolId, inputToken, inputAmount, ...result });
    return result;
  }

  // -- Revenue Sharing --

  createRevenuePool(
    contributors: Array<{ address: string; weight: number }>,
    strategy: DistributionStrategy,
    vesting?: VestingSchedule
  ): RevenuePool {
    return this.revenueManager.createPool(contributors, strategy, vesting || null);
  }

  depositRevenue(poolId: string, amount: number): boolean {
    if (this.revenueManager.deposit(poolId, amount)) {
      this.emit('revenue-deposit', { poolId, amount });
      return true;
    }
    return false;
  }

  distributeRevenue(poolId: string): Map<string, number> {
    const dist = this.revenueManager.distribute(poolId, this.currentEpoch);
    if (dist.size > 0) this.emit('revenue-distribute', { poolId, distributions: Object.fromEntries(dist) });
    return dist;
  }

  claimRevenue(poolId: string, address: string): number {
    const amount = this.revenueManager.claim(poolId, address, this.currentEpoch);
    if (amount > 0) {
      const acc = this.getOrCreateAccount(address);
      acc.balance += amount;
      this.emit('revenue-claim', { poolId, address, amount });
    }
    return amount;
  }

  // -- Epoch Management --

  advanceEpoch(): { minted: number; burned: number; epoch: number } {
    const { minted, burned } = this.supplyController.advanceEpoch();
    this.currentEpoch++;

    // Distribute minted tokens to treasury
    if (minted > 0) {
      const treasury = this.getOrCreateAccount(this.feeCollector);
      treasury.balance += minted;
    }

    this.emit('epoch-advance', { epoch: this.currentEpoch, minted, burned });
    return { minted, burned, epoch: this.currentEpoch };
  }

  tick(): void {
    this.currentTick++;
    // Expire channels
    const expired = this.channelManager.expireChannels(this.currentTick);
    for (const id of expired) {
      const channel = this.channelManager.getChannel(id);
      if (channel) {
        this.settleChannel(channel, {
          senderPayout: channel.senderBalance,
          receiverPayout: channel.receiverBalance,
        });
      }
    }
  }

  // -- Queries --

  getAccount(address: string): Account | undefined { return this.accounts.get(address); }
  getSupply(): number { return this.supplyController.getSupply(); }
  getTotalBurned(): number { return this.supplyController.getBurned(); }
  getTotalStaked(): number { return this.stakingManager.getTotalStaked(); }
  getFeeBalance(): number { return this.feeBalance; }
  getEpoch(): number { return this.currentEpoch; }
  getEvents(limit: number = 50): TokenEvent[] { return this.events.slice(-limit); }

  // -- Events --

  private emit(type: TokenEvent['type'], data: Record<string, unknown>): void {
    this.events.push({ type, timestamp: Date.now(), data });
    if (this.events.length > 10000) {
      this.events = this.events.slice(-5000); // GC
    }
  }
}

// ============================================================
// Presets
// ============================================================

const PRESETS = {
  /**
   * Work token for task marketplaces.
   * Fixed supply, staking enabled, small burn on transfer.
   */
  'task-marketplace': {
    symbol: 'WORK',
    decimals: 6,
    initialSupply: 1_000_000,
    maxSupply: 10_000_000,
    supplySchedule: { kind: 'inflationary' as const, annualRate: 0.05, decayFactor: 0.95 },
    burnRate: 0.001,
    transferFee: 0.002,
    stakingEnabled: true,
    paymentChannelsEnabled: true,
    ammEnabled: true,
  },

  /**
   * Reputation-backed token. Deflationary with staking.
   * Agents must stake to participate in governance.
   */
  'reputation-governance': {
    symbol: 'REP',
    decimals: 6,
    initialSupply: 100_000,
    maxSupply: 100_000,
    supplySchedule: { kind: 'fixed' as const },
    burnRate: 0.005,
    transferFee: 0,
    stakingEnabled: true,
    paymentChannelsEnabled: false,
    ammEnabled: false,
  },

  /**
   * Utility token with bonding curve pricing.
   * Buy/sell against a reserve pool.
   */
  'bonding-curve-utility': {
    symbol: 'UTIL',
    decimals: 6,
    initialSupply: 10_000,
    maxSupply: null,
    supplySchedule: { kind: 'bonding-curve' as const, reserveRatio: 0.5, basePrice: 0.01 },
    burnRate: 0,
    transferFee: 0.001,
    stakingEnabled: false,
    paymentChannelsEnabled: true,
    ammEnabled: true,
  },
};

export {
  TokenEconomyEngine,
  SupplyController,
  StakingManager,
  PaymentChannelManager,
  ConstantProductAMM,
  RevenueManager,
  PRESETS,
  type TokenConfig,
  type Account,
  type StakePosition,
  type PaymentChannel,
  type AMMPool,
  type RevenuePool,
  type TokenEvent,
  type SupplySchedule,
  type DistributionStrategy,
  type VestingSchedule,
};
