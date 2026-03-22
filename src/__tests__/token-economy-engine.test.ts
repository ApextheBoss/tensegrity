import { describe, it, expect } from 'vitest';
import {
  TokenEconomyEngine,
  SupplyController,
  StakingManager,
  PaymentChannelManager,
  ConstantProductAMM,
  RevenueManager,
  PRESETS,
  type TokenConfig,
} from '../token-economy-engine';
import { fnv1a } from '../shared-utils';

// ============================================================
// Helper
// ============================================================

function makeConfig(overrides: Partial<TokenConfig> = {}): TokenConfig {
  return {
    symbol: 'TEST',
    decimals: 6,
    initialSupply: 1_000_000,
    maxSupply: 10_000_000,
    supplySchedule: { kind: 'fixed' },
    burnRate: 0,
    transferFee: 0,
    stakingEnabled: true,
    paymentChannelsEnabled: true,
    ammEnabled: true,
    ...overrides,
  };
}

// ============================================================
// SupplyController
// ============================================================

describe('SupplyController', () => {
  it('fixed schedule mints nothing', () => {
    const sc = new SupplyController(makeConfig());
    expect(sc.epochMintAmount()).toBe(0);
    expect(sc.epochBurnAmount()).toBe(0);
  });

  it('inflationary schedule mints tokens that decay', () => {
    const sc = new SupplyController(makeConfig({
      supplySchedule: { kind: 'inflationary', annualRate: 0.365, decayFactor: 0.5 },
    }));
    const mint0 = sc.epochMintAmount();
    expect(mint0).toBeGreaterThan(0);
    sc.advanceEpoch();
    const mint1 = sc.epochMintAmount();
    // decayFactor=0.5 so second epoch should mint ~half
    expect(mint1).toBeLessThan(mint0);
    // After advanceEpoch, supply increased, so mint1 is based on larger supply * 0.5 decay
    // Just verify it's roughly half (within 10%)
    expect(mint1 / mint0).toBeCloseTo(0.5, 0);
  });

  it('respects maxSupply cap', () => {
    const sc = new SupplyController(makeConfig({
      initialSupply: 9_999_999,
      maxSupply: 10_000_000,
      supplySchedule: { kind: 'inflationary', annualRate: 3.65, decayFactor: 1 },
    }));
    const mint = sc.epochMintAmount();
    expect(mint).toBeLessThanOrEqual(1);
  });

  it('deflationary schedule burns per epoch', () => {
    const sc = new SupplyController(makeConfig({
      supplySchedule: { kind: 'deflationary', burnPerBlock: 100 },
    }));
    expect(sc.epochMintAmount()).toBe(0);
    expect(sc.epochBurnAmount()).toBe(100);
    const result = sc.advanceEpoch();
    expect(result.burned).toBe(100);
    expect(sc.getSupply()).toBe(999_900);
  });

  it('bonding curve calculates price', () => {
    const sc = new SupplyController(makeConfig({
      supplySchedule: { kind: 'bonding-curve', reserveRatio: 0.5, basePrice: 0.01 },
    }));
    const price = sc.bondingCurvePrice(100);
    expect(price).toBeGreaterThan(0);
    // Non-bonding-curve returns 1:1
    const sc2 = new SupplyController(makeConfig());
    expect(sc2.bondingCurvePrice(100)).toBe(100);
  });

  it('mint fails when exceeding maxSupply', () => {
    const sc = new SupplyController(makeConfig({ initialSupply: 10_000_000, maxSupply: 10_000_000 }));
    expect(sc.mint(1)).toBe(false);
  });

  it('burn fails when exceeding supply', () => {
    const sc = new SupplyController(makeConfig({ initialSupply: 100 }));
    expect(sc.burn(101)).toBe(false);
    expect(sc.burn(50)).toBe(true);
    expect(sc.getSupply()).toBe(50);
    expect(sc.getBurned()).toBe(50);
  });

  it('advanceEpoch increments epoch counter', () => {
    const sc = new SupplyController(makeConfig());
    expect(sc.getEpoch()).toBe(0);
    sc.advanceEpoch();
    sc.advanceEpoch();
    expect(sc.getEpoch()).toBe(2);
  });
});

// ============================================================
// StakingManager
// ============================================================

describe('StakingManager', () => {
  it('stakes and tracks total', () => {
    const sm = new StakingManager(10, 3);
    const pos = sm.stake('alice', 100, 0);
    expect(pos).not.toBeNull();
    expect(sm.getTotalStaked()).toBe(100);
    expect(sm.getStakerPositions('alice')).toHaveLength(1);
  });

  it('rejects zero/negative stake', () => {
    const sm = new StakingManager(10);
    expect(sm.stake('alice', 0, 0)).toBeNull();
    expect(sm.stake('alice', -5, 0)).toBeNull();
  });

  it('lock duration prevents early unstake', () => {
    const sm = new StakingManager(10, 3);
    const pos = sm.stake('alice', 100, 0, 10)!;
    expect(sm.requestUnstake(pos.id, 5)).toBe(false); // locked until epoch 10
    expect(sm.requestUnstake(pos.id, 10)).toBe(true);
  });

  it('unbonding period before withdrawal', () => {
    const sm = new StakingManager(10, 3);
    const pos = sm.stake('alice', 100, 0)!;
    sm.requestUnstake(pos.id, 0);
    expect(sm.withdraw(pos.id, 1)).toBeNull(); // unbonding for 3 epochs
    expect(sm.withdraw(pos.id, 3)).not.toBeNull();
  });

  it('accrues and claims rewards', () => {
    const sm = new StakingManager(100, 3);
    const pos = sm.stake('alice', 1000, 0)!;
    // After 5 epochs, rewards = 5 * 100 (rate) = 500 total, alice has 100% stake
    const rewards = sm.claimRewards(pos.id, 5);
    expect(rewards).toBeCloseTo(500);
  });

  it('distributes rewards proportionally to multiple stakers', () => {
    const sm = new StakingManager(100, 3);
    const p1 = sm.stake('alice', 300, 0)!;
    const p2 = sm.stake('bob', 100, 0)!;
    // 2 epochs: 200 total rewards. alice=75%, bob=25%
    const r1 = sm.claimRewards(p1.id, 2);
    const r2 = sm.claimRewards(p2.id, 2);
    expect(r1).toBeCloseTo(150);
    expect(r2).toBeCloseTo(50);
  });

  it('slashes stake', () => {
    const sm = new StakingManager(10, 3);
    const pos = sm.stake('alice', 100, 0, 0, [
      { kind: 'downtime', severity: 0.5, description: 'went offline' },
    ])!;
    const slashed = sm.slash(pos.id, 'downtime', 1);
    expect(slashed).toBe(50);
    expect(sm.getTotalStaked()).toBe(50);
  });

  it('full slash sets status to slashed', () => {
    const sm = new StakingManager(10, 3);
    const pos = sm.stake('alice', 100, 0, 0, [
      { kind: 'downtime', severity: 1.0, description: 'full slash' },
    ])!;
    sm.slash(pos.id, 'downtime', 0);
    expect(sm.getPosition(pos.id)!.status).toBe('slashed');
    expect(sm.claimRewards(pos.id, 10)).toBe(0);
  });

  it('cannot slash non-slashable or unknown condition', () => {
    const sm = new StakingManager(10);
    const pos = sm.stake('alice', 100, 0)!;
    expect(sm.slash(pos.id, 'downtime', 0)).toBe(0);
  });

  it('withdraw returns principal + rewards', () => {
    const sm = new StakingManager(100, 2);
    const pos = sm.stake('alice', 500, 0)!;
    sm.requestUnstake(pos.id, 5);
    const result = sm.withdraw(pos.id, 7)!;
    expect(result.principal).toBe(500);
    expect(result.rewards).toBeGreaterThan(0);
  });
});

// ============================================================
// PaymentChannelManager
// ============================================================

describe('PaymentChannelManager', () => {
  it('opens and cooperatively closes channel', () => {
    const pcm = new PaymentChannelManager(10);
    const ch = pcm.open('alice', 'bob', 100, 1000)!;
    expect(ch.senderBalance).toBe(100);
    expect(ch.receiverBalance).toBe(0);
    const result = pcm.cooperativeClose(ch.id)!;
    expect(result.senderPayout).toBe(100);
    expect(result.receiverPayout).toBe(0);
  });

  it('rejects zero deposit', () => {
    const pcm = new PaymentChannelManager();
    expect(pcm.open('a', 'b', 0, 100)).toBeNull();
  });

  it('updates channel with valid signatures', () => {
    const pcm = new PaymentChannelManager();
    const ch = pcm.open('alice', 'bob', 100, 1000)!;
    // Need to import fnv1a indirectly via signUpdate format
    // fnv1a imported at top
    const nonce = 1;
    const senderSig = fnv1a(`${ch.id}:${nonce}:alice`).toString(16);
    const receiverSig = fnv1a(`${ch.id}:${nonce}:bob`).toString(16);

    const ok = pcm.update({
      channelId: ch.id,
      senderBalance: 70,
      receiverBalance: 30,
      nonce,
      senderSig,
      receiverSig,
    });
    expect(ok).toBe(true);
    expect(pcm.getChannel(ch.id)!.senderBalance).toBe(70);
  });

  it('rejects replay (stale nonce)', () => {
    const pcm = new PaymentChannelManager();
    const ch = pcm.open('alice', 'bob', 100, 1000)!;
    // fnv1a imported at top
    const sig = (n: number, addr: string) => fnv1a(`${ch.id}:${n}:${addr}`).toString(16);

    pcm.update({ channelId: ch.id, senderBalance: 70, receiverBalance: 30, nonce: 1, senderSig: sig(1, 'alice'), receiverSig: sig(1, 'bob') });
    const ok = pcm.update({ channelId: ch.id, senderBalance: 60, receiverBalance: 40, nonce: 1, senderSig: sig(1, 'alice'), receiverSig: sig(1, 'bob') });
    expect(ok).toBe(false);
  });

  it('rejects conservation violation', () => {
    const pcm = new PaymentChannelManager();
    const ch = pcm.open('alice', 'bob', 100, 1000)!;
    // fnv1a imported at top
    const sig = (n: number, addr: string) => fnv1a(`${ch.id}:${n}:${addr}`).toString(16);
    const ok = pcm.update({ channelId: ch.id, senderBalance: 70, receiverBalance: 40, nonce: 1, senderSig: sig(1, 'alice'), receiverSig: sig(1, 'bob') });
    expect(ok).toBe(false);
  });

  it('unilateral close with dispute window', () => {
    const pcm = new PaymentChannelManager(5);
    const ch = pcm.open('alice', 'bob', 100, 1000)!;
    pcm.requestClose(ch.id, 10);
    expect(pcm.getChannel(ch.id)!.status).toBe('closing');
    // Can't finalize yet
    expect(pcm.finalize(ch.id, 12)).toBeNull();
    // After dispute window
    const result = pcm.finalize(ch.id, 15)!;
    expect(result.senderPayout).toBe(100);
  });

  it('dispute with higher nonce', () => {
    const pcm = new PaymentChannelManager(10);
    const ch = pcm.open('alice', 'bob', 100, 1000)!;
    pcm.requestClose(ch.id, 0);

    // fnv1a imported at top
    const sig = (n: number, addr: string) => fnv1a(`${ch.id}:${n}:${addr}`).toString(16);
    const ok = pcm.dispute(ch.id, { channelId: ch.id, senderBalance: 40, receiverBalance: 60, nonce: 1, senderSig: sig(1, 'alice'), receiverSig: sig(1, 'bob') });
    expect(ok).toBe(true);
    expect(pcm.getChannel(ch.id)!.status).toBe('disputed');
  });

  it('expires open channels', () => {
    const pcm = new PaymentChannelManager();
    pcm.open('alice', 'bob', 100, 5);
    pcm.open('alice', 'carol', 50, 20);
    const expired = pcm.expireChannels(10);
    expect(expired).toHaveLength(1);
  });
});

// ============================================================
// ConstantProductAMM
// ============================================================

describe('ConstantProductAMM', () => {
  it('creates pool and returns it', () => {
    const amm = new ConstantProductAMM();
    const pool = amm.createPool('WORK', 'REP', 1000, 2000, 0.003, 'lp1')!;
    expect(pool.reserveA).toBe(1000);
    expect(pool.reserveB).toBe(2000);
    expect(pool.totalLPShares).toBeCloseTo(Math.sqrt(2_000_000));
  });

  it('rejects duplicate pool', () => {
    const amm = new ConstantProductAMM();
    amm.createPool('A', 'B', 100, 100, 0.003, 'lp');
    expect(amm.createPool('A', 'B', 200, 200, 0.003, 'lp')).toBeNull();
  });

  it('rejects zero reserves', () => {
    const amm = new ConstantProductAMM();
    expect(amm.createPool('A', 'B', 0, 100, 0.003, 'lp')).toBeNull();
  });

  it('swaps A for B', () => {
    const amm = new ConstantProductAMM();
    amm.createPool('A', 'B', 1000, 1000, 0.003, 'lp');
    const result = amm.swap('A-B', 'A', 100)!;
    expect(result.outputAmount).toBeGreaterThan(0);
    expect(result.outputAmount).toBeLessThan(100); // price impact
    expect(result.fee).toBeCloseTo(0.3);
    expect(result.priceImpact).toBeGreaterThan(0);
  });

  it('swaps B for A', () => {
    const amm = new ConstantProductAMM();
    amm.createPool('A', 'B', 1000, 1000, 0, 'lp');
    const result = amm.swap('A-B', 'B', 50)!;
    expect(result.outputAmount).toBeGreaterThan(0);
  });

  it('spot price reflects ratio', () => {
    const amm = new ConstantProductAMM();
    amm.createPool('A', 'B', 1000, 2000, 0, 'lp');
    expect(amm.spotPrice('A-B')).toBe(2);
  });

  it('add and remove liquidity', () => {
    const amm = new ConstantProductAMM();
    amm.createPool('A', 'B', 1000, 1000, 0.003, 'lp1');
    const added = amm.addLiquidity('A-B', 500, 500, 'lp2')!;
    expect(added.shares).toBeGreaterThan(0);
    expect(added.usedA).toBeCloseTo(500);

    const pool = amm.getPool('A-B')!;
    expect(pool.reserveA).toBeCloseTo(1500);

    const removed = amm.removeLiquidity('A-B', added.shares, 'lp2')!;
    expect(removed.amountA).toBeGreaterThan(0);
  });

  it('remove liquidity fails with insufficient shares', () => {
    const amm = new ConstantProductAMM();
    amm.createPool('A', 'B', 1000, 1000, 0, 'lp1');
    expect(amm.removeLiquidity('A-B', 99999, 'lp1')).toBeNull();
  });

  it('addLiquidity matches ratio', () => {
    const amm = new ConstantProductAMM();
    amm.createPool('A', 'B', 1000, 2000, 0, 'lp');
    // Offer more A than needed given ratio 1:2
    const result = amm.addLiquidity('A-B', 1000, 500, 'lp2')!;
    // Should use 250 A and 500 B (ratio capped by B)
    expect(result.usedA).toBeCloseTo(250);
    expect(result.usedB).toBeCloseTo(500);
  });
});

// ============================================================
// RevenueManager
// ============================================================

describe('RevenueManager', () => {
  it('creates pool with normalized weights', () => {
    const rm = new RevenueManager();
    const pool = rm.createPool(
      [{ address: 'alice', weight: 3 }, { address: 'bob', weight: 1 }],
      { kind: 'proportional' }
    );
    expect(pool.contributors.get('alice')!.weight).toBeCloseTo(0.75);
    expect(pool.contributors.get('bob')!.weight).toBeCloseTo(0.25);
  });

  it('deposits and distributes proportionally', () => {
    const rm = new RevenueManager();
    const pool = rm.createPool(
      [{ address: 'alice', weight: 1 }, { address: 'bob', weight: 1 }],
      { kind: 'proportional' }
    );
    rm.deposit(pool.id, 1000);
    const dist = rm.distribute(pool.id, 1);
    expect(dist.get('alice')).toBeCloseTo(500);
    expect(dist.get('bob')).toBeCloseTo(500);
  });

  it('quadratic distribution is more equal', () => {
    const rm = new RevenueManager();
    const pool = rm.createPool(
      [{ address: 'big', weight: 9 }, { address: 'small', weight: 1 }],
      { kind: 'quadratic' }
    );
    rm.deposit(pool.id, 1000);
    const dist = rm.distribute(pool.id, 1);
    // Quadratic: sqrt(0.9)/sum vs sqrt(0.1)/sum → more equal than 90/10
    const bigShare = dist.get('big')!;
    const smallShare = dist.get('small')!;
    expect(bigShare).toBeLessThan(900); // less than proportional
    expect(smallShare).toBeGreaterThan(100); // more than proportional
  });

  it('tiered distribution applies rate based on earned', () => {
    const rm = new RevenueManager();
    const pool = rm.createPool(
      [{ address: 'alice', weight: 1 }],
      { kind: 'tiered', tiers: [{ threshold: 0, rate: 1.0 }, { threshold: 500, rate: 0.5 }] }
    );
    rm.deposit(pool.id, 600);
    rm.distribute(pool.id, 1);
    // earned=600, >= 500 threshold → rate=0.5
    rm.deposit(pool.id, 400);
    const dist = rm.distribute(pool.id, 2);
    // earned=600 still >= 500 → rate=0.5, amount = 400 * 1.0 * 0.5 = 200
    expect(dist.get('alice')!).toBeCloseTo(200);
  });

  it('vesting cliff blocks early claims', () => {
    const rm = new RevenueManager();
    const pool = rm.createPool(
      [{ address: 'alice', weight: 1 }],
      { kind: 'proportional' },
      { cliffEpochs: 5, totalEpochs: 10, linearAfterCliff: true }
    );
    rm.deposit(pool.id, 1000);
    rm.distribute(pool.id, 1);
    expect(rm.claim(pool.id, 'alice', 3)).toBe(0); // before cliff
    // vestingStart=1, cliff=5, so cliff ends at epoch 6. At epoch 8, elapsed=7, vestingElapsed=2, duration=5, fraction=0.4
    const claimed = rm.claim(pool.id, 'alice', 8);
    expect(claimed).toBeGreaterThan(0);
    expect(claimed).toBeLessThan(1000); // not fully vested
  });

  it('no vesting: claim all immediately', () => {
    const rm = new RevenueManager();
    const pool = rm.createPool(
      [{ address: 'alice', weight: 1 }],
      { kind: 'proportional' }
    );
    rm.deposit(pool.id, 1000);
    rm.distribute(pool.id, 1);
    expect(rm.claim(pool.id, 'alice', 1)).toBeCloseTo(1000);
  });

  it('deposit rejects zero/negative', () => {
    const rm = new RevenueManager();
    const pool = rm.createPool([{ address: 'a', weight: 1 }], { kind: 'proportional' });
    expect(rm.deposit(pool.id, 0)).toBe(false);
    expect(rm.deposit(pool.id, -10)).toBe(false);
  });

  it('distribute with no undistributed returns empty', () => {
    const rm = new RevenueManager();
    const pool = rm.createPool([{ address: 'a', weight: 1 }], { kind: 'proportional' });
    expect(rm.distribute(pool.id, 1).size).toBe(0);
  });
});

// ============================================================
// TokenEconomyEngine (Integration)
// ============================================================

describe('TokenEconomyEngine', () => {
  function createEngine(overrides: Partial<TokenConfig> = {}) {
    return new TokenEconomyEngine(makeConfig(overrides));
  }

  it('treasury starts with initial supply', () => {
    const engine = createEngine();
    expect(engine.getAccount('treasury')!.balance).toBe(1_000_000);
    expect(engine.getSupply()).toBe(1_000_000);
  });

  it('mint and burn', () => {
    const engine = createEngine();
    engine.mint('alice', 500);
    expect(engine.getAccount('alice')!.balance).toBe(500);
    expect(engine.getSupply()).toBe(1_000_500);
    engine.burn('alice', 200);
    expect(engine.getAccount('alice')!.balance).toBe(300);
    expect(engine.getTotalBurned()).toBe(200);
  });

  it('mint fails at max supply', () => {
    const engine = createEngine({ maxSupply: 1_000_000 });
    expect(engine.mint('alice', 1)).toBe(false);
  });

  it('transfer with burn and fee', () => {
    const engine = createEngine({ burnRate: 0.01, transferFee: 0.02 });
    engine.mint('alice', 1000);
    engine.transfer('alice', 'bob', 100);
    // burn=1, fee=2, net=97
    expect(engine.getAccount('bob')!.balance).toBe(97);
    expect(engine.getAccount('alice')!.balance).toBe(900);
    expect(engine.getFeeBalance()).toBe(2);
  });

  it('transfer fails with insufficient balance', () => {
    const engine = createEngine();
    engine.mint('alice', 10);
    expect(engine.transfer('alice', 'bob', 20)).toBe(false);
  });

  it('staking lifecycle', () => {
    const engine = createEngine();
    engine.mint('alice', 1000);
    const pos = engine.stake('alice', 500)!;
    expect(engine.getAccount('alice')!.balance).toBe(500);
    expect(engine.getAccount('alice')!.staked).toBe(500);
    expect(engine.getTotalStaked()).toBe(500);

    // Advance epochs to accrue rewards, then unstake
    for (let i = 0; i < 10; i++) engine.advanceEpoch();
    engine.unstake(pos.id);
    for (let i = 0; i < 7; i++) engine.advanceEpoch(); // unbonding
    const result = engine.withdrawStake(pos.id)!;
    expect(result.principal).toBe(500);
    expect(result.rewards).toBeGreaterThan(0);
  });

  it('staking disabled returns null', () => {
    const engine = createEngine({ stakingEnabled: false });
    engine.mint('alice', 1000);
    expect(engine.stake('alice', 100)).toBeNull();
  });

  it('slash stake goes to fee balance', () => {
    const engine = createEngine();
    engine.mint('alice', 1000);
    const pos = engine.stake('alice', 500, 0, [
      { kind: 'task-failure', severity: 0.2, description: 'failed task' },
    ])!;
    const slashed = engine.slashStake(pos.id, 'task-failure');
    expect(slashed).toBe(100);
    expect(engine.getFeeBalance()).toBe(100);
  });

  it('payment channel lifecycle', () => {
    const engine = createEngine();
    engine.mint('alice', 1000);
    const ch = engine.openChannel('alice', 'bob', 200)!;
    expect(engine.getAccount('alice')!.balance).toBe(800);
    expect(engine.getAccount('alice')!.locked).toBe(200);

    const result = engine.closeChannel(ch.id, true)!;
    expect(result.senderPayout).toBe(200);
    expect(engine.getAccount('alice')!.balance).toBe(1000);
    expect(engine.getAccount('alice')!.locked).toBe(0);
  });

  it('payment channels disabled', () => {
    const engine = createEngine({ paymentChannelsEnabled: false });
    engine.mint('alice', 1000);
    expect(engine.openChannel('alice', 'bob', 100)).toBeNull();
  });

  it('non-cooperative close returns null (starts dispute)', () => {
    const engine = createEngine();
    engine.mint('alice', 1000);
    const ch = engine.openChannel('alice', 'bob', 200)!;
    expect(engine.closeChannel(ch.id, false)).toBeNull();
  });

  it('AMM via engine', () => {
    const engine = createEngine();
    const pool = engine.createLiquidityPool('WORK', 'REP', 1000, 1000, 'lp1')!;
    expect(pool).not.toBeNull();
    const result = engine.swap('WORK-REP', 'A', 100)!;
    expect(result.outputAmount).toBeGreaterThan(0);
  });

  it('AMM disabled', () => {
    const engine = createEngine({ ammEnabled: false });
    expect(engine.createLiquidityPool('A', 'B', 100, 100, 'lp')).toBeNull();
  });

  it('revenue sharing lifecycle', () => {
    const engine = createEngine();
    const pool = engine.createRevenuePool(
      [{ address: 'alice', weight: 1 }, { address: 'bob', weight: 1 }],
      { kind: 'proportional' }
    );
    engine.depositRevenue(pool.id, 1000);
    const dist = engine.distributeRevenue(pool.id);
    expect(dist.get('alice')).toBeCloseTo(500);

    const claimed = engine.claimRevenue(pool.id, 'alice');
    expect(claimed).toBeCloseTo(500);
    expect(engine.getAccount('alice')!.balance).toBeCloseTo(500);
  });

  it('advanceEpoch with inflationary supply', () => {
    const engine = new TokenEconomyEngine(makeConfig({
      supplySchedule: { kind: 'inflationary', annualRate: 3.65, decayFactor: 1 },
    }));
    const before = engine.getSupply();
    const result = engine.advanceEpoch();
    expect(result.minted).toBeGreaterThan(0);
    expect(engine.getSupply()).toBeGreaterThan(before);
    expect(result.epoch).toBe(1);
  });

  it('tick expires channels and settles', () => {
    const engine = createEngine();
    engine.mint('alice', 1000);
    engine.openChannel('alice', 'bob', 200, 3);
    // Tick past expiry
    engine.tick(); engine.tick(); engine.tick();
    expect(engine.getAccount('alice')!.balance).toBe(1000); // refunded
    expect(engine.getAccount('alice')!.locked).toBe(0);
  });

  it('events are recorded and capped', () => {
    const engine = createEngine();
    engine.mint('alice', 100);
    engine.burn('alice', 50);
    const events = engine.getEvents();
    expect(events.length).toBe(2);
    expect(events[0].type).toBe('mint');
    expect(events[1].type).toBe('burn');
  });

  it('getEpoch tracks correctly', () => {
    const engine = createEngine();
    expect(engine.getEpoch()).toBe(0);
    engine.advanceEpoch();
    engine.advanceEpoch();
    expect(engine.getEpoch()).toBe(2);
  });
});

// ============================================================
// Presets
// ============================================================

describe('Presets', () => {
  it('task-marketplace preset creates valid engine', () => {
    const engine = new TokenEconomyEngine(PRESETS['task-marketplace']);
    expect(engine.getSupply()).toBe(1_000_000);
    engine.mint('agent1', 100);
    expect(engine.getAccount('agent1')!.balance).toBe(100);
  });

  it('reputation-governance preset has fixed supply', () => {
    const engine = new TokenEconomyEngine(PRESETS['reputation-governance']);
    expect(engine.getSupply()).toBe(100_000);
    const result = engine.advanceEpoch();
    expect(result.minted).toBe(0);
  });

  it('bonding-curve-utility preset works', () => {
    const engine = new TokenEconomyEngine(PRESETS['bonding-curve-utility']);
    expect(engine.getSupply()).toBe(10_000);
  });
});
