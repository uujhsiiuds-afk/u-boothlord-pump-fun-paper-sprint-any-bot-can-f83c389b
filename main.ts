// Ignition scalper for the pump.fun bonding-curve replay (agent-fun/replay/v2).
//
// Idea: most pump.fun coins die within minutes of launch, but a launch that has
// survived the opening dump and is now rising on real buy flow tends to pop a
// little further before fading. So the bot buys small on confirmed momentum,
// takes a quick profit or cuts the loss, and never looks back: once a coin is
// exited it is done for the run. That strict tier is where the PnL comes from.
// A second, small-stake tier buys young launches on early buy pressure: its job
// is breadth — many cheap round trips that keep the run above the 20-coin
// guardrail and occasionally catch a runner — capped at two open slots so it
// never crowds out the strict tier. Diversification comes from volume of coins,
// not from sizing up any single one.
//
// Per coin, from its CreateEvent on, the bot keeps a short tape of TradeEvents:
// who bought, who sold, how much SOL flowed in net, and whether the creator sold.
// A coin qualifies for a first buy while it is young (age <= MAX_AGE slots),
// liquid enough to take a qualifying round trip (real SOL >= MIN_REAL_SOL so the
// 1%-of-reserve size cap still allows >= MIN_BUY), crowded enough to be real
// (>= MIN_BUYERS distinct non-creator buyers, >= MIN_TRADES trades, net inflow
// positive), not yet run up (price <= CREATE price * (1 + MAX_RUNUP)), and clean
// (creator has not sold, no single sell bigger than MAX_BIG_SELL of reserves).
//
// Sizing: FIRST_SOL on the first buy, then at most one add of ADD_SOL once the
// price is up ADD_STEP over the first buy and net flow is still positive, for a
// maximum of MAX_INVESTED per coin. Every buy is capped by the harness size cap
// (1% of the coin's real SOL reserve per reply), so on a small reserve the stake
// builds over a few batches as the reserve grows.
//
// Exits (sell everything): take profit at TP_BPS over the cost basis, stop loss
// at SL_BPS under it, a trailing stop of TRAIL_BPS once the position has been up
// ARM_BPS, a hard time limit of MAX_HOLD slots, an immediate exit when the
// creator sells while held, and an exit when the recent net flow turns sharply
// negative. Cost basis is the position's cost_lamports plus the tx fees paid on
// its buy fills; sell value is what the bonding curve would pay for the tokens
// now, less the 125 bps curve fee and one tx fee.
//
// Deterministic by construction: no clock, no randomness, no I/O; iteration only
// over insertion-ordered maps fed by the batch stream, so the two grading runs
// produce identical action logs. Diagnostics go to stderr only; stdout is the
// reply channel owned by the shim.

import { LAMPORTS_PER_SOL, runBot, type Batch, type BotState, type OrderInput, type Pubkey } from './vendor/makefun-bot.ts';

// ------------------------------------------------------------------ parameters

const P = {
  // Entry window (slots since CreateEvent): only coins that survived the
  // opening dump and are now rising on real flow.
  MIN_AGE: 150,
  MAX_AGE: 3000,
  // Real SOL reserve window at decision time.
  MIN_REAL_SOL: 1.5,
  MAX_REAL_SOL: 60,
  // Tape quality at entry.
  MIN_TRADES: 8,
  MIN_BUYERS: 4,
  MIN_NET_SOL: 0.5,
  // Creator's own buy in the create slot must stay under this (SOL).
  MAX_DEV_BUY_SOL: 2.5,
  // Price now over the create price, between these bounds (fraction).
  MIN_RUNUP: 0.05,
  MAX_RUNUP: 0.8,
  // Largest single non-creator sell seen, as a share of real SOL reserve.
  MAX_BIG_SELL: 0.25,
  // Sizing (SOL).
  FIRST_SOL: 0.1,
  ADD_SOL: 0.05,
  MAX_INVESTED_SOL: 0.15,
  ADD_STEP: 0.08,
  // Smallest single buy worth a tx fee (SOL); also the guardrail round-trip floor.
  MIN_BUY_SOL: 0.01,
  // Exits (bps of the cost basis). Tier 1: fixed take-profit plus a trailing
  // stop. Tier 2 (see below): a quick take-profit, a tight stop, no trail —
  // bank the pop fast and move on.
  TP_BPS: 2500,
  SL_BPS: 1000,
  TRAIL_ARM_BPS: 1000,
  TRAIL_BPS: 1500,
  // Hard time limit (slots held).
  MAX_HOLD: 5000,
  // Exit when net SOL over the last FLOW_WINDOW slots falls below this.
  FLOW_WINDOW: 60,
  EXIT_FLOW_SOL: -1.0,
  // --- Tier 2 (breadth) -----------------------------------------------------
  // Loose early entries at a small stake: their job is breadth — many cheap
  // round trips that keep the run above the 20-coin guardrail and occasionally
  // catch a runner — while tier 1 carries the PnL. At most T2_MAX_OPEN tier-2
  // positions at once, so they never crowd out tier 1.
  T2_MAX_AGE: 40,
  T2_MIN_TRADES: 4,
  T2_MIN_BUYERS: 3,
  T2_MIN_NET_SOL: 0.1,
  T2_MAX_RUNUP: 0.4,
  T2_FIRST_SOL: 0.015,
  T2_MAX_HOLD: 800,
  T2_MAX_OPEN: 1,
  T2_TP_BPS: 1000,
  T2_SL_BPS: 800,
  T2_TRAIL_ARM_BPS: 100000,
  T2_TRAIL_BPS: 1500,
  T2_EXIT_FLOW_SOL: -1.0,
  // Slippage: sells always go out wide open so an exit is never refused;
  // tier-1 buys are capped so a fill is refused when the price jumped more
  // than this in the 2-slot latency (chasing a top is worse than missing it).
  SLIPPAGE_BPS: 10_000,
  BUY_SLIPPAGE_BPS: 1500,
};

// ------------------------------------------------------------------ constants

const LAMPORTS = 1_000_000_000n;
const BPS = 10_000n;
/** Protocol (95) + creator (30) curve fee, in bps. */
const CURVE_FEE_BPS = 125n;
const SOL = (n: number): bigint => BigInt(Math.round(n * 1e9));

// ------------------------------------------------------------------ tape

interface Tick {
  slot: number;
  isBuy: boolean;
  sol: bigint;
  user: Pubkey;
}

interface Coin {
  mint: Pubkey;
  createSlot: number;
  creator: Pubkey;
  /** Create-time reserves, for the run-up check. */
  vq0: bigint;
  vt0: bigint;
  devBuy: bigint;
  trades: number;
  buyers: Set<Pubkey>;
  bought: bigint;
  soldByOthers: bigint;
  bigSell: bigint;
  devSold: boolean;
  lastTradeSlot: number;
  recent: Tick[];
  buysSent: number;
  invested: bigint;
  lastBuyVq: bigint;
  lastBuyVt: bigint;
  /** Entry tier: 0 = not yet bought, 1 = strict momentum, 2 = broad small. */
  tier: 0 | 1 | 2;
  /** Tier 2 already had its one shot at this coin (it may still enter tier 1). */
  t2done: boolean;
  /** True once exited, completed, or filtered out for good. */
  done: boolean;
}

interface Held {
  openedSlot: number;
  bestValue: bigint;
  buysFilled: number;
  added: boolean;
  tier: 1 | 2;
}

function newCoin(mint: Pubkey, createSlot: number, creator: Pubkey, vq0: bigint, vt0: bigint): Coin {
  return {
    mint,
    createSlot,
    creator,
    vq0,
    vt0,
    devBuy: 0n,
    trades: 0,
    buyers: new Set(),
    bought: 0n,
    soldByOthers: 0n,
    bigSell: 0n,
    devSold: false,
    lastTradeSlot: createSlot,
    recent: [],
    buysSent: 0,
    invested: 0n,
    lastBuyVq: 0n,
    lastBuyVt: 1n,
    tier: 0,
    t2done: false,
    done: false,
  };
}

/** What selling `tokens` against reserves (vq, vt) pays after the curve fee. */
function sellValue(tokens: bigint, vq: bigint, vt: bigint): bigint {
  if (tokens <= 0n || vt <= 0n) return 0n;
  const q = (tokens * vq) / (vt + tokens);
  return q - (q * CURVE_FEE_BPS) / BPS;
}

/** Tier-2 positions open now: held tier-2 positions plus tier-2 buys in flight. */
function tier2Open(state: BotState, coins: Map<Pubkey, Coin>, held: Map<Pubkey, Held>): number {
  let n = 0;
  for (const h of held.values()) if (h.tier === 2) n += 1;
  for (const po of state.pending.values()) {
    if (po.order.side !== 'buy') continue;
    if (held.has(po.order.mint)) continue;
    if (coins.get(po.order.mint)?.tier === 2) n += 1;
  }
  return n;
}

/** Net SOL (buys minus sells) over the last `window` slots, pruning older ticks. */
function netFlow(c: Coin, now: number, window: number): bigint {
  const from = now - window;
  let drop = 0;
  while (drop < c.recent.length && c.recent[drop]!.slot <= from) drop++;
  if (drop > 0) c.recent.splice(0, drop);
  let net = 0n;
  for (const t of c.recent) net += t.isBuy ? t.sol : -t.sol;
  return net;
}

// ------------------------------------------------------------------ strategy

const coins = new Map<Pubkey, Coin>();
const held = new Map<Pubkey, Held>();

await runBot({
  onBatch(state: BotState, batch: Batch): OrderInput[] {
    const now = batch.slot;
    const tx = state.txFeeLamports();
    const cap = state.hello.caps.max_orders_per_batch;
    const orders: OrderInput[] = [];

    // --- fold this batch's fills into the held book -------------------------
    for (const f of batch.fills) {
      if (f.client_id.startsWith('_')) {
        // A harness close (complete / flatten / write-off): position is gone.
        // A tier-2 close frees the coin for a later tier-1 entry; a tier-1
        // close ends the coin.
        const h = held.get(f.mint);
        held.delete(f.mint);
        const c = coins.get(f.mint);
        if (c !== undefined) {
          if (h?.tier === 2) {
            c.t2done = true;
            c.buysSent = 0;
            c.invested = 0n;
            c.tier = 0;
          } else {
            c.done = true;
          }
        }
        continue;
      }
      if (f.side === 'buy' && f.status === 'filled') {
        const h = held.get(f.mint);
        if (h !== undefined) h.buysFilled += 1;
        else {
          const tier = coins.get(f.mint)?.tier;
          held.set(f.mint, { openedSlot: f.slot, bestValue: 0n, buysFilled: 1, added: false, tier: tier === 2 ? 2 : 1 });
        }
      } else if (f.side === 'buy' && (f.status === 'rejected' || f.status === 'failed')) {
        // The entry never happened (price ran away past slippage, or the tx
        // failed): free the coin to try again instead of stranding it.
        const c = coins.get(f.mint);
        if (c !== undefined && !held.has(f.mint) && !state.positions.has(f.mint)) {
          c.buysSent = 0;
          c.invested = 0n;
          c.tier = 0;
        }
      }
    }

    // --- fold this batch's events into the tapes ----------------------------
    const traded = new Set<Pubkey>();
    for (const e of batch.events) {
      if (e.event === 'CreateEvent') {
        if (!coins.has(e.mint)) {
          coins.set(
            e.mint,
            newCoin(e.mint, e.slot, e.creator, BigInt(e.virtual_quote_reserves), BigInt(e.virtual_token_reserves)),
          );
        }
      } else if (e.event === 'TradeEvent') {
        const c = coins.get(e.mint);
        if (c === undefined) continue;
        const sol = BigInt(e.sol_amount);
        c.trades += 1;
        c.lastTradeSlot = e.slot;
        c.recent.push({ slot: e.slot, isBuy: e.is_buy, sol, user: e.user });
        traded.add(e.mint);
        const isDev = e.user === c.creator;
        if (e.is_buy) {
          c.bought += sol;
          if (isDev && e.slot === c.createSlot) c.devBuy += sol;
          else if (!isDev) c.buyers.add(e.user);
        } else if (isDev) {
          c.devSold = true;
        } else {
          c.soldByOthers += sol;
          const rqBefore = BigInt(e.real_quote_reserves) + sol;
          if (rqBefore > 0n) {
            const share = (sol * BPS) / rqBefore;
            if (share > c.bigSell) c.bigSell = share;
          }
        }
      } else {
        const c = coins.get(e.mint);
        if (c !== undefined) c.done = true;
      }
    }

    // --- pending orders by mint ----------------------------------------------
    const pendingBuy = new Set<Pubkey>();
    const pendingSell = new Set<Pubkey>();
    for (const po of state.pending.values()) {
      if (po.order.side === 'buy') pendingBuy.add(po.order.mint);
      else pendingSell.add(po.order.mint);
    }

    // --- exits first ----------------------------------------------------------
    const marks = new Map<Pubkey, { vq: bigint; vt: bigint }>();
    for (const m of batch.marks) {
      marks.set(m.mint, { vq: BigInt(m.virtual_quote_reserves), vt: BigInt(m.virtual_token_reserves) });
    }
    for (const [mint, pos] of state.positions) {
      if (orders.length >= cap) break;
      if (pendingSell.has(mint)) continue;
      const c = coins.get(mint);
      let h = held.get(mint);
      if (h === undefined) {
        const tier = c?.tier;
        h = { openedSlot: now, bestValue: 0n, buysFilled: 0, added: false, tier: tier === 2 ? 2 : 1 };
        held.set(mint, h);
      }
      let mk = marks.get(mint);
      if (mk === undefined) {
        const cv = state.curves.get(mint);
        if (cv === undefined) continue;
        mk = { vq: cv.virtual_quote_reserves, vt: cv.virtual_token_reserves };
      }
      const value = sellValue(pos.token_amount, mk.vq, mk.vt) - tx;
      const basis = pos.cost_lamports + BigInt(h.buysFilled) * tx;
      if (value > h.bestValue) h.bestValue = value;
      const tp = h.tier === 2 ? P.T2_TP_BPS : P.TP_BPS;
      const sl = h.tier === 2 ? P.T2_SL_BPS : P.SL_BPS;
      const arm = h.tier === 2 ? P.T2_TRAIL_ARM_BPS : P.TRAIL_ARM_BPS;
      const trail = h.tier === 2 ? P.T2_TRAIL_BPS : P.TRAIL_BPS;
      const exitFlow = h.tier === 2 ? P.T2_EXIT_FLOW_SOL : P.EXIT_FLOW_SOL;
      let exit = false;
      if (basis > 0n) {
        if (value * BPS >= basis * (BPS + BigInt(tp))) exit = true;
        if (!exit && value * BPS <= basis * (BPS - BigInt(sl))) exit = true;
        if (
          !exit &&
          h.bestValue * BPS >= basis * (BPS + BigInt(arm)) &&
          value * BPS <= h.bestValue * (BPS - BigInt(trail))
        ) {
          exit = true;
        }
      }
      if (!exit && now - h.openedSlot >= (h.tier === 2 ? P.T2_MAX_HOLD : P.MAX_HOLD)) exit = true;
      if (!exit && c !== undefined) {
        if (c.devSold) exit = true;
        else if (netFlow(c, now, P.FLOW_WINDOW) < SOL(exitFlow)) exit = true;
      }
      if (!exit) continue;
      orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: P.SLIPPAGE_BPS });
      // A tier-2 exit frees the coin for a later tier-1 entry (its one
      // tier-2 shot is spent); a tier-1 exit ends the coin.
      if (c !== undefined) {
        if (h.tier === 2) {
          c.t2done = true;
          c.buysSent = 0;
          c.invested = 0n;
          c.tier = 0;
        } else {
          c.done = true;
        }
      }
      held.delete(mint);
    }

    // --- entries and the one momentum add --------------------------------------
    let free = state.freeCashLamports();
    let openSlots = state.hello.caps.max_open_positions - state.committedPositionCount();
    let t2sent = 0; // tier-2 buys sent in this batch (not yet in state.pending)
    for (const mint of traded) {
      if (orders.length >= cap) break;
      const c = coins.get(mint);
      if (c === undefined || c.done) continue;
      const curve = state.curves.get(mint);
      if (curve === undefined || curve.complete) continue;
      if (pendingBuy.has(mint) || pendingSell.has(mint)) continue;

      const h = held.get(mint);
      const isAdd = h !== undefined || c.buysSent > 0;
      if (isAdd) {
        // One add only, tier 1 only: price up ADD_STEP over the first buy,
        // flow still positive, position still held, room under MAX_INVESTED.
        if (h === undefined || h.added || h.tier === 2) continue;
        if (!state.positions.has(mint)) continue;
        if (c.invested >= SOL(P.MAX_INVESTED_SOL) - SOL(P.MIN_BUY_SOL)) continue;
        const vq = curve.virtual_quote_reserves;
        const vt = curve.virtual_token_reserves;
        if (vq * c.lastBuyVt * BPS < c.lastBuyVq * vt * BigInt(10_000 + Math.round(P.ADD_STEP * 10_000))) continue;
        if (netFlow(c, now, P.FLOW_WINDOW) <= 0n) continue;
        const want = SOL(P.MAX_INVESTED_SOL) - c.invested;
        const amount = want < state.maxBuyLamports(mint) ? want : state.maxBuyLamports(mint);
        if (amount < SOL(P.MIN_BUY_SOL) || free < amount + tx) continue;
        orders.push({ side: 'buy', mint, quote_lamports: amount, max_slippage_bps: P.BUY_SLIPPAGE_BPS });
        free -= amount + tx;
        c.invested += amount;
        c.buysSent += 1;
        c.lastBuyVq = vq;
        c.lastBuyVt = vt;
        h.added = true;
        continue;
      }

      // First buy, two tiers. Tier 1 (strict momentum): survived the opening
      // dump, liquid, crowded, clean, rising but not already run away.
      // Tier 2 (broad small): young launch with early buy pressure; cheap
      // breadth, capped at T2_MAX_OPEN slots so it never crowds out tier 1.
      if (openSlots <= 0) continue;
      const age = now - c.createSlot;
      if (age > P.MAX_AGE) {
        c.done = true;
        continue;
      }
      const rq = curve.real_quote_reserves;
      if (rq < SOL(P.MIN_REAL_SOL) || rq > SOL(P.MAX_REAL_SOL)) continue;
      if (c.devBuy > SOL(P.MAX_DEV_BUY_SOL)) continue;
      if (c.devSold) {
        c.done = true;
        continue;
      }
      if (c.bigSell > BigInt(Math.round(P.MAX_BIG_SELL * 10_000))) continue;
      const vq = curve.virtual_quote_reserves;
      const vt = curve.virtual_token_reserves;
      const runupBps =
        c.vq0 > 0n && c.vt0 > 0n ? Number((vq * c.vt0 * BPS) / (c.vq0 * vt)) - 10_000 : 0;
      const flow = netFlow(c, now, P.FLOW_WINDOW);
      let tier: 1 | 2 | 0 = 0;
      let stake = 0n;
      if (
        age >= P.MIN_AGE &&
        c.trades >= P.MIN_TRADES &&
        c.buyers.size >= P.MIN_BUYERS &&
        flow >= SOL(P.MIN_NET_SOL) &&
        runupBps >= Math.round(P.MIN_RUNUP * 10_000) &&
        runupBps <= Math.round(P.MAX_RUNUP * 10_000)
      ) {
        tier = 1;
        stake = SOL(P.FIRST_SOL);
      } else if (
        !c.t2done &&
        age <= P.T2_MAX_AGE &&
        c.trades >= P.T2_MIN_TRADES &&
        c.buyers.size >= P.T2_MIN_BUYERS &&
        flow >= SOL(P.T2_MIN_NET_SOL) &&
        runupBps <= Math.round(P.T2_MAX_RUNUP * 10_000) &&
        tier2Open(state, coins, held) + t2sent < P.T2_MAX_OPEN
      ) {
        tier = 2;
        stake = SOL(P.T2_FIRST_SOL);
      }
      if (tier === 0) continue;
      const amount = stake < state.maxBuyLamports(mint) ? stake : state.maxBuyLamports(mint);
      if (amount < SOL(P.MIN_BUY_SOL) || free < amount + tx) continue;
      orders.push({
        side: 'buy',
        mint,
        quote_lamports: amount,
        max_slippage_bps: tier === 1 ? P.BUY_SLIPPAGE_BPS : P.SLIPPAGE_BPS,
      });
      free -= amount + tx;
      c.invested += amount;
      c.buysSent += 1;
      c.lastBuyVq = vq;
      c.lastBuyVt = vt;
      c.tier = tier;
      if (tier === 2) t2sent += 1;
      openSlots -= 1;
    }

    return orders;
  },
});
