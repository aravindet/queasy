#!/usr/bin/env node

// === Distributed Rate Coordination via Adaptive Poisson Thinning ===
//
// Problem
// -------
// N independent workers must collectively call a central service at a target
// rate of ~1 call per minute. Workers cannot communicate with each other.
// The only coordination signal: the service responds to each call with the
// time gap since the previous call (from any worker).
//
// Algorithm
// ---------
// Each worker runs an independent Poisson process (exponentially distributed
// inter-call times) with an adaptive mean interval I.
//
// When a worker calls and receives gap G from the service:
//
//     I ← I × (C / G) ^ α          where C = TARGET × e^{-γ}
//
//   • G < C  →  ratio > 1  →  I increases  →  worker slows down
//   • G > C  →  ratio < 1  →  I decreases  →  worker speeds up
//   • α ∈ (0,1) controls reactivity vs. stability
//
// Why it works: with N workers each at interval I = N·TARGET, the aggregate
// Poisson rate is N/(N·TARGET) = 1/TARGET — exactly the desired rate.
// Gap feedback drives each worker's interval toward N·TARGET without any
// worker needing to know N.
//
// Bias correction (the e^{-γ} factor)
// ------------------------------------
// The update operates in log-space: Δlog(I) = α·(log(C) − log(G)).
// For G ~ Exp(mean μ), E[log(G)] = log(μ) − γ  (γ ≈ 0.5772, Euler-Mascheroni).
// Without correction, E[Δlog(I)] = α·γ ≠ 0, causing systematic upward drift.
// Setting C = TARGET·e^{-γ} cancels this: E[Δlog(I)] = 0 at equilibrium.
//
// The correction is exact for any N because the superposition of N independent
// Poisson processes is itself Poisson, so the gap is always exponential.
//
// Practical notes
// ---------------
// • α = 0.3 gives a good stability/convergence tradeoff.
// • The mean-reverting property (drift toward N·TARGET) keeps the system
//   stable. However, individual worker intervals do a random walk with
//   variance proportional to α, so lower α gives tighter steady-state
//   accuracy at the cost of slower adaptation.
// • After adding workers (upscaling), convergence is fast — new workers see
//   short gaps and immediately increase their intervals.
// • After removing workers (downscaling), convergence is slow — remaining
//   workers have large intervals and thus receive feedback infrequently.
//   This is a fundamental limitation of any feedback-only scheme.

const TARGET_GAP = 60_000; // target: 1 call per 60 seconds
const ALPHA = 0.3;

const EULER_MASCHERONI = 0.5772156649;
const ADJUSTED_TARGET = TARGET_GAP * Math.exp(-EULER_MASCHERONI);

const SIM_DURATION = 300 * 60_000; // 300 simulated minutes (5 hours)

// ── Central Service ──────────────────────────────────────────────────────────

class CentralService {
	constructor() {
		this.lastCallTime = null;
		this.log = [];
	}

	receiveCall(time, workerId) {
		const gap = this.lastCallTime != null ? time - this.lastCallTime : null;
		this.lastCallTime = time;
		this.log.push({ time, workerId, gap });
		return gap;
	}
}

// ── Worker ───────────────────────────────────────────────────────────────────

class Worker {
	constructor(id, startTime) {
		this.id = id;
		this.interval = TARGET_GAP;
		this.nextCallTime = startTime + exponentialRandom(this.interval);
	}

	onGapReceived(gap) {
		if (gap == null || gap <= 0) return;
		this.interval *= (ADJUSTED_TARGET / gap) ** ALPHA;
		this.interval = Math.max(500, Math.min(TARGET_GAP * 100_000, this.interval));
	}

	scheduleNext(now) {
		this.nextCallTime = now + exponentialRandom(this.interval);
	}
}

function exponentialRandom(mean) {
	return mean * -Math.log(Math.random());
}

// ── Event-driven simulation ──────────────────────────────────────────────────

function simulate(phases) {
	const service = new CentralService();
	const workers = [];
	let nextId = 0;

	for (let i = 0; i < phases[0].workers; i++) {
		workers.push(new Worker(nextId++, 0));
	}

	let phaseIdx = 0;
	let now = 0;

	while (now < SIM_DURATION) {
		if (phaseIdx + 1 < phases.length && now >= phases[phaseIdx + 1].at) {
			phaseIdx++;
			const target = phases[phaseIdx].workers;
			while (workers.length < target) workers.push(new Worker(nextId++, now));
			if (workers.length > target) workers.length = target;
		}

		if (workers.length === 0) {
			now += 1000;
			continue;
		}

		let next = workers[0];
		for (let i = 1; i < workers.length; i++) {
			if (workers[i].nextCallTime < next.nextCallTime) next = workers[i];
		}

		now = next.nextCallTime;
		if (now >= SIM_DURATION) break;

		const gap = service.receiveCall(now, next.id);
		next.onGapReceived(gap);
		next.scheduleNext(now);
	}

	return service.log;
}

// ── Configuration ────────────────────────────────────────────────────────────

const phases = [
	{ at: 0, workers: 5 },
	{ at: 100 * 60_000, workers: 12 },
	{ at: 200 * 60_000, workers: 5 },
];

const log = simulate(phases);

// ── Output ───────────────────────────────────────────────────────────────────

console.log('Distributed Rate Coordination — Adaptive Poisson Thinning');
console.log(
	`Target: 1 call/${TARGET_GAP / 1000}s · α=${ALPHA} · correction=e^{-γ}≈${Math.exp(-EULER_MASCHERONI).toFixed(4)}`
);
console.log(`Phases: ${phases.map((p) => `${p.workers}w @${p.at / 60_000}m`).join('  →  ')}\n`);

// Per-5-minute windows (expect ~5 calls each)
const W = 5 * 60_000;
const windows = new Map();
for (const e of log) {
	const idx = Math.floor(e.time / W);
	if (!windows.has(idx)) windows.set(idx, []);
	windows.get(idx).push(e);
}

console.log('   Window │ Calls │ Rate/min │ Avg Gap │ Workers');
console.log('  ────────┼───────┼──────────┼─────────┼────────');

for (let i = 0; i < SIM_DURATION / W; i++) {
	const calls = windows.get(i) || [];
	const n = phases.findLast((p) => p.at <= i * W).workers;
	const rate = (calls.length / 5).toFixed(1);
	const gaps = calls.filter((c) => c.gap != null).map((c) => c.gap);
	const avg = gaps.length
		? `${(gaps.reduce((a, b) => a + b, 0) / gaps.length / 1000).toFixed(0)}s`.padStart(6)
		: '   N/A';
	const t0 = i * 5;
	const minLabel = `${t0}–${t0 + 5}`;
	console.log(
		`  ${minLabel.padStart(7)} │  ${String(calls.length).padStart(3)}  │  ${rate.padStart(5)}  │ ${avg}  │   ${n}`
	);
}

// Per-phase summary
console.log('\n── Per-phase summary ──\n');
for (let p = 0; p < phases.length; p++) {
	const start = phases[p].at;
	const end = p + 1 < phases.length ? phases[p + 1].at : SIM_DURATION;
	const phaseDur = (end - start) / 60_000;

	// Full phase stats
	const all = log.filter((e) => e.time >= start && e.time < end);
	const allGaps = all.filter((e) => e.gap != null).map((e) => e.gap);
	const allRate = allGaps.length
		? 60_000 / (allGaps.reduce((a, b) => a + b, 0) / allGaps.length)
		: 0;

	// Settled stats (after 15 min convergence period)
	const settle = start + 15 * 60_000;
	const settled = log.filter((e) => e.time >= settle && e.time < end && e.gap != null);
	const settledGaps = settled.map((e) => e.gap);
	const settledRate = settledGaps.length
		? 60_000 / (settledGaps.reduce((a, b) => a + b, 0) / settledGaps.length)
		: 0;

	console.log(
		`${String(phases[p].workers).padStart(2)} workers (min ${start / 60_000}–${end / 60_000}):`
	);
	console.log(
		`  Full phase:  ${String(all.length).padStart(3)} calls in ${phaseDur} min → ${allRate.toFixed(2)}/min`
	);
	console.log(
		`  Settled:     ${String(settled.length).padStart(3)} calls in ${phaseDur - 15} min → ${settledRate.toFixed(2)}/min`
	);
}

const totalGaps = log.filter((e) => e.gap != null).map((e) => e.gap);
const overallRate = 60_000 / (totalGaps.reduce((a, b) => a + b, 0) / totalGaps.length);
console.log(
	`\nOverall: ${log.length} calls in ${SIM_DURATION / 60_000} min → ${overallRate.toFixed(2)} calls/min`
);
