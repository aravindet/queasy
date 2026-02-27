/**
 * Fuzz test orchestrator.
 *
 * - Spawns NUM_PROCESSES child processes, each running fuzztest/process.ts
 * - Dispatches seed periodic jobs at startup
 * - Reads events from the fuzz:events Redis stream
 * - Checks system invariants after each event
 * - Logs violations without terminating
 * - Prints a summary every 60 seconds and on SIGINT
 */

import { fork } from 'node:child_process';
import type { ChildProcess } from 'node:child_process';
import { createWriteStream } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createClient } from 'redis';
import type { RedisClientType } from 'redis';
import { Client } from '../src/index.ts';
import { STREAM_KEY, readEvents } from './shared/stream.ts';

const __dirname = dirname(fileURLToPath(import.meta.url));

// ── Configuration ──────────────────────────────────────────────────────────────

const NUM_PROCESSES = 4;
const NUM_PERIODIC_JOBS = 5;
const CRASH_INTERVAL_MS = 30_000; // kill a random process this often
const CLOCK_TOLERANCE_MS = 200; // allow this much early-start slop
const STALL_THRESHOLD_MS = 30_000; // no start event → progress violation
const PRIORITY_LAG_MS = 500; // ordering slop between queues
const PROCESS_RESTART_DELAY_MS = 500;
const SUMMARY_INTERVAL_MS = 60_000;
const LOG_FILE = join(__dirname, '..', 'fuzz-output.log');

// ── Logging ────────────────────────────────────────────────────────────────────

const logStream = createWriteStream(LOG_FILE, { flags: 'a' });

function log(level: string, msg: string, data: Record<string, unknown> = {}): void {
    const entry = JSON.stringify({ time: new Date().toISOString(), level, msg, data });
    if (level === 'error') process.stdout.write(`VIOLATION: ${entry}\n`);
    else process.stdout.write(`${entry}\n`);
    logStream.write(`${entry}\n`);
}

// ── Invariant state ────────────────────────────────────────────────────────────

interface ActiveJob {
    queue: string;
    startedAt: number;
    runAt: number;
    pid: number;
}

interface WaitingJob {
    id: string;
    runAt: number;
    dispatchedAt: number;
}

const activeJobs = new Map<string, ActiveJob>();
const succeededJobs = new Set<string>();

/**
 * Per-queue: list of {id, runAt, dispatchedAt} for jobs seen but not yet started.
 * Used to check priority ordering.
 */
const waitingByQueue = new Map<string, WaitingJob[]>();

/** last start event timestamp per queue */
const lastStartPerQueue = new Map<string, number>();

let violationCount = 0;
let eventCount = 0;

function violation(invariant: string, msg: string, data: Record<string, unknown> = {}): void {
    violationCount++;
    log('error', `[${invariant}] ${msg}`, data);
}

// ── Invariant checks ───────────────────────────────────────────────────────────

interface IpcDequeueMsg {
    queue: string;
    jobId: string;
    runAt: number;
}

/**
 * Called when a child process dequeues a job (via IPC).
 */
function onIpcDequeue(pid: number, msg: IpcDequeueMsg): void {
    const { jobId: id, queue, runAt } = msg;

    // Mutual exclusion: job must not already be active
    if (activeJobs.has(id)) {
        const existing = activeJobs.get(id);
        violation('MutualExclusion', `Job ${id} dequeued while already active`, {
            existing,
            newDequeue: { queue, pid },
        });
    }

    activeJobs.set(id, { queue, pid, startedAt: Date.now(), runAt });
}

/**
 * Called when a child process finishes/retries/fails a job (via IPC).
 */
function onIpcJobDone(jobId: string): void {
    activeJobs.delete(jobId);
}

function onStart(event: Record<string, string>): void {
    const { id, queue, runAt: runAtStr, startedAt: startedAtStr } = event;
    const runAt = Number(runAtStr);
    const startedAt = Number(startedAtStr);

    // No re-processing of succeeded jobs (except periodic which re-queues itself)
    if (succeededJobs.has(id) && !id.startsWith('fuzz-periodic-')) {
        violation('NoReprocess', `Job ${id} started after already succeeding`, {
            queue,
            startedAt,
        });
    }

    // Scheduling: not before runAt
    if (runAt > 0 && startedAt < runAt - CLOCK_TOLERANCE_MS) {
        violation('Scheduling', `Job ${id} started ${runAt - startedAt}ms too early`, {
            queue,
            runAt,
            startedAt,
            delta: runAt - startedAt,
        });
    }

    // Priority ordering: no eligible lower-runAt job in same queue waiting
    const waiting = waitingByQueue.get(queue) ?? [];
    for (const w of waiting) {
        if (w.id === id) continue;
        if (w.runAt <= startedAt - CLOCK_TOLERANCE_MS) {
            if (runAt > w.runAt + PRIORITY_LAG_MS) {
                violation(
                    'Ordering',
                    `Job ${id} (runAt=${runAt}) started before ${w.id} (runAt=${w.runAt}) in ${queue}`,
                    {
                        startedId: id,
                        startedRunAt: runAt,
                        waitingId: w.id,
                        waitingRunAt: w.runAt,
                    }
                );
                break;
            }
        }
    }

    lastStartPerQueue.set(queue, startedAt);

    // Remove from waiting list
    if (waitingByQueue.has(queue)) {
        waitingByQueue.set(
            queue,
            (waitingByQueue.get(queue) ?? []).filter((w) => w.id !== id)
        );
    }
}

function onFinish(event: Record<string, string>): void {
    succeededJobs.add(event.id);
}

/**
 * Called when a child process exits. Clears all active jobs belonging to that
 * PID so they don't trigger spurious MutualExclusion violations when the
 * queasy sweep retries them and a new process picks them up.
 */
function onProcessExit(pid: number): void {
    for (const [id, entry] of activeJobs) {
        if (entry.pid === pid) {
            activeJobs.delete(id);
        }
    }
}

/**
 * Called periodically to check queue progress and priority starvation.
 */
function checkProgress(): void {
    const now = Date.now();
    for (const [queue, lastStart] of lastStartPerQueue) {
        const idle = now - lastStart;
        if (idle > STALL_THRESHOLD_MS) {
            violation('Progress', `Queue ${queue} has not processed a job in ${idle}ms`, {
                queue,
                lastStartAt: lastStart,
                idleMs: idle,
            });
        }
    }

    // Priority starvation: cascade-b should not start while periodic has old eligible jobs
    const periodicWaiting = waitingByQueue.get('{fuzz}:periodic') ?? [];
    const eligiblePeriodic = periodicWaiting.filter((w) => w.runAt <= now - PRIORITY_LAG_MS);
    if (eligiblePeriodic.length > 0) {
        const lastBStart = lastStartPerQueue.get('{fuzz}:cascade-b') ?? 0;
        const bStartedAfterPeriodic = eligiblePeriodic.some(
            (w) => lastBStart > w.dispatchedAt + PRIORITY_LAG_MS
        );
        if (bStartedAfterPeriodic) {
            violation(
                'PriorityStarvation',
                'cascade-b processed while periodic had eligible waiting jobs',
                {
                    eligiblePeriodicCount: eligiblePeriodic.length,
                }
            );
        }
    }
}

// ── Event dispatch ─────────────────────────────────────────────────────────────

function handleEvent(event: Record<string, string>): void {
    eventCount++;
    const { type } = event;
    log('info', 'event', event);

    if (type === 'start') {
        const { id, queue, runAt } = event;
        // Register in waiting list for ordering checks (before onStart removes it)
        const runAtNum = Number(runAt);
        const q = waitingByQueue.get(queue) ?? [];
        if (!q.find((w) => w.id === id)) {
            q.push({ id, runAt: runAtNum, dispatchedAt: Date.now() });
            waitingByQueue.set(queue, q);
        }
        onStart(event);
    } else if (type === 'finish') {
        onFinish(event);
    }
}

// ── Summary ────────────────────────────────────────────────────────────────────

function printSummary(): void {
    const summary = {
        events: eventCount,
        violations: violationCount,
        activeJobs: activeJobs.size,
        succeededJobs: succeededJobs.size,
        lastStartPerQueue: Object.fromEntries(lastStartPerQueue),
    };
    log('info', 'Summary', summary);
    console.log(`\n=== Fuzz Summary ===`);
    console.log(`  Events processed : ${eventCount}`);
    console.log(`  Violations found : ${violationCount}`);
    console.log(`  Active jobs      : ${activeJobs.size}`);
    console.log(`  Succeeded jobs   : ${succeededJobs.size}`);
    console.log('===================\n');
}

// ── Child process management ───────────────────────────────────────────────────

const processes = new Set<ChildProcess>();

function spawnProcess(): ChildProcess {
    const child = fork(join(__dirname, 'process.ts'));
    processes.add(child);

    child.on('message', (msg: { type: string; queue: string; jobId: string; runAt: number }) => {
        if (msg.type === 'dequeue') {
            onIpcDequeue(child.pid!, msg);
        } else if (msg.type === 'finish' || msg.type === 'retry' || msg.type === 'fail') {
            onIpcJobDone(msg.jobId);
        }
    });

    child.on('exit', (code, signal) => {
        processes.delete(child);
        log('info', 'Child process exited', { pid: child.pid, code, signal });
        onProcessExit(child.pid!);
        setTimeout(spawnProcess, PROCESS_RESTART_DELAY_MS);
    });

    child.on('error', (err) => {
        log('info', 'Child process error', { pid: child.pid, error: err.message });
    });

    return child;
}

function killRandomProcess(): void {
    const list = [...processes];
    if (list.length === 0) return;
    const target = list[Math.floor(Math.random() * list.length)];
    log('info', 'Killing random child process', { pid: target.pid });
    target.kill('SIGKILL');
}

// ── Redis setup ────────────────────────────────────────────────────────────────

const redis = createClient() as RedisClientType;
await redis.connect();

// Clean up state from previous runs
await redis.del(STREAM_KEY);
log('info', 'Cleared fuzz:events stream from previous run');

// Dispatch seed periodic jobs (await ready to avoid Function not found race)
const dispatchClient = await new Promise<Client>((resolve) => new Client({}, 0, resolve));
const periodicQueue = dispatchClient.queue('{fuzz}:periodic', true);

for (let i = 0; i < NUM_PERIODIC_JOBS; i++) {
    const id = `fuzz-periodic-${i}`;
    await periodicQueue.dispatch({ periodic: true, index: i }, { id });
    log('info', `Dispatched seed job ${id}`);
}

// ── Spawn child processes ──────────────────────────────────────────────────────

for (let i = 0; i < NUM_PROCESSES; i++) {
    spawnProcess();
}

// Periodically kill a random process to simulate crashes
const crashTimer = setInterval(killRandomProcess, CRASH_INTERVAL_MS);

// Periodic progress + starvation check
const progressTimer = setInterval(checkProgress, 10_000);

// Summary timer
const summaryTimer = setInterval(printSummary, SUMMARY_INTERVAL_MS);

log('info', 'Orchestrator started', {
    numProcesses: NUM_PROCESSES,
    numPeriodicJobs: NUM_PERIODIC_JOBS,
    logFile: LOG_FILE,
});

// ── SIGINT handler ─────────────────────────────────────────────────────────────

process.on('SIGINT', () => {
    clearInterval(crashTimer);
    clearInterval(progressTimer);
    clearInterval(summaryTimer);

    for (const child of processes) {
        child.kill('SIGKILL');
    }

    printSummary();
    process.exit(violationCount > 0 ? 1 : 0);
});

// ── Event loop ─────────────────────────────────────────────────────────────────

// Read from the beginning. We cleared the stream above, so '0' reads all
// events from the fresh start without missing anything.
for await (const event of readEvents(redis, '0')) {
    handleEvent(event);
}
