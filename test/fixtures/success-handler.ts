import type { Job } from '../../src/types.ts';

export async function handle(_data: unknown, _job: Job): Promise<void> {
    // Job succeeds immediately
    return;
}
