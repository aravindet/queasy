import type { Job } from '../../src/types.ts';

export async function handle(data: unknown, _job: Job): Promise<void> {
    // Just succeed to process the failure
    console.log('Failure handler called with:', data);
}
