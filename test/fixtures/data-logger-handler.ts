import type { Job } from '../../src/types.ts';

export const receivedJobs: { data: unknown; job: Job }[] = [];

export async function handle(data: unknown, job: Job): Promise<void> {
    receivedJobs.push({ data, job });
}

export function clearLogs(): void {
    receivedJobs.length = 0;
}
