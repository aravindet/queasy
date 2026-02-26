import type { Job } from '../../src/types.ts';

export async function handle(data: { delay?: number }, _job: Job): Promise<void> {
    const delay = data?.delay || 1000;
    await new Promise((resolve) => setTimeout(resolve, delay));
}
