/**
 * Handler that logs received data to a global array (for testing)
 * @param {any} data - Job data
 * @param {import('../../src/types.js').Job} job - Job metadata
 */
export const receivedJobs = [];

export async function handle(data, job) {
    receivedJobs.push({ data, job });
}

export function clearLogs() {
    receivedJobs.length = 0;
}
