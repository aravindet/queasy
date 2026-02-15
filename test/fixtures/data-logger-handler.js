/**
 * Handler that logs received data to a global array (for testing)
 * @param {any} data - Job data
 * @param {import('../../src/types.js').Job} job - Job metadata
 */
/** @type {{ data: any, job: import('../../src/types.js').Job }[]} */
export const receivedJobs = [];

/**
 * @param {any} data - Job data
 * @param {import('../../src/types.js').Job} job - Job metadata
 */
export async function handle(data, job) {
    receivedJobs.push({ data, job });
}

export function clearLogs() {
    receivedJobs.length = 0;
}
