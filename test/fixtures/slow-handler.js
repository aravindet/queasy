/**
 * Handler that takes some time to complete (for testing heartbeat)
 * @param {any} data - Job data
 * @param {import('../../src/types.js').Job} _job - Job metadata
 */
export async function handle(data, _job) {
    const delay = data?.delay || 1000;
    await new Promise((resolve) => setTimeout(resolve, delay));
}
