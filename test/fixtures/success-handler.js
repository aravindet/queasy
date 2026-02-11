/**
 * Handler that always succeeds
 * @param {any} data - Job data
 * @param {import('../../src/types.js').Job} job - Job metadata
 */
export async function handle(data, job) {
    // Job succeeds immediately
    return;
}
