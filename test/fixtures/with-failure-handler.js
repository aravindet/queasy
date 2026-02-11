/**
 * Handler that always fails
 * @param {any} data - Job data
 * @param {import('../../src/types.js').Job} job - Job metadata
 */
export async function handle(data, job) {
    throw new Error('Always fails');
}
