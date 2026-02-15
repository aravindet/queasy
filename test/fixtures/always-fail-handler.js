/**
 * Handler that always fails with a retriable error
 * @param {any} _data - Job data
 * @param {import('../../src/types.js').Job} _job - Job metadata
 */
export async function handle(_data, _job) {
    throw new Error('Always fails');
}
