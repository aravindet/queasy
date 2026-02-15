/**
 * Handler that always succeeds
 * @param {any} _data - Job data
 * @param {import('../../src/types.js').Job} _job - Job metadata
 */
export async function handle(_data, _job) {
    // Job succeeds immediately
    return;
}
