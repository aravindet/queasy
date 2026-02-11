/**
 * Failure handler
 * @param {any} data - Failure data [originalJobId, originalData, error]
 * @param {import('../../src/types.js').Job} job - Job metadata
 */
export async function handle(data, job) {
    // Just succeed to process the failure
    console.log('Failure handler called with:', data);
}
