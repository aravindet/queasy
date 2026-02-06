/**
 * Handler that always fails
 * @param {any} data - Job data
 * @param {import('../../src/types.js').Job} job - Job metadata
 */
export async function handle(data, job) {
	throw new Error('Always fails');
}

/**
 * Failure handler
 * @param {any} data - Failure data [originalJobId, originalData, error]
 * @param {import('../../src/types.js').Job} job - Job metadata
 */
export async function handleFailure(data, job) {
	// Just succeed to process the failure
	console.log('Failure handler called with:', data);
}
