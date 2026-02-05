/**
 * Handler that always throws an error
 * @param {any} data - Job data
 * @param {import('../../src/types.js').Job} job - Job metadata
 */
export async function handle(data, job) {
	throw new Error('Job failed');
}
