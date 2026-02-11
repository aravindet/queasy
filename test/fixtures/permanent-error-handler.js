import { PermanentError } from '../../src/errors.js';

/**
 * Handler that throws a PermanentError
 * @param {any} data - Job data
 * @param {import('../../src/types.js').Job} job - Job metadata
 */
export async function handle(data, job) {
    throw new PermanentError('Permanent failure');
}
