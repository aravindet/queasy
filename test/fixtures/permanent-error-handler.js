import { PermanentError } from '../../src/errors.js';

/**
 * Handler that throws a PermanentError
 * @param {any} _data - Job data
 * @param {import('../../src/types.js').Job} _job - Job metadata
 */
export async function handle(_data, _job) {
    throw new PermanentError('Permanent failure');
}
