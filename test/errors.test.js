import assert from 'node:assert';
import { describe, it } from 'node:test';
import { PermanentError, StallError } from '../src/errors.js';

describe('Error classes', () => {
    it('PermanentError has correct name and message', () => {
        const err = new PermanentError('test message');
        assert.equal(err.name, 'PermanentError');
        assert.equal(err.message, 'test message');
        assert.ok(err instanceof Error);
    });

    it('StallError has correct name and message', () => {
        const err = new StallError('stall message');
        assert.equal(err.name, 'StallError');
        assert.equal(err.message, 'stall message');
        assert.ok(err instanceof Error);
    });
});
