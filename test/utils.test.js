import assert from 'node:assert';
import { describe, it } from 'node:test';
import { compareSemver, parseVersion } from '../src/utils.js';

describe('parseVersion', () => {
	it('should parse a simple major.minor version', () => {
		assert.deepEqual(parseVersion('1.0'), [1, 0]);
	});

	it('should parse a three-part version', () => {
		assert.deepEqual(parseVersion('2.3.4'), [2, 3, 4]);
	});

	it('should return [0] for null input', () => {
		assert.deepEqual(parseVersion(null), [0]);
	});

	it('should return [0] for unparseable input', () => {
		assert.deepEqual(parseVersion('abc'), [0]);
	});

	it('should return [0] for partially unparseable input', () => {
		assert.deepEqual(parseVersion('1.x'), [0]);
	});
});

describe('compareSemver', () => {
	it('should return 0 for equal versions', () => {
		assert.equal(compareSemver([1, 0], [1, 0]), 0);
	});

	it('should return -1 when a < b', () => {
		assert.equal(compareSemver([1, 0], [2, 0]), -1);
	});

	it('should return 1 when a > b', () => {
		assert.equal(compareSemver([2, 0], [1, 0]), 1);
	});

	it('should compare minor versions', () => {
		assert.equal(compareSemver([1, 1], [1, 2]), -1);
		assert.equal(compareSemver([1, 3], [1, 2]), 1);
	});

	it('should handle different-length arrays (a shorter)', () => {
		assert.equal(compareSemver([1, 2], [1, 2, 3]), -1);
	});

	it('should handle different-length arrays (a longer)', () => {
		assert.equal(compareSemver([1, 2, 3], [1, 2]), 1);
	});
});
