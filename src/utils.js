/**
 * Generate a random alphanumeric ID
 * @param {number} length - Length of the ID
 * @returns {string}
 */
export function generateId(length = 20) {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let id = '';
    for (let i = 0; i < length; i++) {
        id += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return id;
}

/**
 * Parse a semver string like '1.0' into { major, minor }
 * Used by compareSemver
 * @param {string?} version
 * @returns {number[]}
 */
export function parseVersion(version) {
    const parsed = String(version).split('.').map(Number);
    if (parsed.some((n) => Number.isNaN(n))) return [0];
    return parsed;
}

/**
 * Compare two semver strings. Returns -1 if a < b, 0 if equal, 1 if a > b.
 * @param {number[]} a
 * @param {number[]} b
 * @returns {-1 | 0 | 1}
 */
export function compareSemver(a, b) {
    for (let i = 0; i < Math.min(a.length, b.length); i++) {
        if (a[i] !== b[i]) return a[i] < b[i] ? -1 : 1;
    }
    if (a.length !== b.length) return a.length < b.length ? -1 : 1;
    return 0;
}
