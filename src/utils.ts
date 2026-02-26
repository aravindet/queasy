export function generateId(length = 20): string {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let id = '';
    for (let i = 0; i < length; i++) {
        id += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return id;
}

export function parseVersion(version: string | null | undefined): number[] {
    const parsed = String(version).split('.').map(Number);
    if (parsed.some((n) => Number.isNaN(n))) return [0];
    return parsed;
}

export function compareSemver(a: number[], b: number[]): -1 | 0 | 1 {
    for (let i = 0; i < Math.min(a.length, b.length); i++) {
        if (a[i] !== b[i]) return a[i] < b[i] ? -1 : 1;
    }
    if (a.length !== b.length) return a.length < b.length ? -1 : 1;
    return 0;
}
