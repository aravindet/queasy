export class PermanentError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'PermanentError';
    }
}

export class StallError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'StallError';
    }
}
