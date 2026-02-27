/**
 * Weighted random chaos behavior picker.
 * All handlers apply the same set of chaos behaviors.
 */

type ChaosAction = 'normal' | 'retriable' | 'permanent' | 'stall' | 'spin' | 'crash';

const BEHAVIORS: { action: ChaosAction; weight: number }[] = [
    { action: 'normal', weight: 65 },
    { action: 'retriable', weight: 15 },
    { action: 'permanent', weight: 5 },
    { action: 'stall', weight: 10 },
    { action: 'spin', weight: 3 },
    { action: 'crash', weight: 2 },
];

const TOTAL_WEIGHT = BEHAVIORS.reduce((sum, b) => sum + b.weight, 0);

/**
 * Pick a chaos action based on weighted probability.
 */
export function pickChaos(): ChaosAction {
    let r = Math.random() * TOTAL_WEIGHT;
    for (const { action, weight } of BEHAVIORS) {
        r -= weight;
        if (r <= 0) return action;
    }
    return 'normal';
}
