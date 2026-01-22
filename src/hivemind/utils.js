export const truncateToDecimals = (value, decimals) => {
    const factor = Math.pow(10, decimals);
    return Math.floor(value * factor) / factor;
};

// export const isValidNumber = (value) => {
//     if (value == null) return false;
//     const num = typeof value === 'string' ? Number(value) : value;
//     return typeof num === 'number' && !isNaN(num) && isFinite(num);
// };

export const isValidNumber = (value) => {
    const fail = (reason) => {
        const error = new Error(reason);
        console.error('Error:', error.message, '\nInput:', value, '\nType:', typeof value, '\nStack:', error.stack);
        throw error;
    };

    if (value == null) return fail(`Invalid number: null or undefined`);

    if (typeof value !== 'string' && typeof value !== 'number') {
        return fail(`Invalid number: unexpected type "${typeof value}"`);
    }

    let num;
    if (typeof value === 'string') {
        if (value === '') return fail('Invalid number: empty string');
        if (/\s/.test(value)) return fail(`Invalid number: contains whitespace "${value}"`);
        if (!/^-?\d*(\.\d+)?([eE][-+]?\d+)?$/.test(value)) {
            return fail(`Invalid number: malformed string "${value}"`);
        }
        num = Number(value);
    } else {
        num = value;
    }

    if (typeof num !== 'number' || isNaN(num) || !isFinite(num)) {
        return fail(`Invalid number: ${value} (NaN or non-finite)`);
    }

    return true;
};