/**
 * @param {number} len
 */
export const createUint8ArrayFromLen = len => new Uint8Array(len)

/**
 * Create Uint8Array with initial content from buffer
 *
 * @param {ArrayBuffer} buffer
 * @param {number} byteOffset
 * @param {number} length
 */
 export const createUint8ArrayViewFromArrayBuffer = (buffer, byteOffset, length) => new Uint8Array(buffer, byteOffset, length)

/**
 * Copy the content of an Uint8Array view to a new ArrayBuffer.
 *
 * @param {Uint8Array} uint8Array
 * @return {Uint8Array}
 */
export const copyUint8Array = uint8Array => {
    const newBuf = createUint8ArrayFromLen(uint8Array.byteLength)
    newBuf.set(uint8Array)
    return newBuf
}