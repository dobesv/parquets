import { ParquetValueArray, PrimitiveType } from '../declare';
import { CursorBuffer, ParquetCodecOptions } from './declare';
import INT53 = require('int53');

const systemIsLittleEndian =
  new DataView(new Int32Array([1]).buffer).getInt32(0, true) === 1;

export function encodeValues(
  type: PrimitiveType,
  values: ParquetValueArray,
  opts?: ParquetCodecOptions
): Buffer {
  switch (type) {
    case 'BOOLEAN':
      return encodeValues_BOOLEAN(values);
    case 'INT32':
      return encodeValues_INT32(values);
    case 'INT64':
      return encodeValues_INT64(values);
    case 'INT96':
      return encodeValues_INT96(values);
    case 'FLOAT':
      return encodeValues_FLOAT(values);
    case 'DOUBLE':
      return encodeValues_DOUBLE(values);
    case 'BYTE_ARRAY':
      return encodeValues_BYTE_ARRAY(values);
    case 'FIXED_LEN_BYTE_ARRAY':
      return encodeValues_FIXED_LEN_BYTE_ARRAY(values as Buffer[], opts);
    default:
      throw new Error(`unsupported type: ${type}`);
  }
}

export function decodeValues(
  type: PrimitiveType,
  cursor: CursorBuffer,
  count: number,
  opts: ParquetCodecOptions
): ParquetValueArray {
  switch (type) {
    case 'BOOLEAN':
      return decodeValues_BOOLEAN(cursor, count);
    case 'INT32':
      return decodeValues_INT32(cursor, count);
    case 'INT64':
      return decodeValues_INT64(cursor, count);
    case 'INT96':
      return decodeValues_INT96(cursor, count);
    case 'FLOAT':
      return decodeValues_FLOAT(cursor, count);
    case 'DOUBLE':
      return decodeValues_DOUBLE(cursor, count);
    case 'BYTE_ARRAY':
      return decodeValues_BYTE_ARRAY(cursor, count);
    case 'FIXED_LEN_BYTE_ARRAY':
      return decodeValues_FIXED_LEN_BYTE_ARRAY(cursor, count, opts);
    default:
      throw new Error(`unsupported type: ${type}`);
  }
}

/**
 * Encode an array of booleans as a bit sequence.
 *
 * The resulting buffer will be rounded up in size to the nearest whole byte.
 *
 * If the parameter is not actually an array of booleans, "truthy" values will
 * be written with 1, other values will be written as 0.
 */
function encodeValues_BOOLEAN(values: ParquetValueArray): Buffer {
  const buf = Buffer.alloc(Math.ceil(values.length / 8));
  buf.fill(0);
  for (let i = 0; i < values.length; i++) {
    if (values[i]) {
      buf[Math.floor(i / 8)] |= 1 << i % 8;
    }
  }
  return buf;
}

/**
 * Read a bit sequence from a buffer to an array of booleans.
 */
function decodeValues_BOOLEAN(cursor: CursorBuffer, count: number): boolean[] {
  const values: boolean[] = [];
  for (let i = 0; i < count; i++) {
    const b = cursor.buffer[cursor.offset + Math.floor(i / 8)];
    values.push((b & (1 << i % 8)) > 0);
  }
  cursor.offset += Math.ceil(count / 8);
  return values;
}

/**
 * Encode INT32 values to binary.
 *
 * Note that if the input is not an array of number or an Int32Array
 * this may throw an exception.
 */
function encodeValues_INT32(values: ParquetValueArray): Buffer {
  // On little-endian systems we can use typed array to avoid data copying
  if (systemIsLittleEndian) {
    const tab =
      values instanceof Int32Array
        ? values
        : Int32Array.from(values as number[]);
    return Buffer.from(tab.buffer.slice(tab.byteOffset, tab.byteLength));
  }
  const buf = Buffer.alloc(4 * values.length);
  for (let i = 0; i < values.length; i++) {
    buf.writeInt32LE(values[i] as number, i * 4);
  }
  return buf;
}

/**
 * Decode values into an Int32Array.
 */
function decodeValues_INT32(cursor: CursorBuffer, count: number): Int32Array {
  const values =
    systemIsLittleEndian && cursor.offset % 4 === 0
      ? // On little-endian systems we can just use the data as-is
        new Int32Array(cursor.buffer.buffer, cursor.buffer.byteOffset + cursor.offset, count)
      : // Otherwise we have to copy and convert the data
        // tslint:disable-next-line:prefer-array-literal
        Int32Array.from(new Array(count), (_, i) =>
          cursor.buffer.readInt32LE(
            cursor.offset + i * Int32Array.BYTES_PER_ELEMENT
          )
        );
  cursor.offset += count * Int32Array.BYTES_PER_ELEMENT;
  return values;
}

/**
 * Encode INT64 values to a buffer.
 */
function encodeValues_INT64(values: ParquetValueArray): Buffer {
  const buf = Buffer.alloc(8 * values.length);
  for (let i = 0; i < values.length; i++) {
    INT53.writeInt64LE(values[i] as number, buf, i * 8);
  }
  return buf;
}

/**
 * Decode INT64 values from a buffer to an array of numbers
 */
function decodeValues_INT64(cursor: CursorBuffer, count: number): number[] {
  const values: number[] = [];
  for (let i = 0; i < count; i++) {
    values.push(INT53.readInt64LE(cursor.buffer, cursor.offset));
    cursor.offset += 8;
  }
  return values;
}

/**
 * Encode INT96 values to a buffer
 */
function encodeValues_INT96(values: ParquetValueArray): Buffer {
  const buf = Buffer.alloc(12 * values.length);
  for (let i = 0; i < values.length; i++) {
    if (values[i] >= 0) {
      INT53.writeInt64LE(values[i] as number, buf, i * 12);
      buf.writeUInt32LE(0, i * 12 + 8); // truncate to 64 actual precision
    } else {
      INT53.writeInt64LE(~-values[i] + 1, buf, i * 12);
      buf.writeUInt32LE(0xffffffff, i * 12 + 8); // truncate to 64 actual precision
    }
  }
  return buf;
}

/**
 * Decode INT96 values to an array of numbers
 */
function decodeValues_INT96(cursor: CursorBuffer, count: number): number[] {
  const values: number[] = [];
  for (let i = 0; i < count; i++) {
    const low = INT53.readInt64LE(cursor.buffer, cursor.offset);
    const high = cursor.buffer.readUInt32LE(cursor.offset + 8);
    if (high === 0xffffffff) {
      values.push(~-low + 1); // truncate to 64 actual precision
    } else {
      values.push(low); // truncate to 64 actual precision
    }
    cursor.offset += 12;
  }
  return values;
}

/**
 * Encode FLOAT values from an array of numbers or a Float32Array
 */
function encodeValues_FLOAT(values: ParquetValueArray): Buffer {
  // On little-endian systems we can use typed array
  if (systemIsLittleEndian) {
    const tab =
      values instanceof Float32Array
        ? values
        : Float32Array.from(values as number[]);
    return Buffer.from(tab.buffer.slice(tab.byteOffset, tab.byteLength));
  }
  const buf = Buffer.alloc(4 * values.length);
  for (let i = 0; i < values.length; i++) {
    buf.writeFloatLE(values[i] as number, i * 4);
  }
  return buf;
}

/**
 * Decode FLOAT values to a Float32Array
 */
function decodeValues_FLOAT(cursor: CursorBuffer, count: number): Float32Array {
  const values =
    systemIsLittleEndian && cursor.offset % 4 === 0
      ? // On little-endian systems with 4-byte aligned data we can avoid data copying
        new Float32Array(cursor.buffer.buffer, cursor.buffer.byteOffset + cursor.offset, count)
      : // Otherwise we have to copy and convert the data
        // tslint:disable-next-line:prefer-array-literal
        Float32Array.from(new Array(count), (_, i) =>
          cursor.buffer.readFloatLE(
            cursor.offset + i * Float32Array.BYTES_PER_ELEMENT
          )
        );
  cursor.offset += count * Float32Array.BYTES_PER_ELEMENT;
  return values;
}

/**
 * Encode DOUBLE values from an array of numbers or a Float64Array.
 */
function encodeValues_DOUBLE(values: ParquetValueArray): Buffer {
  // On little-endian systems with 8-byte aligned data we can avoid data copying
  if (systemIsLittleEndian) {
    const tab =
      values instanceof Float64Array
        ? values
        : Float64Array.from(values as number[]);
    return Buffer.from(tab.buffer.slice(tab.byteOffset, tab.byteLength));
  }
  const buf = Buffer.alloc(8 * values.length);
  for (let i = 0; i < values.length; i++) {
    buf.writeDoubleLE(values[i] as number, i * 8);
  }
  return buf;
}

function decodeValues_DOUBLE(
  cursor: CursorBuffer,
  count: number
): Float64Array {
  const values =
    systemIsLittleEndian && cursor.offset % 8 === 0
      ? // On little-endian systems with 8-byte aligned data we can avoid data copying
        new Float64Array(cursor.buffer.buffer, cursor.buffer.byteOffset + cursor.offset, count)
      : // Otherwise we have to copy and convert the data
        // tslint:disable-next-line:prefer-array-literal
        Float64Array.from(new Array(count), (_, i) =>
          cursor.buffer.readDoubleLE(
            cursor.offset + i * Float64Array.BYTES_PER_ELEMENT
          )
        );
  cursor.offset += count * Float64Array.BYTES_PER_ELEMENT;
  return values;
}

function encodeValues_BYTE_ARRAY(values: ParquetValueArray): Buffer {
  // tslint:disable-next-line:variable-name
  let buf_len = 0;
  for (let i = 0; i < values.length; i++) {
    const value = values[i] as Buffer;
    const buf = (values[i] = Buffer.from(value));
    buf_len += 4 + buf.length;
  }
  const buf = Buffer.alloc(buf_len);
  // tslint:disable-next-line:variable-name
  let buf_pos = 0;
  for (let i = 0; i < values.length; i++) {
    const value = values[i] as Buffer;
    buf.writeUInt32LE(value.length, buf_pos);
    value.copy(buf, buf_pos + 4);
    buf_pos += 4 + value.length;
  }
  return buf;
}

function decodeValues_BYTE_ARRAY(
  cursor: CursorBuffer,
  count: number
): Buffer[] {
  const values: Buffer[] = [];
  for (let i = 0; i < count; i++) {
    const len = cursor.buffer.readUInt32LE(cursor.offset);
    cursor.offset += 4;
    values.push(cursor.buffer.slice(cursor.offset, cursor.offset + len));
    cursor.offset += len;
  }
  return values;
}

function encodeValues_FIXED_LEN_BYTE_ARRAY(
  values: (Buffer | string)[],
  opts: ParquetCodecOptions
): Buffer {
  if (!opts.typeLength) {
    throw new Error(
      'missing option: typeLength (required for FIXED_LEN_BYTE_ARRAY)'
    );
  }
  if (!values.every(val => val.length === opts.typeLength)) {
    throw new Error(
      'not all values for FIXED_LEN_BYTE_ARRAY have the correct length'
    );
  }
  if (values.every(val => Buffer.isBuffer(val))) {
    return Buffer.concat(values as Buffer[]);
  }
  return Buffer.concat(
    values.map(val => (Buffer.isBuffer(val) ? val : Buffer.from(val)))
  );
}

function decodeValues_FIXED_LEN_BYTE_ARRAY(
  cursor: CursorBuffer,
  count: number,
  opts: ParquetCodecOptions
): Buffer[] {
  const values: Buffer[] = [];
  if (!opts.typeLength) {
    throw new Error(
      'missing option: typeLength (required for FIXED_LEN_BYTE_ARRAY)'
    );
  }
  for (let i = 0; i < count; i++) {
    values.push(
      cursor.buffer.slice(cursor.offset, cursor.offset + opts.typeLength)
    );
    cursor.offset += opts.typeLength;
  }
  return values;
}
