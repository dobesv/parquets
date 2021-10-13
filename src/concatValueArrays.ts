import { ParquetValueArray } from './declare';

/**
 * Merge the given arrays into one.  The resulting array will have the
 * same type as the first one.
 *
 * If only the first array is non-empty (or there is just one array)
 * it will be returned unmodified.
 *
 * @param arrays Arrays to merge
 */
export default function concatValueArrays<T extends ParquetValueArray>(
  arrays: T[]
): T {
  if (arrays.length === 0) {
    return ([] as any) as T;
  }

  const head = arrays[0];
  if (arrays.length === 1) {
    return head;
  }

  // If we're dealing with normal arrays, just use Array.prototype.concat
  if (Array.isArray(head)) {
    return (Array.prototype.concat(...arrays) as any) as T;
  }

  // Now we must be dealing with primitive arrays

  // Calculate the total size of all the arrays combined
  const totalSize = arrays.reduce((a, b) => a + b.length, 0);

  // Are all the arrays after the first actually empty?  If so just return the first one
  if (totalSize === head.length) {
    return head;
  }

  const arrayType = head.constructor as {
    new (length: number): Int32Array | Float32Array | Float64Array;
  };
  const result = new arrayType(totalSize);
  let offset = 0;
  for (const array of arrays) {
    if (array.length) {
      result.set(array as Int32Array | Float32Array | Float64Array, offset);
      offset += array.length;
    }
  }
  return (result as any) as T;
}
