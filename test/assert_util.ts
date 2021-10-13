import chai = require('chai');
import { ParquetValueArray } from '../src';
const assert = chai.assert;

const EPSILON_DEFAULT = 0.01;

export function assertArrayEqualEpsilon(
  a: ParquetValueArray,
  b: ParquetValueArray,
  e?: number
): void {
  assert.equal(a.length, b.length);
  for (let i = 0; i < a.length; ++i) {
    const aa = a[i] as number;
    const bb = b[i] as number;
    assert(Math.abs(aa - bb) < (e || EPSILON_DEFAULT));
  }
}

test('Ok', () => void 0);
