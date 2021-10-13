import chai = require('chai');
const assert = chai.assert;
import concatValueArrays from '../src/concatValueArrays';

describe('concatValueArrays', () => {
  it('should work with an empty array', () => {
    assert.deepEqual(concatValueArrays([]), []);
  });

  describe('js arrays', () => {
    it('should concatenate two number arrays', () => {
      assert.deepEqual(
        concatValueArrays([
          [1, 2],
          [3, 4],
        ]),
        [1, 2, 3, 4]
      );
    });
    it('should concatenate three number arrays', () => {
      assert.deepEqual(
        concatValueArrays([
          [1, 2],
          [3, 4],
          [4, 5],
        ]),
        [1, 2, 3, 4, 4, 5]
      );
    });
    it('should concatenate three number arrays with one that is empty', () => {
      assert.deepEqual(concatValueArrays([[1, 2], [], [4, 5]]), [1, 2, 4, 5]);
    });
    it('should work with all arrays except the first empty', () => {
      assert.deepEqual(concatValueArrays([[1, 2], [], []]), [1, 2]);
    });
    it('should work with all arrays empty', () => {
      assert.deepEqual(concatValueArrays([[], [], []]), []);
    });
  });

  describe('Int32Array', () => {
    it('should concatenate two number arrays', () => {
      assert.deepEqual(
        concatValueArrays([Int32Array.from([1, 2]), Int32Array.from([3, 4])]),
        Int32Array.from([1, 2, 3, 4])
      );
    });
    it('should concatenate three number arrays', () => {
      assert.deepEqual(
        concatValueArrays([
          Int32Array.from([1, 2]),
          Int32Array.from([3, 4]),
          Int32Array.from([4, 5]),
        ]),
        Int32Array.from([1, 2, 3, 4, 4, 5])
      );
    });
    it('should concatenate three number arrays with one that is empty', () => {
      assert.deepEqual(
        concatValueArrays([
          Int32Array.from([1, 2]),
          Int32Array.from([]),
          Int32Array.from([4, 5]),
        ]),
        Int32Array.from([1, 2, 4, 5])
      );
    });
    it('should work with all arrays except the first empty', () => {
      assert.deepEqual(
        concatValueArrays([
          Int32Array.from([1, 2]),
          Int32Array.from([]),
          Int32Array.from([]),
        ]),
        Int32Array.from([1, 2])
      );
    });
  });

  describe('Float32Array', () => {
    it('should concatenate two number arrays', () => {
      assert.deepEqual(
        concatValueArrays([
          Float32Array.from([1, 2]),
          Float32Array.from([3, 4]),
        ]),
        Float32Array.from([1, 2, 3, 4])
      );
    });
    it('should concatenate three number arrays', () => {
      assert.deepEqual(
        concatValueArrays([
          Float32Array.from([1, 2]),
          Float32Array.from([3, 4]),
          Float32Array.from([4, 5]),
        ]),
        Float32Array.from([1, 2, 3, 4, 4, 5])
      );
    });
    it('should concatenate three number arrays with one that is empty', () => {
      assert.deepEqual(
        concatValueArrays([
          Float32Array.from([1, 2]),
          Float32Array.from([]),
          Float32Array.from([4, 5]),
        ]),
        Float32Array.from([1, 2, 4, 5])
      );
    });
    it('should work with all arrays except the first empty', () => {
      assert.deepEqual(
        concatValueArrays([
          Float32Array.from([1, 2]),
          Float32Array.from([]),
          Float32Array.from([]),
        ]),
        Float32Array.from([1, 2])
      );
    });
  });

  describe('Float64Array', () => {
    it('should concatenate two number arrays', () => {
      assert.deepEqual(
        concatValueArrays([
          Float64Array.from([1, 2]),
          Float64Array.from([3, 4]),
        ]),
        Float64Array.from([1, 2, 3, 4])
      );
    });
    it('should concatenate three number arrays', () => {
      assert.deepEqual(
        concatValueArrays([
          Float64Array.from([1, 2]),
          Float64Array.from([3, 4]),
          Float64Array.from([4, 5]),
        ]),
        Float64Array.from([1, 2, 3, 4, 4, 5])
      );
    });
    it('should concatenate three number arrays with one that is empty', () => {
      assert.deepEqual(
        concatValueArrays([
          Float64Array.from([1, 2]),
          Float64Array.from([]),
          Float64Array.from([4, 5]),
        ]),
        Float64Array.from([1, 2, 4, 5])
      );
    });
    it('should work with all arrays except the first empty', () => {
      assert.deepEqual(
        concatValueArrays([
          Float64Array.from([1, 2]),
          Float64Array.from([]),
          Float64Array.from([]),
        ]),
        Float64Array.from([1, 2])
      );
    });
  });
});
