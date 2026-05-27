import { describe, it } from 'node:test';
import chai = require('chai');
const assert: Chai.AssertStatic = chai.assert;
import parquet = require('../src');

// tslint:disable:ter-prefer-arrow-callback

describe('ParquetBufferWriter', function () {
  const testSchema = new parquet.ParquetSchema({
    name: { type: 'UTF8' },
    value: { type: 'INT32' },
  });

  interface TestRow {
    name: string;
    value: number;
  }

  describe('zero rows', function () {
    it('should produce valid Parquet readable by ParquetBufferReader', function () {
      const writer = parquet.ParquetBufferWriter.openBuffer<TestRow>(testSchema);
      const buf = writer.toBuffer();

      assert.instanceOf(buf, Buffer);
      assert.isTrue(buf.length > 0);

      const reader = parquet.ParquetBufferReader.openBuffer<TestRow>(buf);
      assert.equal(reader.getRowCount(), 0);

      const rows = [...reader];
      assert.deepEqual(rows, []);
    });
  });

  describe('single row', function () {
    it('should write one row and read it back with correct values', function () {
      const writer = parquet.ParquetBufferWriter.openBuffer<TestRow>(testSchema);
      writer.appendRow({ name: 'alice', value: 42 });
      const buf = writer.toBuffer();

      const reader = parquet.ParquetBufferReader.openBuffer<TestRow>(buf);
      assert.equal(reader.getRowCount(), 1);

      const rows = [...reader];
      assert.deepEqual(rows, [{ name: 'alice', value: 42 }]);
    });
  });

  describe('multi row group flush', function () {
    it('should write rowGroupSize + 1 rows and read all back correctly', function () {
      const rowGroupSize = 3;
      const writer = parquet.ParquetBufferWriter.openBuffer<TestRow>(testSchema, {
        rowGroupSize,
      });

      // Write rowGroupSize + 1 rows to trigger a flush
      const expectedRows: TestRow[] = [];
      for (let i = 0; i < rowGroupSize + 1; i++) {
        const row = { name: `item${i}`, value: i * 10 };
        expectedRows.push(row);
        writer.appendRow(row);
      }

      const buf = writer.toBuffer();

      const reader = parquet.ParquetBufferReader.openBuffer<TestRow>(buf);
      assert.equal(reader.getRowCount(), rowGroupSize + 1);

      const rows = [...reader];
      assert.deepEqual(rows, expectedRows);
    });
  });

  describe('toBuffer twice', function () {
    it('should throw "writer was closed" on second call', function () {
      const writer = parquet.ParquetBufferWriter.openBuffer<TestRow>(testSchema);
      writer.appendRow({ name: 'test', value: 1 });
      writer.toBuffer();

      assert.throws(function () {
        writer.toBuffer();
      }, 'writer was closed');
    });
  });

  describe('appendRow after toBuffer', function () {
    it('should throw "writer was closed"', function () {
      const writer = parquet.ParquetBufferWriter.openBuffer<TestRow>(testSchema);
      writer.appendRow({ name: 'test', value: 1 });
      writer.toBuffer();

      assert.throws(function () {
        writer.appendRow({ name: 'after', value: 2 });
      }, 'writer was closed');
    });
  });

  describe('round-trip', function () {
    it('should write N rows and read back exact same values', function () {
      const numRows = 50;
      const writer = parquet.ParquetBufferWriter.openBuffer<TestRow>(testSchema, {
        rowGroupSize: 10,
      });

      const expectedRows: TestRow[] = [];
      for (let i = 0; i < numRows; i++) {
        const row = { name: `row${i}`, value: i * 100 };
        expectedRows.push(row);
        writer.appendRow(row);
      }

      const buf = writer.toBuffer();

      const reader = parquet.ParquetBufferReader.openBuffer<TestRow>(buf);
      assert.equal(reader.getRowCount(), numRows);

      const rows = [...reader];
      assert.deepEqual(rows, expectedRows);
    });

    it('should round-trip correctly with useDataPageV2 enabled', function () {
      const numRows = 50;
      const writer = parquet.ParquetBufferWriter.openBuffer<TestRow>(testSchema, {
        rowGroupSize: 10,
        useDataPageV2: true,
      });

      const expectedRows: TestRow[] = [];
      for (let i = 0; i < numRows; i++) {
        const row = { name: `row${i}`, value: i * 100 };
        expectedRows.push(row);
        writer.appendRow(row);
      }

      const buf = writer.toBuffer();

      const reader = parquet.ParquetBufferReader.openBuffer<TestRow>(buf);
      assert.equal(reader.getRowCount(), numRows);

      const rows = [...reader];
      assert.deepEqual(rows, expectedRows);
    });

    it('should round-trip correctly with small pageSize forcing multiple pages', function () {
      const numRows = 20;
      const writer = parquet.ParquetBufferWriter.openBuffer<TestRow>(testSchema, {
        rowGroupSize: 20,
        pageSize: 3,
      });

      const expectedRows: TestRow[] = [];
      for (let i = 0; i < numRows; i++) {
        const row = { name: `row${i}`, value: i };
        expectedRows.push(row);
        writer.appendRow(row);
      }

      const buf = writer.toBuffer();
      const reader = parquet.ParquetBufferReader.openBuffer<TestRow>(buf);
      assert.equal(reader.getRowCount(), numRows);
      assert.deepEqual([...reader], expectedRows);
    });

    it('should round-trip repeated field rows with small pageSize', function () {
      const schema = new parquet.ParquetSchema({
        name: { type: 'UTF8' },
        tags: { type: 'UTF8', repeated: true },
      });
      interface RepRow { name: string; tags?: string[]; }
      const rows: RepRow[] = [
        { name: 'a', tags: ['x', 'y'] },
        { name: 'b', tags: ['z'] },
        { name: 'c', tags: ['p', 'q', 'r'] },
        { name: 'd' },
        { name: 'e', tags: ['s'] },
      ];

      const writer = parquet.ParquetBufferWriter.openBuffer<RepRow>(schema, {
        rowGroupSize: 10,
        pageSize: 2,
      });
      for (const row of rows) {
        writer.appendRow(row);
      }

      const buf = writer.toBuffer();
      const reader = parquet.ParquetBufferReader.openBuffer<RepRow>(buf);
      assert.equal(reader.getRowCount(), rows.length);
      const readRows = [...reader];
      assert.deepEqual(readRows, rows);
    });
  });
});

describe('generateParquetBuffer', function () {
  it('should produce the same result as ParquetBufferWriter', function () {
    const schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      value: { type: 'INT32' },
    });
    const rows = [
      { name: 'a', value: 1 },
      { name: 'b', value: 2 },
      { name: 'c', value: 3 },
    ];

    const buf = parquet.generateParquetBuffer(schema, rows);

    const reader = parquet.ParquetBufferReader.openBuffer<{ name: string; value: number }>(buf);
    assert.equal(reader.getRowCount(), rows.length);
    assert.deepEqual([...reader], rows);
  });
});
