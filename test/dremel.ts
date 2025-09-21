import chai = require('chai');
const assert = chai.assert;
import parquet = require('../src');
import { ParquetWriteBuffer, shredStatisticsBuffers } from '../src/shred';

// tslint:disable:ter-prefer-arrow-callback
describe('ParquetShredder', function () {
  it('should shred Dremel example', function () {
    const schema = new parquet.ParquetSchema({
      DocId: { type: 'INT64' },
      Links: {
        optional: true,
        fields: {
          Backward: {
            repeated: true,
            type: 'INT64',
          },
          Forward: {
            repeated: true,
            type: 'INT64',
          },
        },
      },
      Name: {
        repeated: true,
        fields: {
          Language: {
            repeated: true,
            fields: {
              Code: { type: 'UTF8' },
              Country: { type: 'UTF8', optional: true },
            },
          },
          Url: { type: 'UTF8', optional: true },
        },
      },
    });

    const r1 = {
      DocId: 10,
      Links: {
        Forward: [20, 40, 60],
      },
      Name: [
        {
          Language: [{ Code: 'en-us', Country: 'us' }, { Code: 'en' }],
          Url: 'http://A',
        },
        {
          Url: 'http://B',
        },
        {
          Language: [{ Code: 'en-gb', Country: 'gb' }],
        },
      ],
    };

    const r2 = {
      DocId: 20,
      Links: {
        Backward: [10, 30],
        Forward: [80],
      },
      Name: [
        {
          Url: 'http://C',
        },
      ],
    };

    const buffer: ParquetWriteBuffer = new ParquetWriteBuffer(schema);
    schema.shredRecord(r1, buffer);
    schema.shredRecord(r2, buffer);

    const expected = {
      rowCount: 2,
      columnData: {
        DocId: {
          count: 2,
          rLevels: [0, 0],
          dLevels: [0, 0],
          values: [10, 20],
        },
        [['Links', 'Forward'].join()]: {count: 4,
          rLevels: [0, 1, 1, 0],
          dLevels: [2, 2, 2, 2],
          values: [20, 40, 60, 80],
        },
        [['Links', 'Backward'].join()]: {
          count: 3,
          rLevels: [0, 0, 1],
          dLevels: [1, 2, 2],
          values: [10, 30],
        },
        [['Name', 'Url'].join()]: {
          count: 4,
          rLevels: [0, 1, 1, 0],
          dLevels: [2, 2, 1, 2],
          values: ['http://A', 'http://B', 'http://C'].map(s => Buffer.from(s)),
        },
        [['Name', 'Language', 'Code'].join()]: {
          count: 5,
          rLevels: [0, 2, 1, 1, 0],
          dLevels: [2, 2, 1, 2, 1],
          values: ['en-us', 'en', 'en-gb'].map(s => Buffer.from(s)),
        },
        [['Name', 'Language', 'Country'].join()]: {
          count: 5,
          rLevels: [0, 2, 1, 1, 0],
          dLevels: [3, 2, 1, 3, 1],
          values: ['us', 'gb'].map(s => Buffer.from(s)),
        },
      },
      statistics: shredStatisticsBuffers(schema),
    };

    assert.deepEqual(buffer.rowCount, expected.rowCount);
    assert.deepEqual(buffer.columnData, expected.columnData);

    const records = schema.materializeRecords(buffer);
    assert.deepEqual(records[0], r1);
    assert.deepEqual(records[1], r2);
  });

  it('should shred a optional nested record with blank optional value', function () {
    const schema = new parquet.ParquetSchema({
      fruit: {
        optional: true,
        fields: {
          color: { type: 'UTF8', repeated: true },
          type: { type: 'UTF8', optional: true },
        },
      },
    });

    const buffer: ParquetWriteBuffer = new ParquetWriteBuffer(schema);
    schema.shredRecord({}, buffer);
    schema.shredRecord({ fruit: {} }, buffer);
    schema.shredRecord({ fruit: { color: [] } }, buffer);
    schema.shredRecord(
      { fruit: { color: ['red', 'blue'], type: 'x' } },
      buffer
    );

    const records = schema.materializeRecords(buffer);
    assert.deepEqual(records[0], {});
    assert.deepEqual(records[1], { fruit: {} });
    assert.deepEqual(records[2], { fruit: {} });
    assert.deepEqual(records[3], {
      fruit: { color: ['red', 'blue'], type: 'x' },
    });
  });
});
