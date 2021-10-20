import chai = require('chai');
import parquet = require('../src');

const assert = chai.assert;
import { ParquetBuffer, SchemaDefinition } from '../src/declare';
import {
  materializeColumn,
  materializeRecords,
  ParquetWriteBuffer,
  shredRecord,
} from '../src/shred';
import { ParquetSchema } from '../src';

const testShred = function (
  schemaDefn: SchemaDefinition,
  records: Record<string, any>[],
  expectedShredResult: ParquetBuffer
) {
  const schema = new ParquetSchema(schemaDefn);
  const buf: ParquetWriteBuffer = new ParquetWriteBuffer(schema);
  for (const record of records) {
    shredRecord(schema, record, buf);
  }
  assert.deepEqual(buf, expectedShredResult);
  assert.deepEqual(materializeRecords(schema, buf), records);
};

// tslint:disable:ter-prefer-arrow-callback
describe('ParquetShredder', function () {
  it('should shred a single simple record', function () {
    testShred(
      {
        name: { type: 'UTF8' },
        quantity: { type: 'INT64' },
        price: { type: 'DOUBLE' },
        listDate: { type: 'DATE' },
      },
      [
        {
          name: 'apple',
          quantity: 10,
          price: 23.5,
          listDate: new Date('2021-01-13T00:00:00.000Z'),
        },
      ],
      {
        columnData: {
          name: {
            count: 1,
            dLevels: [0],
            rLevels: [0],
            values: [Buffer.from('apple')],
          },
          quantity: {
            count: 1,
            dLevels: [0],
            rLevels: [0],
            values: [10],
          },
          price: {
            count: 1,
            dLevels: [0],
            rLevels: [0],
            values: [23.5],
          },
          listDate: {
            count: 1,
            dLevels: [0],
            rLevels: [0],
            values: [18640],
          },
        },
        rowCount: 1,
      }
    );
  });

  it('should shred a list of simple records', function () {
    testShred(
      {
        name: { type: 'UTF8' },
        quantity: { type: 'INT64' },
        price: { type: 'DOUBLE' },
      },
      [
        { name: 'apple', quantity: 10, price: 23.5 },
        { name: 'orange', quantity: 20, price: 17.1 },
        { name: 'banana', quantity: 15, price: 42 },
      ],
      {
        rowCount: 3,
        columnData: {
          name: {
            count: 3,
            dLevels: [0, 0, 0],
            rLevels: [0, 0, 0],
            values: ['apple', 'orange', 'banana'].map(s => Buffer.from(s)),
          },
          quantity: {
            count: 3,
            dLevels: [0, 0, 0],
            rLevels: [0, 0, 0],
            values: [10, 20, 15],
          },
          price: {
            count: 3,
            dLevels: [0, 0, 0],
            rLevels: [0, 0, 0],
            values: [23.5, 17.1, 42],
          },
        },
      }
    );
  });

  it('should shred a list of simple records with optional scalar fields', function () {
    testShred(
      {
        name: { type: 'UTF8' },
        quantity: { type: 'INT64', optional: true },
        price: { type: 'DOUBLE' },
      },
      [
        { name: 'apple', quantity: 10, price: 23.5 },
        { name: 'orange', price: 17.1 },
        { name: 'banana', quantity: 15, price: 42 },
      ],
      {
        rowCount: 3,
        columnData: {
          name: {
            count: 3,
            dLevels: [0, 0, 0],
            rLevels: [0, 0, 0],
            values: ['apple', 'orange', 'banana'].map(s => Buffer.from(s)),
          },
          quantity: {
            count: 3,
            dLevels: [1, 0, 1],
            rLevels: [0, 0, 0],
            values: [10, 15],
          },
          price: {
            count: 3,
            dLevels: [0, 0, 0],
            rLevels: [0, 0, 0],
            values: [23.5, 17.1, 42],
          },
        },
      }
    );
  });

  it('should shred a list of simple records with repeated scalar fields', function () {
    testShred(
      {
        name: { type: 'UTF8' },
        colours: { type: 'UTF8', repeated: true },
        price: { type: 'DOUBLE' },
      },
      [
        { name: 'apple', price: 23.5, colours: ['red', 'green'] },
        { name: 'orange', price: 17.1, colours: ['orange'] },
        { name: 'banana', price: 42, colours: ['yellow'] },
      ],
      {
        rowCount: 3,
        columnData: {
          name: {
            count: 3,
            dLevels: [0, 0, 0],
            rLevels: [0, 0, 0],
            values: ['apple', 'orange', 'banana'].map(s => Buffer.from(s)),
          },
          colours: {
            count: 4,
            dLevels: [1, 1, 1, 1],
            rLevels: [0, 1, 0, 0],
            values: ['red', 'green', 'orange', 'yellow'].map(s =>
              Buffer.from(s)
            ),
          },
          price: {
            count: 3,
            dLevels: [0, 0, 0],
            rLevels: [0, 0, 0],
            values: [23.5, 17.1, 42],
          },
        },
      }
    );
  });

  it('should shred a nested record without repetition modifiers', function () {
    testShred(
      {
        name: { type: 'UTF8' },
        stock: {
          fields: {
            quantity: { type: 'INT64' },
            warehouse: { type: 'UTF8' },
          },
        },
        price: { type: 'DOUBLE' },
      },
      [
        {
          name: 'apple',
          stock: { quantity: 10, warehouse: 'A' },
          price: 23.5,
        },
        {
          name: 'banana',
          stock: { quantity: 20, warehouse: 'B' },
          price: 42.0,
        },
      ],
      {
        rowCount: 2,
        columnData: {
          name: {
            count: 2,
            dLevels: [0, 0],
            rLevels: [0, 0],
            values: ['apple', 'banana'].map(s => Buffer.from(s)),
          },
          [['stock', 'quantity'].join()]: {
            count: 2,
            dLevels: [0, 0],
            rLevels: [0, 0],
            values: [10, 20],
          },
          [['stock', 'warehouse'].join()]: {
            count: 2,
            dLevels: [0, 0],
            rLevels: [0, 0],
            values: ['A', 'B'].map(s => Buffer.from(s)),
          },
          price: {
            count: 2,
            dLevels: [0, 0],
            rLevels: [0, 0],
            values: [23.5, 42],
          },
        },
      }
    );
  });

  it('should shred a nested record with optional fields', function () {
    testShred(
      {
        name: { type: 'UTF8' },
        stock: {
          fields: {
            quantity: { type: 'INT64', optional: true },
            warehouse: { type: 'UTF8' },
          },
        },
        price: { type: 'DOUBLE' },
      },
      [
        {
          name: 'apple',
          stock: { quantity: 10, warehouse: 'A' },
          price: 23.5,
        },
        { name: 'banana', stock: { warehouse: 'B' }, price: 42.0 },
      ],
      {
        rowCount: 2,
        columnData: {
          name: {
            count: 2,
            dLevels: [0, 0],
            rLevels: [0, 0],
            values: ['apple', 'banana'].map(s => Buffer.from(s)),
          },
          [['stock', 'quantity'].join()]: {
            count: 2,
            dLevels: [1, 0],
            rLevels: [0, 0],
            values: [10],
          },
          [['stock', 'warehouse'].join()]: {
            count: 2,
            dLevels: [0, 0],
            rLevels: [0, 0],
            values: ['A', 'B'].map(s => Buffer.from(s)),
          },
          price: {
            count: 2,
            dLevels: [0, 0],
            rLevels: [0, 0],
            values: [23.5, 42],
          },
        },
      }
    );
  });

  it('should shred a nested record with nested optional fields', function () {
    testShred(
      {
        name: { type: 'UTF8' },
        stock: {
          optional: true,
          fields: {
            quantity: { type: 'INT64', optional: true },
            warehouse: { type: 'UTF8' },
          },
        },
        price: { type: 'DOUBLE' },
      },
      [
        {
          name: 'apple',
          stock: { quantity: 10, warehouse: 'A' },
          price: 23.5,
        },
        { name: 'orange', price: 17.0 },
        { name: 'banana', stock: { warehouse: 'B' }, price: 42.0 },
      ],
      {
        rowCount: 3,
        columnData: {
          name: {
            count: 3,
            dLevels: [0, 0, 0],
            rLevels: [0, 0, 0],
            values: ['apple', 'orange', 'banana'].map(s => Buffer.from(s)),
          },
          [['stock', 'quantity'].join()]: {
            count: 3,
            dLevels: [2, 0, 1],
            rLevels: [0, 0, 0],
            values: [10],
          },
          [['stock', 'warehouse'].join()]: {
            count: 3,
            dLevels: [1, 0, 1],
            rLevels: [0, 0, 0],
            values: ['A', 'B'].map(s => Buffer.from(s)),
          },
          price: {
            count: 3,
            dLevels: [0, 0, 0],
            rLevels: [0, 0, 0],
            values: [23.5, 17.0, 42.0],
          },
        },
      }
    );
  });

  it('should shred a nested record with repeated fields', function () {
    testShred(
      {
        name: { type: 'UTF8' },
        stock: {
          fields: {
            quantity: { type: 'INT64', repeated: true },
            warehouse: { type: 'UTF8' },
          },
        },
        price: { type: 'DOUBLE' },
      },
      [
        {
          name: 'apple',
          stock: { quantity: [10], warehouse: 'A' },
          price: 23.5,
        },
        {
          name: 'orange',
          stock: { quantity: [50, 75], warehouse: 'B' },
          price: 17.0,
        },
        { name: 'banana', stock: { warehouse: 'C' }, price: 42.0 },
      ],
      {
        rowCount: 3,
        columnData: {
          name: {
            count: 3,
            dLevels: [0, 0, 0],
            rLevels: [0, 0, 0],
            values: ['apple', 'orange', 'banana'].map(s => Buffer.from(s)),
          },
          [['stock', 'quantity'].join()]: {
            count: 4,
            dLevels: [1, 1, 1, 0],
            rLevels: [0, 0, 1, 0],
            values: [10, 50, 75],
          },
          [['stock', 'warehouse'].join()]: {
            count: 3,
            dLevels: [0, 0, 0],
            rLevels: [0, 0, 0],
            values: ['A', 'B', 'C'].map(s => Buffer.from(s)),
          },
          price: {
            count: 3,
            dLevels: [0, 0, 0],
            rLevels: [0, 0, 0],
            values: [23.5, 17.0, 42.0],
          },
        },
      }
    );
  });

  it('should shred a nested record with nested repeated fields', function () {
    testShred(
      {
        name: { type: 'UTF8' },
        stock: {
          repeated: true,
          fields: {
            quantity: { type: 'INT64', repeated: true },
            warehouse: { type: 'UTF8' },
          },
        },
        price: { type: 'DOUBLE' },
      },
      [
        {
          name: 'apple',
          stock: [
            { quantity: [10], warehouse: 'A' },
            { quantity: [20], warehouse: 'B' },
          ],
          price: 23.5,
        },
        {
          name: 'orange',
          stock: [{ quantity: [50, 75], warehouse: 'X' }],
          price: 17.0,
        },
        { name: 'kiwi', price: 99.0 },
        { name: 'banana', stock: [{ warehouse: 'C' }], price: 42.0 },
      ],
      {
        rowCount: 4,
        columnData: {
          name: {
            count: 4,
            dLevels: [0, 0, 0, 0],
            rLevels: [0, 0, 0, 0],
            values: ['apple', 'orange', 'kiwi', 'banana'].map(s =>
              Buffer.from(s)
            ),
          },
          [['stock', 'quantity'].join()]: {
            count: 6,
            dLevels: [2, 2, 2, 2, 0, 1],
            rLevels: [0, 1, 0, 2, 0, 0],
            values: [10, 20, 50, 75],
          },
          [['stock', 'warehouse'].join()]: {
            count: 5,
            dLevels: [1, 1, 1, 0, 1],
            rLevels: [0, 1, 0, 0, 0],
            values: ['A', 'B', 'X', 'C'].map(s => Buffer.from(s)),
          },
          price: {
            count: 4,
            dLevels: [0, 0, 0, 0],
            rLevels: [0, 0, 0, 0],
            values: [23.5, 17.0, 99.0, 42.0],
          },
        },
      }
    );
  });

  it('should materialize a nested record with scalar repeated fields', function () {
    const schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      price: { type: 'DOUBLE', repeated: true },
    });

    const buffer: ParquetBuffer = {
      rowCount: 4,
      columnData: {
        name: {
          dLevels: [0, 0, 0, 0],
          rLevels: [0, 0, 0, 0],
          values: [
            Buffer.from([97, 112, 112, 108, 101]),
            Buffer.from([111, 114, 97, 110, 103, 101]),
            Buffer.from([107, 105, 119, 105]),
            Buffer.from([98, 97, 110, 97, 110, 97]),
          ],
          count: 4,
        },
        price: {
          dLevels: [1, 1, 1, 1, 1, 1],
          rLevels: [0, 0, 1, 0, 1, 0],
          values: [23.5, 17, 23, 99, 100, 42],
          count: 6,
        },
      },
    };

    const records = parquet.ParquetShredder.materializeRecords(schema, buffer);

    assert.deepEqual(records, [
      { name: 'apple', price: [23.5] },
      { name: 'orange', price: [17, 23] },
      { name: 'kiwi', price: [99, 100] },
      { name: 'banana', price: [42] },
    ]);

    const names = Array.from(
      materializeColumn(schema, buffer.columnData.name, ['name'])
    );
    assert.deepEqual(
      names,
      records.map(r => r.name)
    );

    const prices = Array.from(
      materializeColumn(schema, buffer.columnData.price, ['price'])
    );
    assert.deepEqual(
      prices,
      records.map(r => r.price)
    );
  });

  it('should materialize a nested record with nested repeated fields', function () {
    const schema = new parquet.ParquetSchema({
      name: { type: 'UTF8' },
      stock: {
        repeated: true,
        fields: {
          quantity: { type: 'INT64', repeated: true },
          warehouse: { type: 'UTF8' },
        },
      },
      price: { type: 'DOUBLE' },
    });

    const buffer: ParquetBuffer = {
      rowCount: 4,
      columnData: {
        name: {
          dLevels: [0, 0, 0, 0],
          rLevels: [0, 0, 0, 0],
          values: [
            Buffer.from([97, 112, 112, 108, 101]),
            Buffer.from([111, 114, 97, 110, 103, 101]),
            Buffer.from([107, 105, 119, 105]),
            Buffer.from([98, 97, 110, 97, 110, 97]),
          ],
          count: 4,
        },
        [['stock', 'quantity'].join()]: {
          dLevels: [2, 2, 2, 2, 0, 1],
          rLevels: [0, 1, 0, 2, 0, 0],
          values: [10, 20, 50, 75],
          count: 6,
        },
        [['stock', 'warehouse'].join()]: {
          dLevels: [1, 1, 1, 0, 1],
          rLevels: [0, 1, 0, 0, 0],
          values: [
            Buffer.from([65]),
            Buffer.from([66]),
            Buffer.from([88]),
            Buffer.from([67]),
          ],
          count: 5,
        },
        price: {
          dLevels: [0, 0, 0, 0],
          rLevels: [0, 0, 0, 0],
          values: [23.5, 17, 99, 42],
          count: 4,
        },
      },
    };

    const records = parquet.ParquetShredder.materializeRecords(schema, buffer);

    assert.deepEqual(records, [
      {
        name: 'apple',
        stock: [
          { quantity: [10], warehouse: 'A' },
          { quantity: [20], warehouse: 'B' },
        ],
        price: 23.5,
      },
      {
        name: 'orange',
        stock: [{ quantity: [50, 75], warehouse: 'X' }],
        price: 17.0,
      },
      { name: 'kiwi', price: 99.0 },
      {
        name: 'banana',
        stock: [{ warehouse: 'C' }],
        price: 42.0,
      },
    ]);

    const names = Array.from(
      materializeColumn(schema, buffer.columnData.name, ['name'])
    );
    assert.deepEqual(
      names,
      records.map(r => r.name)
    );

    const quantities = Array.from(
      materializeColumn(schema, buffer.columnData['stock,quantity'], [
        'stock',
        'quantity',
      ])
    );
    assert.deepEqual(
      quantities,
      records.map(r =>
        r.stock?.some((s: any) => !!s.quantity)
          ? r.stock.map((s: any) => s.quantity ?? [])
          : []
      )
    );

    const warehouses = Array.from(
      materializeColumn(schema, buffer.columnData['stock,warehouse'], [
        'stock',
        'warehouse',
      ])
    );
    assert.deepEqual(
      warehouses,
      records.map(r => (r.stock ? r.stock.map((s: any) => s.warehouse) : []))
    );
  });

  it('should materialize a static nested record with blank optional value', function () {
    const schema = new parquet.ParquetSchema({
      fruit: {
        fields: {
          name: { type: 'UTF8' },
          colour: { type: 'UTF8', optional: true },
        },
      },
    });

    const buffer: ParquetBuffer = {
      rowCount: 1,
      columnData: {
        fruit: {
          dLevels: [],
          rLevels: [],
          values: [],
          count: 0,
        },
        ['fruit,name']: {
          dLevels: [0],
          rLevels: [0],
          values: [Buffer.from([97, 112, 112, 108, 101])],
          count: 1,
        },
        ['fruit,colour']: {
          dLevels: [0],
          rLevels: [0],
          values: [],
          count: 1,
        },
      },
    };

    const records = parquet.ParquetShredder.materializeRecords(schema, buffer);
    assert.deepEqual(records, [{ fruit: { name: 'apple' } }]);

    const names = Array.from(
      materializeColumn(schema, buffer.columnData['fruit,name'], [
        'fruit',
        'name',
      ])
    );
    assert.deepEqual(
      names,
      records.map(r => r.fruit.name)
    );
  });
});
