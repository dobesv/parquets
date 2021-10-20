import {
  ParquetBuffer,
  ParquetColumnData,
  ParquetField,
  ParquetRecord,
  ParquetValueArray,
} from './declare';
import { ParquetSchema } from './schema';
import * as Types from './types';
import { CustomError } from 'ts-custom-error';

export class ParquetShredError extends CustomError {
  constructor(message: string) {
    super(message);
  }
}

export class MissingRequiredFieldShredError extends CustomError {
  constructor(public fieldName: string) {
    super(`Missing required field: ${fieldName}`);
  }
}

export class TooManyValuesShredError extends CustomError {
  constructor(public fieldName: string) {
    super(`Multiple values for non-repeated field: ${fieldName}`);
  }
}

export interface ParquetWriteColumnData {
  dLevels: number[];
  rLevels: number[];
  values: ParquetValueArray;
  count: number;
}

export class ParquetWriteBuffer {
  rowCount: number;
  columnData: Record<string, ParquetWriteColumnData>;
  constructor(schema: ParquetSchema) {
    this.columnData = shredColumnBuffers(schema);
    this.rowCount = 0;
  }
}

const shredColumnBuffers = (
  schema: ParquetSchema
): Record<string, ParquetWriteColumnData> =>
  Object.fromEntries(
    schema.fieldList
      .filter(field => !field.isNested)
      .map(field => [
        field.key,
        {
          dLevels: [],
          rLevels: [],
          values: [],
          count: 0,
        },
      ])
  );

/**
 * 'Shred' a record into a list of <value, repetition_level, definition_level>
 * tuples per column using the Google Dremel Algorithm..
 *
 * The buffer argument must point to an object into which the shredded record
 * will be returned. You may re-use the buffer for repeated calls to this function
 * to append to an existing buffer, as long as the schema is unchanged.
 *
 * The format in which the shredded records will be stored in the buffer is as
 * follows:
 *
 *   buffer = {
 *     columnData: [
 *       'my_col': {
 *          dLevels: [d1, d2, .. dN],
 *          rLevels: [r1, r2, .. rN],
 *          values: [v1, v2, .. vN],
 *        }, ...
 *      ],
 *      rowCount: X,
 *   }
 */
export function shredRecord(
  schema: ParquetSchema,
  record: any,
  buffer: ParquetWriteBuffer
): void {
  // Shred the record fields; this may process fields recursively if the record
  // has nested records or arrays in it
  shredRecordFields(schema.fields, record, buffer.columnData, 0, 0);
  // Increment the row count
  buffer.rowCount += 1;
}

/**
 * Shred a record or nested object into the output buffer.  This updates the data parameter in place.
 *
 * Note that because fields can be optional or repeated, the number of elements pushed
 * onto the arrays in data can vary.
 *
 * @param fields Schema information
 * @param record Record to shred
 * @param data Output buffer
 * @param rLevel Current repetition level (used if this is a nested record inside one or more repeated fields)
 * @param dLevel Current definition level (used if this is a ensted record inside one or more optional fields)
 */
function shredRecordFields(
  fields: Record<string, ParquetField>,
  record: any,
  data: Record<string, ParquetWriteColumnData>,
  rLevel: number,
  dLevel: number
) {
  for (const name in fields) {
    const field = fields[name];

    // fetch values
    let values: ParquetValueArray;
    if (
      record &&
      field.name in record &&
      record[field.name] !== undefined &&
      record[field.name] !== null
    ) {
      const value = record[field.name];
      if (value.constructor === Array) {
        values = value;
      } else {
        values = [value];
      }
    } else {
      // Value missing / null
      values = [];
    }

    // check values
    if (
      values.length === 0 &&
      !!record &&
      field.repetitionType === 'REQUIRED'
    ) {
      throw new MissingRequiredFieldShredError(field.name);
    }
    if (values.length > 1 && field.repetitionType !== 'REPEATED') {
      throw new TooManyValuesShredError(field.name);
    }

    // Check if there's a value to emit
    if (values.length === 0) {
      if (field.isNested) {
        // If it's a nested object we'll want push null for all its elements
        shredRecordFields(field.fields, null, data, rLevel, dLevel);
      } else {
        // If it's a primitive value, mark it as missing
        const fieldData = data[field.key];
        fieldData.count += 1;
        fieldData.rLevels.push(rLevel);
        fieldData.dLevels.push(dLevel);
      }
      continue;
    }

    // push values
    for (let i = 0; i < values.length; i++) {
      const rlvl = i === 0 ? rLevel : field.rLevelMax;
      if (field.isNested) {
        shredRecordFields(field.fields, values[i], data, rlvl, field.dLevelMax);
      } else {
        const fieldData = data[field.key];
        fieldData.count += 1;
        fieldData.rLevels.push(rlvl);
        fieldData.dLevels.push(field.dLevelMax);
        (fieldData.values as any[]).push(
          Types.toPrimitive(
            field.originalType || field.primitiveType,
            values[i]
          )
        );
      }
    }
  }
}

/**
 * 'Materialize' a list of <value, repetition_level, definition_level>
 * tuples back to nested records (objects/arrays) using the Google Dremel
 * Algorithm..
 *
 * The buffer argument must point to an object with the following structure (i.e.
 * the same structure that is returned by shredRecords):
 *
 *   buffer = {
 *     columnData: [
 *       'my_col': {
 *          dlevels: [d1, d2, .. dN],
 *          rlevels: [r1, r2, .. rN],
 *          values: [v1, v2, .. vN],
 *        }, ...
 *      ],
 *      rowCount: X,
 *   }
 */
export function materializeRecords(
  schema: ParquetSchema,
  buffer: ParquetBuffer
): ParquetRecord[] {
  const records: ParquetRecord[] = [];
  for (let i = 0; i < buffer.rowCount; i++) records.push({});
  for (const key in buffer.columnData) {
    materializeColumnIntoRecords(schema, buffer, key, records);
  }
  return records;
}

/**
 * Support iteration over the values in a single column.
 *
 * For a simple column which is not repeated and not nested in a repeated
 * field, this will give one value for each row in the input.
 *
 * If the column is repeated or nested in a repeated column, it will give an
 * array for each row in the input.
 *
 * When there are multiple levels of repetition the iterator will yield
 * nested arrays.
 */
export function* materializeColumn(
  schema: ParquetSchema,
  data: ParquetColumnData,
  columnPath: string[]
) {
  const field = schema.findField(columnPath);
  if (!field) {
    throw new Error(`No field in schema for ${columnPath}`);
  }
  const { dLevelMax, rLevelMax } = field;
  const rLevelArrays: Array<null | any[]> = [];
  let vIndex = 0;
  const count = data.count;
  for (let i = 0; i < count; i++) {
    const dLevel = data.dLevels[i];
    const rLevel = data.rLevels[i];

    // Yield back the top-level array if we're moving to the next row
    if (rLevelMax > 0 && rLevel === 0 && i > 0) {
      yield rLevelArrays[0] ?? [];
    }

    // Reset arrays for all rLevels >= rLevel
    rLevelArrays.length = rLevel;

    // Check if we actually have a value here
    if (dLevel >= dLevelMax) {
      const value = Types.fromPrimitive(
        field.originalType || field.primitiveType,
        data.values[vIndex]
      );
      vIndex++;
      if (rLevelMax > 0) {
        // Insert as array element
        for (let n = 0; n < rLevelMax; n++) {
          const v = rLevelArrays[n];
          if (!v) {
            const ary: any[] = [];
            rLevelArrays[n] = ary;
            if (n > 0) {
              rLevelArrays[n - 1].push(ary);
            }
          }
        }
        // Push value onto the leaf-level array
        rLevelArrays[rLevelMax - 1].push(value);
      } else {
        // Emit value
        yield value;
      }
    } else if (rLevelMax === 0) {
      // Emit null
      yield null;
    }
  }
  // Yield back the top-level array at the end if this was a repeated field (or nested in one)
  if (rLevelMax > 0 && count > 0) {
    yield rLevelArrays[0] ?? [];
  }
}

/**
 * Read values from a column and update the records array with the values that are
 * found.
 *
 * If a column is in a nested record or array this will create the necessary parent
 * objects and arrays leading up to it, as well as creating the actual record if there's
 * no record at the given position in the records array.
 *
 * @param schema Parquet schema
 * @param buffer Data we are parsing
 * @param key Field key for the column we are loading
 * @param records records are added or updated in this array as necessary
 */
function materializeColumnIntoRecords(
  schema: ParquetSchema,
  buffer: ParquetBuffer,
  key: string,
  records: ParquetRecord[]
) {
  const data = buffer.columnData[key];
  if (!data.count) return;

  const field = schema.findField(key);
  const branch = schema.findFieldBranch(key);
  const repeated = field.repetitionType === 'REPEATED';

  // tslint:disable-next-line:prefer-array-literal
  const rLevels: number[] = new Array(field.rLevelMax + 1).fill(0);
  let vIndex = 0;
  for (let i = 0; i < data.count; i++) {
    const dLevel = data.dLevels[i];
    const rLevel = data.rLevels[i];
    rLevels[rLevel]++;
    rLevels.fill(0, rLevel + 1);

    let rIndex = 0;
    let record = records[rLevels[rIndex++] - 1];

    // Internal nodes
    for (const step of branch) {
      if (step === field) break;
      if (dLevel < step.dLevelMax) break;
      if (step.repetitionType === 'REPEATED') {
        if (!(step.name in record)) record[step.name] = [];
        const ix = rLevels[rIndex++];
        while (record[step.name].length <= ix) record[step.name].push({});
        record = record[step.name][ix];
      } else {
        record[step.name] = record[step.name] || {};
        record = record[step.name];
      }
    }

    // Leaf node
    if (dLevel === field.dLevelMax) {
      const value = Types.fromPrimitive(
        field.originalType || field.primitiveType,
        data.values[vIndex]
      );
      vIndex++;
      if (repeated) {
        if (!(field.name in record)) record[field.name] = [];
        const ix = rLevels[rIndex];
        while (record[field.name].length <= ix) record[field.name].push(null);
        record[field.name][ix] = value;
      } else {
        record[field.name] = value;
      }
    }
  }
}
