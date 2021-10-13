export type ParquetCodec = 'PLAIN' | 'RLE';
export type ParquetCompression =
  | 'UNCOMPRESSED'
  | 'GZIP'
  | 'SNAPPY'
  | 'LZO'
  | 'BROTLI'
  | 'LZ4';
export type RepetitionType = 'REQUIRED' | 'OPTIONAL' | 'REPEATED';
export type ParquetType = PrimitiveType | OriginalType;

export type PrimitiveType =
  // Base Types
  | 'BOOLEAN' // 0
  | 'INT32' // 1
  | 'INT64' // 2
  | 'INT96' // 3
  | 'FLOAT' // 4
  | 'DOUBLE' // 5
  | 'BYTE_ARRAY' // 6,
  | 'FIXED_LEN_BYTE_ARRAY'; // 7

export type OriginalType =
  // Converted Types
  | 'UTF8' // 0
  // | 'MAP' // 1
  // | 'MAP_KEY_VALUE' // 2
  // | 'LIST' // 3
  // | 'ENUM' // 4
  // | 'DECIMAL' // 5
  | 'DATE' // 6
  | 'TIME_MILLIS' // 7
  | 'TIME_MICROS' // 8
  | 'TIMESTAMP_MILLIS' // 9
  | 'TIMESTAMP_MICROS' // 10
  | 'UINT_8' // 11
  | 'UINT_16' // 12
  | 'UINT_32' // 13
  | 'UINT_64' // 14
  | 'INT_8' // 15
  | 'INT_16' // 16
  | 'INT_32' // 17
  | 'INT_64' // 18
  | 'JSON' // 19
  | 'BSON' // 20
  | 'INTERVAL'; // 21

export interface SchemaDefinition {
  [string: string]: FieldDefinition;
}

export interface FieldDefinition {
  type?: ParquetType;
  typeLength?: number;
  encoding?: ParquetCodec;
  compression?: ParquetCompression;

  /**
   * Optional fields can be null instead of having a value of the given schema type.
   *
   * When an optional type is not provided, it is not included into the data.
   *
   * Instead there is an array of "dlevels" indicated whether optional values are present at
   * each row offset into a column chunk.
   *
   * Note that fields should not be marked both optional and repeated - the underlying parquet
   * schema does not have a way to represent fields that are both optional and repeated.
   */
  optional?: boolean;

  /**
   * Repeated fields can occur more than once.  They represent arrays or lists of values.
   *
   * The "rlevels" data is used to indicate whether values in the data are part of a new
   * array or part of the same array as the prior value.
   *
   * Note that fields should not be marked both optional and repeated - the underlying parquet
   * schema does not have a way to represent fields that are both optional and repeated.
   */
  repeated?: boolean;
  fields?: SchemaDefinition;
}

export interface ParquetField {
  name: string;
  path: string[];
  key: string;
  primitiveType?: PrimitiveType;
  originalType?: OriginalType;
  repetitionType: RepetitionType;
  typeLength?: number;
  encoding?: ParquetCodec;
  compression?: ParquetCompression;

  /**
   * The maximum repetition level is a count of repeated fields in this field's
   * path (e.g. this field and its ancestors).
   *
   * When scanning values in the data, the rLevelMax is used to determine whether a REPEATEDvalue
   * should be added to an existing array or if a new array (or arrays) should be created to
   * add the value to.
   *
   * If the value is not repeated (and neither are any of its ancestor fields) then rLevelMax
   * should be zero.
   *
   * If the rLevelMax is one and repetitionType === 'REPEATED', this field is itself repeated
   * in its parent object (which is the root for a top-level field).
   */
  rLevelMax: number;

  /**
   * The maximum definition level is a count of optional fields in this field's
   * path (e.g. this field and its ancestors).
   *
   * dLevelMax is used when decoding to determine whether to expect a value to be
   * present in the output for a given column and row.
   *
   * If the dLeveLMax is zero, the field is not optional.
   *
   * If the dLevelMax is one, and the repetitionType === 'OPTIONAL', this field is itself
   * optional in its parent object.
   *
   * If this field is not optional but its parent is an optional value then it will have
   * a non-zero dLevelMax.
   */
  dLevelMax: number;
  isNested?: boolean;
  fieldCount?: number;
  fields?: Record<string, ParquetField>;
}

export interface ParquetBuffer {
  rowCount?: number;
  columnData?: Record<string, ParquetColumnData>;
}

export type ParquetValueArray =
  | boolean[]
  | number[]
  | string[]
  | Buffer[]
  | (string | Buffer)[]
  | Int32Array
  | Float32Array
  | Float64Array;

export interface ParquetColumnData {
  /**
   * Definition levels specify how many optional fields in the path for the column are defined.
   *
   * If the definition level for a row is less than the number of optional fields in field path
   * (dLevelMax), there will not be a value or repetition level encoded for that field.
   *
   * The field path refers to the field and its parent fields if it is nested.
   */
  dLevels: Int32Array | number[];

  /**
   * For fields which have REPEATED fields in their field path, repetition levels may be specified.
   *
   * The repetition level indicates the number of repeated fields in the field path that are being
   * "repeated" (rather than started anew in their parent record / array / row).
   *
   * The field path refers to the field and its parent fields if it is nested.
   *
   * A repetition level of zero indicates the first value for all the repeated fields in the path;
   * they would all get a fresh new array.
   *
   * When the repetition level is greater than zero and less than the number of repeated fields
   * (rLevelMax) in the field path we can keep that number of parent arrays but create new arrays
   * after that.
   *
   * When the repetition level is equal to the number of repeated fields (rLevelMax) we are simply
   * adding to the end of the most recently created array.
   *
   * If the field path has only one repeated field, the repetition level will be 0 for the first
   * element of a given row and 1 for the ones that follow.
   *
   * If the field path has two repeated fields, the repetition level will be 0 for the first element
   * in a row, 1 for the first element of the second set of values for the first repeated field in
   * the field path, and 2 for subsequent elements in a single group/array.
   */
  rLevels: Int32Array | number[];

  /**
   * Values read from the column.  May be returned as a primitive
   * array format if it is able to.
   *
   * - UInt8Array: BOOLEAN
   * - Int32Array: INT32
   * - Float32Array: FLOAT
   * - Float64Array: DOUBLE
   */
  values: ParquetValueArray;

  /**
   * Value count
   */
  count: number;
}

export type ParquetRecord = Record<string, any>;
