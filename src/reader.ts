import { CursorBuffer, ParquetCodecOptions, PARQUET_CODEC } from './codec';
import * as Compression from './compression';
import {
  ParquetCodec,
  ParquetCompression,
  ParquetField,
  ParquetRecord,
  ParquetType,
  ParquetValueArray,
  PrimitiveType,
  SchemaDefinition,
} from './declare';
import { ParquetSchema } from './schema';
import * as Shred from './shred';
// tslint:disable-next-line:max-line-length
import {
  ColumnChunk,
  CompressionCodec,
  ConvertedType,
  Encoding,
  FieldRepetitionType,
  FileMetaData,
  PageHeader,
  PageType,
  RowGroup,
  SchemaElement,
  Type,
} from './thrift';
import * as Util from './util';
import concatValueArrays from './concatValueArrays';
import { findColumnChunk } from './util';
import { materializeColumn } from './shred';

/**
 * Parquet File Magic String
 */
const PARQUET_MAGIC = 'PAR1';

/**
 * Parquet File Format Version
 */
const PARQUET_VERSION = 1;

/**
 * Internal type used for repetition/definition levels
 */
const PARQUET_RDLVL_TYPE = 'INT32';
const PARQUET_RDLVL_ENCODING = 'RLE';

/**
 * Variation of ParquetData which always has Int32Array for dLevels and rLevels.
 */
export interface ParquetReadData {
  dLevels: Int32Array;
  rLevels: Int32Array;
  values: ParquetValueArray;
  count: number;
}

/**
 * Variation of ParquetBuffer which always has Int32Array for dLevels and rLevels.
 */
export interface ParquetReadBuffer {
  rowCount: number;
  columnData: Record<string, ParquetReadData>;
}

/**
 * A parquet cursor is used to retrieve rows from a parquet file in order
 */
export class ParquetCursor<T> implements AsyncIterable<T> {
  public metadata: FileMetaData;
  public envelopeReader: ParquetEnvelopeReader;
  public schema: ParquetSchema;
  public columnList: string[][];
  public rowGroup: ParquetRecord[];
  public rowGroupIndex: number;
  public cursorIndex: number;

  /**
   * Create a new parquet reader from the file metadata and an envelope reader.
   * It is usually not recommended to call this constructor directly except for
   * advanced and internal use cases. Consider using getCursor() on the
   * ParquetReader instead
   */
  constructor(
    metadata: FileMetaData,
    envelopeReader: ParquetEnvelopeReader,
    schema: ParquetSchema,
    columnList: string[][]
  ) {
    this.metadata = metadata;
    this.envelopeReader = envelopeReader;
    this.schema = schema;
    this.columnList = columnList;
    this.rowGroup = [];
    this.rowGroupIndex = 0;
    this.cursorIndex = 0;
  }

  /**
   * Retrieve the next row from the cursor. Returns a row or NULL if the end
   * of the file was reached
   */
  async next<T = any>(): Promise<T> {
    if (this.cursorIndex >= this.rowGroup.length) {
      if (this.rowGroupIndex >= this.metadata.row_groups.length) {
        return null;
      }
      const rowBuffer = await this.envelopeReader.readRowGroup(
        this.schema,
        this.metadata.row_groups[this.rowGroupIndex],
        this.columnList
      );
      this.rowGroup = Shred.materializeRecords(this.schema, rowBuffer);
      this.rowGroupIndex++;
      this.cursorIndex = 0;
    }
    return this.rowGroup[this.cursorIndex++] as any;
  }

  /**
   * Rewind the cursor the the beginning of the file
   */
  rewind(): void {
    this.rowGroup = [];
    this.rowGroupIndex = 0;
  }

  /**
   * Implement AsyncIterable
   */
  // tslint:disable-next-line:function-name
  [Symbol.asyncIterator](): AsyncIterator<T> {
    let done = false;
    return {
      next: async () => {
        if (done) {
          return { done, value: null };
        }
        const value = await this.next();
        if (value === null) {
          return { done: true, value };
        }
        return { done: false, value };
      },
      return: async () => {
        done = true;
        return { done, value: null };
      },
      throw: async () => {
        done = true;
        return { done: true, value: null };
      },
    };
  }
}

/**
 * A parquet reader allows retrieving the rows from a parquet file in order.
 * The basic usage is to create a reader and then retrieve a cursor/iterator
 * which allows you to consume row after row until all rows have been read. It is
 * important that you call close() after you are finished reading the file to
 * avoid leaking file descriptors.
 */
export class ParquetReader<T> implements AsyncIterable<T> {
  /**
   * Open the parquet file pointed to by the specified path and return a new
   * parquet reader
   */
  static async openFile<T>(filePath: string): Promise<ParquetReader<T>> {
    const envelopeReader = await ParquetEnvelopeReader.openFile(filePath);
    try {
      await envelopeReader.readHeader();
      const metadata = await envelopeReader.readFooter();
      return new ParquetReader<T>(metadata, envelopeReader);
    } catch (err) {
      await envelopeReader.close();
      throw err;
    }
  }

  static async openBuffer<T>(buffer: Buffer): Promise<ParquetReader<T>> {
    const envelopeReader = await ParquetEnvelopeReader.openBuffer(buffer);
    try {
      await envelopeReader.readHeader();
      const metadata = await envelopeReader.readFooter();
      return new ParquetReader<T>(metadata, envelopeReader);
    } catch (err) {
      await envelopeReader.close();
      throw err;
    }
  }

  public metadata: FileMetaData;
  public envelopeReader: ParquetEnvelopeReader;
  public schema: ParquetSchema;

  /**
   * Create a new parquet reader from the file metadata and an envelope reader.
   * It is not recommended to call this constructor directly except for advanced
   * and internal use cases. Consider using one of the open{File,Buffer} methods
   * instead
   */
  constructor(metadata: FileMetaData, envelopeReader: ParquetEnvelopeReader) {
    if (metadata.version !== PARQUET_VERSION) {
      throw new Error('invalid parquet version');
    }

    this.metadata = metadata;
    this.envelopeReader = envelopeReader;
    const root = this.metadata.schema[0];
    const { schema } = decodeSchema(this.metadata.schema, 1, root.num_children);
    this.schema = new ParquetSchema(schema);
  }

  /**
   * Return a cursor to the file. You may open more than one cursor and use
   * them concurrently. All cursors become invalid once close() is called on
   * the reader object.
   *
   * The required_columns parameter controls which columns are actually read
   * from disk. An empty array or no value implies all columns. A list of column
   * names means that only those columns should be loaded from disk.
   */
  getCursor(): ParquetCursor<T>;
  getCursor<K extends keyof T>(
    columnList: (K | K[])[]
  ): ParquetCursor<Pick<T, K>>;
  getCursor(columnList: (string | string[])[]): ParquetCursor<Partial<T>>;
  getCursor(columnList?: (string | string[])[]): ParquetCursor<Partial<T>> {
    if (!columnList) {
      // tslint:disable-next-line:no-parameter-reassignment
      columnList = [];
    }

    // tslint:disable-next-line:no-parameter-reassignment
    columnList = columnList.map(x => (Array.isArray(x) ? x : [x]));

    return new ParquetCursor<T>(
      this.metadata,
      this.envelopeReader,
      this.schema,
      columnList as string[][]
    );
  }


  /**
   * Get an iterable over a single column.  The column is specified as an array of
   * strings in order to support nested records.
   *
   * The path should not reference a nested record column.
   *
   * When a column is repeated the iterable will an array for each row.
   *
   * When a column is optional the iterable will produce null for any row missing
   * the value.
   *
   * If a column is repeated and also nested inside another repeated object, then an array of arrays
   * is returned for each row in the dataset.
   *
   * If a column is optional and also nested inside a repeated nested object, then it will be in an array
   * where the array elements may be null.
   *
   * This means you can iterate multiple of these in parallel to walk multiple
   * columns at once and they will stay in sync as long as the calls to next()
   * are made in sync.
   *
   * @param columnPath
   */
  async *getColumnValues(columnPath: string[]): AsyncIterable<any> {
    for(const rowGroup of this.metadata.row_groups) {
      const colChunk = findColumnChunk(rowGroup, columnPath);
      const data = await this.envelopeReader.readColumnChunk(this.schema, colChunk);
      yield * materializeColumn(
          this.schema,
          data,
          columnPath
      );
    }
  }

  /**
   * Return the number of rows in this file. Note that the number of rows is
   * not neccessarily equal to the number of rows in each column.
   */
  getRowCount(): number {
    return +this.metadata.num_rows;
  }

  /**
   * Returns the ParquetSchema for this file
   */
  getSchema(): ParquetSchema {
    return this.schema;
  }

  /**
   * Returns the user (key/value) metadata for this file
   */
  getMetadata(): Record<string, string> {
    const md: Record<string, string> = {};
    for (const kv of this.metadata.key_value_metadata) {
      md[kv.key] = kv.value;
    }
    return md;
  }

  /**
   * Close this parquet reader. You MUST call this method once you're finished
   * reading rows
   */
  async close(): Promise<void> {
    await this.envelopeReader.close();
    this.envelopeReader = null;
    this.metadata = null;
  }

  /**
   * Implement AsyncIterable
   */
  // tslint:disable-next-line:function-name
  [Symbol.asyncIterator](): AsyncIterator<T> {
    return this.getCursor()[Symbol.asyncIterator]();
  }
}

/**
 * The parquet envelope reader allows direct, unbuffered access to the individual
 * sections of the parquet file, namely the header, footer and the row groups.
 * This class is intended for advanced/internal users; if you just want to retrieve
 * rows from a parquet file use the ParquetReader instead
 */
export class ParquetEnvelopeReader {
  static async openFile(filePath: string): Promise<ParquetEnvelopeReader> {
    const fileStat = await Util.fstat(filePath);
    const fileDescriptor = await Util.fopen(filePath);

    const readFn = Util.fread.bind(undefined, fileDescriptor);
    const closeFn = Util.fclose.bind(undefined, fileDescriptor);

    return new ParquetEnvelopeReader(readFn, closeFn, fileStat.size);
  }

  /**
   * Read parquet data from an in-memory buffer.  This provides an asynchronous
   * interface compatible with reading from a file.
   *
   * Note that you can also use ParquetEnvelopeBufferReader if you don't need your code to be able
   * to handle files and buffers both.  It may offer some performance benefit because it does not yield
   * to the event loop in between operations.
   */
  static async openBuffer(buffer: Buffer): Promise<ParquetEnvelopeReader> {
    const readFn = (position: number, length: number) =>
      Promise.resolve(buffer.slice(position, position + length));
    const closeFn = () => Promise.resolve();
    return new ParquetEnvelopeReader(readFn, closeFn, buffer.length);
  }

  constructor(
    public read: (position: number, length: number) => Promise<Buffer>,
    public close: () => Promise<void>,
    public fileSize: number
  ) {}

  async readHeader(): Promise<void> {
    const buf = await this.read(0, PARQUET_MAGIC.length);

    if (buf.toString() !== PARQUET_MAGIC) {
      throw new Error('not valid parquet file');
    }
  }

  async readRowGroup(
    schema: ParquetSchema,
    rowGroup: RowGroup,
    columnList: string[][]
  ): Promise<ParquetReadBuffer> {
    const buffer: ParquetReadBuffer = {
      rowCount: +rowGroup.num_rows,
      columnData: {},
    };
    for (const colChunk of rowGroup.columns) {
      const colMetadata = colChunk.meta_data;
      const colKey = colMetadata.path_in_schema;
      if (columnList.length > 0 && Util.fieldIndexOf(columnList, colKey) < 0) {
        continue;
      }
      buffer.columnData[colKey.join()] = await this.readColumnChunk(
        schema,
        colChunk
      );
    }
    return buffer;
  }

  async readColumnChunk(
    schema: ParquetSchema,
    colChunk: ColumnChunk
  ): Promise<ParquetReadData> {
    if (colChunk.file_path !== undefined && colChunk.file_path !== null) {
      throw new Error('external references are not supported');
    }
    const pagesOffset = +colChunk.meta_data.data_page_offset;
    const pagesSize = +colChunk.meta_data.total_compressed_size;
    const pagesBuf = await this.read(pagesOffset, pagesSize);
    return decodeColumnChunk(schema, colChunk, pagesBuf);
  }

  async readFooter(): Promise<FileMetaData> {
    const trailerLen = PARQUET_MAGIC.length + 4;
    const trailerBuf = await this.read(this.fileSize - trailerLen, trailerLen);

    if (trailerBuf.slice(4).toString() !== PARQUET_MAGIC) {
      throw new Error('not a valid parquet file');
    }

    const metadataSize = trailerBuf.readUInt32LE(0);
    const metadataOffset = this.fileSize - metadataSize - trailerLen;
    if (metadataOffset < PARQUET_MAGIC.length) {
      throw new Error('invalid metadata size');
    }

    const metadataBuf = await this.read(metadataOffset, metadataSize);
    // let metadata = new parquet_thrift.FileMetaData();
    // parquet_util.decodeThrift(metadata, metadataBuf);
    const { metadata } = Util.decodeFileMetadata(metadataBuf);
    return metadata;
  }
}

/**
 * A parquet cursor is used to retrieve rows from a parquet file in order
 */
export class ParquetBufferCursor<T> implements Iterable<T> {
  public metadata: FileMetaData;
  public envelopeReader: ParquetEnvelopeBufferReader;
  public schema: ParquetSchema;
  public columnList: string[][];
  public rows: ParquetRecord[];
  public rowsIndex: number;
  public cursorIndex: number;

  /**
   * Create a new parquet reader from the file metadata and an envelope reader.
   * It is usually not recommended to call this constructor directly except for
   * advanced and internal use cases. Consider using getCursor() on the
   * ParquetReader instead
   */
  constructor(
    metadata: FileMetaData,
    envelopeReader: ParquetEnvelopeBufferReader,
    schema: ParquetSchema,
    columnList: string[][]
  ) {
    this.metadata = metadata;
    this.envelopeReader = envelopeReader;
    this.schema = schema;
    this.columnList = columnList;
    this.rows = [];
    this.rowsIndex = 0;
    this.cursorIndex = 0;
  }

  /**
   * Retrieve the next row from the cursor. Returns a row or NULL if the end
   * of the file was reached
   */
  next<T = any>(): T {
    if (this.cursorIndex >= this.rows.length) {
      if (this.rowsIndex >= this.metadata.row_groups.length) {
        return null;
      }
      const rowBuffer = this.envelopeReader.readRowGroup(
        this.schema,
        this.metadata.row_groups[this.rowsIndex],
        this.columnList
      );
      this.rows = Shred.materializeRecords(this.schema, rowBuffer);
      this.rowsIndex++;
      this.cursorIndex = 0;
    }
    return this.rows[this.cursorIndex++] as any;
  }

  /**
   * Rewind the cursor the the beginning of the file
   */
  rewind(): void {
    this.rows = [];
    this.rowsIndex = 0;
  }

  /**
   * Implement Iterable
   */
  // tslint:disable-next-line:function-name
  [Symbol.iterator](): Iterator<T> {
    let done = false;
    return {
      next: () => {
        if (done) {
          return { done, value: null };
        }
        const value = this.next();
        if (value === null) {
          return { done: true, value };
        }
        return { done: false, value };
      },
      return: () => {
        done = true;
        return { done, value: null };
      },
      throw: () => {
        done = true;
        return { done: true, value: null };
      },
    };
  }
}


/**
 * A parquet reader allows retrieving the rows from a parquet file in order.
 * The basic usage is to create a reader and then retrieve a cursor/iterator
 * which allows you to consume row after row until all rows have been read. It is
 * important that you call close() after you are finished reading the file to
 * avoid leaking file descriptors.
 */
export class ParquetBufferReader<T> implements Iterable<T> {
  static openBuffer<T>(buffer: Buffer): ParquetBufferReader<T> {
    return new ParquetBufferReader(buffer);
  }

  public metadata: FileMetaData;
  public envelopeReader: ParquetEnvelopeBufferReader;
  public schema: ParquetSchema;

  /**
   * Create a new parquet reader from a buffer.  This version of ParquetReader
   * runs synchronously so it may be more efficient when reading from a Buffer.
   *
   * However, it doesn't have a compatible API with ParquetReader.
   */
  constructor(public buffer: Buffer) {
    this.envelopeReader = new ParquetEnvelopeBufferReader(buffer);
    this.metadata = this.envelopeReader.readFooter();
    if (this.metadata.version !== PARQUET_VERSION) {
      throw new Error('invalid parquet version');
    }
    const root = this.metadata.schema[0];
    const { schema } = decodeSchema(this.metadata.schema, 1, root.num_children);
    this.schema = new ParquetSchema(schema);
  }

  /**
   * Return a cursor to the buffer. You may open more than one cursor and use
   * them concurrently.
   *
   * The required_columns parameter controls which columns are actually read
   * from disk. An empty array or no value implies all columns. A list of column
   * names means that only those columns should be loaded from disk.
   *
   * When the schema has nested records, you will need to specify each column as an array
   * of strings specifying the "path" to the actual leaf column to fetch.
   */
  getCursor(): ParquetBufferCursor<T>;
  getCursor<K extends keyof T>(
    columnList: (K | K[])[]
  ): ParquetBufferCursor<Pick<T, K>>;
  getCursor(columnList: (string | string[])[]): ParquetBufferCursor<Partial<T>>;
  getCursor(
    columnList?: (string | string[])[]
  ): ParquetBufferCursor<Partial<T>> {
    const normalizedColumnList = (columnList || []).map(x =>
      Array.isArray(x) ? x : [x]
    );
    return new ParquetBufferCursor<T>(
      this.metadata,
      this.envelopeReader,
      this.schema,
      normalizedColumnList
    );
  }

  /**
   * Get an iterable over a single column.  The column is specified as an array of
   * strings in order to support nested records.
   *
   * The path should not reference a nested record column.
   *
   * When a column is repeated the iterable will an array for each row.
   *
   * When a column is optional the iterable will produce null for any row missing
   * the value.
   *
   * If a column is repeated and also nested inside another repeated object, then an array of arrays
   * is returned for each row in the dataset.
   *
   * If a column is optional and also nested inside a repeated nested object, then it will be in an array
   * where the array elements may be null.
   *
   * This means you can iterate multiple of these in parallel to walk multiple
   * columns at once and they will stay in sync as long as the calls to next()
   * are made in sync.
   *
   * @param columnPath
   */
  *getColumnValues(columnPath: string[]): Iterable<any> {
    for(const rowGroup of this.metadata.row_groups) {
      const colChunk = findColumnChunk(rowGroup, columnPath);
      const data = this.envelopeReader.readColumnChunk(this.schema, colChunk);
      yield * materializeColumn(
          this.schema,
          data,
          columnPath
      );
    }
  }

  /**
   * Return the number of rows in this file. Note that the number of rows is
   * not necessarily equal to the number of rows in each column.
   */
  getRowCount(): number {
    return +this.metadata.num_rows;
  }

  /**
   * Returns the ParquetSchema for this file
   */
  getSchema(): ParquetSchema {
    return this.schema;
  }

  /**
   * Returns the user (key/value) metadata for this file
   */
  getMetadata(): Record<string, string> {
    const md: Record<string, string> = {};
    for (const kv of this.metadata.key_value_metadata) {
      md[kv.key] = kv.value;
    }
    return md;
  }

  /**
   * Implement Iterable
   */
  // tslint:disable-next-line:function-name
  [Symbol.iterator](): Iterator<T> {
    return this.getCursor()[Symbol.iterator]();
  }
}

export class ParquetEnvelopeBufferReader {
  constructor(public buffer: Buffer) {}

  read(offset: number, length: number): Buffer {
    return this.buffer.slice(offset, offset + length);
  }

  readHeader(): void {
    const buf = this.read(0, PARQUET_MAGIC.length);

    if (buf.toString() !== PARQUET_MAGIC) {
      throw new Error('not valid parquet file');
    }
  }

  readRowGroup(
    schema: ParquetSchema,
    rowGroup: RowGroup,
    columnList: string[][]
  ): ParquetReadBuffer {
    const buffer: ParquetReadBuffer = {
      rowCount: +rowGroup.num_rows,
      columnData: {},
    };
    for (const colChunk of rowGroup.columns) {
      const colMetadata = colChunk.meta_data;
      const colKey = colMetadata.path_in_schema;
      if (columnList.length > 0 && Util.fieldIndexOf(columnList, colKey) < 0) {
        continue;
      }
      buffer.columnData[colKey.join()] = this.readColumnChunk(schema, colChunk);
    }
    return buffer;
  }

  readColumnChunk(
    schema: ParquetSchema,
    colChunk: ColumnChunk
  ): ParquetReadData {
    if (colChunk.file_path !== undefined && colChunk.file_path !== null) {
      throw new Error('external references are not supported');
    }
    const pagesOffset = +colChunk.meta_data.data_page_offset;
    const pagesSize = +colChunk.meta_data.total_compressed_size;
    const pagesBuf = this.read(pagesOffset, pagesSize);
    return decodeColumnChunk(schema, colChunk, pagesBuf);
  }

  readFooter(): FileMetaData {
    const trailerLen = PARQUET_MAGIC.length + 4;
    const trailerBuf = this.read(this.buffer.length - trailerLen, trailerLen);

    if (trailerBuf.slice(4).toString() !== PARQUET_MAGIC) {
      throw new Error('not a valid parquet file');
    }

    const metadataSize = trailerBuf.readUInt32LE(0);
    const metadataOffset = this.buffer.length - metadataSize - trailerLen;
    if (metadataOffset < PARQUET_MAGIC.length) {
      throw new Error('invalid metadata size');
    }

    const metadataBuf = this.read(metadataOffset, metadataSize);
    // let metadata = new parquet_thrift.FileMetaData();
    // parquet_util.decodeThrift(metadata, metadataBuf);
    const { metadata } = Util.decodeFileMetadata(metadataBuf);
    return metadata;
  }
}

/**
 * Decode column chunk
 *
 * This calculates the field type and compression setting using
 * the schema and column chunk metadata and calls decodeDataPages
 * to do the heavy lifting.
 */
function decodeColumnChunk(
  schema: ParquetSchema,
  colChunk: ColumnChunk,
  pagesBuf: Buffer
): ParquetReadData {
  const field = schema.findField(colChunk.meta_data.path_in_schema);
  const type: PrimitiveType = Util.getThriftEnum(
    Type,
    colChunk.meta_data.type
  ) as any;
  if (type !== field.primitiveType) {
    throw new Error('chunk type not matching schema: ' + type);
  }

  const compression: ParquetCompression = Util.getThriftEnum(
    CompressionCodec,
    colChunk.meta_data.codec
  ) as any;

  return decodeDataPages(pagesBuf, field, compression);
}

/**
 * Decode a consecutive array of data using one of the parquet encodings
 */
function decodeValues(
  type: PrimitiveType,
  encoding: ParquetCodec,
  cursor: CursorBuffer,
  count: number,
  opts: ParquetCodecOptions
): ParquetValueArray {
  if (!(encoding in PARQUET_CODEC)) {
    throw new Error(`invalid encoding: ${encoding}`);
  }
  return PARQUET_CODEC[encoding].decodeValues(type, cursor, count, opts);
}

function decodeDataPages(
  buffer: Buffer,
  column: ParquetField,
  compression: ParquetCompression
): ParquetReadData {
  const cursor: CursorBuffer = {
    buffer,
    offset: 0,
    size: buffer.length,
  };

  const rLevelPages: Int32Array[] = [];
  const dLevelPages: Int32Array[] = [];
  const valuePages: ParquetValueArray[] = [];
  let count = 0;

  while (cursor.offset < cursor.size) {
    // const pageHeader = new parquet_thrift.PageHeader();
    // cursor.offset += parquet_util.decodeThrift(pageHeader, cursor.buffer);

    const { pageHeader, length } = Util.decodePageHeader(cursor.buffer);
    cursor.offset += length;

    const pageType = Util.getThriftEnum(PageType, pageHeader.type);

    if (pageType !== 'DATA_PAGE_V2' && pageType !== 'DATA_PAGE') {
      throw new Error(`Unsupported data page type ${pageType}`);
    }

    const pageData: ParquetReadData =
      pageType === 'DATA_PAGE_V2'
        ? decodeDataPageV2(cursor, pageHeader, column, compression)
        : decodeDataPage(cursor, pageHeader, column, compression);

    rLevelPages.push(pageData.rLevels);
    dLevelPages.push(pageData.dLevels);
    valuePages.push(pageData.values);
    count += pageData.count;
  }

  return {
    rLevels: concatValueArrays(rLevelPages),
    dLevels: concatValueArrays(dLevelPages),
    values: concatValueArrays(valuePages),
    count,
  };
}

function decodeDataPage(
  cursor: CursorBuffer,
  header: PageHeader,
  column: ParquetField,
  compression: ParquetCompression
): ParquetReadData {
  const cursorEnd = cursor.offset + header.compressed_page_size;
  const valueCount = header.data_page_header.num_values;

  // uncompress page
  let dataCursor = cursor;
  if (compression !== 'UNCOMPRESSED') {
    const valuesBuf = Compression.inflate(
      compression,
      cursor.buffer.slice(cursor.offset, cursorEnd),
      header.uncompressed_page_size
    );
    dataCursor = {
      buffer: valuesBuf,
      offset: 0,
      size: valuesBuf.length,
    };
    cursor.offset = cursorEnd;
  }

  // read repetition levels
  const rLevelEncoding = Util.getThriftEnum(
    Encoding,
    header.data_page_header.repetition_level_encoding
  ) as ParquetCodec;
  // tslint:disable-next-line:prefer-array-literal
  const rLevels: Int32Array =
    column.rLevelMax > 0
      ? (decodeValues(
          PARQUET_RDLVL_TYPE,
          rLevelEncoding,
          dataCursor,
          valueCount,
          {
            bitWidth: Util.getBitWidth(column.rLevelMax),
            disableEnvelope: false,
            // column: opts.column
          }
        ) as Int32Array)
      : new Int32Array(valueCount);

  // read definition levels
  const dLevelEncoding = Util.getThriftEnum(
    Encoding,
    header.data_page_header.definition_level_encoding
  ) as ParquetCodec;
  // tslint:disable-next-line:prefer-array-literal
  const dLevels: Int32Array =
    column.dLevelMax > 0
      ? (decodeValues(
          PARQUET_RDLVL_TYPE,
          dLevelEncoding,
          dataCursor,
          valueCount,
          {
            bitWidth: Util.getBitWidth(column.dLevelMax),
            disableEnvelope: false,
            // column: opts.column
          }
        ) as Int32Array)
      : new Int32Array(valueCount);
  let valueCountNonNull = 0;
  for (const dlvl of dLevels) {
    if (dlvl === column.dLevelMax) {
      valueCountNonNull++;
    }
  }

  /* read values */
  const valueEncoding = Util.getThriftEnum(
    Encoding,
    header.data_page_header.encoding
  ) as ParquetCodec;
  const values = decodeValues(
    column.primitiveType,
    valueEncoding,
    dataCursor,
    valueCountNonNull,
    {
      typeLength: column.typeLength,
      bitWidth: column.typeLength,
    }
  );

  return {
    dLevels,
    rLevels,
    values,
    count: valueCount,
  };
}

function decodeDataPageV2(
  cursor: CursorBuffer,
  header: PageHeader,
  column: ParquetField,
  compression: ParquetCompression
): ParquetReadData {
  const cursorEnd = cursor.offset + header.compressed_page_size;

  const valueCount = header.data_page_header_v2.num_values;
  const valueCountNonNull = valueCount - header.data_page_header_v2.num_nulls;
  const valueEncoding = Util.getThriftEnum(
    Encoding,
    header.data_page_header_v2.encoding
  ) as ParquetCodec;

  // read repetition levels
  const rLevels =
    column.rLevelMax > 0
      ? (decodeValues(
          PARQUET_RDLVL_TYPE,
          PARQUET_RDLVL_ENCODING,
          cursor,
          valueCount,
          {
            bitWidth: Util.getBitWidth(column.rLevelMax),
            disableEnvelope: true,
          }
        ) as Int32Array)
      : new Int32Array(valueCount);

  // read definition levels
  const dLevels: ParquetValueArray =
    column.dLevelMax > 0
      ? (decodeValues(
          PARQUET_RDLVL_TYPE,
          PARQUET_RDLVL_ENCODING,
          cursor,
          valueCount,
          {
            bitWidth: Util.getBitWidth(column.dLevelMax),
            disableEnvelope: true,
          }
        ) as Int32Array)
      : new Int32Array(valueCount);

  /* read values */
  let valuesBufCursor = cursor;

  if (header.data_page_header_v2.is_compressed) {
    const valuesBuf = Compression.inflate(
      compression,
      cursor.buffer.slice(cursor.offset, cursorEnd),
      header.uncompressed_page_size
    );

    valuesBufCursor = {
      buffer: valuesBuf,
      offset: 0,
      size: valuesBuf.length,
    };

    cursor.offset = cursorEnd;
  }

  const values = decodeValues(
    column.primitiveType,
    valueEncoding,
    valuesBufCursor,
    valueCountNonNull,
    {
      typeLength: column.typeLength,
      bitWidth: column.typeLength,
    }
  );

  return {
    dLevels,
    rLevels,
    values,
    count: valueCount,
  };
}

function decodeSchema(
  schemaElements: SchemaElement[],
  offset: number,
  len: number
): {
  offset: number;
  next: number;
  schema: SchemaDefinition;
} {
  const schema: SchemaDefinition = {};
  let next = offset;
  for (let i = 0; i < len; i++) {
    const schemaElement = schemaElements[next];

    const repetitionType =
      next > 0
        ? Util.getThriftEnum(FieldRepetitionType, schemaElement.repetition_type)
        : 'ROOT';

    const optional = repetitionType === 'OPTIONAL';
    const repeated = repetitionType === 'REPEATED';

    if (schemaElement.num_children > 0) {
      const res = decodeSchema(
        schemaElements,
        next + 1,
        schemaElement.num_children
      );
      next = res.next;
      schema[schemaElement.name] = {
        // type: undefined,
        optional,
        repeated,
        fields: res.schema,
      };
    } else {
      let logicalType = Util.getThriftEnum(Type, schemaElement.type);

      if (schemaElement.converted_type != null) {
        logicalType = Util.getThriftEnum(
          ConvertedType,
          schemaElement.converted_type
        );
      }

      schema[schemaElement.name] = {
        type: logicalType as ParquetType,
        typeLength: schemaElement.type_length,
        optional,
        repeated,
      };
      next++;
    }
  }
  return { schema, offset, next };
}
