import { Transform, TransformCallback, Writable } from 'stream';
import { ParquetSchema } from './schema';
import { RowGroup } from './thrift';
import {
  PARQUET_DEFAULT_PAGE_SIZE,
  PARQUET_DEFAULT_ROW_GROUP_SIZE,
  PARQUET_MAGIC,
  encodeFooter,
  encodeRowGroup,
  ParquetWriterOptions,
} from './encoding';
import * as Util from './util';
import {
  ParquetWriteBuffer,
  shredRecord,
} from './shred';

export { ParquetWriterOptions };

/**
 * Write a parquet file to an output stream. The ParquetWriter will perform
 * buffering/batching for performance, so close() must be called after all rows
 * are written.
 */
export class ParquetWriter<T> {
  /**
   * Convenience method to create a new buffered parquet writer that writes to
   * the specified file
   */
  static async openFile<T>(
    schema: ParquetSchema,
    path: string,
    opts?: ParquetWriterOptions
  ): Promise<ParquetWriter<T>> {
    const outputStream = await Util.osopen(path, opts);
    return ParquetWriter.openStream(schema, outputStream, opts);
  }

  /**
   * Convenience method to create a new buffered parquet writer that writes to
   * the specified stream
   */
  static async openStream<T>(
    schema: ParquetSchema,
    outputStream: Writable,
    opts?: ParquetWriterOptions
  ): Promise<ParquetWriter<T>> {
    if (!opts) {
      // tslint:disable-next-line:no-parameter-reassignment
      opts = {};
    }

    const envelopeWriter = await ParquetEnvelopeWriter.openStream(
      schema,
      outputStream,
      opts
    );

    return new ParquetWriter(schema, envelopeWriter, opts);
  }

  public schema: ParquetSchema;
  public envelopeWriter: ParquetEnvelopeWriter;
  public rowBuffer: ParquetWriteBuffer;
  public rowGroupSize: number;
  public closed: boolean;
  public headerWritten: boolean;
  public userMetadata: Record<string, string>;

  /**
   * Create a new buffered parquet writer for a given envelope writer
   */
  constructor(
    schema: ParquetSchema,
    envelopeWriter: ParquetEnvelopeWriter,
    opts: ParquetWriterOptions
  ) {
    this.schema = schema;
    this.envelopeWriter = envelopeWriter;
    this.rowBuffer = new ParquetWriteBuffer(schema);
    this.rowGroupSize = opts.rowGroupSize || PARQUET_DEFAULT_ROW_GROUP_SIZE;
    this.closed = false;
    this.headerWritten = false;
    this.userMetadata = {};
  }

  /**
   * Write the header if it was not already written
   */
  async ensureHeaderWritten(): Promise<void> {
    if(!this.headerWritten) {
      try {
        // Set the flag before making the call so that a concurrent call while the header
        // is being written will not write the header a second time
        this.headerWritten = true;

        // Go ahead and write the header
        await this.envelopeWriter.writeHeader();
      } catch (err) {
        this.envelopeWriter.close();
        throw err;
      }
    }
  }

  /**
   * Append a single row to the parquet file. Rows are buffered in memory until
   * rowGroupSize rows are in the buffer or close() is called
   */
  async appendRow<T>(row: T): Promise<void> {
    if (this.closed) {
      throw new Error('writer was closed');
    }
    shredRecord(this.schema, row, this.rowBuffer);
    if (this.rowBuffer.rowCount >= this.rowGroupSize) {
      await this.ensureHeaderWritten();
      await this.envelopeWriter.writeRowGroup(this.rowBuffer);
      this.rowBuffer = new ParquetWriteBuffer(this.schema);
    }
  }

  /**
   * Finish writing the parquet file and commit the footer to disk. This method
   * MUST be called after you are finished adding rows. You must not call this
   * method twice on the same object or add any rows after the close() method has
   * been called
   */
  async close(callback?: () => void): Promise<void> {
    if (this.closed) {
      throw new Error('writer was closed');
    }

    this.closed = true;

    // Make sure we have written the header even if the file is empty
    await this.ensureHeaderWritten();

    if (
      this.rowBuffer.rowCount > 0 ||
      this.rowBuffer.rowCount >= this.rowGroupSize
    ) {
      await this.envelopeWriter.writeRowGroup(this.rowBuffer);
      this.rowBuffer = new ParquetWriteBuffer(this.schema);
    }

    await this.envelopeWriter.writeFooter(this.userMetadata);
    await this.envelopeWriter.close();
    this.envelopeWriter = null;

    if (callback) {
      callback();
    }
  }

  /**
   * Add key<>value metadata to the file
   */
  setMetadata(key: string, value: string): void {
    // TODO: value to be any, obj -> JSON
    this.userMetadata[String(key)] = String(value);
  }

  /**
   * Set the parquet row group size. This values controls the maximum number
   * of rows that are buffered in memory at any given time as well as the number
   * of rows that are co-located on disk. A higher value is generally better for
   * read-time I/O performance at the tradeoff of write-time memory usage.
   */
  setRowGroupSize(cnt: number): void {
    this.rowGroupSize = cnt;
  }

  /**
   * Set the parquet data page size. The data page size controls the maximum
   * number of column values that are written to disk as a consecutive array
   */
  setPageSize(cnt: number): void {
    this.envelopeWriter.setPageSize(cnt);
  }
}

/**
 * Create a parquet file from a schema and a number of row groups. This class
 * performs direct, unbuffered writes to the underlying output stream and is
 * intendend for advanced and internal users; the writeXXX methods must be
 * called in the correct order to produce a valid file.
 */
export class ParquetEnvelopeWriter {
  /**
   * Create a new parquet envelope writer that writes to the specified stream
   */
  static async openStream(
    schema: ParquetSchema,
    outputStream: Writable,
    opts: ParquetWriterOptions
  ): Promise<ParquetEnvelopeWriter> {
    const writeFn = Util.oswrite.bind(undefined, outputStream);
    const closeFn = Util.osclose.bind(undefined, outputStream);
    return new ParquetEnvelopeWriter(schema, writeFn, closeFn, 0, opts);
  }

  public schema: ParquetSchema;
  public write: (buf: Buffer) => Promise<void>;
  public close: () => Promise<void>;
  public offset: number;
  public rowCount: number;
  public rowGroups: RowGroup[];
  public pageSize: number;
  public useDataPageV2: boolean;

  constructor(
    schema: ParquetSchema,
    writeFn: (buf: Buffer) => Promise<void>,
    closeFn: () => Promise<void>,
    fileOffset: number,
    opts: ParquetWriterOptions
  ) {
    this.schema = schema;
    this.write = writeFn;
    this.close = closeFn;
    this.offset = fileOffset;
    this.rowCount = 0;
    this.rowGroups = [];
    this.pageSize = opts.pageSize || PARQUET_DEFAULT_PAGE_SIZE;
    this.useDataPageV2 = 'useDataPageV2' in opts ? opts.useDataPageV2 : false;
  }

  writeSection(buf: Buffer): Promise<void> {
    this.offset += buf.length;
    return this.write(buf);
  }

  /**
   * Encode the parquet file header
   */
  writeHeader(): Promise<void> {
    return this.writeSection(Buffer.from(PARQUET_MAGIC));
  }

  /**
   * Encode a parquet row group. The records object should be created using the
   * shredRecord method
   */
  writeRowGroup(records: ParquetWriteBuffer): Promise<void> {
    const rowGroup = encodeRowGroup(this.schema, records, {
      baseOffset: this.offset,
      pageSize: this.pageSize,
      useDataPageV2: this.useDataPageV2,
    });

    this.rowCount += records.rowCount;
    this.rowGroups.push(rowGroup.metadata);
    return this.writeSection(rowGroup.body);
  }

  /**
   * Write the parquet file footer
   */
  writeFooter(userMetadata: Record<string, string>): Promise<void> {
    if (!userMetadata) {
      // tslint:disable-next-line:no-parameter-reassignment
      userMetadata = {};
    }

    return this.writeSection(
      encodeFooter(this.schema, this.rowCount, this.rowGroups, userMetadata)
    );
  }

  /**
   * Set the parquet data page size. The data page size controls the maximum
   * number of column values that are written to disk as a consecutive array
   */
  setPageSize(cnt: number): void {
    this.pageSize = cnt;
  }
}

/**
 * Create a parquet transform stream
 */
export class ParquetTransformer<T> extends Transform {
  public writer: ParquetWriter<T>;
  waiting: [() => void, (reason?: any) => void][] = [];

  constructor(schema: ParquetSchema, opts: ParquetWriterOptions = {}) {
    super({ objectMode: true });
    const writeFn = (function (t: ParquetTransformer<any>) {
      return function (b: any): Promise<void> {
        if (!t.push(b)) {
          // stop writing until the readable is ready again
          return new Promise((resolve, reject) => {
            t.waiting.push([resolve, reject]);
          });
        }
        return Promise.resolve();
      };
    })(this);
    const closeFn = (function (t: ParquetTransformer<any>) {
      return function (): Promise<void> {
        t.push(null);
        return Promise.resolve();
      };
    })(this);
    this.writer = new ParquetWriter(
      schema,
      new ParquetEnvelopeWriter(schema, writeFn, closeFn, 0, opts),
      opts
    );
  }

  // If I/O was delayed due to backpressure and then the stream is destroyed,
  // propagate an error back to the callee of the I/O operation(s)
  // tslint:disable-next-line:function-name
  _destroy(error: Error | null, callback: (error: Error | null) => void): void {
    try {
      if (this.waiting.length) {
        const waiting = this.waiting;
        this.waiting = [];
        waiting.forEach(([resolve, reject]) =>
          error ? reject(error) : resolve()
        );
      }
      callback(null);
    } catch (err) {
      callback(err);
    }
  }

  // If we get backpressure we will delay returning from a call to write until
  // the next call to _read
  // tslint:disable-next-line:function-name
  _read(arg?: any) {
    if (this.waiting.length) {
      const waiting = this.waiting;
      this.waiting = [];
      waiting.forEach(([resolve]) => resolve());
    }
    return super._read(arg);
  }

  // tslint:disable-next-line:function-name
  _transform(row: any, encoding: string, callback: TransformCallback) {
    if (row) {
      this.writer.appendRow(row).then(
        () => callback(),
        err => callback(err)
      );
    } else {
      callback();
    }
  }

  // tslint:disable-next-line:function-name
  _flush(callback: (val?: any) => void) {
    this.writer.close(callback);
  }
}

