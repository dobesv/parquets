import { ParquetSchema } from './schema';
import {
  ParquetWriteBuffer,
  shredRecord,
} from './shred';
import {
  encodeRowGroup,
  encodeFooter,
  PARQUET_MAGIC,
  PARQUET_DEFAULT_PAGE_SIZE,
  PARQUET_DEFAULT_ROW_GROUP_SIZE,
} from './encoding';
import { RowGroup } from './thrift';

/**
 * Options for creating a ParquetBufferWriter
 */
export interface ParquetBufferWriterOptions {
  rowGroupSize?: number;
  pageSize?: number;
  useDataPageV2?: boolean;
}

/**
 * Synchronous in-memory Parquet writer that accumulates rows and returns a Buffer.
 * Browser-safe: no imports from stream, fs, or zlib.
 */
export class ParquetBufferWriter<T> {
  private schema: ParquetSchema;
  private opts: ParquetBufferWriterOptions;
  private chunks: Buffer[];
  private offset: number;
  private rowBuffer: ParquetWriteBuffer;
  private rowGroups: RowGroup[];
  private rowCount: number;
  private closed: boolean;
  private headerWritten: boolean;
  private rowGroupSize: number;
  private pageSize: number;

  /**
   * Create a new ParquetBufferWriter
   */
  static openBuffer<T>(
    schema: ParquetSchema,
    opts?: ParquetBufferWriterOptions
  ): ParquetBufferWriter<T> {
    return new ParquetBufferWriter<T>(schema, opts);
  }

  /**
   * Constructor
   */
  constructor(schema: ParquetSchema, opts?: ParquetBufferWriterOptions) {
    this.schema = schema;
    this.opts = opts || {};
    this.chunks = [];
    this.offset = 0;
    this.rowBuffer = new ParquetWriteBuffer(schema);
    this.rowGroups = [];
    this.rowCount = 0;
    this.closed = false;
    this.headerWritten = false;
    this.rowGroupSize = this.opts.rowGroupSize || PARQUET_DEFAULT_ROW_GROUP_SIZE;
    this.pageSize = this.opts.pageSize || PARQUET_DEFAULT_PAGE_SIZE;
  }

  /**
   * Append a row to the buffer. Synchronous.
   */
  appendRow(row: T): void {
    if (this.closed) {
      throw new Error('writer was closed');
    }

    // Write header on first row
    if (!this.headerWritten) {
      const headerBuf = Buffer.from(PARQUET_MAGIC);
      this.writeSection(headerBuf);
      this.headerWritten = true;
    }

    // Shred the record into the buffer
    shredRecord(this.schema, row, this.rowBuffer);
    this.rowCount++;

    // Flush row group if we've reached the row group size
    if (this.rowBuffer.rowCount >= this.rowGroupSize) {
      this.flushRowGroup();
    }
  }

  /**
   * Finalize and return the complete Parquet file as a Buffer.
   * Terminal operation: sets closed=true, throws if called twice.
   */
  toBuffer(): Buffer {
    if (this.closed) {
      throw new Error('writer was closed');
    }

    this.closed = true;

    // Write header if no rows were added (empty file still needs PAR1 magic)
    if (!this.headerWritten) {
      const headerBuf = Buffer.from(PARQUET_MAGIC);
      this.writeSection(headerBuf);
    }

    // Flush final row group if there are remaining rows
    if (this.rowBuffer.rowCount > 0) {
      this.flushRowGroup();
    }

    // Write footer
    const footerBuf = encodeFooter(this.schema, this.rowCount, this.rowGroups, {});
    this.writeSection(footerBuf);

    return Buffer.concat(this.chunks);
  }

  /**
   * Write a buffer section and update offset
   */
  private writeSection(buf: Buffer): void {
    this.chunks.push(buf);
    this.offset += buf.length;
  }

  /**
   * Flush the current row group to chunks
   */
  private flushRowGroup(): void {
    const { body, metadata } = encodeRowGroup(this.schema, this.rowBuffer, {
      baseOffset: this.offset,
      pageSize: this.pageSize,
      useDataPageV2: this.opts.useDataPageV2,
    });

    this.writeSection(body);
    this.rowGroups.push(metadata);

    // Reset row buffer for next row group
    this.rowBuffer = new ParquetWriteBuffer(this.schema);
  }
}
