import { ParquetCodecOptions, PARQUET_CODEC } from './codec';
import { encodeValues as plainEncode } from './codec/plain';
import * as Compression from './compression';
import {
  ParquetCodec,
  ParquetField,
  ParquetValueArray,
  PrimitiveType,
} from './declare';
import { ParquetSchema } from './schema';
import {
  ColumnChunk,
  ColumnMetaData,
  CompressionCodec,
  ConvertedType,
  DataPageHeader,
  DataPageHeaderV2,
  Encoding,
  FieldRepetitionType,
  FileMetaData,
  KeyValue,
  PageHeader,
  PageType,
  RowGroup,
  SchemaElement,
  Statistics,
  Type,
} from './thrift';
import * as Util from './util';
import Int64 = require('node-int64');
import {
  ParquetWriteBuffer,
  ParquetWriteColumnData,
} from './shred';
export interface ParquetWriterOptions {
  baseOffset?: number;
  rowGroupSize?: number;
  pageSize?: number;
  useDataPageV2?: boolean;

  // Write Stream Options
  flags?: string;
  encoding?: string;
  fd?: number;
  mode?: number;
  autoClose?: boolean;
  start?: number;
}

/**
 * Parquet File Magic String
 */
export const PARQUET_MAGIC = 'PAR1';

/**
 * Parquet Version
 */
export const PARQUET_VERSION = 1;

/**
 * Default Page and Row Group sizes
 */
export const PARQUET_DEFAULT_PAGE_SIZE = 8192;
export const PARQUET_DEFAULT_ROW_GROUP_SIZE = 4096;

/**
 * Repetition and Definition Level encodings
 */
export const PARQUET_RDLVL_TYPE = 'INT32';
export const PARQUET_RDLVL_ENCODING = 'RLE';

/**
 * Encode a consecutive array of data using one of the parquet encodings
 */
function encodeValues(
  type: PrimitiveType,
  encoding: ParquetCodec,
  values: ParquetValueArray,
  opts: ParquetCodecOptions
) {
  if (!(encoding in PARQUET_CODEC)) {
    throw new Error(`invalid encoding: ${encoding}`);
  }
  return PARQUET_CODEC[encoding].encodeValues(type, values, opts);
}

/**
 * Encode a parquet data page
 */
function encodeDataPage(
  column: ParquetField,
  data: ParquetWriteColumnData
): {
  header: PageHeader;
  headerSize: number;
  page: Buffer;
} {
  /* encode repetition and definition levels */
  let rLevelsBuf = Buffer.alloc(0);
  if (column.rLevelMax > 0) {
    rLevelsBuf = encodeValues(
      PARQUET_RDLVL_TYPE,
      PARQUET_RDLVL_ENCODING,
      data.rLevels,
      {
        bitWidth: Util.getBitWidth(column.rLevelMax),
        // disableEnvelope: false
      }
    );
  }

  let dLevelsBuf = Buffer.alloc(0);
  if (column.dLevelMax > 0) {
    dLevelsBuf = encodeValues(
      PARQUET_RDLVL_TYPE,
      PARQUET_RDLVL_ENCODING,
      data.dLevels,
      {
        bitWidth: Util.getBitWidth(column.dLevelMax),
        // disableEnvelope: false
      }
    );
  }

  /* encode values */
  const valuesBuf = encodeValues(
    column.primitiveType,
    column.encoding,
    data.values,
    { typeLength: column.typeLength, bitWidth: column.typeLength }
  );

  const dataBuf = Buffer.concat([rLevelsBuf, dLevelsBuf, valuesBuf]);

  // compression = column.compression === 'UNCOMPRESSED' ? (compression || 'UNCOMPRESSED') : column.compression;
  const compressedBuf = Compression.deflate(column.compression, dataBuf);

  /* build page header */
  const header = new PageHeader({
    type: PageType.DATA_PAGE,
    data_page_header: new DataPageHeader({
      num_values: data.count,
      encoding: Encoding[column.encoding] as any,
      definition_level_encoding: Encoding[PARQUET_RDLVL_ENCODING],
      repetition_level_encoding: Encoding[PARQUET_RDLVL_ENCODING],
    }),
    uncompressed_page_size: dataBuf.length,
    compressed_page_size: compressedBuf.length,
  });

  /* concat page header, repetition and definition levels and values */
  const headerBuf = Util.serializeThrift(header);
  const page = Buffer.concat([headerBuf, compressedBuf]);

  return { header, headerSize: headerBuf.length, page };
}

/**
 * Encode a parquet data page (v2)
 */
function encodeDataPageV2(
  column: ParquetField,
  data: ParquetWriteColumnData,
  rowCount: number
): {
  header: PageHeader;
  headerSize: number;
  page: Buffer;
} {
  /* encode values */
  const valuesBuf = encodeValues(
    column.primitiveType,
    column.encoding,
    data.values,
    {
      typeLength: column.typeLength,
      bitWidth: column.typeLength,
    }
  );

  const compressedBuf = Compression.deflate(column.compression, valuesBuf);

  /* encode repetition and definition levels */
  let rLevelsBuf = Buffer.alloc(0);
  if (column.rLevelMax > 0) {
    rLevelsBuf = encodeValues(
      PARQUET_RDLVL_TYPE,
      PARQUET_RDLVL_ENCODING,
      data.rLevels,
      {
        bitWidth: Util.getBitWidth(column.rLevelMax),
        disableEnvelope: true,
      }
    );
  }

  let dLevelsBuf = Buffer.alloc(0);
  if (column.dLevelMax > 0) {
    dLevelsBuf = encodeValues(
      PARQUET_RDLVL_TYPE,
      PARQUET_RDLVL_ENCODING,
      data.dLevels,
      {
        bitWidth: Util.getBitWidth(column.dLevelMax),
        disableEnvelope: true,
      }
    );
  }

  /* build page header */
  const header = new PageHeader({
    type: PageType.DATA_PAGE_V2,
    data_page_header_v2: new DataPageHeaderV2({
      num_values: data.count,
      num_nulls: data.count - data.values.length,
      num_rows: rowCount,
      encoding: Encoding[column.encoding] as any,
      definition_levels_byte_length: dLevelsBuf.length,
      repetition_levels_byte_length: rLevelsBuf.length,
      is_compressed: column.compression !== 'UNCOMPRESSED',
    }),
    uncompressed_page_size:
      rLevelsBuf.length + dLevelsBuf.length + valuesBuf.length,
    compressed_page_size:
      rLevelsBuf.length + dLevelsBuf.length + compressedBuf.length,
  });

  /* concat page header, repetition and definition levels and values */
  const headerBuf = Util.serializeThrift(header);
  const page = Buffer.concat([
    headerBuf,
    rLevelsBuf,
    dLevelsBuf,
    compressedBuf,
  ]);
  return { header, headerSize: headerBuf.length, page };
}

/**
 * Encode an array of values into a parquet column chunk
 */
function encodeColumnChunk(
  column: ParquetField,
  buffer: ParquetWriteBuffer,
  offset: number,
  opts: ParquetWriterOptions
): {
  body: Buffer;
  metadata: ColumnMetaData;
  metadataOffset: number;
} {
  const data = buffer.columnData[column.path.join()];
  const stats = buffer.statistics[column.path.join()];
  const baseOffset = (opts.baseOffset || 0) + offset;
  let pageBuf: Buffer;
  let total_uncompressed_size = 0;
  let total_compressed_size = 0;
  {
    let result: any;
    if (opts.useDataPageV2) {
      result = encodeDataPageV2(column, data, buffer.rowCount);
    } else {
      result = encodeDataPage(column, data);
    }
    pageBuf = result.page;
    total_uncompressed_size +=
      result.header.uncompressed_page_size + result.headerSize;
    total_compressed_size +=
      result.header.compressed_page_size + result.headerSize;
  }

  const metadata = new ColumnMetaData({
    path_in_schema: column.path,
    num_values: data.count,
    data_page_offset: baseOffset,
    encodings: [],
    total_uncompressed_size,
    total_compressed_size,
    type: Type[column.primitiveType],
    codec: CompressionCodec[column.compression],
    statistics: new Statistics({
      min_value: encodeValue(stats.min, column),
      max_value: encodeValue(stats.max, column),
      null_count: new Int64(stats.null_count),
      distinct_count: new Int64(stats.distinct_values.size),
    }),
  });

  metadata.encodings.push(Encoding[PARQUET_RDLVL_ENCODING]);
  metadata.encodings.push(Encoding[column.encoding]);

  const metadataOffset = baseOffset + pageBuf.length;
  const body = Buffer.concat([pageBuf, Util.serializeThrift(metadata)]);
  return { body, metadata, metadataOffset };
}

function encodeValue(value: any, column: ParquetField): Buffer | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }

  return plainEncode(column.primitiveType, [value], {
    typeLength: column.typeLength,
    bitWidth: column.typeLength,
  });
}

/**
 * Encode a list of column values into a parquet row group
 */
export function encodeRowGroup(
  schema: ParquetSchema,
  data: ParquetWriteBuffer,
  opts: ParquetWriterOptions
): {
  body: Buffer;
  metadata: RowGroup;
} {
  const metadata = new RowGroup({
    num_rows: data.rowCount,
    columns: [],
    total_byte_size: 0,
  });

  let body = Buffer.alloc(0);
  for (const field of schema.fieldList) {
    if (field.isNested) {
      continue;
    }

    const cchunkData = encodeColumnChunk(field, data, body.length, opts);

    const cchunk = new ColumnChunk({
      file_offset: cchunkData.metadataOffset,
      meta_data: cchunkData.metadata,
    });

    metadata.columns.push(cchunk);
    metadata.total_byte_size = new Int64(
      +metadata.total_byte_size + cchunkData.body.length
    );

    body = Buffer.concat([body, cchunkData.body]);
  }

  return { body, metadata };
}

/**
 * Encode a parquet file metadata footer
 */
export function encodeFooter(
  schema: ParquetSchema,
  rowCount: number,
  rowGroups: RowGroup[],
  userMetadata: Record<string, string>
): Buffer {
  const metadata = new FileMetaData({
    version: PARQUET_VERSION,
    created_by: 'parquets',
    num_rows: rowCount,
    row_groups: rowGroups,
    schema: [],
    key_value_metadata: [],
  });

  for (const key in userMetadata) {
    const kv = new KeyValue({
      key,
      value: userMetadata[key],
    });
    metadata.key_value_metadata.push(kv);
  }

  {
    const schemaRoot = new SchemaElement({
      name: 'root',
      num_children: Object.keys(schema.fields).length,
    });
    metadata.schema.push(schemaRoot);
  }

  for (const field of schema.fieldList) {
    const relt = FieldRepetitionType[field.repetitionType];
    const schemaElem = new SchemaElement({
      name: field.name,
      repetition_type: relt as any,
    });

    if (field.isNested) {
      schemaElem.num_children = field.fieldCount;
    } else {
      schemaElem.type = Type[field.primitiveType] as Type;
    }

    if (field.originalType) {
      schemaElem.converted_type = ConvertedType[
        field.originalType
      ] as ConvertedType;
    }

    schemaElem.type_length = field.typeLength;

    metadata.schema.push(schemaElem);
  }

  const metadataEncoded = Util.serializeThrift(metadata);
  const footerEncoded = Buffer.alloc(metadataEncoded.length + 8);
  metadataEncoded.copy(footerEncoded);
  footerEncoded.writeUInt32LE(metadataEncoded.length, metadataEncoded.length);
  footerEncoded.write(PARQUET_MAGIC, metadataEncoded.length + 4);
  return footerEncoded;
}
