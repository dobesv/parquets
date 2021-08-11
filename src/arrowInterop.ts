// import { ParquetReader } from './reader';
// import { Table } from 'apache-arrow/table';
// import * as Util from './util';
//
// /**
//  * Convert a parquet file into an arrow table.
//  *
//  * @param reader
//  * @param columns If non-empty, columns will be filtered down to the ones
//  *    selected by the paths in the array.  Paths support MQTT wildcards:
//  *    + to select all immediate children and # to select all descendants.
//  */
// export async function loadParquetFileAsArrowTable(
//   reader: ParquetReader<unknown>,
//   columnList: string[][] = []
// ): Table {
//   const schema = reader.getSchema();
//   const rowCount = reader.getRowCount();
//   for (const rowGroup of reader.metadata.row_groups) {
//     for (const colChunk of rowGroup.columns) {
//       const colMetadata = colChunk.meta_data;
//       const colKey = colMetadata.path_in_schema;
//       if (columnList.length > 0 && Util.fieldIndexOf(columnList, colKey) < 0) {
//         continue;
//       }
//       const data = await reader.envelopeReader.readColumnChunk(
//         schema,
//         colChunk
//       );
//       if (!data.count) {
//         continue;
//       }
//
//     }
//   }
// }
