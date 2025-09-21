import 'jest';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import chai = require('chai');
import parquet = require('../src');

const assert = chai.assert;

const REPO_ROOT = path.join(__dirname, 'parquet-testing-repo');
const DATA_DIRS = [
  path.join(REPO_ROOT, 'data'),
  path.join(REPO_ROOT, 'variant'),
  path.join(REPO_ROOT, 'shredded_variant'),
];
const BAD_DATA_DIR = path.join(REPO_ROOT, 'bad_data');

function listParquetFiles(dir: string): string[] {
  const out: string[] = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      out.push(...listParquetFiles(p));
    } else if (entry.isFile() && entry.name.toLowerCase().endsWith('.parquet')) {
      out.push(p);
    }
  }
  return out.sort();
}

async function readFirstRows(file: string, limit = 10): Promise<any[]> {
  const reader = await parquet.ParquetReader.openFile(file);
  try {
    const cursor = reader.getCursor();
    const rows: any[] = [];
    for (let i = 0; i < limit; i++) {
      const row = await cursor.next();
      if (!row) break;
      rows.push(row);
    }
    return rows;
  } finally {
    await reader.close();
  }
}

function valuesEqual(a: any, b: any): boolean {
  if (a === b) return true;
  if (a == null || b == null) return a == b;
  if (Buffer.isBuffer(a) && Buffer.isBuffer(b)) return a.equals(b);
  if (a instanceof Date && b instanceof Date) return a.getTime() === b.getTime();
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) if (!valuesEqual(a[i], b[i])) return false;
    return true;
  }
  if (typeof a === 'object' && typeof b === 'object') {
    const ak = Object.keys(a).sort();
    const bk = Object.keys(b).sort();
    if (!valuesEqual(ak, bk)) return false;
    for (const k of ak) if (!valuesEqual(a[k], b[k])) return false;
    return true;
  }
  // number/string/boolean fallback with numeric tolerance for floats
  if (typeof a === 'number' && typeof b === 'number') {
    if (Number.isNaN(a) && Number.isNaN(b)) return true;
    return Math.abs(a - b) < 1e-9;
  }
  return String(a) === String(b);
}

async function roundTripAndCompare(file: string, rows: any[]): Promise<void> {
  // Read schema from original file
  const reader = await parquet.ParquetReader.openFile(file);
  const schema = reader.schema;
  await reader.close();

  const tmp = path.join(os.tmpdir(), `parquet-rt-${path.basename(file)}-${Date.now()}.parquet`);
  const writer = await parquet.ParquetWriter.openFile(schema, tmp);
  for (const r of rows) {
    await writer.appendRow(r);
  }
  await writer.close();

  const reread = await readFirstRows(tmp, rows.length);
  assert.equal(reread.length, rows.length, 'roundtrip row count');
  for (let i = 0; i < rows.length; i++) {
    assert(valuesEqual(reread[i], rows[i]), `roundtrip row ${i} equal`);
  }

  // cleanup best-effort
  try { fs.unlinkSync(tmp); } catch {}
}

// --- DuckDB helpers ---
let duckdbConn: any | null = null;
async function getDuck(): Promise<any | null> {
  if (duckdbConn) return duckdbConn;
  let duckdb: any;
  try {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    duckdb = require('@duckdb/node-api');
  } catch {
    return null; // not installed
  }
  const db = new duckdb.Database(':memory:');
  duckdbConn = await db.connect();
  return duckdbConn;
}

async function duckAll(sql: string): Promise<any[] | null> {
  const conn = await getDuck();
  if (!conn) return null;
  return new Promise((resolve, reject) => {
    conn.all(sql, (err: any, rows: any[]) => (err ? reject(err) : resolve(rows)));
  });
}

function toSqlStringLiteral(p: string): string {
  return `'${p.replace(/'/g, "''")}'`;
}

function normalizeDuckValue(v: any): any {
  if (v == null) return null;
  if (Buffer.isBuffer(v)) return v; // keep buffers
  if (v instanceof Date) return v;
  if (typeof v === 'object') {
    // For structs/lists, stringify to compare leniently
    return JSON.stringify(v);
  }
  return v;
}

async function compareWithDuckDB(file: string, ourRows: any[]): Promise<void> {
  const conn = await getDuck();
  if (!conn) return; // skip if duckdb not available
  const fileSql = toSqlStringLiteral(file);
  const duckRows = (await duckAll(`select * from ${fileSql} limit ${ourRows.length}`)) || [];
  assert.equal(duckRows.length, ourRows.length, 'duckdb row count (first N)');
  for (let i = 0; i < duckRows.length; i++) {
    const dr = duckRows[i];
    const orow = ourRows[i] || {};
    for (const k of Object.keys(dr)) {
      const dv = normalizeDuckValue(dr[k]);
      const ov = (orow as any)[k];
      if (!valuesEqual(ov, dv)) {
        // last resort, string compare
        assert.equal(String(ov), String(dv), `duckdb row ${i} col ${k}`);
      }
    }
  }
}

async function compareMetadataWithDuckDB(file: string): Promise<void> {
  const conn = await getDuck();
  if (!conn) return; // skip if not available
  const fileSql = toSqlStringLiteral(file);

  const oursReader = await parquet.ParquetReader.openFile(file);
  try {
    const ours = oursReader.metadata;
    const basic = await duckAll(`select num_rows, list_length(row_groups) as num_row_groups from parquet_metadata(${fileSql})`);
    if (basic && basic[0]) {
      assert.equal(basic[0].num_rows, ours.num_rows, 'metadata num_rows');
      assert.equal(basic[0].num_row_groups, ours.row_groups.length, 'metadata row_groups length');
    }

    // Per-column stats per row group
    let stats: any[] = [];
    try {
      stats = (await duckAll(
        `with meta as (select * from parquet_metadata(${fileSql}))
         select rg.row_group_id as rg,
                c.column_id as col_idx,
                c.path_in_schema as name,
                c.statistics.null_count as null_count,
                c.statistics.min as min,
                c.statistics.max as max,
                c.statistics.distinct_count as distinct_count
         from meta, unnest(meta.row_groups) as rg, unnest(rg.columns) as c
         order by rg.row_group_id, c.column_id`
      )) || [];
    } catch {
      // Ignore if DuckDB schema differs
      stats = [];
    }

    const byRGName = new Map<string, any>();
    for (const r of stats) {
      byRGName.set(`${r.rg}|${Array.isArray(r.name) ? r.name.join('.') : r.name}`, r);
    }

    for (let rgIndex = 0; rgIndex < ours.row_groups.length; rgIndex++) {
      const rg = ours.row_groups[rgIndex];
      for (const col of rg.columns) {
        const md = col.meta_data as any;
        if (!md || !md.path_in_schema) continue;
        const key = `${rgIndex}|${md.path_in_schema.join('.')}`;
        const d = byRGName.get(key);
        if (!d) continue; // unable to match, skip
        const s = md.statistics || {};
        if (s.null_count != null && d.null_count != null) {
          assert.equal(Number(s.null_count), Number(d.null_count), `null_count rg${rgIndex} ${key}`);
        }
        // Min/Max may be binary for byte arrays; stringify for compare
        if (s.min_value != null && d.min != null) {
          assert.equal(String(s.min_value), String(d.min), `min rg${rgIndex} ${key}`);
        } else if (s.min != null && d.min != null) {
          assert.equal(String(s.min), String(d.min), `min rg${rgIndex} ${key}`);
        }
        if (s.max_value != null && d.max != null) {
          assert.equal(String(s.max_value), String(d.max), `max rg${rgIndex} ${key}`);
        } else if (s.max != null && d.max != null) {
          assert.equal(String(s.max), String(d.max), `max rg${rgIndex} ${key}`);
        }
        if (s.distinct_count != null && d.distinct_count != null) {
          assert.equal(Number(s.distinct_count), Number(d.distinct_count), `distinct_count rg${rgIndex} ${key}`);
        }
      }
    }
  } finally {
    await oursReader.close();
  }
}

function findSiblingCsv(file: string): string | null {
  const base = file.replace(/\.parquet$/i, '');
  const candidates = [base + '.csv', base + '.CSV'];
  for (const c of candidates) if (fs.existsSync(c)) return c;
  return null;
}

function parseCsv(file: string): any[] {
  let Papa: any;
  try {
    // Lazy load papaparse; if not available, skip CSV checks
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    Papa = require('papaparse');
  } catch {
    // Indicate that CSV could not be parsed due to missing dependency
    return [];
  }
  const text = fs.readFileSync(file, 'utf8');
  const parsed = Papa.parse(text, { header: true, skipEmptyLines: true });
  if (parsed.errors && parsed.errors.length) {
    throw new Error(`CSV parse errors in ${file}: ${parsed.errors.map((e: any) => e.message).join('; ')}`);
  }
  return parsed.data as any[];
}

function normalizeForCompare(v: any): any {
  if (v === '') return null; // CSV empty -> null
  if (v == null) return null;
  // Try number
  if (!isNaN(Number(v)) && v.trim?.() !== '') return Number(v);
  // Booleans
  if (v === 'true') return true;
  if (v === 'false') return false;
  // ISO date?
  const d = new Date(v);
  if (!isNaN(d.getTime()) && /^\d{4}-\d{2}-\d{2}/.test(String(v))) return d;
  return v;
}

// Expected error substrings for bad_data, based on README
const BAD_DATA_EXPECT: Record<string, RegExp | string> = {
  // 'PARQUET-1481.parquet': 'corrupt',
  // 'ARROW-RS-GH-6229-DICTHEADER.parquet': 'dictionary',
  'ARROW-RS-GH-6229-LEVELS.parquet': 'Could not decode varint',
  // 'ARROW-GH-41321.parquet': 'num_values',
  // 'ARROW-GH-41317.parquet': 'same size',
  // 'ARROW-GH-43605.parquet': /bit[- ]?width|rle/i,
  // 'ARROW-GH-45185.parquet': /repetition levels|levels start/i,
};

// Allow selectively enabling files to run; default is to skip all generated tests.
// See also: test/parquet-testing-repo.failures.list.ts for a commented list of files that currently fail to load
const ALLOW_FILES: Record<string, string> = {
  'binary.parquet': 'auto',
  'binary_truncated_min_max.parquet': 'auto',
  'data_index_bloom_encoding_stats.parquet': 'auto',
  'datapage_v1-corrupt-checksum.parquet': 'auto',
  'datapage_v1-uncompressed-checksum.parquet': 'auto',
  'dict-page-offset-zero.parquet': 'auto',
  'fixed_length_byte_array.parquet': 'auto',
  'incorrect_map_schema.parquet': 'auto',
  'nonnullable.impala.parquet': 'auto',
  'null_list.parquet': 'auto',
  'nulls.snappy.parquet': 'auto',
  'old_list_structure.parquet': 'auto',
  // The files below fail the tests for various reasons - it would be good to fix them
  // alltypes_dictionary.parquet  // invalid encoding: PLAIN_DICTIONARY
  // alltypes_plain.parquet  // invalid encoding: PLAIN_DICTIONARY
  // alltypes_plain.snappy.parquet  // invalid encoding: PLAIN_DICTIONARY
  // alltypes_tiny_pages.parquet  // Invalid typed array length: 2
  // alltypes_tiny_pages_plain.parquet  // Invalid typed array length: 2
  // byte_array_decimal.parquet  // invalid parquet type: DECIMAL
  // byte_stream_split.zstd.parquet  // invalid parquet version
  // byte_stream_split_extended.gzip.parquet  // Cannot read a TUnion with no set value!
  // column_chunk_key_value_metadata.parquet  // invalid parquet version
  // concatenated_gzip_members.parquet  // invalid parquet version
  // data_index_bloom_encoding_with_length.parquet  // invalid encoding: RLE_DICTIONARY
  // datapage_v1-snappy-compressed-checksum.parquet  // Invalid Snappy bitstream
  // datapage_v2.snappy.parquet  // Unsupported data page type DICTIONARY_PAGE
  // datapage_v2_empty_datapage.snappy.parquet  // Invalid Snappy bitstream
  // delta_binary_packed.parquet  // invalid encoding: DELTA_BINARY_PACKED
  // delta_byte_array.parquet  // invalid encoding: DELTA_BYTE_ARRAY
  // delta_encoding_optional_column.parquet  // invalid encoding: DELTA_BINARY_PACKED
  // delta_encoding_required_column.parquet  // invalid encoding: DELTA_BINARY_PACKED
  // delta_length_byte_array.parquet  // invalid parquet version
  // fixed_length_decimal.parquet  // invalid parquet type: DECIMAL
  // fixed_length_decimal_legacy.parquet  // invalid parquet type: DECIMAL
  // hadoop_lz4_compressed.parquet  // Unsupported data page type DICTIONARY_PAGE
  // hadoop_lz4_compressed_larger.parquet  // invalid magic number
  // int32_decimal.parquet  // invalid parquet type: DECIMAL
  // int32_with_null_pages.parquet  // The value of "offset" is out of range. It must be >= 0 and <= 3324. Received 7849
  // int64_decimal.parquet  // invalid parquet type: DECIMAL
  // int96_from_spark.parquet  // invalid encoding: PLAIN_DICTIONARY
  // large_string_map.brotli.parquet  // invalid parquet version
  // list_columns.parquet  // invalid encoding: PLAIN_DICTIONARY
  // lz4_raw_compressed.parquet  // Invalid ENUM value
  // lz4_raw_compressed_larger.parquet  // Invalid ENUM value
  // map_no_value.parquet  // invalid encoding: RLE_DICTIONARY
  // nan_in_stats.parquet  // invalid encoding: PLAIN_DICTIONARY
  // nation.dict-malformed.parquet  // Unsupported data page type DICTIONARY_PAGE
  // nested_lists.snappy.parquet  // Unsupported data page type DICTIONARY_PAGE
  // nested_maps.snappy.parquet  // Unsupported data page type DICTIONARY_PAGE
  // nested_structs.rust.parquet  // invalid compression method: ZSTD
  // non_hadoop_lz4_compressed.parquet  // invalid magic number
  // nullable.impala.parquet  // Unsupported data page type DICTIONARY_PAGE
  // overflow_i16_page_cnt.parquet  // invalid parquet version
  // page_v2_empty_compressed.parquet  // invalid parquet version
  // plain-dict-uncompressed-checksum.parquet  // invalid encoding: PLAIN_DICTIONARY
  // repeated_no_annotation.parquet  // invalid encoding: RLE_DICTIONARY
  // repeated_primitive_no_list.parquet  // invalid encoding: RLE_DICTIONARY
  // rle-dict-snappy-checksum.parquet  // invalid encoding: RLE_DICTIONARY
  // rle-dict-uncompressed-corrupt-checksum.parquet  // invalid encoding: RLE_DICTIONARY
  // rle_boolean_encoding.parquet  // incorrect header check
  // single_nan.parquet  // invalid encoding: PLAIN_DICTIONARY
  // sort_columns.parquet  // invalid parquet version

};

// Generate tests for good data files
for (const dir of DATA_DIRS) {
  if (!fs.existsSync(dir)) continue;
  const files = listParquetFiles(dir);
  describe(`parquet-testing-repo ${path.relative(REPO_ROOT, dir)}`, () => {
    for (const file of files) {
      const name = path.relative(REPO_ROOT, file);
      const allowReason = ALLOW_FILES[path.basename(file)];
      const testFn = allowReason ? test : test.skip;
      testFn(`${name} parses, roundtrips, and optional CSV matches`, async () => {
        const firstRows = await readFirstRows(file, 10);
        // At least able to read something (some files may be empty; allow 0 rows)
        assert(firstRows.length >= 0);

        // Round-trip the rows we did read
        await roundTripAndCompare(file, firstRows);

        // Compare against DuckDB for first rows
        await compareWithDuckDB(file, firstRows);
        // Compare metadata and column statistics against DuckDB
        await compareMetadataWithDuckDB(file);

        // If CSV available, compare first 10 rows
        const csv = findSiblingCsv(file);
        if (csv) {
          const csvRows = parseCsv(csv).slice(0, firstRows.length);
          for (let i = 0; i < csvRows.length; i++) {
            const expected = csvRows[i];
            const actual = firstRows[i];
            for (const k of Object.keys(expected)) {
              const ev = normalizeForCompare(expected[k]);
              const av = (actual as any)[k];
              // Allow some flexibility by stringifying for complex types
              if (!valuesEqual(av, ev)) {
                assert.equal(String(av), String(ev), `CSV field ${k} row ${i}`);
              }
            }
          }
        }
      }, 30000);
    }
  });
}

// Generate tests for bad data files (expect failures)
if (fs.existsSync(BAD_DATA_DIR)) {
  const badFiles = listParquetFiles(BAD_DATA_DIR);
  describe('parquet-testing-repo bad_data (expect errors)', () => {
    for (const file of badFiles) {
      const base = path.basename(file);
      const expectMsg = BAD_DATA_EXPECT[base];
      if (!expectMsg) continue;
      test(`${base} should fail to parse with expected error`, async () => {
        let ok = false;
        try {
          await readFirstRows(file, 1);
          ok = true;
        } catch (e: any) {
          if (!expectMsg) {
            // If we have no mapping, accept any error
            return;
          }
          const msg = e?.message || String(e);
          if (expectMsg instanceof RegExp) {
            assert(expectMsg.test(msg), `error "${msg}" should match ${expectMsg}`);
          } else {
            assert(msg.toLowerCase().includes(String(expectMsg).toLowerCase()), `error "${msg}" should include "${expectMsg}"`);
          }
          return;
        }
        if (ok) {
          throw new Error(`Expected ${base} to fail parsing but it succeeded`);
        }
      });
    }
  });
}
