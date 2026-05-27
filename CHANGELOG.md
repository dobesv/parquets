# @dobesv/parquets

## 0.13.0

### Minor Changes

- eaab057: Restore `ParquetBufferWriter`, `generateParquetBuffer`, and related encoding refactor that were accidentally deleted in a squash commit. These were originally introduced in PRs #10 and #12.

## 0.12.0

### Minor Changes

- 5f14c10: Add `ParquetBufferWriter` for synchronous in-memory Parquet writing and `generateParquetBuffer` utility for producing a complete Parquet file as a `Buffer` in a single call.
- 5f14c10: Add support for writing and reading Parquet column statistics (min/max values, null count) in both DataPageHeaderV1 and DataPageHeaderV2 formats.

### Patch Changes

- 5f14c10: Replace Jest with the Node.js built-in test runner (`node:test`), removing ~300 transitive dependencies and eliminating the Babel/ts-jest compilation layer from the test pipeline.
- 5f14c10: Improve RLE decoder performance by pushing decoded values independently instead of accumulating them, reducing memory allocations on large datasets.
