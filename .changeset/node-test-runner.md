---
"@dobesv/parquets": patch
---

Replace Jest with the Node.js built-in test runner (`node:test`), removing ~300 transitive dependencies and eliminating the Babel/ts-jest compilation layer from the test pipeline.
