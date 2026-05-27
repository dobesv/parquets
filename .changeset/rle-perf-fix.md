---
"@dobesv/parquets": patch
---

Improve RLE decoder performance by pushing decoded values independently instead of accumulating them, reducing memory allocations on large datasets.
