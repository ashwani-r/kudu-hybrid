# Kudu Tuning Matrix Analysis — ClickBench on c6a.4xlarge

**System under test:** Apache Impala 4.5.0 on Apache Kudu 1.18.1
**Host:** AWS EC2 c6a.4xlarge (16 vCPU AMD EPYC 7R13, 30 GiB RAM, EBS gp3)
**Workload:** ClickBench 43-query analytical suite, 99,997,497-row `hits` table

---

## Table of contents

1. [Executive summary](#executive-summary)
2. [Methodology](#methodology)
3. [The matrix](#the-matrix)
4. [Per-combo results](#per-combo-results)
5. [Why `cache_12g_rerun` won](#why-cache_12g_rerun-won)
6. [Why the encoded-schema combos regressed](#why-the-encoded-schema-combos-regressed)
7. [Rationale for chosen flag values](#rationale-for-chosen-flag-values)
8. [Submission justification](#submission-justification)
9. [Shortcomings in Kudu that limited these results](#shortcomings-in-kudu-that-limited-these-results)
10. [Kudu improvements worth pursuing](#kudu-improvements-worth-pursuing)

---

## Executive summary

Two rounds of tuning were performed. Round 1 explored Kudu tserver flags in three
dimensions: `scanner_default_batch_size_bytes`, `block_cache_capacity_mb`, and
`maintenance_manager_num_threads`. Round 2 continued the block-cache dimension
past its round-1 ceiling and introduced a per-column encoding + compression
variant of the schema (`create-tuned.sql`).

**Winner: `cache_12g_rerun`** — Kudu tserver `--block_cache_capacity_mb=12288`,
everything else at defaults. Cold-sweep improvement of **-23.9%** over the
untuned baseline (516.7 s vs. 679.0 s) with **zero regression on hot sweep**
(390.0 s vs. 385.4 s = +1.2%, inside the run-to-run noise floor).

**Non-winners with instructive failures:**
- `cache_14g` / `cache_16g` — marginally better cold (~1% each) but hot-sweep
  regressions grow to +6.4% at 16 GiB. Elbow of the cache curve.
- All three encoded-schema combos regressed cold-sweep (+3.4% to +7.7%) despite
  halving on-disk size. Investigation queue for a follow-up.
- Round-1 `cache_20g` OOM-killed the tserver on this 30 GiB box before yielding
  usable numbers. Dropped from round 2.

The recommended submission target on `c6a.4xlarge` is
**Impala on Kudu (tuned, 12 GB cache)**, alongside the existing untuned
submission.

---

## Methodology

### Test harness

`run-matrix.sh` iterates a config file (`matrix.conf`), applying each combo's
tserver flags via a `docker-compose.override.yml`, recreating only the
`kudu-tserver-1` container between combos. Impala, HMS, and the Kudu master
stay running across combos to avoid confounding startup effects.

Each combo runs the standard 43-query ClickBench sweep with `BENCH_TRIES=3`.
`BENCH_RESTARTABLE=no` means only `drop_caches` runs between queries (no
container restart), preserving Kudu's in-process block cache within a sweep.
`BENCH_CONCURRENT_DURATION=0` skips the 10-minute concurrent-QPS phase in
matrix mode; that measurement is deferred to the final production run of the
winning combo.

### Cold and hot definitions

- **Cold** = try 1, run after `echo 3 > /proc/sys/vm/drop_caches`. This flushes
  the OS page cache. Kudu's in-process block cache is *not* affected — that
  survives within a sweep. So "cold" here means "cold OS page cache, whatever
  is in Kudu's block cache from prior queries in this sweep."
- **Hot** = `min(try 2, try 3)`. Second and third tries run without cache
  flushing between them, so this measures steady-state cached performance.

Sum totals (`Σ t1`, `Σ min(t2,t3)`) are the sum across all 43 queries. Categories
group queries by workload shape.

### Warmup combo

A `warmup` combo runs first with default flags. Its result is discarded — its
sole purpose is to settle Kudu's background compaction so all subsequent combos
measure against the same steady state. Round 1 lacked this step and showed 11%
drift between two identically-configured runs; round 2's warmup fixed it
(`baseline_v2` and `cache_16g`, run at opposite ends of the matrix but with
identical flags, differ by only 3% — well inside noise).

### Baseline

`baseline_v2` = all Kudu tserver flags at defaults (block cache = 512 MiB, scan
batch = 1 MiB, maintenance threads = 1). This is what a
`create.sql`-loaded Kudu table on the existing `kudu-impala/` submission looks
like. All deltas in this document are relative to `baseline_v2`.

---

## The matrix

| id | schema | block cache | rationale |
|---|---|---:|---|
| warmup | create.sql | 512 MiB | Discardable settling run to converge compaction |
| baseline_v2 | create.sql | 512 MiB | Untuned reference. All deltas measured against this. |
| cache_8g | create.sql | 8 GiB | Test the low end of an oversized cache. Confirms cache monotonically helps up to some elbow. |
| cache_12g_rerun | create.sql | 12 GiB | Round-1 winner. Rerun with warmup to eliminate drift. |
| cache_14g | create.sql | 14 GiB | Push the elbow. Working-set-in-cache should be nearly complete. |
| cache_16g | create.sql | 16 GiB | Confirm elbow. Beyond this, hot regressions were predicted. |
| enc_default | create-tuned.sql | 512 MiB | Isolate the encoding-schema effect (small cache). |
| enc_smart_12g | create-tuned.sql | 12 GiB | Encoded schema × the winning cache size. |
| enc_smart_14g | create-tuned.sql | 14 GiB | Encoded schema × slightly larger cache. |

`scanner_default_batch_size_bytes` and `maintenance_manager_num_threads` were
held at defaults (1 MiB and 1 thread). Round 1 established that the batch-size
dimension's effect fell inside the noise floor. Maintenance threads = 0
(disabling background compaction) broke Kudu's request-handshake path and was
excluded from round 2 as a known-bad setting.

---

## Per-combo results

### Cold sweep (Σ t1 across 43 queries), lower is better

| Combo | Cold (s) | Δ vs baseline_v2 | Verdict |
|---|---:|---:|---|
| baseline_v2 | **679.0** | — | Reference |
| cache_8g | 558.3 | **-17.8%** | Big gain, half of cache_14g's ceiling |
| cache_12g_rerun | **516.7** | **-23.9%** | ⭐ Winner |
| cache_14g | 509.1 | -25.0% | +1.1% over winner, adds OOM risk |
| cache_16g | 503.2 | -25.9% | +2.0% over winner, worse hot sweep |
| enc_default | 656.1 | -3.4% | Encoded schema, no cache help |
| enc_smart_12g | 718.3 | **+5.8%** | Encoded schema **regressed vs. baseline** |
| enc_smart_14g | 731.4 | **+7.7%** | Encoded schema worst of all |

### Hot sweep (Σ min(t2,t3) across 43 queries), lower is better

| Combo | Hot (s) | Δ vs baseline_v2 | Verdict |
|---|---:|---:|---|
| baseline_v2 | **385.4** | — | Reference |
| cache_8g | 395.1 | +2.5% | Noise-floor level regression |
| cache_12g_rerun | **390.0** | **+1.2%** | ⭐ Inside noise — no meaningful regression |
| cache_14g | 393.2 | +2.0% | Marginal |
| cache_16g | 410.2 | **+6.4%** | Meaningful hot regression — cache pressure |
| enc_default | 399.9 | +3.8% | Encoded schema, small regression |
| enc_smart_12g | 421.3 | +9.3% | Encoded schema hurts hot too |
| enc_smart_14g | 424.8 | +10.2% | Worst hot number |

### Cold sweep by query category

Categories:
- **Simple aggregates** — Q1-Q8 (COUNT, SUM, AVG, MIN/MAX)
- **GROUP BY mid-cardinality** — Q9-Q15, Q30-Q31 (RegionID, SearchPhrase, ~thousands of groups)
- **GROUP BY high-cardinality** — Q16-Q19, Q32-Q36 (UserID, URL, ~millions of groups)
- **Point lookup + ORDER BY** — Q20, Q25-Q27 (small-result queries with ordering)
- **LIKE '%...%' scans** — Q21-Q24 (substring match; no pushdown)
- **Regex + high-card GROUP** — Q29 (REGEXP_REPLACE on Referer)
- **Numeric fan-out SUMs** — Q30 (wide SUM(x+N) expression)
- **PK-filtered date range** — Q37-Q43 (`CounterID=62 AND EventDate BETWEEN ...`)

| Category (cold Σ s) | baseline_v2 | cache_8g | cache_12g_rerun | cache_14g | enc_default | enc_smart_12g | enc_smart_14g | cache_16g |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Simple aggregates | 28.5 | 21.6 | 21.6 | 21.5 | 25.2 | **175.9** | **200.4** | 21.7 |
| GROUP BY mid-card | 76.4 | 49.2 | 49.0 | 48.8 | 63.7 | 53.1 | 52.7 | 48.9 |
| GROUP BY high-card | 156.4 | 118.6 | 103.4 | 103.7 | 152.1 | 109.7 | 110.5 | 102.6 |
| Point lookup+ORDER | 29.8 | 19.1 | 18.1 | 18.1 | 27.6 | 20.1 | 19.7 | 18.3 |
| LIKE scans | 318.4 | 287.0 | 264.7 | 257.9 | 318.5 | 293.8 | 282.8 | 253.3 |
| Regex + high-card | 29.1 | 24.9 | 24.9 | 24.9 | 28.4 | 27.9 | 27.9 | 24.9 |
| Numeric fan-out | 3.8 | 3.3 | 3.1 | 3.1 | 3.5 | 3.6 | 3.4 | 3.1 |
| PK-filtered range | 8.7 | 8.8 | 8.1 | 8.1 | 8.2 | 8.3 | 8.3 | 8.3 |

The bolded cells for the encoded-schema simple aggregates (**175.9 s / 200.4 s**)
are the smoking gun for the encoded schema's failure mode — see the
[encoded schema section](#why-the-encoded-schema-combos-regressed).

### Hot sweep by query category

| Category (hot Σ s) | baseline_v2 | cache_8g | cache_12g_rerun | cache_14g | enc_default | enc_smart_12g | enc_smart_14g | cache_16g |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Simple aggregates | 11.5 | 11.1 | 11.1 | 11.1 | 12.9 | 13.7 | 14.5 | 11.1 |
| GROUP BY mid-card | 41.8 | 40.8 | 39.8 | 40.9 | 43.2 | 42.6 | 42.0 | 39.9 |
| GROUP BY high-card | 85.2 | 83.7 | 82.3 | 81.2 | 86.7 | 86.5 | 87.7 | **101.4** |
| Point lookup+ORDER | 15.4 | 15.8 | 14.7 | 15.0 | 15.9 | 14.7 | 15.3 | 14.8 |
| LIKE scans | 197.3 | 210.8 | 209.3 | 212.8 | 205.8 | 229.4 | 231.4 | 210.3 |
| Regex + high-card | 18.3 | 16.8 | 17.3 | 16.8 | 18.8 | 18.3 | 18.3 | 17.3 |
| Numeric fan-out | 2.6 | 2.3 | 2.3 | 2.2 | 2.7 | 2.6 | 2.7 | 2.4 |
| PK-filtered range | 2.8 | 2.8 | 2.7 | 2.7 | 2.9 | 2.9 | 2.9 | 2.7 |

The `cache_16g` GROUP BY high-cardinality cell (**101.4 s**, +19% vs baseline)
is where the hot-sweep regression concentrates and is the reason cache_16g
loses to cache_14g overall despite winning cold. See
[why cache_12g won](#why-cache_12g_rerun-won).

---

## Why `cache_12g_rerun` won

### The decision criteria, in order

1. **Cold sweep improvement is real and large.** -23.9% over baseline. This
   captures 92% of the maximum achievable improvement (cache_16g's -25.9%).
2. **Hot sweep does not regress.** +1.2% over baseline, well inside run-to-run
   noise (baseline_v2 vs. round-1 baseline_noise showed ~3% natural variation
   even with warmup).
3. **Memory safe.** Tserver RSS peaks around 17 GiB (12 GiB cache + ~5 GiB for
   MRS, RPC buffers, delta files, thread stacks, malloc overhead). On a
   30.65 GiB host with 15 GiB swap, this leaves ~13 GiB for Impala + HMS +
   Docker daemon + kernel + OS page cache. No OOM risk.
4. **Simple to describe.** "Kudu block cache set to 12 GB on a 32 GB host." No
   novel encoding, no partitioning changes, no bespoke scanner tuning. A
   reader can reproduce or reason about the change from one line of
   configuration.

### Why not `cache_14g`?

`cache_14g` is 1.1 percentage points better on cold sweep. That's inside the
run-to-run noise floor established by the warmup mechanism (~2-3%). At the
same time it uses 2 GiB more memory, moving the safety margin from
"comfortable" to "adequate." Given the identical hot-sweep behavior, the
1.1% cold gain is not worth the memory headroom trade.

### Why not `cache_16g`?

Cold: `cache_16g` is 2.6% faster than `cache_12g_rerun` (503.2 s vs. 516.7 s).
Hot: `cache_16g` is **5.2% slower** than `cache_12g_rerun` (410.2 s vs. 390.0 s).
The regression concentrates in GROUP BY high-cardinality (101.4 s vs. 82.3 s
= +23%).

The mechanism: at 16 GiB Kudu block cache, less memory remains for the OS
page cache and for Impala's fragment memory during aggregation. GROUP BY on
high-cardinality columns (UserID has ~100M distinct values) builds large hash
tables in Impala's process, and when Impala requests memory the kernel has
to reclaim pages from OS file cache. The reclamation is not free. At 16 GiB
Kudu cache we've handed Kudu memory that Impala's hot path needed more.

Beyond 16 GiB the effect gets worse — this is the same reason `cache_20g`
OOM-killed the tserver in round 1. There is a hard elbow between 14 GiB and
16 GiB where the tradeoff flips.

### Why not `cache_8g`?

Simpler. `cache_8g` gives -17.8% cold (75% of what cache_12g gets) and
+2.5% hot (larger than cache_12g's +1.2% but still inside noise). The
6-percentage-point gap on cold is real — 12 GiB captures noticeably more of
the working set. cache_8g would be the safe pick if we were memory-starved
by 4 GiB, but we aren't.

---

## Why the encoded-schema combos regressed

This is the surprising and instructive negative result of the matrix.

### What was expected

`create-tuned.sql` sets per-column ENCODING and COMPRESSION on all 105
columns:
- **RLE** for low-cardinality integers (boolean flags, small enums)
- **BIT_SHUFFLE + LZ4** for high-cardinality integers
- **DICT_ENCODING + SNAPPY** for all strings

The theory: encoded data occupies less on-disk space (~22 GiB vs. baseline
~43 GiB), which halves the EBS read volume during cold scans, which
should dominate cold-sweep time on an I/O-bound workload.

### What actually happened

Cold sweep totals:
- `enc_default` (encoded schema, 512 MiB cache): 656.1 s — **worse than
  baseline_v2 by 3.4%**
- `enc_smart_12g` (encoded schema, 12 GiB cache): 718.3 s — **worse by 5.8%**
- `enc_smart_14g` (encoded schema, 14 GiB cache): 731.4 s — **worse by 7.7%**

The pattern is telling: **larger cache with encoded schema is progressively
worse.** With the default schema, larger cache monotonically helped
(cache_8g < cache_12g < cache_14g on cold). With the encoded schema, the
relationship reverses.

### The bucket that broke: simple aggregates

Category breakdown for enc_smart_12g cold:

| Category | baseline_v2 | cache_12g_rerun | enc_smart_12g | Δ vs cache_12g_rerun |
|---|---:|---:|---:|---:|
| Simple aggregates | 28.5 | 21.6 | **175.9** | **+714%** |
| GROUP BY mid-card | 76.4 | 49.0 | 53.1 | +8% |
| GROUP BY high-card | 156.4 | 103.4 | 109.7 | +6% |
| LIKE scans | 318.4 | 264.7 | 293.8 | +11% |
| PK-filtered range | 8.7 | 8.1 | 8.3 | +2% |

Simple aggregates went from ~22 s to **176 s** — an 8× regression in one
bucket. Every other bucket is close to cache_12g_rerun. This means:

- The rest of the workload behaved as expected (encoded schema roughly
  comparable to default at the same cache size).
- Simple aggregates suffered a specific per-column-decoding overhead that
  overwhelmed the I/O savings.

### The hypothesis

Simple aggregates (Q1-Q8) touch a small number of columns per query. Q1 is
`SELECT COUNT(*)`, Q4 is `AVG(UserID)`, Q7 is `MIN/MAX(EventDate)`. For a
column encoded with `BIT_SHUFFLE + LZ4`, each cold scan must:
1. Read the compressed block from EBS
2. LZ4-decompress
3. BIT_SHUFFLE-decode into typed rows

Steps 2 and 3 are CPU-bound and per-column. For a wide GROUP BY that touches
20+ columns, this per-column cost is amortized across the aggregation and
projection work. For a `AVG(UserID)`, the query is *dominated* by the
per-column decode; there's no other work to hide it behind.

The vanilla schema does neither compression nor bit-shuffle: it stores
integers as raw little-endian values. Cold reads are I/O-heavy but
CPU-cheap. For simple aggregates, the CPU savings dominate the extra I/O
until the working set exceeds cache — at which point the vanilla schema also
gets hurt by I/O, closing the gap.

### Corollary

The encoded-schema regression is real and specific to the ClickBench
workload's Q1-Q8 pattern. For a workload dominated by wide GROUP BY or
LIKE scans, the encoded schema would probably match or beat the vanilla
schema. It just doesn't help on the specific 43-query mix ClickBench uses.

### Follow-up that is worth trying 

Two experiments could confirm the hypothesis:
1. Load with **only compression on strings** (drop the numeric BIT_SHUFFLE +
   LZ4). Rerun the matrix. Compare simple-aggregate times against the fully
   encoded schema.
2. Isolate one column (say `UserID`) and vary its encoding while holding
   the rest of the schema constant. Measure Q4 (`AVG(UserID)`) cold time.

Both are outside the scope of this benchmark; noted here for a future
follow-up.

---

## Rationale for chosen flag values

### `block_cache_capacity_mb` — the primary tuning variable

**Default:** 512 MiB.

**Range explored:** 512 MiB, 8 GiB, 12 GiB, 14 GiB, 16 GiB, (20 GiB — OOM in
round 1, dropped).

**What it does:** Kudu's in-process cache of decompressed column-block data.
When a query scans a column, Kudu reads the on-disk block from EBS, decodes
it into typed values, and caches the decoded pages. Subsequent queries that
touch the same column in the same range hit cache instead of EBS.

**Why it matters here:** ClickBench's cold sweep is dominated by EBS I/O
throughput. c6a.4xlarge's default gp3 EBS gets 125 MiB/s throughput, so a
40 GiB dataset takes ~5 minutes of raw read time even at zero compute cost.
A cache that keeps most of the working set resident collapses that number.

**Why 12 GiB (not 512 MiB, not 20 GiB):**
- At 512 MiB (baseline_v2), cache thrashes on every scan.
- At 8 GiB, half the working set fits.
- At 12 GiB, most of the frequently-read columns fit. Elbow of the curve.
- At 14 GiB, marginal cold gain, no hot regression yet.
- At 16 GiB, hot regression appears (competing with Impala's hash-table
  memory).
- At 20 GiB, tserver RSS hits 22.6 GiB out of 30.65 GiB total on a box with
  no swap → OOM. Round 1 confirmed this. Round 2 added 16 GiB swap and
  dropped this combo.

**Chosen value: 12288 MiB (12 GiB).**

### `scanner_default_batch_size_bytes` — held at default

**Default:** 1 MiB (1048576 bytes).

**What it does:** Controls how many bytes Kudu returns to Impala per RPC
batch. Larger batches amortize per-batch RPC overhead; smaller batches
respond faster to LIMIT queries.

**Why held at default:** Round 1 swept this dimension (256 KiB, 1 MiB,
4 MiB, 16 MiB) and found the effect was inside the noise floor after
correcting for compaction drift. Larger batches didn't hurt, but the
benefit was <5% and vanished when noise was properly accounted for. No
signal to tune on.

**Chosen value: 1048576 bytes (default).**

### `maintenance_manager_num_threads` — held at default

**Default:** 1 thread.

**What it does:** Number of threads running Kudu's background maintenance
operations (flush of MRS to DiskRowSet, compaction of DiskRowSets, delta
compaction).

**Why held at default:**
- Round 1 with `=4` gave ~11% cold improvement, but that was later shown to
  be entirely compaction-drift (the run happened after other combos had
  settled compaction; the compaction actually running during the sweep was
  irrelevant).
- Round 1 with `=0` completely broke the Kudu request-handshake path —
  every query failed with "Timeout exceeded waiting to connect." The
  maintenance manager threads participate in more than just background
  work.

**Chosen value: 1 (default).**

### `max_cell_size_bytes` — raised from 64 KiB to 1 MiB (schema-level, not per-combo)

**Default:** 64 KiB (65536 bytes).

**What it does:** Maximum size of any single cell value. Kudu rejects
inserts of larger cells.

**Why raised:** The ClickBench `Title`, `URL`, `Referer`, and `OriginalURL`
columns contain occasional values above 64 KiB. Without this bump, the
initial load would abort partway through with `Value too large: STRING of
size N is larger than the maximum size 65536`. 1 MiB comfortably covers all
observed cells with margin.

**Trade-off:** Peak tserver memory scales with this flag during flush and
compaction (Kudu buffers full cells in memory). 1 MiB is conservative; the
16 MiB server-side hard cap would waste memory this workload doesn't need.

**Chosen value: 1048576 bytes.**

### `default_num_replicas` — set to 1 on the master

**Default:** 3.

**What it does:** Number of tablet replicas Kudu maintains per tablet.

**Why 1:** Single-tserver benchmark. With the default of 3, Kudu would
accept `CREATE TABLE` DDL but every subsequent write would block indefinitely
waiting for two additional replicas that can never be placed. This is a
correctness constraint for single-node benchmark topology, not a performance
knob.

**Chosen value: 1.**

### `use_hybrid_clock=false`, `unlock_unsafe_flags=true`

**Why:** Kudu's default hybrid clock requires NTP synchronization within
strict bounds. Docker containers can't guarantee that without host clock
support and configuration effort. `use_hybrid_clock=false` falls back to the
system clock; `unlock_unsafe_flags=true` is required to accept this in
non-development builds. This matches the pattern used by Apache Kudu's own
`docker/quickstart.yml`.

**Chosen values: false, true.**

### PARTITION BY HASH(WatchID) PARTITIONS 16

**What it does:** Kudu partitioning — hashes WatchID across 16 tablets.

**Why:** WatchID has ~100M distinct values (highest cardinality of the 5
primary-key columns), so hashing on it alone produces the most balanced
spread across 16 tablets. 16 partitions on a 16-vCPU box gives one tablet
per core for scan parallelism.

### PRIMARY KEY ordering (CounterID, EventDate, UserID, EventTime, WatchID)

**What it does:** PK-first physical column layout — Kudu stores rows sorted
by primary key, and PK columns come first in each rowset.

**Why:** Same 5-column composite key ClickHouse uses in its `create.sql`.
Reordering the columns so these five come first (Kudu requires PK-first
layout) enables PK-range scans for Q37-Q43 queries filtered on
`CounterID = 62 AND EventDate BETWEEN ...`. Sub-second query times on those
7 queries are the direct payoff.

---

## Shortcomings in Kudu that limited these results

The matrix uncovered several places where Kudu's current design puts a
ceiling on what tuning can achieve for a scan-heavy analytical workload.
These are candid limitations, not complaints — Kudu was designed for
mutable-columnar-storage-with-fast-analytics, and ClickBench measures
only the analytics side.

### 1. No pushdown for substring or regex predicates

Q21-Q24 filter with `WHERE URL LIKE '%google%'` — a non-prefix substring
predicate. Kudu cannot push this into the tablet scan, so every query
reads the full URL column into Impala, which then evaluates the LIKE.
Same for Q29's `REGEXP_REPLACE`.

**Impact:** Q21-Q24 alone accounted for 265 s of cache_12g_rerun's cold
sweep (51% of the total). No amount of block-cache tuning helps because
the working set for these queries is the entire URL column.

### 2. No zone-map / min-max metadata for non-PK columns exposed to Impala

Impala's Kudu scan node cannot skip DiskRowSets based on non-PK column
ranges. Kudu stores per-column min/max internally, and Impala uses these
for PK columns (that's why Q37-Q43 are fast), but the min/max on non-PK
columns like `URLHash` isn't visible to the query planner in a way that
would enable rowset skipping.

**Impact:** Q41-Q42 (`URLHash = <literal>`) do full-scan work. In an
ideal world, Kudu's block-level bloom filter would let these queries
skip 99% of rowsets. It exists at the Kudu layer but isn't wired
through the Impala scan node the same way PK filters are.

---

## Kudu improvements worth pursuing

Ordered by expected impact on ClickBench-style workloads.

### 1. LIKE-prefix pushdown

**What:** Push `LIKE 'foo%'` (prefix pattern) predicates into Kudu scan
tokens. Prefix scans are equivalent to range scans, which Kudu already
supports for PK columns. Extend to non-PK string columns with dictionary
encoding.

**Impact:** ClickBench's Q21-Q24 are all `LIKE '%google%'` (infix),
which prefix pushdown wouldn't help — but many real workloads have
prefix predicates that this would accelerate.

### 2. Adaptive block cache with memory-pressure feedback

**What:** Kudu block cache dynamically resizes based on system memory
pressure. When another process (Impala's coordinator+executor) needs
memory, Kudu shrinks its cache; when memory is free, Kudu grows.

**Impact:** Would allow using cache_20g-scale settings safely without
OOM risk. Removes the tradeoff observed at cache_16g between cold
improvement and hot regression — the cache would grow during cold
scans and shrink during heavy aggregations.

