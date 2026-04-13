# ipc-inspect

A Rust CLI tool that inspects Arrow IPC files and dumps per-column buffer sizes, null bitmap sizes, null counts, data types, batch counts, and schema metadata. Purpose: debugging why two functionally equivalent IPC files have different sizes.

## Usage

```bash
cargo run -- old.arrow new.arrow
```

## What it reports

For each file:
- File size in bytes
- Full Arrow schema (including metadata)
- Per-batch row counts
- Per-column details: data type, null count, data buffer sizes, null bitmap bytes

## Key findings from benchmarking

A 9-test benchmark was run to identify which factors actually cause size differences in arrow-rs:

| Factor | Impact |
|--------|--------|
| **Batch count** | Dominant factor (~700 bytes overhead per batch) |
| **Lost compression (ZSTD→none)** | 2-3x bloat |
| **Lost dictionary encoding** | +71% for low-cardinality strings |
| **Wider types (Int32→Int64, Utf8→LargeUtf8)** | +29-88% |
| **Schema metadata** | Modest, scales with key count |
| **Null bitmaps** | Zero impact — arrow-rs strips them automatically |

## Requirements

- Rust 2021 edition
- arrow 55 with `ipc` and `ipc_compression` features

## Building

```bash
cargo build --release
```

The binary will be at `target/release/ipc-inspect`.
