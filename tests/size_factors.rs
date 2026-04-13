//! 9-test benchmark proving which factors cause size differences in Arrow IPC files.

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use std::io::Cursor;
use std::sync::Arc;

/// Write batches to an IPC file in memory and return the byte size.
fn ipc_size(batches: &[RecordBatch]) -> usize {
    let mut buf = Cursor::new(Vec::new());
    let mut writer = FileWriter::try_new(&mut buf, &batches[0].schema()).unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    buf.into_inner().len()
}

/// Write batches with ZSTD compression and return the byte size.
fn ipc_size_compressed(batches: &[RecordBatch]) -> usize {
    use arrow::ipc::writer::IpcWriteOptions;
    use arrow::ipc::CompressionType;

    let options = IpcWriteOptions::try_new(8, false, arrow::ipc::MetadataVersion::V5)
        .unwrap()
        .try_with_compression(Some(CompressionType::ZSTD))
        .unwrap();
    let mut buf = Cursor::new(Vec::new());
    let mut writer =
        FileWriter::try_new_with_options(&mut buf, &batches[0].schema(), options).unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    buf.into_inner().len()
}

fn make_int32_batch(n: usize) -> RecordBatch {
    let values: Vec<i32> = (0..n as i32).collect();
    let schema = Schema::new(vec![Field::new("x", DataType::Int32, false)]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(Int32Array::from(values))],
    )
    .unwrap()
}

fn make_int64_batch(n: usize) -> RecordBatch {
    let values: Vec<i64> = (0..n as i64).collect();
    let schema = Schema::new(vec![Field::new("x", DataType::Int64, false)]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(Int64Array::from(values))],
    )
    .unwrap()
}

// ---------- Test 1: Batch count overhead ----------

#[test]
fn test_batch_count_overhead() {
    let rows = 10_000;
    let one_batch = vec![make_int32_batch(rows)];

    let mut ten_batches = Vec::new();
    for _ in 0..10 {
        ten_batches.push(make_int32_batch(rows / 10));
    }

    let size_1 = ipc_size(&one_batch);
    let size_10 = ipc_size(&ten_batches);

    // More batches = larger file due to per-batch overhead (~700 bytes each).
    assert!(
        size_10 > size_1,
        "10 batches ({} bytes) should be larger than 1 batch ({} bytes)",
        size_10,
        size_1
    );
    let overhead = size_10 - size_1;
    assert!(
        overhead > 1000,
        "batch overhead ({} bytes) should be significant",
        overhead
    );
}

// ---------- Test 2: Compression impact (ZSTD vs none) ----------

#[test]
fn test_compression_impact() {
    // Repetitive data compresses well.
    let values: Vec<i32> = (0..10_000).map(|i| i % 10).collect();
    let schema = Schema::new(vec![Field::new("x", DataType::Int32, false)]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(Int32Array::from(values))],
    )
    .unwrap();
    let batches = vec![batch];

    let uncompressed = ipc_size(&batches);
    let compressed = ipc_size_compressed(&batches);

    assert!(
        uncompressed > compressed * 2,
        "uncompressed ({}) should be >2x compressed ({})",
        uncompressed,
        compressed
    );
}

// ---------- Test 3: Dictionary encoding impact ----------

#[test]
fn test_dictionary_encoding_impact() {
    let n = 10_000;
    let categories = ["alpha", "beta", "gamma", "delta"];
    let values: Vec<&str> = (0..n).map(|i| categories[i % categories.len()]).collect();

    // Plain string array
    let plain_schema = Schema::new(vec![Field::new("s", DataType::Utf8, false)]);
    let plain_batch = RecordBatch::try_new(
        Arc::new(plain_schema),
        vec![Arc::new(StringArray::from(values.clone()))],
    )
    .unwrap();

    // Dictionary-encoded string array
    let dict_array: DictionaryArray<Int32Type> = values.iter().copied().collect();
    let dict_schema = Schema::new(vec![Field::new(
        "s",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        false,
    )]);
    let dict_batch = RecordBatch::try_new(
        Arc::new(dict_schema),
        vec![Arc::new(dict_array)],
    )
    .unwrap();

    let plain_size = ipc_size(&[plain_batch]);
    let dict_size = ipc_size(&[dict_batch]);

    // Dictionary encoding should be significantly smaller for low-cardinality strings.
    assert!(
        plain_size > dict_size,
        "plain ({}) should be larger than dictionary-encoded ({})",
        plain_size,
        dict_size
    );
    let savings_pct = ((plain_size - dict_size) as f64 / plain_size as f64) * 100.0;
    assert!(
        savings_pct > 30.0,
        "dictionary encoding should save >30%, saved {:.1}%",
        savings_pct
    );
}

// ---------- Test 4: Int32 vs Int64 ----------

#[test]
fn test_wider_int_type() {
    let n = 10_000;
    let size_32 = ipc_size(&[make_int32_batch(n)]);
    let size_64 = ipc_size(&[make_int64_batch(n)]);

    // Int64 should be roughly 2x the data buffer size.
    let ratio = size_64 as f64 / size_32 as f64;
    assert!(
        ratio > 1.25,
        "Int64 ({}) should be >1.25x Int32 ({}), ratio={:.2}",
        size_64,
        size_32,
        ratio
    );
}

// ---------- Test 5: Utf8 vs LargeUtf8 ----------

#[test]
fn test_utf8_vs_large_utf8() {
    let n = 10_000;
    let values: Vec<&str> = (0..n).map(|_| "hello").collect();

    let utf8_schema = Schema::new(vec![Field::new("s", DataType::Utf8, false)]);
    let utf8_batch = RecordBatch::try_new(
        Arc::new(utf8_schema),
        vec![Arc::new(StringArray::from(values.clone()))],
    )
    .unwrap();

    let large_schema = Schema::new(vec![Field::new("s", DataType::LargeUtf8, false)]);
    let large_batch = RecordBatch::try_new(
        Arc::new(large_schema),
        vec![Arc::new(LargeStringArray::from(values))],
    )
    .unwrap();

    let utf8_size = ipc_size(&[utf8_batch]);
    let large_size = ipc_size(&[large_batch]);

    // LargeUtf8 has 8-byte offsets vs 4-byte, so offsets buffer doubles.
    assert!(
        large_size > utf8_size,
        "LargeUtf8 ({}) should be larger than Utf8 ({})",
        large_size,
        utf8_size
    );
}

// ---------- Test 6: Schema metadata impact ----------

#[test]
fn test_schema_metadata() {
    use std::collections::HashMap;

    let batch_no_meta = make_int32_batch(1000);

    let mut metadata = HashMap::new();
    for i in 0..50 {
        metadata.insert(format!("key_{}", i), format!("value_{}", i));
    }
    let schema_with_meta = Schema::new_with_metadata(
        vec![Field::new("x", DataType::Int32, false)],
        metadata,
    );
    let values: Vec<i32> = (0..1000).collect();
    let batch_with_meta = RecordBatch::try_new(
        Arc::new(schema_with_meta),
        vec![Arc::new(Int32Array::from(values))],
    )
    .unwrap();

    let size_no_meta = ipc_size(&[batch_no_meta]);
    let size_with_meta = ipc_size(&[batch_with_meta]);

    assert!(
        size_with_meta > size_no_meta,
        "metadata schema ({}) should be larger than plain ({})",
        size_with_meta,
        size_no_meta
    );
}

// ---------- Test 7: Null bitmaps are stripped automatically ----------

#[test]
fn test_null_bitmap_zero_impact() {
    let n = 10_000;

    // Non-nullable column
    let schema_nn = Schema::new(vec![Field::new("x", DataType::Int32, false)]);
    let values: Vec<i32> = (0..n as i32).collect();
    let batch_nn = RecordBatch::try_new(
        Arc::new(schema_nn),
        vec![Arc::new(Int32Array::from(values.clone()))],
    )
    .unwrap();

    // Nullable column but with zero nulls — arrow-rs should strip the bitmap.
    let schema_n = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
    let batch_n = RecordBatch::try_new(
        Arc::new(schema_n),
        vec![Arc::new(Int32Array::from(values))],
    )
    .unwrap();

    let size_nn = ipc_size(&[batch_nn]);
    let size_n = ipc_size(&[batch_n]);

    // Sizes should be equal or very close (bitmap stripped when no nulls present).
    let diff = (size_n as i64 - size_nn as i64).unsigned_abs();
    assert!(
        diff < 100,
        "null bitmap impact should be negligible, but diff={} (nullable={}, non-nullable={})",
        diff,
        size_n,
        size_nn
    );
}

// ---------- Test 8: Actual nulls present ----------

#[test]
fn test_actual_nulls_bitmap_present() {
    let n = 10_000;

    let no_nulls: Vec<Option<i32>> = (0..n as i32).map(Some).collect();
    let with_nulls: Vec<Option<i32>> = (0..n as i32)
        .map(|i| if i % 3 == 0 { None } else { Some(i) })
        .collect();

    let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);

    let batch_no_nulls = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int32Array::from(no_nulls))],
    )
    .unwrap();

    let batch_with_nulls = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(Int32Array::from(with_nulls))],
    )
    .unwrap();

    let size_no = ipc_size(&[batch_no_nulls]);
    let size_with = ipc_size(&[batch_with_nulls]);

    // With actual nulls, a bitmap is written — but it's small (n/8 bytes).
    // The difference should be modest.
    let diff = (size_with as i64 - size_no as i64).unsigned_abs();
    assert!(
        diff < 5000,
        "null bitmap should have modest impact, diff={} bytes",
        diff
    );
}

// ---------- Test 9: Multiple columns compound the effect ----------

#[test]
fn test_multiple_columns_compound() {
    let n = 5_000;

    // 1-column batch
    let schema_1 = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
    let vals: Vec<i64> = (0..n as i64).collect();
    let batch_1 = RecordBatch::try_new(
        Arc::new(schema_1),
        vec![Arc::new(Int64Array::from(vals.clone()))],
    )
    .unwrap();

    // 4-column batch (same data repeated)
    let schema_4 = Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
        Field::new("c", DataType::Int64, false),
        Field::new("d", DataType::Int64, false),
    ]);
    let batch_4 = RecordBatch::try_new(
        Arc::new(schema_4),
        vec![
            Arc::new(Int64Array::from(vals.clone())),
            Arc::new(Int64Array::from(vals.clone())),
            Arc::new(Int64Array::from(vals.clone())),
            Arc::new(Int64Array::from(vals)),
        ],
    )
    .unwrap();

    let size_1 = ipc_size(&[batch_1]);
    let size_4 = ipc_size(&[batch_4]);

    let ratio = size_4 as f64 / size_1 as f64;
    assert!(
        ratio > 3.0,
        "4 columns ({}) should be >3x 1 column ({}), ratio={:.2}",
        size_4,
        size_1,
        ratio
    );
}
