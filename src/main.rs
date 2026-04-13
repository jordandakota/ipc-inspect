use arrow::ipc::reader::FileReader;
use std::fs::{self, File};

fn inspect(path: &str) {
    let size = match fs::metadata(path) {
        Ok(m) => m.len(),
        Err(e) => {
            eprintln!("Error: cannot read '{}': {}", path, e);
            return;
        }
    };
    println!("=== {} ({} bytes) ===", path, size);

    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Error: cannot open '{}': {}", path, e);
            return;
        }
    };

    let reader = match FileReader::try_new(file, None) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: not a valid Arrow IPC file '{}': {}", path, e);
            return;
        }
    };

    println!("Schema: {:#?}", reader.schema());

    for (batch_idx, batch_result) in reader.enumerate() {
        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => {
                eprintln!("  Error reading batch {}: {}", batch_idx, e);
                continue;
            }
        };
        println!("\nBatch {}: {} rows", batch_idx, batch.num_rows());
        for (i, col) in batch.columns().iter().enumerate() {
            let data = col.to_data();
            let buf_sizes: Vec<_> = data.buffers().iter().map(|b| b.len()).collect();
            println!(
                "  col {} '{}' ({}): null_count={}, data_buffers={:?}, null_bitmap_bytes={}",
                i,
                batch.schema().field(i).name(),
                col.data_type(),
                data.null_count(),
                buf_sizes,
                data.nulls().map_or(0, |n| n.buffer().len())
            );
        }
    }
    println!();
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("Usage: ipc-inspect <file.arrow> [file2.arrow ...]");
        std::process::exit(1);
    }
    for path in &args {
        inspect(path);
    }
}
