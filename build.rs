#![allow(missing_docs)]
use std::io::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(&["src/remote_write.proto"], &["src/"])?;
    Ok(())
}
