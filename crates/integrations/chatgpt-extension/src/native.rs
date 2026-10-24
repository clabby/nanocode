use serde_json::{Value, json};
use std::io::{self, Read, Write};

pub const MAX_NATIVE: usize = 8 * 1024 * 1024;
pub const MAX_RESPONSE: usize = 1024 * 1024;

pub fn read_body(reader: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut length = [0; 4];
    reader.read_exact(&mut length)?;
    let size = u32::from_ne_bytes(length) as usize;
    if size > MAX_NATIVE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "native message exceeds 8 MiB",
        ));
    }
    // Read what arrives rather than allocating the claimed size up front.
    let mut body = Vec::new();
    if reader.take(size as u64).read_to_end(&mut body)? != size {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    Ok(body)
}

pub fn write_body(writer: &mut impl Write, body: &[u8]) -> io::Result<()> {
    writer.write_all(&(body.len() as u32).to_ne_bytes())?;
    writer.write_all(body)?;
    writer.flush()
}

pub(crate) fn frame(value: &Value) -> io::Result<Vec<u8>> {
    let body = serde_json::to_vec(value)?;
    if body.len() > MAX_RESPONSE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "native response exceeds 1 MiB",
        ));
    }
    Ok(body)
}

pub fn standalone_reply(message: &Value) -> Option<Value> {
    let id = message.get("id").filter(|id| !id.is_null())?;
    message["method"].as_str()?;
    Some(if message["method"] == "ping" {
        json!({"jsonrpc":"2.0","id":id,"result":{}})
    } else {
        json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"Method not found"}})
    })
}
