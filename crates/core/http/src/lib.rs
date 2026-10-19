#![cfg(not(target_family = "wasm"))]

use std::{
    error::Error,
    io::{self, Write},
};

use futures_util::{Stream, StreamExt, stream};
use nanocode_zstd::{BlockHeader, BlockType, FrameHeader, StreamingDecoder, parse_frame_header};

mod brotli;

pub fn zstd_stream<S, B, E>(source: S) -> impl Stream<Item = io::Result<Vec<u8>>>
where
    S: Stream<Item = Result<B, E>>,
    B: AsRef<[u8]>,
    E: Error + Send + Sync + 'static,
{
    let state = (
        Box::pin(source),
        StreamingDecoder::default(),
        Framing::default(),
        None::<(B, usize)>,
    );
    stream::try_unfold(
        state,
        |(mut source, mut decoder, mut framing, mut pending)| async move {
            loop {
                if decoder.pending_output_len() != 0 {
                    let output = decoder.take_output();
                    return Ok(Some((output, (source, decoder, framing, pending))));
                }
                if let Some((chunk, offset)) = pending.as_mut() {
                    let bytes = chunk.as_ref();
                    if *offset == bytes.len() {
                        pending = None;
                        continue;
                    }
                    *offset += framing.feed(&bytes[*offset..], &mut decoder)?;
                    continue;
                }
                match source.next().await {
                    Some(Ok(chunk)) => pending = Some((chunk, 0)),
                    Some(Err(error)) => return Err(io::Error::other(error)),
                    None => {
                        if !framing.header.is_empty() {
                            decoder.push(&framing.header).map_err(decode_error)?;
                        }
                        decoder.finish().map_err(decode_error)?;
                        return Ok(None);
                    }
                }
            }
        },
    )
}

#[derive(Default)]
struct Framing {
    state: Section,
    header: Vec<u8>,
    checksum: bool,
}

#[derive(Default)]
enum Section {
    #[default]
    Frame,
    Block,
    Payload {
        remaining: usize,
        last: bool,
    },
    Checksum {
        remaining: usize,
    },
    Skip {
        remaining: usize,
    },
}

impl Framing {
    const fn after_block(&self, last: bool) -> Section {
        if !last {
            Section::Block
        } else if self.checksum {
            Section::Checksum {
                remaining: size_of::<u32>(),
            }
        } else {
            Section::Frame
        }
    }

    fn feed(&mut self, bytes: &[u8], decoder: &mut StreamingDecoder) -> io::Result<usize> {
        match &mut self.state {
            Section::Frame => {
                self.header.push(bytes[0]);
                match parse_frame_header(&self.header) {
                    Ok(header) => {
                        decoder.push(&self.header).map_err(decode_error)?;
                        self.header.clear();
                        self.state = match header {
                            FrameHeader::Zstandard(header) => {
                                self.checksum = header.checksum;
                                Section::Block
                            }
                            FrameHeader::Skippable(header) if header.size == 0 => Section::Frame,
                            FrameHeader::Skippable(header) => Section::Skip {
                                remaining: header.size as usize,
                            },
                        };
                    }
                    Err(nanocode_zstd::Error::UnexpectedEof) => {}
                    Err(error) => return Err(decode_error(error)),
                }
                Ok(1)
            }
            Section::Block => {
                let count = (BlockHeader::SIZE - self.header.len()).min(bytes.len());
                self.header.extend_from_slice(&bytes[..count]);
                if self.header.len() == BlockHeader::SIZE {
                    let header = BlockHeader::parse(&self.header).map_err(decode_error)?;
                    decoder.push(&self.header).map_err(decode_error)?;
                    self.header.clear();
                    let remaining = match header.block_type {
                        BlockType::Rle => 1,
                        BlockType::Raw | BlockType::Compressed => header.block_size as usize,
                    };
                    self.state = if remaining == 0 {
                        self.after_block(header.last_block)
                    } else {
                        Section::Payload {
                            remaining,
                            last: header.last_block,
                        }
                    };
                }
                Ok(count)
            }
            Section::Payload { remaining, last } => {
                let count = (*remaining).min(bytes.len());
                decoder.push(&bytes[..count]).map_err(decode_error)?;
                *remaining -= count;
                if *remaining == 0 {
                    let last = *last;
                    self.state = self.after_block(last);
                }
                Ok(count)
            }
            Section::Checksum { remaining } | Section::Skip { remaining } => {
                let count = (*remaining).min(bytes.len());
                decoder.push(&bytes[..count]).map_err(decode_error)?;
                *remaining -= count;
                if *remaining == 0 {
                    self.state = Section::Frame;
                }
                Ok(count)
            }
        }
    }
}

fn decode_error(error: nanocode_zstd::Error) -> io::Error {
    let kind = if matches!(error, nanocode_zstd::Error::UnexpectedEof) {
        io::ErrorKind::UnexpectedEof
    } else {
        io::ErrorKind::InvalidData
    };
    io::Error::new(kind, error)
}

pub fn gzip(bytes: &[u8], level: u32) -> io::Result<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::new(level));
    encoder.write_all(bytes)?;
    encoder.finish()
}

pub fn decode_response(mut response: reqwest::Response) -> reqwest::Response {
    use http::header::{CONTENT_ENCODING, CONTENT_LENGTH};
    use reqwest::ResponseBuilderExt;

    let mut encodings = response.headers().get_all(CONTENT_ENCODING).iter();
    let Some(encoding) = encodings.next() else {
        return response;
    };
    if encodings.next().is_some() {
        return response;
    }
    let encoding = match encoding.to_str().map(str::trim) {
        Ok(value) if value.eq_ignore_ascii_case("zstd") => "zstd",
        Ok(value) if value.eq_ignore_ascii_case("br") => "br",
        _ => return response,
    };
    if response
        .headers()
        .get(CONTENT_LENGTH)
        .is_some_and(|value| value == "0")
    {
        return response;
    }
    let url = response.url().clone();
    let status = response.status();
    let version = response.version();
    let mut headers = response.headers().clone();
    headers.remove(CONTENT_ENCODING);
    headers.remove(CONTENT_LENGTH);
    let extensions = std::mem::take(response.extensions_mut());
    let body = match encoding {
        "zstd" => reqwest::Body::wrap_stream(zstd_stream(response.bytes_stream())),
        "br" => reqwest::Body::wrap_stream(brotli::stream(response.bytes_stream())),
        _ => unreachable!("encoding is selected above"),
    };
    let mut decoded = http::Response::builder()
        .url(url)
        .body(body)
        .expect("response builder only sets a URL extension");
    *decoded.status_mut() = status;
    *decoded.version_mut() = version;
    *decoded.headers_mut() = headers;
    decoded.extensions_mut().extend(extensions);
    decoded.into()
}
