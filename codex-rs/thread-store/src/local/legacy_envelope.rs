use std::fs::File;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;

use serde::de::DeserializeSeed;
use serde::de::Deserializer;
use serde::de::Error;
use serde::de::IgnoredAny;
use serde::de::MapAccess;
use serde::de::Visitor;

const REVERSE_SCAN_CHUNK_SIZE: u64 = 64 * 1024;

/// Returns the byte offset after the last complete, valid legacy JSONL envelope.
pub(super) fn last_complete_rollout_envelope_offset(path: &Path) -> io::Result<u64> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let Some(end) = last_newline_end(&mut file, file_len)? else {
        return Ok(0);
    };
    let start = previous_newline_start(&mut file, end - 1)?;
    if !is_rollout_envelope(&mut file, start, end - 1)? {
        return Ok(0);
    }
    Ok(end)
}

/// Validates that `end_byte_offset` ends a complete legacy JSONL envelope.
pub(super) fn validate_rollout_envelope_cutoff(
    path: &Path,
    end_byte_offset: u64,
) -> io::Result<bool> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    if end_byte_offset == 0 || end_byte_offset > file_len {
        return Ok(false);
    }
    file.seek(SeekFrom::Start(end_byte_offset - 1))?;
    let mut newline = [0_u8; 1];
    file.read_exact(&mut newline)?;
    if newline[0] != b'\n' {
        return Ok(false);
    }
    let start = previous_newline_start(&mut file, end_byte_offset - 1)?;
    is_rollout_envelope(&mut file, start, end_byte_offset - 1)
}

fn last_newline_end(file: &mut File, end: u64) -> io::Result<Option<u64>> {
    if end == 0 {
        return Ok(None);
    }
    let mut cursor = end;
    let mut buffer = [0_u8; REVERSE_SCAN_CHUNK_SIZE as usize];
    while cursor > 0 {
        let chunk_start = cursor.saturating_sub(REVERSE_SCAN_CHUNK_SIZE);
        let chunk_len = usize::try_from(cursor - chunk_start)
            .expect("reverse scan chunk is bounded by REVERSE_SCAN_CHUNK_SIZE");
        file.seek(SeekFrom::Start(chunk_start))?;
        file.read_exact(&mut buffer[..chunk_len])?;
        if let Some(index) = buffer[..chunk_len].iter().rposition(|byte| *byte == b'\n') {
            return Ok(Some(
                chunk_start + u64::try_from(index + 1).expect("buffer index fits in u64"),
            ));
        }
        cursor = chunk_start;
    }
    Ok(None)
}

fn previous_newline_start(file: &mut File, end: u64) -> io::Result<u64> {
    Ok(last_newline_end(file, end)?.unwrap_or(0))
}

fn is_rollout_envelope(file: &mut File, start: u64, end: u64) -> io::Result<bool> {
    if start >= end {
        return Ok(false);
    }
    file.seek(SeekFrom::Start(start))?;
    let mut reader = RangeReader {
        file: file.try_clone()?,
        remaining: end - start,
    };
    let mut deserializer = serde_json::Deserializer::from_reader(&mut reader);
    EnvelopeSeed
        .deserialize(&mut deserializer)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    deserializer
        .end()
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    Ok(true)
}

struct RangeReader {
    file: File,
    remaining: u64,
}

impl Read for RangeReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let length = buffer
            .len()
            .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        if length == 0 {
            return Ok(0);
        }
        let read = self.file.read(&mut buffer[..length])?;
        self.remaining -= u64::try_from(read).expect("read length fits in u64");
        Ok(read)
    }
}

struct EnvelopeSeed;

impl<'de> DeserializeSeed<'de> for EnvelopeSeed {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(EnvelopeVisitor)
    }
}

struct EnvelopeVisitor;

impl<'de> Visitor<'de> for EnvelopeVisitor {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a rollout envelope object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut timestamp = false;
        let mut item_type = false;
        let mut payload = false;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "timestamp" => {
                    map.next_value::<String>()?;
                    timestamp = true;
                }
                "type" => {
                    map.next_value::<String>()?;
                    item_type = true;
                }
                "payload" => {
                    map.next_value_seed(PayloadSeed)?;
                    payload = true;
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        if !timestamp {
            return Err(A::Error::missing_field("timestamp"));
        }
        if !item_type {
            return Err(A::Error::missing_field("type"));
        }
        if !payload {
            return Err(A::Error::missing_field("payload"));
        }
        Ok(())
    }
}

struct PayloadSeed;

impl<'de> DeserializeSeed<'de> for PayloadSeed {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(PayloadVisitor)
    }
}

struct PayloadVisitor;

impl<'de> Visitor<'de> for PayloadVisitor {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a rollout payload object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
        Ok(())
    }
}
