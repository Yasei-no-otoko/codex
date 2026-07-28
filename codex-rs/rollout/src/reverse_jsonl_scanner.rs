use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;

use serde::de::DeserializeOwned;

const READ_CHUNK_SIZE: usize = 8 * 1024;

#[derive(Debug)]
pub enum ScanOutcome<T> {
    /// The record was valid JSON and deserialized as the requested type.
    Parsed(T),
    /// The record was not valid JSON for the requested type.
    #[allow(dead_code)]
    Rejected(serde_json::Error),
}

/// Read-only scanner for newline-delimited JSON records, starting from the end.
pub struct ReverseJsonlScanner<R> {
    reader: R,
    next_chunk_end: u64,
    record_end_offset: u64,
    record_terminated_by_newline: bool,
    chunk_position: usize,
    chunk: Vec<u8>,
    record_reversed: Vec<u8>,
    last_record_end_offset: Option<u64>,
    last_record_terminated_by_newline: Option<bool>,
}

impl<R> ReverseJsonlScanner<R>
where
    R: Read + Seek,
{
    pub fn new(mut reader: R) -> io::Result<Self> {
        let next_chunk_end = reader.seek(SeekFrom::End(0))?;
        Self::new_at(reader, next_chunk_end)
    }

    /// Creates a reverse scanner whose logical end is the given byte offset.
    ///
    /// This lets callers scan a frozen JSONL prefix without reading records appended after that
    /// prefix was captured.
    pub fn new_at(mut reader: R, end_byte_offset: u64) -> io::Result<Self> {
        let file_len = reader.seek(SeekFrom::End(0))?;
        if end_byte_offset > file_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "reverse JSONL scan end is past the file",
            ));
        }
        let record_terminated_by_newline = if end_byte_offset == 0 {
            false
        } else {
            reader.seek(SeekFrom::Start(end_byte_offset - 1))?;
            let mut byte = [0; 1];
            reader.read_exact(&mut byte)?;
            byte[0] == b'\n'
        };
        Ok(Self {
            reader,
            next_chunk_end: end_byte_offset,
            record_end_offset: end_byte_offset,
            record_terminated_by_newline,
            chunk_position: 0,
            chunk: vec![0; READ_CHUNK_SIZE],
            record_reversed: Vec::new(),
            last_record_end_offset: None,
            last_record_terminated_by_newline: None,
        })
    }

    /// Returns the byte offset immediately after the most recently returned record.
    pub fn last_record_end_offset(&self) -> Option<u64> {
        self.last_record_end_offset
    }

    /// Reports whether the most recently returned record ended at a JSONL newline.
    pub fn last_record_terminated_by_newline(&self) -> Option<bool> {
        self.last_record_terminated_by_newline
    }

    /// Scans the next nonblank record.
    ///
    /// I/O failures are returned as [`Err`]. Invalid JSON records are returned as
    /// [`ScanOutcome::Rejected`], and the scanner remains usable.
    pub fn scan_next<T>(&mut self) -> io::Result<Option<ScanOutcome<T>>>
    where
        T: DeserializeOwned,
    {
        loop {
            let Some(byte) = self.read_previous_byte()? else {
                let outcome = self.finish_record();
                if outcome.is_some() {
                    self.last_record_end_offset = Some(self.record_end_offset);
                    self.last_record_terminated_by_newline =
                        Some(self.record_terminated_by_newline);
                }
                return Ok(outcome);
            };

            if byte != b'\n' {
                self.record_reversed.push(byte);
                continue;
            }

            let record_end_offset = self.record_end_offset;
            let record_terminated_by_newline = self.record_terminated_by_newline;
            let newline_offset = self
                .next_chunk_end
                .checked_add(self.chunk_position as u64)
                .ok_or_else(|| io::Error::other("JSONL record offset overflow"))?;
            self.record_end_offset = newline_offset
                .checked_add(1)
                .ok_or_else(|| io::Error::other("JSONL record offset overflow"))?;
            self.record_terminated_by_newline = true;
            if let Some(outcome) = self.finish_record() {
                self.last_record_end_offset = Some(record_end_offset);
                self.last_record_terminated_by_newline = Some(record_terminated_by_newline);
                return Ok(Some(outcome));
            }
        }
    }

    fn read_previous_byte(&mut self) -> io::Result<Option<u8>> {
        if self.chunk_position == 0 {
            if self.next_chunk_end == 0 {
                return Ok(None);
            }

            let read_size = usize::try_from(self.next_chunk_end.min(READ_CHUNK_SIZE as u64))
                .map_err(io::Error::other)?;
            self.next_chunk_end -= read_size as u64;
            self.reader.seek(SeekFrom::Start(self.next_chunk_end))?;
            self.reader.read_exact(&mut self.chunk[..read_size])?;
            self.chunk_position = read_size;
        }

        self.chunk_position -= 1;
        Ok(Some(self.chunk[self.chunk_position]))
    }

    fn finish_record<T>(&mut self) -> Option<ScanOutcome<T>>
    where
        T: DeserializeOwned,
    {
        self.record_reversed.reverse();
        let outcome = if self.record_reversed.iter().all(u8::is_ascii_whitespace) {
            None
        } else {
            Some(match serde_json::from_slice::<T>(&self.record_reversed) {
                Ok(value) => ScanOutcome::Parsed(value),
                Err(error) => ScanOutcome::Rejected(error),
            })
        };
        self.record_reversed.clear();
        outcome
    }
}

#[cfg(test)]
#[path = "reverse_jsonl_scanner_tests.rs"]
mod tests;
