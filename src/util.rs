use std::fmt::{Display, Formatter, Result as FmtResult};
use std::str::{from_utf8, from_utf8_unchecked};

#[derive(Clone, Copy)]
pub struct Utf8Lossy<'a>(pub &'a [u8]);

impl<'a> From<&'a [u8]> for Utf8Lossy<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        Utf8Lossy(bytes)
    }
}

impl Display for Utf8Lossy<'_> {
    // Copied from https://doc.rust-lang.org/std/str/struct.Utf8Error.html#examples
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let mut input = self.0;
        loop {
            match from_utf8(input) {
                Ok(valid) => return valid.fmt(f),
                Err(e) => {
                    let (valid, after_valid) = input.split_at(e.valid_up_to());
                    unsafe {
                        write!(f, "{}", from_utf8_unchecked(valid))?;
                    }
                    write!(f, "\u{FFDD}")?;
                    if let Some(invalid_sequence_length) = e.error_len() {
                        input = &after_valid[invalid_sequence_length..]
                    } else {
                        return Ok(());
                    }
                }
            }
        }
    }
}
