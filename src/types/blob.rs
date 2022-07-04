// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

use ref_cast::RefCast;
use serde::{Deserialize, Serialize};

/// Binary large object.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize)]
pub struct Blob(Vec<u8>);

impl From<&[u8]> for Blob {
    fn from(bytes: &[u8]) -> Self {
        Blob(bytes.into())
    }
}

impl From<Vec<u8>> for Blob {
    fn from(vec: Vec<u8>) -> Self {
        Blob(vec)
    }
}

impl Borrow<BlobRef> for Blob {
    fn borrow(&self) -> &BlobRef {
        &*self
    }
}

impl AsRef<BlobRef> for Blob {
    fn as_ref(&self) -> &BlobRef {
        &*self
    }
}

impl Deref for Blob {
    type Target = BlobRef;

    fn deref(&self) -> &Self::Target {
        BlobRef::new(&self.0)
    }
}

/// An error which can be returned when parsing a blob.
#[derive(thiserror::Error, Debug, Clone, PartialEq)]
#[error("parse blob error")]
pub enum ParseBlobError {
    Int(#[from] std::num::ParseIntError),
    Length,
    InvalidChar,
}

impl FromStr for Blob {
    type Err = ParseBlobError;

    fn from_str(mut s: &str) -> Result<Self, Self::Err> {
        let mut v = Vec::with_capacity(s.len());
        while !s.is_empty() {
            if let Some(ss) = s.strip_prefix("\\x") {
                if ss.len() < 2 {
                    return Err(ParseBlobError::Length);
                }
                if !s.is_char_boundary(2) {
                    return Err(ParseBlobError::InvalidChar);
                }
                v.push(u8::from_str_radix(&ss[..2], 16)?);
                s = &ss[2..];
            } else {
                if !s.is_char_boundary(1) {
                    return Err(ParseBlobError::InvalidChar);
                }
                v.push(s.as_bytes()[0] as u8);
                s = &s[1..];
            }
        }
        Ok(Blob(v))
    }
}

impl fmt::Debug for Blob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.as_ref())
    }
}

impl fmt::Display for Blob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

/// A slice of a blob.
#[repr(transparent)]
#[derive(PartialEq, PartialOrd, RefCast, Hash, Eq)]
pub struct BlobRef([u8]);

impl BlobRef {
    pub fn new(bytes: &[u8]) -> &Self {
        // SAFETY: `&BlobRef` and `&[u8]` have the same layout.
        BlobRef::ref_cast(bytes)
    }
}

impl ToOwned for BlobRef {
    type Owned = Blob;

    fn to_owned(&self) -> Self::Owned {
        self.as_ref().into()
    }
}

impl AsRef<[u8]> for BlobRef {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Deref for BlobRef {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Debug for BlobRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "'{self}'")
    }
}

impl fmt::Display for BlobRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for &b in &self.0 {
            match b {
                b'\\' => write!(f, "\\\\")?,
                b'\'' => write!(f, "''")?,
                32..=126 => write!(f, "{}", b as char)?,
                _ => write!(f, "\\x{b:02X}")?,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_blob() {
        assert_eq!("\\xAA".parse::<Blob>(), Ok(Blob::from([170].as_slice())));
        assert_eq!("AB".parse::<Blob>(), Ok(Blob::from(b"AB".as_slice())));
    }

    #[test]
    fn blob_to_string() {
        let b = Blob::from([170].as_slice());
        assert_eq!(b.to_string(), "\\xAA");
        assert_eq!(format!("{b:?}"), "'\\xAA'");
    }
}
