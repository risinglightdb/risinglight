// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

use ref_cast::RefCast;
use serde::{Deserialize, Serialize};

use super::{Vector, F64};

/// Binary large object.
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize, Default)]
pub struct Blob(Box<[u8]>);

impl From<&[u8]> for Blob {
    fn from(bytes: &[u8]) -> Self {
        Blob(bytes.into())
    }
}

impl From<Vec<u8>> for Blob {
    fn from(vec: Vec<u8>) -> Self {
        Blob(vec.into())
    }
}

impl Borrow<BlobRef> for Blob {
    fn borrow(&self) -> &BlobRef {
        self
    }
}

impl AsRef<BlobRef> for Blob {
    fn as_ref(&self) -> &BlobRef {
        self
    }
}

impl Deref for Blob {
    type Target = BlobRef;

    fn deref(&self) -> &Self::Target {
        BlobRef::new(&self.0)
    }
}

impl Blob {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// An error which can be returned when parsing a blob.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum ParseBlobError {
    #[error("invalid hex: {0}")]
    Int(#[from] std::num::ParseIntError),
    #[error("unexpected end of string")]
    UnexpectedEof,
    #[error("invalid character")]
    InvalidChar,
}

impl FromStr for Blob {
    type Err = ParseBlobError;

    fn from_str(mut s: &str) -> Result<Self, Self::Err> {
        let mut v = Vec::with_capacity(s.len());
        while !s.is_empty() {
            if let Some(ss) = s.strip_prefix("\\x") {
                if ss.len() < 2 {
                    return Err(ParseBlobError::UnexpectedEof);
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
                v.push(s.as_bytes()[0]);
                s = &s[1..];
            }
        }
        Ok(v.into())
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
#[derive(PartialEq, Eq, PartialOrd, Ord, RefCast, Hash)]
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

/// A slice of a vector.
#[repr(transparent)]
#[derive(PartialEq, Eq, PartialOrd, Ord, RefCast, Hash)]
pub struct VectorRef(pub(crate) [F64]);

impl VectorRef {
    pub fn new(values: &[F64]) -> &Self {
        // SAFETY: `&VectorRef` and `&[F64]` have the same layout.
        VectorRef::ref_cast(values)
    }
}

impl ToOwned for VectorRef {
    type Owned = Vector;

    fn to_owned(&self) -> Self::Owned {
        self.as_ref().into()
    }
}

impl AsRef<[F64]> for VectorRef {
    fn as_ref(&self) -> &[F64] {
        &self.0
    }
}

impl Deref for VectorRef {
    type Target = [F64];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Debug for VectorRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.as_ref())
    }
}

impl fmt::Display for VectorRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, &v) in self.as_ref().iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "{}", v)?;
        }
        write!(f, "]")
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
