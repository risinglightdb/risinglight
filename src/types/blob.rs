use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

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
}

impl FromStr for Blob {
    type Err = ParseBlobError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(mut s) = s.strip_prefix("\\x") {
            let mut v = Vec::with_capacity(s.len() / 2);
            while !s.is_empty() {
                if s.len() < 2 {
                    return Err(ParseBlobError::Length);
                }
                v.push(u8::from_str_radix(&s[..2], 16)?);
                s = &s[2..];
            }
            Ok(Blob(v))
        } else {
            Ok(Blob(s.as_bytes().into()))
        }
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
#[derive(PartialEq, PartialOrd)]
pub struct BlobRef([u8]);

impl BlobRef {
    pub fn new(bytes: &[u8]) -> &Self {
        // SAFETY: `&BlobRef` and `&[u8]` have the same layout.
        unsafe { std::mem::transmute(bytes) }
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
        write!(f, "'\\x{self}'")
    }
}

impl fmt::Display for BlobRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0 {
            write!(f, "{b:02X}")?;
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
        assert_eq!(b.to_string(), "AA");
        assert_eq!(format!("{b:?}"), "'\\xAA'");
    }
}
