use std::borrow::Borrow;
use std::ops::Deref;
use std::str::FromStr;

use serde::Serialize;

use super::{VectorRef, F64};

/// A vector is a specialized array type for floating point numbers.
#[derive(PartialOrd, Ord, PartialEq, Eq, Debug, Clone, Default, Hash, Serialize)]
pub struct Vector(Box<[F64]>);

impl Vector {
    pub fn new(values: Vec<f64>) -> Self {
        Self(values.into_iter().map(F64::from).collect())
    }

    pub fn new_from_ordered_f64(values: Vec<F64>) -> Self {
        Self(values.into())
    }
}

impl std::fmt::Display for Vector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}]",
            self.0
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

impl From<&[F64]> for Vector {
    fn from(values: &[F64]) -> Self {
        Vector(values.into())
    }
}

impl From<Vec<F64>> for Vector {
    fn from(vec: Vec<F64>) -> Self {
        Vector(vec.into())
    }
}

impl Borrow<VectorRef> for Vector {
    fn borrow(&self) -> &VectorRef {
        self
    }
}

impl AsRef<VectorRef> for Vector {
    fn as_ref(&self) -> &VectorRef {
        self
    }
}

impl Deref for Vector {
    type Target = VectorRef;

    fn deref(&self) -> &Self::Target {
        VectorRef::new(&self.0)
    }
}

impl VectorRef {
    pub fn norm_squared(&self) -> F64 {
        let sum: f64 = self.0.iter().map(|a| a.powi(2)).sum();
        F64::from(sum)
    }

    pub fn norm(&self) -> F64 {
        F64::from(self.norm_squared().sqrt())
    }

    pub fn l2_distance(&self, other: &VectorRef) -> F64 {
        let sum = self
            .0
            .iter()
            .zip(other.0.iter())
            .map(|(a, b)| (a.0 - b.0).powi(2))
            .sum::<f64>();
        F64::from(sum.sqrt())
    }

    pub fn cosine_distance(&self, other: &VectorRef) -> F64 {
        let dot_product = self.dot_product(other);
        let norm_self_squared = self.norm_squared();
        let norm_other_squared = other.norm_squared();
        F64::from(1.0) - dot_product / (norm_self_squared * norm_other_squared).sqrt()
    }

    pub fn dot_product(&self, other: &VectorRef) -> F64 {
        let sum = self
            .0
            .iter()
            .zip(other.0.iter())
            .map(|(a, b)| a.0 * b.0)
            .sum::<f64>();
        F64::from(sum)
    }

    pub fn to_vector(&self) -> Vector {
        Vector::new_from_ordered_f64(self.as_ref().to_vec())
    }
}

/// An error which can be returned when parsing a blob.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum ParseVectorError {
    #[error("invalid number: {0}")]
    Float(#[from] std::num::ParseFloatError),
    #[error("unexpected end of string")]
    UnexpectedEof,
    #[error("invalid character")]
    InvalidChar,
}

impl FromStr for Vector {
    type Err = ParseVectorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        let s = s.strip_prefix('[').ok_or(ParseVectorError::InvalidChar)?;
        let s = s.strip_suffix(']').ok_or(ParseVectorError::UnexpectedEof)?;
        let s = s.trim();
        if s.is_empty() {
            return Ok(Vector::new(vec![]));
        }
        let values = s
            .split(',')
            .map(|s| s.trim().parse::<F64>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(ParseVectorError::Float)?;
        Ok(Vector::from(values))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_vector() {
        assert_eq!(
            " [1, 2.0, 3]".parse::<Vector>(),
            Ok(Vector::new(vec![1.0, 2.0, 3.0]))
        );
        assert_eq!(
            "[1,2.0,3] ".parse::<Vector>(),
            Ok(Vector::new(vec![1.0, 2.0, 3.0]))
        );
        assert_eq!("[]".parse::<Vector>(), Ok(Vector::new(vec![])));
        assert_eq!(" [  ]".parse::<Vector>(), Ok(Vector::new(vec![])));
    }
}
