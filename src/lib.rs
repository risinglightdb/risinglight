// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

//! RisingLight -- an educational OLAP database.

#![warn(clippy::doc_markdown)]
#![warn(clippy::explicit_into_iter_loop)]
#![warn(clippy::explicit_iter_loop)]
#![warn(clippy::inconsistent_struct_constructor)]
#![warn(clippy::map_flatten)]
#![deny(unused_must_use)]
#![feature(array_chunks)]
#![feature(portable_simd)]
#![feature(generators)]
#![feature(error_generic_member_access)]
#![feature(provide_any)]
#![feature(type_alias_impl_trait)]
#![feature(stmt_expr_attributes)]
#![feature(proc_macro_hygiene)]
#![feature(core_intrinsics)]
#![feature(trusted_len)]
#![feature(once_cell)]
#![feature(array_methods)]
#![feature(iterator_try_collect)]
#![feature(async_fn_in_trait)]
#![feature(return_position_impl_trait_in_trait)]
#![allow(incomplete_features)]

/// Top-level structure of the database.
pub mod db;

/// Parse the SQL string into an Abstract Syntax Tree (AST).
pub mod parser;

/// Convert the parser AST to planner AST.
pub mod binder_v2;

/// Egg-based planner and optimizer.
pub mod planner;

/// Execute the queries.
pub mod executor_v2;

/// The legacy query engine.
pub mod v1 {
    /// Resolve all expressions referring with their names.
    pub mod binder;

    /// Transform the parse tree into a logical operations tree.
    pub mod logical_planner;

    /// Do query optimization.
    pub mod optimizer;

    /// Execute the queries.
    pub mod executor;

    /// Functions
    pub mod function;
}

/// In-memory representations of a column values.
pub mod array;
/// Metadata of database objects.
pub mod catalog;
/// Persistent storage engine.
pub mod storage;
/// Basic type definitions.
pub mod types;
/// Utilities.
pub mod utils;

#[cfg(feature = "jemalloc")]
use tikv_jemallocator::Jemalloc;

pub use self::db::{Database, Error};

/// Jemalloc can significantly improve performance compared to the default system allocator.
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use std::path::PathBuf;

/// Python Extension
use pyo3::{prelude::*, wrap_pyfunction};
use storage::SecondaryStorageOptions;
/// Open a database for user, user can specify the path of database file
#[pyfunction]
fn open(path: String) -> PyResult<PythonDatabase> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut options = SecondaryStorageOptions::default_for_cli();
    options.path = PathBuf::new().join(path);

    let database = runtime.block_on(async move { Database::new_on_disk(options).await });
    Ok(PythonDatabase { runtime, database })
}

#[pymodule]
fn risinglight(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(open, m)?)?;
    Ok(())
}

use tokio::runtime::Runtime;
#[pyclass]
pub struct PythonDatabase {
    runtime: Runtime,
    database: Database,
}
use pyo3::exceptions::PyException;

use crate::array::{Chunk};
#[pymethods]
impl PythonDatabase {
    fn query(&self, py: Python<'_>, sql: String) -> PyResult<Vec<Vec<PyObject>>> {
        let result = self
            .runtime
            .block_on(async { self.database.run(&sql).await });
        match result {
            Ok(chunks) => {
                let mut rows = vec![];
                for chunk in chunks {
                    let mut table = datachunk_to_python_list(py, &chunk);
                    rows.append(&mut table);
                }
                Ok(rows)
            }
            Err(e) => Err(PyException::new_err(e.to_string())),
        }
    }
}
use pyo3::conversion::ToPyObject;

use crate::types::DataValue;
/// Convert datachunk into Python List
pub fn datachunk_to_python_list(py: Python, chunk: &Chunk) -> Vec<Vec<PyObject>> {
    let mut output = vec![];
    for data_chunk in chunk.data_chunks() {
        for row in 0..data_chunk.cardinality() {
            let mut row_vec = vec![];

            for array in data_chunk.arrays() {
                let s = match array.get(row) {
                    DataValue::Null => "null".to_string().to_object(py),
                    DataValue::Bool(v) => v.to_object(py),
                    DataValue::Int32(v) => v.to_object(py),
                    DataValue::Int64(v) => (v).to_object(py),
                    DataValue::Float64(v) => v.to_object(py),
                    DataValue::String(s) if s.is_empty() => "(empty)".to_string().to_object(py),
                    DataValue::String(s) => s.to_object(py),
                    DataValue::Blob(s) if s.is_empty() => "(empty)".to_string().to_object(py),
                    DataValue::Blob(s) => s.to_string().to_object(py),
                    DataValue::Decimal(v) => v.to_string().to_object(py),
                    DataValue::Date(v) => v.to_string().to_object(py),
                    DataValue::Interval(v) => v.to_string().to_object(py),
                };
                row_vec.push(s);
            }
            output.push(row_vec);
        }
    }
    output
}
