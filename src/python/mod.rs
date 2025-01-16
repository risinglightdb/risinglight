// Copyright 2024 RisingLight Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;

use pyo3::prelude::*;
use tokio::runtime::Runtime;

use crate::storage::SecondaryStorageOptions;
use crate::Database;

#[pyclass]
pub struct PythonDatabase {
    runtime: Runtime,
    database: Database,
}
use pyo3::exceptions::PyException;

use crate::array::Chunk;

#[pymethods]
impl PythonDatabase {
    pub fn query(&self, py: Python<'_>, sql: String) -> PyResult<Vec<Vec<PyObject>>> {
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

/// Open a database for user, user can specify the path of database file
#[pyfunction]
pub fn open(path: String) -> PyResult<PythonDatabase> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let mut options = SecondaryStorageOptions::default_for_cli();
    options.path = PathBuf::new().join(path);

    let database = runtime.block_on(async move { Database::new_on_disk(options).await });
    Ok(PythonDatabase { runtime, database })
}

/// Open a database for user in memory
#[pyfunction]
pub fn open_in_memory() -> PyResult<PythonDatabase> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let database = Database::new_in_memory();
    Ok(PythonDatabase { runtime, database })
}

#[pymodule]
fn risinglight(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(open, m)?)?;
    m.add_function(wrap_pyfunction!(open_in_memory, m)?)?;
    Ok(())
}

use crate::types::DataValue;
/// Convert datachunk into Python List
pub fn datachunk_to_python_list(py: Python, chunk: &Chunk) -> Vec<Vec<PyObject>> {
    let mut output = vec![];
    for data_chunk in chunk.data_chunks() {
        for row in 0..data_chunk.cardinality() {
            let mut row_vec = vec![];

            for array in data_chunk.arrays() {
                let s = match array.get(row) {
                    DataValue::Null => "null".to_object(py),
                    DataValue::Bool(v) => v.to_object(py),
                    DataValue::Int16(v) => v.to_object(py),
                    DataValue::Int32(v) => v.to_object(py),
                    DataValue::Int64(v) => (v).to_object(py),
                    DataValue::Float64(v) => v.to_object(py),
                    DataValue::String(s) => s.to_object(py),
                    DataValue::Blob(s) => s.to_string().to_object(py),
                    DataValue::Decimal(v) => v.to_string().to_object(py),
                    DataValue::Date(v) => v.to_string().to_object(py),
                    DataValue::Timestamp(v) => v.to_string().to_object(py),
                    DataValue::TimestampTz(v) => v.to_string().to_object(py),
                    DataValue::Interval(v) => v.to_string().to_object(py),
                    DataValue::Vector(v) => v
                        .iter()
                        .map(|s| s.to_object(py))
                        .collect::<Vec<_>>()
                        .to_object(py),
                };
                row_vec.push(s);
            }
            output.push(row_vec);
        }
    }
    output
}
