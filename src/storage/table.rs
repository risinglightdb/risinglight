use std::sync::Arc;

pub enum Table {
    DataTable,
    MaterializedView
}

pub type TableRef = Arc<Table>;