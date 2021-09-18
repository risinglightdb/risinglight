#[derive(Debug, Default)]
pub struct BinderContext {
    pub regular_tables: HashMap<String, TableRefId>,
    // Mapping the table name to column names
    pub column_names: HashMap<String, HashSet<String>>,
    // Mapping table name to its column ids
    pub column_ids: HashMap<String, Vec<ColumnId>>,
}
