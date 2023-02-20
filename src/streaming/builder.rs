use std::sync::Arc;

use egg::Id;

use crate::catalog::RootCatalogRef;
use crate::planner::{Expr, TypeSchemaAnalysis};
use crate::storage::Storage;

/// Build the stream pipeline of a query.
pub struct Builder<S: Storage> {
    storage: Arc<S>,
    catalog: RootCatalogRef,
    egraph: egg::EGraph<Expr, TypeSchemaAnalysis>,
    root: Id,
}
