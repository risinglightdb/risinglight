use super::*;

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnRef {
    /// Table name. If it's not set at the transforming time, we need to search
    /// for the corresponding table name within the binder context.
    pub table_name: Option<String>,
    /// Column name.
    pub column_name: String,
    // TODO: binder variables
    pub column_ref_id: Option<ColumnRefId>,
    pub column_index: Option<ColumnId>,
}

impl Expression {
    pub const fn column_ref(column_name: String, table_name: Option<String>) -> Self {
        Expression {
            kind: ExprKind::ColumnRef(ColumnRef {
                table_name,
                column_name,
                column_ref_id: None,
                column_index: None,
            }),
            alias: None,
            return_type: None,
        }
    }

    pub const fn star() -> Self {
        Expression {
            kind: ExprKind::Star,
            alias: None,
            return_type: None,
        }
    }
}

impl TryFrom<&pg::nodes::ColumnRef> for Expression {
    type Error = ParseError;

    fn try_from(node: &pg::nodes::ColumnRef) -> Result<Self, Self::Error> {
        match node.fields.as_ref().unwrap().as_slice() {
            [pg::Node::A_Star(_)] => Ok(Self::star()),
            [pg::Node::Value(v)] => {
                let column_name = v.string.as_ref().map(|s| s.to_lowercase()).unwrap();
                Ok(Self::column_ref(column_name, None))
            }
            [pg::Node::Value(v1), pg::Node::Value(v2)] => {
                let table_name = v1.string.as_ref().map(|s| s.to_lowercase());
                let column_name = v2.string.as_ref().map(|s| s.to_lowercase()).unwrap();
                Ok(Self::column_ref(column_name, table_name))
            }
            _ => todo!("unsupported column type"),
        }
    }
}
