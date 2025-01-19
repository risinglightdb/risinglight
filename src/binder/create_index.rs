// Copyright 2025 RisingLight Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::str::FromStr;

use pretty_xmlish::helper::delegate_fmt;
use pretty_xmlish::Pretty;
use serde::{Deserialize, Serialize};

use super::*;
use crate::catalog::{ColumnId, SchemaId, TableId};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize)]
pub enum VectorDistance {
    Cosine,
    L2,
    NegativeDotProduct,
}

impl FromStr for VectorDistance {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "cosine" => Ok(VectorDistance::Cosine),
            "<=>" => Ok(VectorDistance::Cosine),
            "l2" => Ok(VectorDistance::L2),
            "<->" => Ok(VectorDistance::L2),
            "dotproduct" => Ok(VectorDistance::NegativeDotProduct),
            "<#>" => Ok(VectorDistance::NegativeDotProduct),
            _ => Err(format!("invalid vector distance: {}", s)),
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize)]
pub enum IndexType {
    Hnsw,
    IvfFlat {
        distance: VectorDistance,
        nlists: usize,
        nprobe: usize,
    },
    Btree,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize)]
pub struct CreateIndex {
    pub schema_id: SchemaId,
    pub index_name: String,
    pub table_id: TableId,
    pub columns: Vec<ColumnId>,
    pub index_type: IndexType,
}

impl fmt::Display for CreateIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let explainer = Pretty::childless_record("CreateIndex", self.pretty_index());
        delegate_fmt(&explainer, f, String::with_capacity(1000))
    }
}

impl CreateIndex {
    pub fn pretty_index<'a>(&self) -> Vec<(&'a str, Pretty<'a>)> {
        vec![
            ("schema_id", Pretty::display(&self.schema_id)),
            ("name", Pretty::display(&self.index_name)),
            ("table_id", Pretty::display(&self.table_id)),
            (
                "columns",
                Pretty::Array(self.columns.iter().map(Pretty::display).collect()),
            ),
        ]
    }
}

impl FromStr for Box<CreateIndex> {
    type Err = ();

    fn from_str(_s: &str) -> std::result::Result<Self, Self::Err> {
        Err(())
    }
}

impl Binder {
    fn parse_index_type(&self, using: Option<Ident>, with: Vec<Expr>) -> Result<IndexType> {
        let Some(using) = using else {
            return Err(ErrorKind::InvalidIndex("using clause is required".to_string()).into());
        };
        match using.to_string().to_lowercase().as_str() {
            "hnsw" => Ok(IndexType::Hnsw),
            "ivfflat" => {
                let mut distfn = None;
                let mut nlists = None;
                let mut nprobe = None;
                for expr in with {
                    let Expr::BinaryOp { left, op, right } = expr else {
                        return Err(
                            ErrorKind::InvalidIndex("invalid with clause".to_string()).into()
                        );
                    };
                    if op != BinaryOperator::Eq {
                        return Err(
                            ErrorKind::InvalidIndex("invalid with clause".to_string()).into()
                        );
                    }
                    let Expr::Identifier(Ident { value: key, .. }) = *left else {
                        return Err(
                            ErrorKind::InvalidIndex("invalid with clause".to_string()).into()
                        );
                    };
                    let key = key.to_lowercase();
                    let Expr::Value(v) = *right else {
                        return Err(
                            ErrorKind::InvalidIndex("invalid with clause".to_string()).into()
                        );
                    };
                    let v: DataValue = v.into();
                    match key.as_str() {
                        "distfn" => {
                            let v = v.as_str();
                            distfn = Some(v.to_lowercase());
                        }
                        "nlists" => {
                            let Some(v) = v.as_usize().unwrap() else {
                                return Err(ErrorKind::InvalidIndex(
                                    "invalid with clause".to_string(),
                                )
                                .into());
                            };
                            nlists = Some(v);
                        }
                        "nprobe" => {
                            let Some(v) = v.as_usize().unwrap() else {
                                return Err(ErrorKind::InvalidIndex(
                                    "invalid with clause".to_string(),
                                )
                                .into());
                            };
                            nprobe = Some(v);
                        }
                        _ => {
                            return Err(
                                ErrorKind::InvalidIndex("invalid with clause".to_string()).into()
                            );
                        }
                    }
                }
                Ok(IndexType::IvfFlat {
                    distance: VectorDistance::from_str(distfn.unwrap().as_str()).unwrap(),
                    nlists: nlists.unwrap(),
                    nprobe: nprobe.unwrap(),
                })
            }
            _ => Err(ErrorKind::InvalidIndex("invalid index type".to_string()).into()),
        }
    }

    pub(super) fn bind_create_index(&mut self, stat: crate::parser::CreateIndex) -> Result {
        let Some(ref name) = stat.name else {
            return Err(
                ErrorKind::InvalidIndex("index must have a name".to_string()).with_spanned(&stat),
            );
        };
        let crate::parser::CreateIndex {
            table_name,
            columns,
            using,
            with,
            ..
        } = stat;
        let index_name = lower_case_name(name);
        let (_, index_name) = split_name(&index_name)?;
        let table_obj: ObjectName = table_name.clone();
        let table_name = lower_case_name(&table_name);
        let (schema_name, table_name) = split_name(&table_name)?;
        let schema = self
            .catalog
            .get_schema_by_name(schema_name)
            .ok_or_else(|| ErrorKind::InvalidSchema(schema_name.into()).with_spanned(&table_obj))?;
        let Some(table) = schema.get_table_by_name(table_name) else {
            return Err(ErrorKind::InvalidTable(table_name.into()).with_spanned(&table_obj));
        };
        // Check if every column exists in the table and get the column ids
        let mut column_ids = Vec::new();
        for column in &columns {
            // Ensure column expr is a column reference
            let OrderByExpr { expr, .. } = column;
            let Expr::Identifier(column_name) = expr else {
                return Err(
                    ErrorKind::InvalidColumn("column reference expected".to_string())
                        .with_spanned(column),
                );
            };
            let column_name = column_name.value.to_lowercase();
            let column_catalog = table
                .get_column_by_name(&column_name)
                .ok_or_else(|| ErrorKind::InvalidColumn(column_name).with_spanned(column))?;
            column_ids.push(column_catalog.id());
        }

        let create = self.egraph.add(Node::CreateIndex(Box::new(CreateIndex {
            schema_id: schema.id(),
            index_name: index_name.into(),
            table_id: table.id(),
            columns: column_ids,
            index_type: self.parse_index_type(using, with)?,
        })));
        Ok(create)
    }
}
