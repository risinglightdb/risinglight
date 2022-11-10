// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use serde::Serialize;

use super::*;

/// A bound expression with alias
#[derive(PartialEq, Clone, Serialize)]
pub struct BoundExprWithAlias {
    pub expr: Box<BoundExpr>,
    pub alias: String,
}

impl std::fmt::Debug for BoundExprWithAlias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} (alias to {})", self.expr, self.alias)
    }
}

impl std::fmt::Display for BoundExprWithAlias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} (alias to {})", self.expr, self.alias)
    }
}

/// An alias reference to a bound expression
#[derive(PartialEq, Clone, Serialize)]
pub struct BoundAlias {
    pub alias: String,
    pub expr: Box<BoundExpr>,
}

impl std::fmt::Debug for BoundAlias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.alias)
    }
}

impl Binder {
    /// Bind an alias to a bound expression
    pub fn bind_alias(&mut self, expr: BoundExpr, ident: Ident) -> BoundExpr {
        let alias = ident.value;
        self.context.aliases.push(alias.clone());
        self.context.aliases_expressions.push(expr.clone());
        BoundExpr::ExprWithAlias(BoundExprWithAlias {
            expr: Box::new(expr),
            alias,
        })
    }
}
