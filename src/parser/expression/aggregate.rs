use super::*;
use std::str::FromStr;

#[derive(Debug, PartialEq, Clone)]
pub struct Aggregate {
    pub kind: AggregateKind,
    pub child: Box<Expression>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum AggregateKind {
    Min,
    Max,
    Sum,
    Avg,
    Count,
}

impl FromStr for AggregateKind {
    type Err = ParseError;

    fn from_str(op: &str) -> Result<Self, Self::Err> {
        match op {
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            "sum" => Ok(Self::Sum),
            "avg" => Ok(Self::Avg),
            "count" => Ok(Self::Count),
            _ => Err(ParseError::InvalidInput("function")),
        }
    }
}

impl Expression {
    pub fn aggregate(kind: AggregateKind, child: Expression) -> Self {
        Expression {
            kind: ExprKind::Aggregate(Aggregate {
                kind,
                child: Box::new(child),
            }),
            alias: None,
            return_type: None,
        }
    }
}

impl TryFrom<&pg::nodes::FuncCall> for Expression {
    type Error = ParseError;

    fn try_from(node: &pg::nodes::FuncCall) -> Result<Self, Self::Error> {
        let names = node.funcname.as_ref().unwrap();
        if names.len() != 1 {
            todo!("unsupported function");
        }
        let name = node_to_string(&names[0])?.to_lowercase();
        if let Some(_over) = &node.over {
            todo!("window function");
        }
        if let Ok(kind) = name.parse::<AggregateKind>() {
            if node.agg_star || kind == AggregateKind::Count && node.args.is_none() {
                return Ok(Expression::aggregate(kind, Expression::star()));
            }
            let args = node.args.as_ref().unwrap();
            if args.is_empty() || args.len() >= 2 {
                todo!("only support aggregate over 1 column");
            }
            let child = Expression::try_from(&args[0])?;
            if matches!(child.kind, ExprKind::Aggregate(_)) {
                return Err(ParseError::InvalidInput(
                    "Aggregate function calls cannot be nested.",
                ));
            }
            return Ok(Expression::aggregate(kind, child));
        }
        todo!("unsupported function");
    }
}
