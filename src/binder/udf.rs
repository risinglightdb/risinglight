use std::collections::HashMap;

use super::*;

#[derive(Clone, Debug, Default)]
pub struct UdfContext {
    /// The mapping from `sql udf parameters` to a bound `Id` generated from `ast
    /// expressions` Note: The expressions are constructed during runtime, correspond to the
    /// actual users' input
    udf_param_context: HashMap<String, Id>,

    /// The global counter that records the calling stack depth
    /// of the current binding sql udf chain
    udf_global_counter: u32,
}

impl UdfContext {
    pub fn new() -> Self {
        Self {
            udf_param_context: HashMap::new(),
            udf_global_counter: 0,
        }
    }

    pub fn global_count(&self) -> u32 {
        self.udf_global_counter
    }

    pub fn incr_global_count(&mut self) {
        self.udf_global_counter += 1;
    }

    pub fn _is_empty(&self) -> bool {
        self.udf_param_context.is_empty()
    }

    pub fn update_context(&mut self, context: HashMap<String, Id>) {
        self.udf_param_context = context;
    }

    pub fn _clear(&mut self) {
        self.udf_global_counter = 0;
        self.udf_param_context.clear();
    }

    pub fn get_expr(&self, name: &str) -> Option<&Id> {
        self.udf_param_context.get(name)
    }

    pub fn get_context(&self) -> HashMap<String, Id> {
        self.udf_param_context.clone()
    }

    /// A common utility function to extract sql udf
    /// expression out from the input `ast`
    pub fn extract_udf_expression(ast: Vec<Statement>) -> Result<Expr> {
        if ast.len() != 1 {
            return Err(BindError::InvalidExpression(
                "the query for sql udf should contain only one statement".to_string(),
            ));
        }

        // Extract the expression out
        let Statement::Query(query) = ast[0].clone() else {
            return Err(BindError::InvalidExpression(
                "invalid function definition, please recheck the syntax".to_string(),
            ));
        };

        let SetExpr::Select(select) = *query.body else {
            return Err(BindError::InvalidExpression(
                "missing `select` body for sql udf expression, please recheck the syntax"
                    .to_string(),
            ));
        };

        if select.projection.len() != 1 {
            return Err(BindError::InvalidExpression(
                "`projection` should contain only one `SelectItem`".to_string(),
            ));
        }

        let SelectItem::UnnamedExpr(expr) = select.projection[0].clone() else {
            return Err(BindError::InvalidExpression(
                "expect `UnnamedExpr` for `projection`".to_string(),
            ));
        };

        Ok(expr)
    }

    pub fn create_udf_context(
        args: &[FunctionArg],
        catalog: &Arc<FunctionCatalog>,
    ) -> Result<HashMap<String, Expr>> {
        let mut ret: HashMap<String, Expr> = HashMap::new();
        for (i, current_arg) in args.iter().enumerate() {
            if let FunctionArg::Unnamed(_arg) = current_arg {
                match current_arg {
                    FunctionArg::Unnamed(arg) => {
                        let FunctionArgExpr::Expr(e) = arg else {
                            return Err(BindError::InvalidExpression("invalid syntax".to_string()));
                        };
                        if catalog.arg_names[i].is_empty() {
                            ret.insert(format!("${}", i + 1), e.clone());
                        } else {
                            // The index mapping here is accurate
                            // So that we could directly use the index
                            ret.insert(catalog.arg_names[i].clone(), e.clone());
                        }
                    }
                    _ => return Err(BindError::InvalidExpression("invalid syntax".to_string())),
                }
            }
        }
        Ok(ret)
    }
}
