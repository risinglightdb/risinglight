// Copyright 2022 RisingLight Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::*;
use crate::optimizer::plan_nodes::LogicalExchange;

pub struct ExchangeRule;

impl Rule for ExchangeRule {
    fn apply(&self, plan: PlanRef) -> Result<PlanRef, ()> {
        let table_scan = plan.as_logical_table_scan()?;
        if !table_scan.exchanged() {
            Ok(Arc::new(LogicalExchange::new(Arc::new(
                table_scan.clone().exchange(),
            ))))
        } else {
            Err(())
        }
    }
}
