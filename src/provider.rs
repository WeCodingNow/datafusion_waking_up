use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::{TableProvider, TableType},
    error::Result,
    execution::context::SessionState,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};

use crate::{exec::FooExec, schema};

pub struct FooTableProvider {}

impl FooTableProvider {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl TableProvider for FooTableProvider {
    #[doc = r" Returns the table provider as [`Any`](std::any::Any) so that it can be"]
    #[doc = r" downcast to a specific implementation."]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[doc = r" Get a reference to the schema for this table"]
    fn schema(&self) -> SchemaRef {
        schema::stub()
    }

    #[doc = r" Get the type of this table for metadata/catalog purposes."]
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    #[doc = r" Create an ExecutionPlan that will scan the table."]
    #[doc = r" The table provider will be usually responsible of grouping"]
    #[doc = r" the source data into partitions that can be efficiently"]
    #[doc = r" parallelized or distributed."]
    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(FooExec::new()))
    }
}
