use std::{fmt::Formatter, pin::Pin, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::Statistics,
    error::{DataFusionError, Result},
    execution::SendableRecordBatchStream,
    physical_expr::PhysicalSortExpr,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning},
};

use crate::{schema, stream::FooStream};

#[derive(Debug)]
pub struct FooExec {}

impl FooExec {
    pub fn new() -> Self {
        Self {}
    }
}

impl ExecutionPlan for FooExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        schema::stub()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!(
            "Children cannot be replaced in {self:?}"
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        dbg!("execute() start");

        let ret: Result<SendableRecordBatchStream> = Ok(Box::pin(FooStream::new(5, Some(3))));

        dbg!("execute() end");

        ret
    }

    fn statistics(&self) -> Statistics {
        Default::default()
    }
}

impl DisplayAs for FooExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "FooExec")
    }
}
