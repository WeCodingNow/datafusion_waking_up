use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use datafusion::{
    arrow::{array::Int32Array, record_batch::RecordBatch},
    error::{DataFusionError, Result},
    execution::RecordBatchStream,
};
use futures::Stream;

use crate::schema;

pub struct FooStream {
    batches: i32,
    wait_every: Option<i32>,

    cur_i: i32,
}

impl FooStream {
    pub fn new(batches: i32, wait_every: Option<i32>) -> Self {
        Self {
            batches,
            wait_every,
            cur_i: 0,
        }
    }
}

impl RecordBatchStream for FooStream {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        schema::stub()
    }
}

impl Stream for FooStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        dbg!("poll_next() start");
        self.cur_i += 1;

        if let Some(iters) = self.wait_every {
            let should_wait = (self.cur_i % iters) == 0;

            if should_wait {
                dbg!("poll_next() return Pending");
                _cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }

        if self.cur_i > self.batches {
            dbg!("poll_next() return Ready(None)");
            return Poll::Ready(None);
        }

        let rb = RecordBatch::try_new(
            self.schema(),
            vec![Arc::new(Int32Array::from(vec![self.cur_i]))],
        )
        .map_err(DataFusionError::from);

        dbg!("poll_next() return Ready(Some(rb))");
        Poll::Ready(Some(rb))
    }
}
