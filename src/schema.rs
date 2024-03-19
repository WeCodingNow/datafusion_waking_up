use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};

pub fn stub() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("foo", DataType::Int32, true)]))
}
