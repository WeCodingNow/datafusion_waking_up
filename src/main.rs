mod exec;
mod provider;
mod schema;
mod stream;

use std::sync::Arc;

use anyhow::Result;
use datafusion::execution::context::SessionContext;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    let tbl = Arc::new(provider::FooTableProvider::new());

    ctx.register_table("foo", tbl)?;

    let df = ctx.sql(r"select * from foo").await?;

    df.show().await?;

    Ok(())
}
