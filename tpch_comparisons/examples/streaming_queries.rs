use datafusion::common::exec_datafusion_err;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use tpch_queries::get_tpch_query;
use tpch_streaming_provider::{
    CustomerStreamingProvider, LineItemStreamingProvider, NationStreamingProvider,
    OrderStreamingProvider, PartStreamingProvider, PartSuppStreamingProvider,
    RegionStreamingProvider, SupplierStreamingProvider,
};

#[tokio::main]
async fn main() -> DataFusionResult<()> {
    let ctx = SessionContext::new();

    let query_number = 2;
    let scale_factor = 0.1;
    let num_partitions = 5;

    let _ = ctx.register_table(
        "customer",
        Arc::new(CustomerStreamingProvider::new(scale_factor, num_partitions)),
    )?;
    let _ = ctx.register_table(
        "lineitem",
        Arc::new(LineItemStreamingProvider::new(scale_factor, num_partitions)),
    )?;
    let _ = ctx.register_table(
        "nation",
        Arc::new(NationStreamingProvider::new(scale_factor)),
    )?;
    let _ = ctx.register_table(
        "orders",
        Arc::new(OrderStreamingProvider::new(scale_factor, num_partitions)),
    )?;
    let _ = ctx.register_table(
        "part",
        Arc::new(PartStreamingProvider::new(scale_factor, num_partitions)),
    )?;
    let _ = ctx.register_table(
        "partsupp",
        Arc::new(PartSuppStreamingProvider::new(scale_factor, num_partitions)),
    )?;
    let _ = ctx.register_table(
        "region",
        Arc::new(RegionStreamingProvider::new(scale_factor)),
    )?;
    let _ = ctx.register_table(
        "supplier",
        Arc::new(SupplierStreamingProvider::new(scale_factor, num_partitions)),
    )?;

    let query = get_tpch_query(query_number).map_err(|err| exec_datafusion_err!("{err}"))?;

    if query_number == 15 {
        // Special case Q15 because it has multiple statements
        let query_lines = query
            .split(";")
            .map(|part| part.trim())
            .filter(|part| !part.is_empty())
            .collect::<Vec<&str>>();

        for (idx, line) in query_lines.iter().enumerate() {
            if idx == 1 {
                let df = ctx.sql(line).await?;
                df.limit(0, Some(100))?.show().await?;
            } else {
                let _ = ctx.sql(line).await?.collect().await?;
            }
        }
    } else {
        let df = ctx.sql(query).await?;

        df.limit(0, Some(100))?.show().await?;
    }

    Ok(())
}
