use datafusion::catalog::TableFunctionImpl;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::lit;
use datafusion::prelude::SessionContext;
use datafusion_tpch::{
    TpchCustomer, TpchLineitem, TpchNation, TpchOrders, TpchPart, TpchPartsupp, TpchRegion,
    TpchSupplier,
};
use std::sync::Arc;
use tpch_partitioned_provider::LineItemPartitionedProvider;
use tpch_streaming_provider::{
    CustomerStreamingProvider, LineItemStreamingProvider, NationStreamingProvider,
    OrderStreamingProvider, PartStreamingProvider, PartSuppStreamingProvider,
    RegionStreamingProvider, SupplierStreamingProvider,
};

pub fn register_streaming_tables(
    ctx: &SessionContext,
    scale_factor: f64,
    num_partitions: i32,
) -> DataFusionResult<()> {
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

    Ok(())
}

pub fn register_in_memory_tables(ctx: &SessionContext, scale_factor: f64) -> DataFusionResult<()> {
    let _ = ctx.register_table("customer", TpchCustomer {}.call(&[lit(scale_factor)])?)?;
    let _ = ctx.register_table("lineitem", TpchLineitem {}.call(&[lit(scale_factor)])?)?;
    let _ = ctx.register_table("nation", TpchNation {}.call(&[lit(scale_factor)])?)?;
    let _ = ctx.register_table("orders", TpchOrders {}.call(&[lit(scale_factor)])?)?;
    let _ = ctx.register_table("part", TpchPart {}.call(&[lit(scale_factor)])?)?;
    let _ = ctx.register_table("partsupp", TpchPartsupp {}.call(&[lit(scale_factor)])?)?;
    let _ = ctx.register_table("region", TpchRegion {}.call(&[lit(scale_factor)])?)?;
    let _ = ctx.register_table("supplier", TpchSupplier {}.call(&[lit(scale_factor)])?)?;

    Ok(())
}

pub fn register_streaming_with_partitioned_tables(
    ctx: &SessionContext,
    scale_factor: f64,
    num_partitions: i32,
) -> DataFusionResult<()> {
    // Special case: line item we have a custom exec
    let _ = ctx.register_table(
        "lineitem",
        Arc::new(LineItemPartitionedProvider::new(
            scale_factor,
            num_partitions,
        )),
    )?;

    let _ = ctx.register_table(
        "customer",
        Arc::new(CustomerStreamingProvider::new(scale_factor, num_partitions)),
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

    Ok(())
}
