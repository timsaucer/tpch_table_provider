use datafusion::common::exec_datafusion_err;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::prelude::{SessionConfig, SessionContext};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::task::spawn_blocking;
use tpch_queries::get_tpch_query;
use tpch_streaming_provider::{
    CustomerStreamingProvider, LineItemStreamingProvider, NationStreamingProvider,
    OrderStreamingProvider, PartStreamingProvider, PartSuppStreamingProvider,
    RegionStreamingProvider, SupplierStreamingProvider,
};

async fn query_execution_time(
    query_number: usize,
    scale_factor: f64,
    num_partitions: usize,
) -> DataFusionResult<Duration> {
    let config = SessionConfig::new().with_target_partitions(num_partitions);
    let ctx = SessionContext::new();
    let num_partitions = num_partitions as i32;



    let query = get_tpch_query(query_number).map_err(|err| exec_datafusion_err!("{err}"))?;

    let query_lines = query
        .split(";")
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .collect::<Vec<&str>>();

    let start_time = Instant::now();

    for line in query_lines {
        // ctx.sql(line).await?.limit(0, Some(1))?.show().await?;
        _ = Some(ctx.sql(line).await?.collect().await?);
    }

    let duration = start_time.elapsed();

    Ok(duration)
}

#[tokio::main]
async fn main() -> DataFusionResult<()> {
    let queries_of_interest = [9, 21, 2, 20, 17];

    let partitions = [1, 2, 4, 8, 16];

    let scale_factors = [0.1, 1.0, 10.0];

    // We don't want first time loading to influence values, so run the first query once and discard the results
    let _ = query_execution_time(1, 0.1, 1).await?;

    println!("query_number,scale_factor,num_partitions,duration_us");

    for query in queries_of_interest {
        for num_partitions in partitions {
            for scale_factor in scale_factors {
                let duration =
                    query_execution_time(query, scale_factor, num_partitions).await?.as_micros();
                println!("{query},{scale_factor:3},{num_partitions},{duration}");
            }
        }
    }



    Ok(())
}
