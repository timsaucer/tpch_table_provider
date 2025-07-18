use datafusion::common::exec_datafusion_err;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::fmt::Display;
use std::time::{Duration, Instant};
use tpch_comparisons::{
    register_in_memory_tables, register_streaming_tables,
    register_streaming_with_partitioned_tables,
};
use tpch_queries::get_tpch_query;

#[derive(Clone, Copy)]
enum RegistrationType {
    InMemory,
    StreamingProviders,
    PartitionedProviders,
}

impl Display for RegistrationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            RegistrationType::InMemory => write!(f, "InMemory"),
            RegistrationType::StreamingProviders => write!(f, "StreamingProviders"),
            RegistrationType::PartitionedProviders => write!(f, "PartitionedProviders"),
        }
    }
}

async fn query_execution_time(
    query_number: usize,
    scale_factor: f64,
    registration_type: RegistrationType,
) -> DataFusionResult<Duration> {
    let num_partitions = 14;

    let config = SessionConfig::new().with_target_partitions(num_partitions);
    let ctx = SessionContext::new_with_config(config);
    let num_partitions = num_partitions as i32;

    let start_time = Instant::now();

    match registration_type {
        RegistrationType::InMemory => {
            register_in_memory_tables(&ctx, scale_factor)?;
        }
        RegistrationType::StreamingProviders => {
            register_streaming_tables(&ctx, scale_factor, num_partitions)?;
        }
        RegistrationType::PartitionedProviders => {
            register_streaming_with_partitioned_tables(&ctx, scale_factor, num_partitions)?;
        }
    }

    let query = get_tpch_query(query_number).map_err(|err| exec_datafusion_err!("{err}"))?;

    let query_lines = query
        .split(";")
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .collect::<Vec<&str>>();

    for line in query_lines {
        // ctx.sql(line).await?.limit(0, Some(1))?.show().await?;
        _ = Some(ctx.sql(line).await?.collect().await?);
    }

    let duration = start_time.elapsed();

    Ok(duration)
}

async fn print_outs() -> DataFusionResult<()> {
    let cols = [
        "l_returnflag",
        "l_linestatus",
        "l_quantity",
        "l_extendedprice",
        "l_discount",
        "l_tax",
        "l_shipdate",
    ];
    {
        let ctx = SessionContext::new();
        register_in_memory_tables(&ctx, 0.1)?;
        ctx.table("lineitem").await?.show_limit(5).await?;
    }

    {
        let ctx = SessionContext::new();
        register_streaming_with_partitioned_tables(&ctx, 0.1, 2)?;
        ctx.table("lineitem")
            .await?
            .select_columns(&cols)?
            .show_limit(5)
            .await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> DataFusionResult<()> {
    print_outs().await?;

    // let queries_of_interest = [9, 21, 2, 20, 17];
    let queries_of_interest = (1..23).collect::<Vec<_>>();

    // let scale_factors = (1..21).map(f64::from).collect::<Vec<_>>();
    let scale_factors = [0.1, 1.0];

    let run_types = [
        RegistrationType::InMemory,
        RegistrationType::StreamingProviders,
        RegistrationType::PartitionedProviders,
    ];

    // We don't want first time loading to influence values, so run the first query once and discard the results
    let _ = query_execution_time(1, 0.1, RegistrationType::InMemory).await?;
    let _ = query_execution_time(1, 0.1, RegistrationType::StreamingProviders).await?;

    println!("query_number,run_type,scale_factor,duration_us");

    for query in queries_of_interest {
        for run_type in run_types {
            for scale_factor in &scale_factors {
                let duration = query_execution_time(query, *scale_factor, run_type)
                    .await?
                    .as_micros();
                let duration = duration as f64 / 1_000_000.0;
                println!("{query},{run_type},{scale_factor:3},{duration}");
            }
        }
    }

    Ok(())
}
