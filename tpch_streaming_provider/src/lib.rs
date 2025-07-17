use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::TableType;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::sync::Arc;
use tpchgen_arrow::RecordBatchIterator;

macro_rules! table_provider_new_fn {
    (true) => {
        pub fn new(scale_factor: f64, target_partitions: i32) -> Self {
            Self {
                scale_factor,
                target_partitions,
            }
        }
    };
    (false) => {
        pub fn new(scale_factor: f64) -> Self {
            Self {
                scale_factor,
                target_partitions: 1,
            }
        }
    };
}

macro_rules! define_tpch_table_provider {
    ($TABLE_PROVIDER_NAME:ident, $TABLE_STREAM_NAME:ident, $GENERATOR:ty, $ARROW_GENERATOR:ty, $USES_PARTITIONS:tt) => {
        #[derive(Debug)]
        struct $TABLE_STREAM_NAME {
            schema: SchemaRef,
            scale_factor: f64,
            partition: i32,
            num_partitions: i32,
        }

        impl $TABLE_STREAM_NAME {
            fn new(scale_factor: f64, partition: i32, num_partitions: i32) -> Self {
                let schema = Self::schema();
                Self {
                    schema,
                    partition,
                    num_partitions,
                    scale_factor,
                }
            }

            fn schema() -> SchemaRef {
                // Temporarily create a generator to get the schema since it isn't exposed elsewhere.
                let gen = <$GENERATOR>::new(1.0, 1, 1);
                let arrow_gen = <$ARROW_GENERATOR>::new(gen);
                Arc::clone(arrow_gen.schema())
            }
        }

        impl PartitionStream for $TABLE_STREAM_NAME {
            fn schema(&self) -> &SchemaRef {
                &self.schema
            }

            fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
                let gen = <$GENERATOR>::new(self.scale_factor, self.partition, self.num_partitions);
                let iter = <$ARROW_GENERATOR>::new(gen);
                let schema = Arc::clone(iter.schema());

                let stream = futures::stream::iter(iter.map(Ok));
                let adapter = RecordBatchStreamAdapter::new(schema, stream);
                Box::pin(adapter)
            }
        }

        #[derive(Debug)]
        pub struct $TABLE_PROVIDER_NAME {
            scale_factor: f64,
            target_partitions: i32,
        }

        impl $TABLE_PROVIDER_NAME {
            table_provider_new_fn!($USES_PARTITIONS);
            // pub fn new(scale_factor: f64, target_partitions: i32) -> Self {
            //     num_partition_streams!($USES_PARTITIONS);
            //
            //     Self {
            //         scale_factor,
            //         target_partitions,
            //     }
            // }
        }

        #[async_trait]
        impl TableProvider for $TABLE_PROVIDER_NAME {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn schema(&self) -> SchemaRef {
                $TABLE_STREAM_NAME::schema()
            }

            fn table_type(&self) -> TableType {
                TableType::Base
            }

            async fn scan(
                &self,
                _state: &dyn Session,
                projection: Option<&Vec<usize>>,
                _filters: &[Expr],
                limit: Option<usize>,
            ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
                let partitions = (0..self.target_partitions)
                    .into_iter()
                    .map(|idx| {
                        <$TABLE_STREAM_NAME>::new(self.scale_factor, idx, self.target_partitions)
                    })
                    .map(|stream| Arc::new(stream) as Arc<dyn PartitionStream>)
                    .collect::<Vec<_>>();

                let x = StreamingTableExec::try_new(
                    <$TABLE_STREAM_NAME>::schema(),
                    partitions,
                    projection,
                    vec![],
                    false,
                    limit,
                );

                Ok(Arc::new(x?) as Arc<dyn ExecutionPlan>)
            }
        }
    };
}

define_tpch_table_provider!(
    CustomerStreamingProvider,
    CustomerStream,
    tpchgen::generators::CustomerGenerator,
    tpchgen_arrow::CustomerArrow,
    true
);
define_tpch_table_provider!(
    LineItemStreamingProvider,
    LineItemStream,
    tpchgen::generators::LineItemGenerator,
    tpchgen_arrow::LineItemArrow,
    true
);
define_tpch_table_provider!(
    NationStreamingProvider,
    NationStream,
    tpchgen::generators::NationGenerator,
    tpchgen_arrow::NationArrow,
    false
);
define_tpch_table_provider!(
    OrderStreamingProvider,
    OrderStream,
    tpchgen::generators::OrderGenerator,
    tpchgen_arrow::OrderArrow,
    true
);
define_tpch_table_provider!(
    PartStreamingProvider,
    PartStream,
    tpchgen::generators::PartGenerator,
    tpchgen_arrow::PartArrow,
    true
);
define_tpch_table_provider!(
    PartSuppStreamingProvider,
    PartSuppStream,
    tpchgen::generators::PartSuppGenerator,
    tpchgen_arrow::PartSuppArrow,
    true
);
define_tpch_table_provider!(
    RegionStreamingProvider,
    RegionStream,
    tpchgen::generators::RegionGenerator,
    tpchgen_arrow::RegionArrow,
    false
);
define_tpch_table_provider!(
    SupplierStreamingProvider,
    SupplierStream,
    tpchgen::generators::SupplierGenerator,
    tpchgen_arrow::SupplierArrow,
    true
);

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_all_providers_sf1() -> DataFusionResult<()> {
        let ctx = SessionContext::new();

        let customer = Arc::new(CustomerStreamingProvider::new(1.0, 5));
        let df = ctx.read_table(customer)?;
        assert_eq!(150000, df.count().await?);

        let line_item = Arc::new(LineItemStreamingProvider::new(1.0, 5));
        let df = ctx.read_table(line_item)?;
        assert_eq!(6000810, df.count().await?);

        let nation = Arc::new(NationStreamingProvider::new(1.0));
        let df = ctx.read_table(nation)?;
        assert_eq!(25, df.count().await?);

        let order = Arc::new(OrderStreamingProvider::new(1.0, 5));
        let df = ctx.read_table(order)?;
        assert_eq!(1500000, df.count().await?);

        let part = Arc::new(PartStreamingProvider::new(1.0, 5));
        let df = ctx.read_table(part)?;
        assert_eq!(200000, df.count().await?);

        let part_supp = Arc::new(PartSuppStreamingProvider::new(1.0, 5));
        let df = ctx.read_table(part_supp)?;
        assert_eq!(800000, df.count().await?);

        let region = Arc::new(RegionStreamingProvider::new(1.0));
        let df = ctx.read_table(region)?;
        assert_eq!(5, df.count().await?);

        let supplier = Arc::new(SupplierStreamingProvider::new(1.0, 5));
        let df = ctx.read_table(supplier)?;
        assert_eq!(10000, df.count().await?);

        Ok(())
    }
}
