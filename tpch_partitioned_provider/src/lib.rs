use async_trait::async_trait;
use datafusion::arrow::array::{
    Date32Array, Decimal128Array, Int32Array, Int64Array, RecordBatch, StringViewArray,
};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::hash_utils::HashValue;
use datafusion::common::{exec_datafusion_err, exec_err};
use datafusion::datasource::TableType;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, Partitioning, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::limit::LimitStream;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::Stream;
use futures::StreamExt;
use std::any::Any;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::runtime::Handle;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tpchgen::generators::{LineItem, LineItemGenerator, LineItemGeneratorIterator};
use tpchgen_arrow::conversions::{decimal128_array_from_iter, to_arrow_date32};
use tpchgen_arrow::{LineItemArrow, RecordBatchIterator};

#[derive(Debug)]
struct LineItemPartitionedExec {
    projected_schema: SchemaRef,
    scale_factor: f64,
    target_partitions: usize,
    props: PlanProperties,
    projection: Option<Vec<usize>>,
}

impl DisplayAs for LineItemPartitionedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        f.write_str("LineItemPartitionedExec")
    }
}

impl LineItemPartitionedExec {
    fn new(scale_factor: f64, target_partitions: usize, projection: Option<Vec<usize>>) -> Self {
        // let schema = Self::schema();
        let projected_schema = match &projection {
            None => Self::schema(),
            Some(proj) => Arc::new(
                Self::schema()
                    .project(proj)
                    .expect("Unable to project schema"),
            ),
        };

        let order_key = "l_orderkey";
        let orderings = projected_schema
            .fields()
            .iter()
            .enumerate()
            .find(|(_, field)| field.name() == order_key)
            .map(|(idx, _)| Column::new(order_key, idx))
            .map(|order_col| Arc::new(order_col) as Arc<dyn PhysicalExpr>)
            .map(|order_col| {
                vec![PhysicalSortExpr::new(
                    order_col,
                    SortOptions::new(false, true),
                )]
            })
            .map(|physical_ordering| vec![LexOrdering::new(physical_ordering)])
            .unwrap_or_default();

        let eq_properties =
            EquivalenceProperties::new_with_orderings(Arc::clone(&projected_schema), &orderings);

        let output_partitioning = Partitioning::UnknownPartitioning(target_partitions);

        let props = PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            projected_schema,
            scale_factor,
            target_partitions,
            props,
            projection,
        }
    }

    pub fn schema() -> SchemaRef {
        Arc::clone(LineItemArrow::new(LineItemGenerator::new(0.1, 1, 1)).schema())
    }
}

impl ExecutionPlan for LineItemPartitionedExec {
    fn name(&self) -> &str {
        "LineItemPartitionedExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let gen = LineItemGenerator::new(
            self.scale_factor,
            partition as i32,
            self.target_partitions as i32,
        );
        let line_item_iter = LineItemArrow::new(gen);

        let stream = futures::stream::iter(line_item_iter.map(Ok));

        let projected_stream: SendableRecordBatchStream = match self.projection.clone() {
            Some(projection) => Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.projected_schema),
                stream.map(move |x| {
                    x.and_then(|b| b.project(projection.as_ref()).map_err(Into::into))
                }),
            )),
            None => Box::pin(RecordBatchStreamAdapter::new(Self::schema(), stream)),
        };
        // Ok(match self.limit {
        //     None => projected_stream,
        //     Some(fetch) => {
        //         let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        //         Box::pin(LimitStream::new(
        //             projected_stream,
        //             0,
        //             Some(fetch),
        //             baseline_metrics,
        //         ))
        //     }
        // })

        Ok(projected_stream)
    }
}

#[derive(Debug)]
pub struct LineItemPartitionedProvider {
    scale_factor: f64,
    target_partitions: i32,
}
impl LineItemPartitionedProvider {
    pub fn new(scale_factor: f64, target_partitions: i32) -> Self {
        Self {
            scale_factor,
            target_partitions,
        }
    }
}
#[async_trait]
impl TableProvider for LineItemPartitionedProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        LineItemPartitionedExec::schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let num_partitions = state.config().target_partitions();
        let exec =
            LineItemPartitionedExec::new(self.scale_factor, num_partitions, projection.cloned());
        Ok(Arc::new(exec) as Arc<dyn ExecutionPlan>)
    }
}
