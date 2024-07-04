//! Abstractions based on SDK [crate] for defining workflow and activity definitions in idiomatic Async Rust
//! This is an experimental interface. Other convenience facilities and macros will be included if needed.
//!
//! Refer to the proposal for a future production ready API [here]((https://github.com/temporalio/sdk-core/pull/550).
//!
//! There are two main types of high-level functions.
//! 1) Register functions (Example: [into_workflow]) to register Workflow functions.
//! 2) Command functions (Example: [execute_activity], [execute_child_workflow]) for workflow [commands](https://docs.temporal.io/workflows#command).
//!
//! Defining workflows should feel like just a normal Rust(Async) program. Use [Prelude](crate::prelude) for easy import of types needed.
//!
//! An example workflow definition is below. For more, refer to tests and examples.
//! ```no_run
//! use temporal_sdk::prelude::registry::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut worker = worker::worker().await.unwrap();
//!
//!     worker.register_activity(
//!         "sdk_example_activity",
//!         activity::sdk_example_activity,
//!     );
//!
//!     worker.register_wf(
//!         "sdk_example_workflow",
//!         into_workflow(workflow::sdk_example_workflow),
//!     );
//!
//!     worker.run().await?;
//!
//!     Ok(())
//! }
//!
//! mod worker {
//!     use std::{str::FromStr, sync::Arc};
//!     use temporal_sdk::prelude::worker::*;
//!
//!     pub(crate) async fn worker() -> Result<Worker, Box<dyn std::error::Error>> {
//!         let server_options = sdk_client_options(Url::from_str("http://localhost:7233")?).build()?;
//!         let client = server_options.connect("default", None).await?;
//!         let telemetry_options = TelemetryOptionsBuilder::default().build()?;
//!         let runtime = CoreRuntime::new_assume_tokio(telemetry_options)?;
//!         let task_queue = "example-task-queue";
//!         let worker_config = WorkerConfigBuilder::default()
//!             .namespace("default")
//!             .task_queue(task_queue)
//!             .worker_build_id("example-rust-worker")
//!             .build()?;
//!         let core_worker = init_worker(&runtime, worker_config, client)?;
//!         Ok(Worker::new_from_core(Arc::new(core_worker), task_queue))
//!     }
//! }
//!
//! mod activity {
//!     use temporal_sdk::prelude::activity::*;
//!
//!     #[derive(Default, Deserialize, Serialize, Debug, Clone)]
//!     pub struct ActivityInput {
//!         pub language: String,
//!         pub kind: String,
//!     }
//!
//!     #[derive(Default, Deserialize, Serialize, Debug, Clone)]
//!     pub struct ActivityOutput {
//!         pub kind: String,
//!         pub platform: String,
//!         pub features: Vec<String>,
//!     }
//!
//!     pub async fn sdk_example_activity(
//!         _ctx: ActContext,
//!         input: ActivityInput,
//!     ) -> Result<(String, ActivityOutput), ActivityError> {
//!         Ok((
//!             format!("Workflow written in {} {}", input.kind, input.language),
//!             ActivityOutput {
//!                 kind: "worker".to_string(),
//!                 platform: "temporal".to_string(),
//!                 features: vec![
//!                     "performance".to_string(),
//!                     "async".to_string(),
//!                     "type-safe".to_string(),
//!                     "resource-efficient".to_string(),
//!                 ],
//!             },
//!         ))
//!     }
//! }
//!
//! mod workflow {
//!     use super::activity::*;
//!     use temporal_sdk::prelude::workflow::*;
//!
//!     #[derive(Default, Deserialize, Serialize, Debug, Clone)]
//!     pub struct WorkflowInput {
//!         pub code: String,
//!         pub kind: String,
//!     }
//!
//!     pub async fn sdk_example_workflow(
//!         ctx: WfContext,
//!         input: WorkflowInput,
//!     ) -> Result<ActivityOutput, anyhow::Error> {
//!         let output = execute_activity(
//!             &ctx,
//!             ActivityOptions {
//!                 activity_id: Some("sdk_example_activity".to_string()),
//!                 activity_type: "sdk_example_activity".to_string(),
//!                 schedule_to_close_timeout: Some(Duration::from_secs(5)),
//!                 ..Default::default()
//!             },
//!             sdk_example_activity,
//!             ActivityInput {
//!                 language: input.code,
//!                 kind: input.kind,
//!             },
//!         )
//!         .await;
//!         match output {
//!             Ok(output) => Ok(output.1),
//!             Err(e) => Err(anyhow::Error::from(e)),
//!         }
//!     }
//! }
//! ```

use crate::{
    ActContext, ActivityError, ActivityOptions, ChildWorkflowOptions, WfContext, WfExitValue,
};
use anyhow::anyhow;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::time::Duration;
use std::{fmt::Debug, future::Future};
use temporal_sdk_core_protos::coresdk::{
    activity_result::activity_resolution, child_workflow::child_workflow_result, AsJsonPayloadExt,
    FromJsonPayloadExt,
};

/// Trait to represent an async function with 2 arguments
pub trait AsyncFn<Arg0, Arg1>: Fn(Arg0, Arg1) -> Self::OutputFuture {
    /// Output type of the async function which implements serde traits
    type Output;
    /// Future of the output
    type OutputFuture: Future<Output = <Self as AsyncFn<Arg0, Arg1>>::Output> + Send + 'static;
}

impl<F: ?Sized, Fut, Arg0, Arg1> AsyncFn<Arg0, Arg1> for F
where
    F: Fn(Arg0, Arg1) -> Fut,
    Fut: Future + Send + 'static,
{
    type Output = Fut::Output;
    type OutputFuture = Fut;
}

/// Execute activity which takes [ActContext] and an argument and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [into_activity] to register the activity with the worker.
pub async fn execute_activity<A, F, R>(
    ctx: &WfContext,
    options: ActivityOptions,
    _f: F,
    a: A,
) -> Result<R, anyhow::Error>
where
    F: AsyncFn<ActContext, A, Output = Result<R, ActivityError>> + Send + Sync + 'static,
    A: AsJsonPayloadExt + Debug,
    R: FromJsonPayloadExt + Debug,
{
    let input = A::as_json_payload(&a).expect("serializes fine");
    let activity_type = if options.activity_type.is_empty() {
        std::any::type_name::<F>().to_string()
    } else {
        options.activity_type
    };
    let options = ActivityOptions {
        activity_type,
        input,
        ..options
    };
    let activity_resolution = ctx.activity(options).await;

    match activity_resolution.status {
        Some(status) => match status {
            activity_resolution::Status::Completed(success) => {
                Ok(R::from_json_payload(&success.result.unwrap()).unwrap())
            }
            activity_resolution::Status::Failed(failure) => Err(anyhow::anyhow!("{:?}", failure)),
            activity_resolution::Status::Cancelled(reason) => Err(anyhow::anyhow!("{:?}", reason)),
            activity_resolution::Status::Backoff(reason) => Err(anyhow::anyhow!("{:?}", reason)),
        },
        None => panic!("activity task failed {activity_resolution:?}"),
    }
}

/// Register child workflow which takes [WfContext] and an argument and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [execute_child_workflow] to execute the workflow in the workflow definition.
pub fn into_workflow<A, F, R, O>(
    f: F,
) -> impl Fn(WfContext) -> BoxFuture<'static, Result<WfExitValue<O>, anyhow::Error>> + Send + Sync
where
    A: FromJsonPayloadExt + Send,
    F: AsyncFn<WfContext, A, Output = Result<R, anyhow::Error>> + Send + Sync + 'static,
    R: Into<WfExitValue<O>>,
    O: AsJsonPayloadExt + Debug,
{
    move |ctx: WfContext| match A::from_json_payload(&ctx.get_args()[0]) {
        Ok(a) => (f)(ctx, a).map(|r| r.map(|r| r.into())).boxed(),
        Err(e) => async move { Err(e.into()) }.boxed(),
    }
}

/// Execute child workflow which takes [WfContext] and an argument and returns a [Result] with 'R'
/// and [anyhow::Error] where 'R' implements [serde] traits for
/// serialization into Temporal [Payload](temporal_sdk_core_protos::temporal::api::common::v1::Payload).
/// Use [into_workflow] to register the workflow with the worker
pub async fn execute_child_workflow<A, F, R>(
    ctx: &WfContext,
    options: ChildWorkflowOptions,
    _f: F,
    a: A,
) -> Result<R, anyhow::Error>
where
    F: AsyncFn<WfContext, A, Output = Result<R, anyhow::Error>> + Send + Sync + 'static,
    A: AsJsonPayloadExt + Debug,
    R: FromJsonPayloadExt + Debug,
{
    let input = A::as_json_payload(&a).expect("serializes fine");
    let workflow_type = if options.workflow_type.is_empty() {
        std::any::type_name::<F>().to_string()
    } else {
        options.workflow_type
    };

    let child = ctx.child_workflow(ChildWorkflowOptions {
        workflow_type,
        input: vec![input],
        ..options
    });

    let started = child
        .start(ctx)
        .await
        .into_started()
        .expect("Child should start OK");

    match started.result().await.status {
        Some(status) => match status {
            child_workflow_result::Status::Completed(success) => {
                Ok(R::from_json_payload(&success.result.unwrap()).unwrap())
            }
            child_workflow_result::Status::Failed(failure) => Err(anyhow::anyhow!("{:?}", failure)),
            child_workflow_result::Status::Cancelled(reason) => {
                Err(anyhow::anyhow!("{:?}", reason))
            }
        },
        None => Err(anyhow!("Unexpected child WF status")),
    }
}

/// Sleep for a given duration
pub async fn sleep(ctx: &WfContext, duration: Duration) {
    ctx.timer(duration).await;
}
