//! Prelude for easy importing of required types when defining workflow and activity definitions

/// Registry prelude
#[allow(unused_imports)]
pub mod registry {
    pub use crate::workflow::into_workflow;
}

/// Activity prelude
#[allow(unused_imports)]
pub mod activity {
    pub use crate::{ActContext, ActExitValue, ActivityError};
    pub use serde::{Deserialize, Serialize};
    pub use temporal_sdk_core_protos::temporal::api::failure::v1::Failure;
}

/// Workflow prelude
#[allow(unused_imports)]
pub mod workflow {
    pub use crate::workflow::AsyncFn;
    pub use crate::workflow::{execute_activity, execute_child_workflow, sleep};
    pub use crate::{
        ActivityOptions, ChildWorkflowOptions, LocalActivityOptions, Signal, SignalData,
        SignalWorkflowOptions, WfContext, WfExitValue, WorkflowResult,
    };
    pub use futures::FutureExt;
    pub use serde::{Deserialize, Serialize};
    pub use std::{
        fmt::{Debug, Display},
        future::Future,
        time::Duration,
    };
    pub use temporal_sdk_core_protos::{
        coresdk::{
            activity_result::{self, activity_resolution},
            child_workflow::{child_workflow_result, Failure, Success},
            workflow_commands::ActivityCancellationType,
            AsJsonPayloadExt, FromPayloadsExt,
        },
        temporal::api::common::v1::Payload,
    };
}

/// Worker prelude
#[allow(unused_imports)]
pub mod worker {
    pub use crate::{sdk_client_options, Worker};
    pub use temporal_sdk_core::{init_worker, CoreRuntime, Url};
    pub use temporal_sdk_core_api::{
        telemetry::TelemetryOptionsBuilder, worker::WorkerConfigBuilder,
    };
}

/// Client prelude
#[allow(unused_imports)]
pub mod client {
    pub use crate::sdk_client_options;
    pub use temporal_client::{
        Client, RetryClient, WfClientExt, WorkflowClientTrait, WorkflowOptions,
    };
}
