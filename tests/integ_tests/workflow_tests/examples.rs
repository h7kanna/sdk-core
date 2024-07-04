use serde_json::json;
use temporal_client::WorkflowOptions;
use temporal_sdk::prelude::registry::*;
use temporal_sdk_core_protos::coresdk::AsJsonPayloadExt;
use temporal_sdk_core_test_utils::CoreWfStarter;

mod activity {
    use temporal_sdk::prelude::activity::*;

    #[derive(Default, Deserialize, Serialize, Debug, Clone)]
    pub(crate) struct ActivityInput(pub(crate) String, pub(crate) i32);
    #[derive(Default, Deserialize, Serialize, Debug, Clone)]
    pub(crate) struct ActivityOutput(pub(crate) String, pub(crate) i32);
    pub(crate) async fn activity(
        _ctx: ActContext,
        input: ActivityInput,
    ) -> Result<ActivityOutput, ActivityError> {
        Ok(ActivityOutput(input.0, input.1))
    }
}

mod workflow {
    use crate::integ_tests::workflow_tests::examples::activity::{self, *};
    use temporal_sdk::prelude::workflow::*;

    #[derive(Default, Deserialize, Serialize, Debug, Clone)]
    pub(crate) struct ChildInput {
        pub(crate) echo: (String,),
    }
    #[derive(Default, Deserialize, Serialize, Debug, Clone)]
    pub(crate) struct ChildOutput {
        pub(crate) result: (String,),
    }

    pub(crate) async fn child_workflow(
        _ctx: WfContext,
        _input: ChildInput,
    ) -> Result<ChildOutput, anyhow::Error> {
        Ok(ChildOutput {
            result: ("success".to_string(),),
        })
    }

    #[derive(Default, Deserialize, Serialize, Debug, Clone)]
    pub(crate) struct Input {
        pub(crate) one: String,
        pub(crate) two: i64,
    }
    #[derive(Default, Deserialize, Serialize, Debug, Clone)]
    pub(crate) struct Output {
        pub(crate) result: (String,),
    }

    pub(crate) async fn examples_workflow(
        ctx: WfContext,
        _input: Input,
    ) -> Result<Output, anyhow::Error> {
        let activity_timeout = Duration::from_secs(5);

        let _stop = execute_activity(
            &ctx,
            ActivityOptions {
                activity_id: Some("activity".to_string()),
                schedule_to_close_timeout: Some(activity_timeout),
                ..Default::default()
            },
            activity::activity,
            ActivityInput("hello".to_string(), 7),
        )
        .await;

        let child_output = execute_child_workflow(
            &ctx,
            ChildWorkflowOptions {
                workflow_id: "child_workflow".to_owned(),
                ..Default::default()
            },
            child_workflow,
            ChildInput {
                echo: ("child_workflow".to_string(),),
            },
        )
        .await;
        match child_output {
            Ok(r) => Ok(Output { result: r.result }),
            Err(e) => Err(e),
        }
    }
}

#[tokio::test]
async fn example_workflows_test() {
    use crate::integ_tests::workflow_tests::examples::activity;
    use crate::integ_tests::workflow_tests::examples::workflow;

    let mut starter = CoreWfStarter::new("sdk-example-workflows");
    let mut worker = starter.worker().await;

    // Register activities

    worker.register_activity(
        "integ_tests::integ_tests::workflow_tests::examples::activity::activity",
        activity::activity,
    );

    // Register child workflows

    worker.register_wf(
        "integ_tests::integ_tests::workflow_tests::examples::workflow::child_workflow",
        into_workflow(workflow::child_workflow),
    );

    worker.register_wf(
        "integ_tests::integ_tests::workflow_tests::examples::workflow::examples_workflow",
        into_workflow(workflow::examples_workflow),
    );

    // Run parent workflow

    worker
        .submit_wf(
            "examples_workflow".to_string(),
            "integ_tests::integ_tests::workflow_tests::examples::workflow::examples_workflow"
                .to_string(),
            vec![json!({
                "one":"one",
                "two":42
            })
            .as_json_payload()
            .expect("serialize fine")],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}
