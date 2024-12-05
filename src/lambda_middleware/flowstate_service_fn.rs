/// COPIED FROM `service_fn` from `lambda_middleware`
/// Differences:
/// - Expects a function that also takes a flowstate client
/// - Error type must be a BoxError (see if we can make this more generic later)
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use lambda_runtime::LambdaEvent;
use tower_service::Service;

use anyhow::Result;
use lambda_runtime_api_client::BoxError;
use serde_json::Value;

use crate::aws_client::AWSClient;
use crate::flowstate_client::{FlowstateClient, InputWrapper};

pub fn flowstate_service_fn<T>(f: T, aws_region: &'static str) -> FlowstateServiceFn<T> {
    FlowstateServiceFn { f, aws_region }
}

/// A [`Service`] implemented by a closure.
///
/// See [`service_fn`] for more details.
#[derive(Copy, Clone)]
pub struct FlowstateServiceFn<T> {
    f: T,
    aws_region: &'static str,
}

impl<T> fmt::Debug for FlowstateServiceFn<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlowstateServiceFn")
            .field("f", &format_args!("{}", std::any::type_name::<T>()))
            .finish()
    }
}

// Note that we're doing a more specifc (weaker) implementation than the generic `ServiceFn`:
// - We require a `LambdaEvent<Value>` rather than any request
// - `Error` must be a `BoxError`
// - `T` must be cloneable and static
//
// We'll probably only use these types anyway so it's fine for now, but we can see how to make them
// more generic in the future if necessary
impl<T, F> Service<LambdaEvent<Value>> for FlowstateServiceFn<T>
where
    T: FnMut(LambdaEvent<Value>, FlowstateClient) -> F + Clone + 'static,
    F: Future<Output = Result<(String, FlowstateClient), BoxError>>,
{
    // make `Response` more generic later? in order to do so, we'll need to modify `wrapper_end`.
    // some proposals:
    // 1. make it a `serde_json::Value`
    // 2. keep it generic, and have the user implement ways to serialize it/add into the db they're using.
    //    we can include 1 as the default implementation
    // i think we should do 1 and add 2 if we need
    type Response = String;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<String, BoxError>>>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), BoxError>> {
        Ok(()).into()
    }

    fn call(&mut self, req: LambdaEvent<Value>) -> Self::Future {
        let mut inner_function = self.f.clone();
        let aws_region = self.aws_region;

        let future = async move {
            // Create the flowstate client here, it'll get passed to the call
            let function_name = &req.context.invoked_function_arn;
            let mut flowstate_client: FlowstateClient =
                create_flowstate_client(aws_region, function_name).await?;

            // Pre-function wrapper logic
            let input = req.payload.clone();
            // check: is it correct that the caller step is always 0 here? i think it should be but make sure
            let wrapped_input = InputWrapper::new(&flowstate_client, 0, input);
            flowstate_client.wrapper_start(&wrapped_input).await?;

            // User-written logic
            let (result, flowstate_client) = inner_function(req, flowstate_client).await?;

            // Post-function wrapper logic
            flowstate_client
                .wrapper_end(wrapped_input, Ok(result.clone()))
                .await?;

            Ok(result)
        };

        Box::pin(future)
    }
}

/* ------------------------------------------- UTILS ------------------------------------------- */

async fn create_flowstate_client(
    region_name: &str,
    function_name: &str,
) -> Result<FlowstateClient> {
    let aws_client = Arc::new(AWSClient::new(region_name).await);
    let flowstate_client = FlowstateClient::new_async(aws_client, function_name)
        .await
        .expect("Failed to create FlowstateClient");

    Ok(flowstate_client)
}
