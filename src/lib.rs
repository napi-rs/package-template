#![deny(clippy::all)]

use aws_config::BehaviorVersion;
use napi_derive::napi;
use napi::{Result, error};
use aws_sdk_lambda;
use aws_sdk_lambda::types::InvocationType::RequestResponse;

use serde_json::json;

pub mod aws_client;
pub mod flowstate_client;
pub mod flowstate_env;
pub mod function_registration;
pub mod lambda_middleware;
pub mod linked_daal;

mod utils;

// Any library-wide code or documentation can go here
#[napi]
pub fn plus_100(input: u32) -> u32 {
  input + 110
}

#[napi]
pub fn hi(input: u32) -> u32 {
  input + 500
}

#[napi]
pub async fn continuously_retry_function(lambda_function_arn: String) -> Result<String> {
  let config = aws_config::defaults(BehaviorVersion::latest())
            .region("us-east-1")
            .load()
            .await;
  
  let lambda_client = aws_sdk_lambda::Client::new(&config);

  loop {
    let sebastian = json!({
        "key": "value",
    });

    let serialized_input = serde_json::to_vec(&sebastian).map_err(
      |_err| napi::Error::new(napi::Status::InvalidArg, "serialize input failed")
    )?;

    let invoke_result = lambda_client
    .invoke()
    .invocation_type(RequestResponse)
    .payload(serialized_input.into())
    .function_name(&lambda_function_arn)
    .send()
    .await;

    match invoke_result {
      Ok(invoke_output) => {
        let serialized_output: String = invoke_output.payload.map_or_else(
          || "".to_string(),
          |blob| String::from_utf8_lossy(&blob.into_inner()).into_owned(),
        );

        return Ok(serialized_output)
      },
      Err(e) => {
        println!("{:?}", e);
      }
    }
  }
}