use std::sync::Arc;

use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::types::{AttributeValue, ProvisionedThroughput, AttributeDefinition, KeySchemaElement};
use aws_sdk_lambda::types::InvocationType::RequestResponse;
use flowstate::flowstate_client::FlowstateClient;
use flowstate::linked_daal::LinkedDAAL;
use flowstate::{aws_client::AWSClient, linked_daal};
use napi::{Error, Result};
use napi_derive::napi;
use serde_json::json;

const CRASH_TABLE: &str = "crash_table";
const INVENTORY_TABLE: &str = "inventory_table";
const AWS_ENDPOINT_URL: &str = "http://localhost:4566";

#[napi]
pub async fn continuously_retry_function(lambda_function_arn: String) -> Result<String> {
  let config = aws_config::defaults(BehaviorVersion::latest())
    .region("us-east-1")
    .endpoint_url(AWS_ENDPOINT_URL)
    .load()
    .await;

  let lambda_client = aws_sdk_lambda::Client::new(&config);

  loop {
    let sebastian = json!({
        "key": "value",
    });

    let serialized_input = serde_json::to_vec(&sebastian)
      .map_err(|_err| napi::Error::new(napi::Status::InvalidArg, "serialize input failed"))?;

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

        return Ok(serialized_output);
      }
      Err(e) => {
        println!("{:?}", e);
      }
    }
  }
}

#[napi]
pub async fn create_inventory_table() -> Result<()> {
  let aws_client =
    AWSClient::new_with_endpoints("us-east-1", Some(AWS_ENDPOINT_URL.to_string()), None).await;
  let mut flowstate_client = FlowstateClient::new_async(Arc::new(aws_client), "ignore")
    .await
    .map_err(|e| Error::from_reason(format!("Creating flowstate client failed {:?}", e)))?;

  let table_name = INVENTORY_TABLE;
  let inventory_table = LinkedDAAL::use_linked_daal(&flowstate_client.aws_client, table_name).await;
  flowstate_client.register_daal(table_name, inventory_table.clone());

  flowstate_client.register_daal(table_name, inventory_table);
  flowstate_client
    .write(table_name, "website_inventory", "1000")
    .await
    .map_err(|e| Error::from_reason(format!("Initializing website inventory failed {:?}", e)))?;

  println!("finished creating inventory table!");
  Ok(())
}

#[napi]
pub async fn create_crash_table() -> Result<()> {
  let config = aws_config::defaults(BehaviorVersion::latest())
    .region("us-east-1")
    .endpoint_url(AWS_ENDPOINT_URL)
    .load()
    .await;

  let aws_client = aws_sdk_dynamodb::Client::new(&config);

  let throughput = ProvisionedThroughput::builder()
    .read_capacity_units(5)
    .write_capacity_units(5)
    .build()
    .map_err(|e| Error::from_reason(format!("Throughput provisioning {:?}", e)))?;

  let attribute_dfns = AttributeDefinition::builder()
    .attribute_name("id")
    .attribute_type("S".into())
    .build()
    .map_err(|e| Error::from_reason(format!("Attribute definitions {:?}", e)))?;

  let key_schema = KeySchemaElement::builder()
    .attribute_name("id")
    .key_type("HASH".into())
    .build()
    .map_err(|e| Error::from_reason(format!("Key scheme {:?}", e)))?;

  aws_client
    .create_table()
    .table_name(CRASH_TABLE)
    .attribute_definitions(attribute_dfns)
    .key_schema(key_schema)
    .provisioned_throughput(throughput)
    .send()
    .await
    .map_err(|e| Error::from_reason(format!("Creating crash table failed {:?}", e)))?;

  aws_client
    .put_item()
    .table_name(CRASH_TABLE)
    .item("id", AttributeValue::S("mode".to_string()))
    .item("value", AttributeValue::S("0".to_string()))
    .send()
    .await
    .map_err(|e| Error::from_reason(format!("Inserting initial value failed {:?}", e)))?;

  println!("finished creating crash table!");
  Ok(())
}

#[napi]
pub async fn toggle_crash_table() -> Result<String> {
  let config = aws_config::defaults(BehaviorVersion::latest())
    .region("us-east-1")
    .endpoint_url(AWS_ENDPOINT_URL)
    .load()
    .await;

  let aws_client = aws_sdk_dynamodb::Client::new(&config);

  let curr_val = aws_client
    .get_item()
    .table_name(CRASH_TABLE)
    .key("id", AttributeValue::S("mode".to_string()))
    .send()
    .await
    .map_err(|e| Error::from_reason(format!("Reading from crash table failed {:?}", e)))?;

  if let Some(value_map) = curr_val.item() {
    if let Some(crash_value) = value_map.get("value") {
      let current_value = crash_value.as_s().map_err(|e| {
        Error::from_reason(format!(
          "Crash value didn't map to string correctly {:?}",
          e
        ))
      })?;

      // prob add some null / not found whatever checks here...
      println!("current value: {}", current_value);
      let new_value = if current_value == "0" { "1" } else { "0" };

      // then actually update the table with this new value
      aws_client
        .put_item()
        .table_name(CRASH_TABLE)
        .item("id", AttributeValue::S("mode".to_string()))
        .item("value", AttributeValue::S(new_value.to_string()))
        .send()
        .await
        .map_err(|e| Error::from_reason(format!("Reading from crash table failed {:?}", e)))?;
      return Ok(new_value.to_string());
    } else {
      return Err(Error::from_reason("Couldn't get crash value correctly!"));
    }
  } else {
    return Err(Error::from_reason("Couldn't get item value map correctly!"));
  }
}
