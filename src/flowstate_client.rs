use serde_json;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use anyhow::{anyhow, Result};
use aws_sdk_dynamodb::types::AttributeValue;

use crate::aws_client::AWSClient;
use crate::flowstate_env::FlowstateEnv;
use crate::linked_daal::{ConditionalWriteResult, LinkedDAAL};

// Any library-wide code or documentation can go here

pub struct FlowstateClient {
    linked_daals: HashMap<String, LinkedDAAL>,
    env: FlowstateEnv,
    pub aws_client: Arc<AWSClient>,
}

pub struct InputWrapper {
    pub caller_name: String,
    pub caller_id: String,
    pub caller_step: u32,
    pub instance_id: String,
    pub input: serde_json::Value,
    pub is_async: bool,
}

impl InputWrapper {
    pub fn new(
        flowstate_client: &FlowstateClient,
        caller_step: u32,
        input: serde_json::Value,
    ) -> Self {
        InputWrapper {
            caller_name: flowstate_client.env.get_lambda_id(),
            caller_id: flowstate_client.env.get_instance_id(),
            caller_step,
            instance_id: Uuid::new_v4().to_string(), // Generate a new callee ID
            input,
            is_async: false,
        }
    }
}

impl FlowstateClient {
    pub async fn new_async(aws_client: Arc<AWSClient>, function_name: &str) -> Result<Self> {
        let linked_daals = HashMap::new();
        let env = FlowstateEnv::new_async(&aws_client, function_name).await?;

        Ok(FlowstateClient {
            linked_daals,
            env,
            aws_client,
        })
    }

    pub async fn read(&self, table: &str, key: &str) -> Result<String> {
        let linked_daal = self.linked_daals.get(table).unwrap();
        let aws_client = linked_daal.get_client();

        let tail_value = linked_daal.get_tail_value(key).await?;

        if let Some(tail_value) = tail_value {
            let next_step_number = self.env.increment_step();
            let log_key = HashMap::from([
                (
                    "InstanceId".to_string(),
                    AttributeValue::S(self.env.get_instance_id().clone()),
                ),
                (
                    "StepNumber".to_string(),
                    AttributeValue::N(next_step_number.to_string()),
                ),
            ]);

            match aws_client
                .put_item()
                .table_name(self.env.get_read_log_name())
                .set_item(Some(log_key.clone()))
                .item(
                    LinkedDAAL::VALUE_ATTRIBUTE.to_string(),
                    AttributeValue::S(tail_value.clone()),
                )
                .condition_expression("attribute_not_exists(#value)")
                .expression_attribute_names("#value", LinkedDAAL::VALUE_ATTRIBUTE.to_string())
                .send()
                .await
            {
                Ok(_) => Ok(tail_value),
                Err(e) => {
                    match aws_client
                        .get_item()
                        .table_name(self.env.get_read_log_name())
                        .set_key(Some(log_key.clone()))
                        .send()
                        .await
                    {
                        Ok(output) => Ok(output
                            .item()
                            .unwrap()
                            .get(LinkedDAAL::VALUE_ATTRIBUTE)
                            .unwrap()
                            .as_s()
                            .unwrap()
                            .to_string()),
                        Err(e) => Err(anyhow!("Error getting item: {:?}", e)),
                    }
                }
            }
        } else {
            Err(anyhow!("No value found"))
        }
    }

    pub async fn write(&self, table: &str, key: &str, value: &str) -> Result<()> {
        let id: String = self.env.as_key();
        let linked_daal = self
            .linked_daals
            .get(table)
            .ok_or(anyhow!("Table does not exist"))?;

        let skeleton = linked_daal
            .get_skeleton(
                key,
                vec![
                    LinkedDAAL::LOGS_ATTRIBUTE,
                    LinkedDAAL::NEXT_ROW_HASH_ATTRIBUTE,
                    LinkedDAAL::ROW_HASH_ATTRIBUTE,
                ],
            )
            .await?;

        let num = skeleton.iter().count();
        if num == 0 {
            linked_daal.create_new_row(key, None).await?;
        }
        
        let contains_log_id = linked_daal.skeleton_contains_log_id(&skeleton, &id).await?;
        if contains_log_id {
            return Ok(()); // write operation has already been previously executed
        }

        let tail_candidate_row_hash = linked_daal.get_tail_hash_from_skeleton(skeleton).await?;

        // now we can try to write using this tail candidate
        if self
            .try_write(linked_daal, key, &tail_candidate_row_hash, value)
            .await
            .is_ok()
        {
            self.env.increment_step();
        }

        return Ok(());
    }

    async fn try_write(
        &self,
        linked_daal: &LinkedDAAL,
        key: &str,
        row_hash: &str,
        value: &str,
    ) -> Result<()> {
        let id = self.env.as_key();

        let conditional_write_result = linked_daal
            .write_value_to_logs_if_space_in_logs(value, &self.env)
            .await?;
        match conditional_write_result {
            ConditionalWriteResult::Success => {
                // CASE B
                return Ok(());
            }
            ConditionalWriteResult::Failure => {}
        }

        let already_exists = linked_daal
            .log_entry_exists_in_row(key, row_hash, &self.env)
            .await?;
        if already_exists {
            // CASE A
            return Ok(());
        } else if !linked_daal.next_row_exists(key, row_hash).await? {
            // CASE D
            let next_row_hash = linked_daal.create_new_row(key, Some(row_hash)).await?;
            return Box::pin(self.try_write(linked_daal, key, &next_row_hash, value)).await;
        } else {
            // CASE C
            let next_row_hash = linked_daal.get_next_row(key, row_hash).await?;
            return Box::pin(self.try_write(linked_daal, key, &next_row_hash, value)).await;
        }
    }

    pub fn register_daal(&mut self, table_name: &str, daal: LinkedDAAL) {
        self.linked_daals.insert(table_name.to_string(), daal);
    }

    pub fn decrement_step(&mut self) {
        self.env.decrement_step();
    }

    async fn sync_invoke_callback(
        &self,
        input: &InputWrapper,
        result: Result<String>,
    ) -> Result<()> {
        let invoke_key = HashMap::from([
            (
                FlowstateEnv::INVOKE_LOG_CALLER_ID_ATTR.to_string(),
                AttributeValue::S(input.caller_id.clone()),
            ),
            (
                FlowstateEnv::INVOKE_LOG_CALLER_STEP_ATTR.to_string(),
                AttributeValue::N(input.caller_step.to_string()),
            ),
        ]);

        // Convert the result to a string - success or error message
        let result_str = match result {
            Ok(value) => value,
            Err(e) => format!("Error: {}", e),
        };

        // Update the invoke log with the result
        match self
            .aws_client
            .dynamodb_client
            .update_item()
            .table_name(self.env.get_invoke_log_name())
            .set_key(Some(invoke_key.clone()))
            .update_expression("SET #result = :result")
            .expression_attribute_names("#result", FlowstateEnv::INVOKE_LOG_RESULT_ATTR.to_string())
            .expression_attribute_values(":result", AttributeValue::S(result_str))
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow!("Failed to update invoke log with result: {:?}", e)),
        }
    }

    pub async fn wrapper_start(&mut self, input: &InputWrapper) -> Result<()> {
        // Set up environment with callee ID and initial step
        self.env.set_instance_id(input.instance_id.clone());

        // Log initial intent
        let start_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();

        let intent_item = HashMap::from([
            (
                FlowstateEnv::INTENT_LOG_INSTANCE_ID_ATTR.to_string(),
                AttributeValue::S(input.instance_id.clone()),
            ),
            (
                FlowstateEnv::INTENT_LOG_DONE_ATTR.to_string(),
                AttributeValue::S("false".to_string()),
            ),
            (
                FlowstateEnv::INTENT_LOG_ASYNC_ATTR.to_string(),
                AttributeValue::S(input.is_async.to_string()),
            ),
            (
                FlowstateEnv::INTENT_LOG_INPUT_ATTR.to_string(),
                AttributeValue::S(input.input.to_string()),
            ),
            (
                FlowstateEnv::INTENT_LOG_START_TIMESTAMP_ATTR.to_string(),
                AttributeValue::N(start_timestamp.to_string()),
            ),
        ]);

        self.aws_client
            .dynamodb_client
            .put_item()
            .table_name(self.env.get_intent_log_name())
            .set_item(Some(intent_item))
            .send()
            .await?;

        Ok(())
    }

    pub async fn wrapper_end(&self, input: InputWrapper, result: Result<String>) -> Result<()> {
        // Log completion and handle the result
        let end_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();

        // Convert result to string once, then use it for both callback and final logging
        let result_str = match &result {
            Ok(val) => val.clone(),
            Err(e) => format!("Error: {}", e),
        };

        // Call the callback with the result
        self.sync_invoke_callback(&input, Ok(result_str.clone()))
            .await?;

        let update_expr = "SET #done = :done, #end = :end, #return = :return";
        let names: HashMap<String, String> = HashMap::from([
            (
                "#done".to_string(),
                FlowstateEnv::INTENT_LOG_DONE_ATTR.to_string(),
            ),
            (
                "#end".to_string(),
                FlowstateEnv::INTENT_LOG_END_TIMESTAMP_ATTR.to_string(),
            ),
            (
                "#return".to_string(),
                FlowstateEnv::INTENT_LOG_RETURN_ATTR.to_string(),
            ),
        ]);
        let values: HashMap<String, AttributeValue> = HashMap::from([
            (":done".to_string(), AttributeValue::S("true".to_string())),
            (
                ":end".to_string(),
                AttributeValue::N(end_timestamp.to_string()),
            ),
            (":return".to_string(), AttributeValue::S(result_str)),
        ]);

        // Update the intent log with the result
        self.aws_client
            .dynamodb_client
            .update_item()
            .table_name(self.env.get_intent_log_name())
            .key(
                FlowstateEnv::INTENT_LOG_INSTANCE_ID_ATTR,
                AttributeValue::S(input.instance_id),
            )
            .update_expression(update_expr)
            .set_expression_attribute_names(Some(names))
            .set_expression_attribute_values(Some(values))
            .send()
            .await?;

        Ok(())
    }

    // async fn sync_invoke_wrapper(&self, input: InputWrapper) -> Result<()> {
    //     self.wrapper_start(&input).await?;

    //     // Run the main function with the provided input
    //     // TODO: do we do the raw_run here or in `sync_invoke`?
    //     let result = self.raw_run(input.input.clone()).await;

    //     self.wrapper_end(input, result).await?;

    //     Ok(())
    // }

    // Helper method to run the main function (you'll need to implement this based on your needs)
    async fn raw_sync_invoke(
        &self,
        input: serde_json::Value,
        callee_lambda_arn: &str,
    ) -> Result<String> {
        let serialized_input = serde_json::to_vec(&input)?;

        let invoke_output = self
            .aws_client
            .lambda_client
            .invoke()
            .function_name(callee_lambda_arn)
            .payload(serialized_input.into())
            .send()
            .await?;

        let serialized_output: String = invoke_output.payload.map_or_else(
            || "".to_string(),
            |blob| String::from_utf8_lossy(&blob.into_inner()).into_owned(),
        );

        Ok(serialized_output)
    }

    pub async fn sync_invoke(
        &self,
        input: serde_json::Value,
        callee_lambda_arn: &str,
    ) -> Result<Option<String>> {
        let next_step = self.env.increment_step();

        // Wrap input
        let wrapped_input = InputWrapper::new(&self, next_step, input.clone());

        // Generate invoke key
        let invoke_key = HashMap::from([
            (
                FlowstateEnv::INVOKE_LOG_CALLER_ID_ATTR.to_string(),
                AttributeValue::S(wrapped_input.caller_id.clone()),
            ),
            (
                FlowstateEnv::INVOKE_LOG_CALLER_STEP_ATTR.to_string(),
                AttributeValue::N(wrapped_input.caller_step.to_string()),
            ),
        ]);

        let write_to_invoke_log_attempt = self
            .aws_client
            .dynamodb_client
            .put_item()
            .table_name(self.env.get_invoke_log_name())
            .set_item(Some(invoke_key.clone()))
            .item(
                FlowstateEnv::INVOKE_LOG_CALLEE_ID_ATTR.to_string(),
                AttributeValue::S(wrapped_input.instance_id.clone()),
            )
            .condition_expression(
                "attribute_not_exists(#caller_id) AND attribute_not_exists(#caller_step)",
            )
            .expression_attribute_names(
                "#caller_id",
                FlowstateEnv::INVOKE_LOG_CALLER_ID_ATTR.to_string(),
            )
            .expression_attribute_names(
                "#caller_step",
                FlowstateEnv::INVOKE_LOG_CALLER_STEP_ATTR.to_string(),
            )
            .send()
            .await;

        if write_to_invoke_log_attempt.is_ok() {
            let invoke_output = self.raw_sync_invoke(input, callee_lambda_arn).await?;
            Ok(Some(invoke_output))
        } else {
            let existing_invoke_log_result = self
                .aws_client
                .dynamodb_client
                .get_item()
                .table_name(self.env.get_invoke_log_name())
                .set_key(Some(invoke_key.clone()))
                .send()
                .await;

            match existing_invoke_log_result {
                Ok(output) => {
                    if let Some(item) = output.item {
                        let callee_id = item
                            .get(FlowstateEnv::INVOKE_LOG_CALLEE_ID_ATTR)
                            .and_then(|av| av.as_s().ok())
                            .ok_or_else(|| anyhow!("CalleeId not found in record"))?;

                        let result = item
                            .get(FlowstateEnv::INVOKE_LOG_RESULT_ATTR)
                            .and_then(|av| av.as_s().ok());

                        if result.is_none() {
                            // If no result exists yet, invoke the function
                            // TODO: Implement rawSyncInvoke equivalent
                            let invoke_output =
                                self.raw_sync_invoke(input, callee_lambda_arn).await?;
                            Ok(Some(invoke_output))
                        } else {
                            Ok(None)
                        }
                    } else {
                        return Err(anyhow!("No item found after conditional write failure"));
                        // This should never happen since we check for existence of the item above
                    }
                }
                Err(e) => Err(anyhow!(
                    "Error getting item after conditional write failure: {:?}",
                    e
                )),
            }
        }
    }
}
