use crate::aws_client::AWSClient;
use anyhow::{anyhow, Result};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, KeySchemaElement, KeyType, ProvisionedThroughput, ScalarAttributeType,
    TableStatus,
};
use aws_sdk_dynamodb::Client;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

use crate::utils::{get_log_table_name, LogType};

pub struct FlowstateEnv {
    instance_id: String,
    // TODO: why do we need shared state across threads here? im (brian) just getting this to compile first since
    // it's coming from a big merge; i will look more into it later
    step_number: AtomicU32,
    read_log_name: String,
    invoke_log_name: String,
    intent_log_name: String,
    lambda_id: String,
}

pub struct ReadLog {
    client: Arc<Client>,
    table_name: String,
}

pub struct InvokeLog {
    client: Arc<Client>,
    table_name: String,
}

pub struct IntentLog {
    client: Arc<Client>,
    table_name: String,
}

impl FlowstateEnv {
    pub const INVOKE_LOG_CALLER_ID_ATTR: &str = "CallerId";
    pub const INVOKE_LOG_CALLER_STEP_ATTR: &str = "CallerStep";
    pub const INVOKE_LOG_CALLEE_ID_ATTR: &str = "CalleeId";
    pub const INVOKE_LOG_RESULT_ATTR: &str = "Result";

    pub const INTENT_LOG_INSTANCE_ID_ATTR: &str = "InstanceId";
    pub const INTENT_LOG_DONE_ATTR: &str = "Done";
    pub const INTENT_LOG_ASYNC_ATTR: &str = "Async";
    pub const INTENT_LOG_INPUT_ATTR: &str = "Input";
    pub const INTENT_LOG_RETURN_ATTR: &str = "Return";
    pub const INTENT_LOG_START_TIMESTAMP_ATTR: &str = "StartTimestamp";
    pub const INTENT_LOG_END_TIMESTAMP_ATTR: &str = "EndTimestamp";

    pub async fn new_async(aws_client: &AWSClient, function_name: &str) -> Result<Self> {
        let id = Uuid::new_v4();
        let read_log_name = get_log_table_name(function_name, LogType::Read);
        let invoke_log_name = get_log_table_name(function_name, LogType::Invoke);
        let intent_log_name = get_log_table_name(function_name, LogType::Intent);

        let env = FlowstateEnv {
            instance_id: id.to_string(),
            step_number: AtomicU32::new(0),
            read_log_name,
            invoke_log_name,
            intent_log_name,
            lambda_id: function_name.to_string(),
        };

        Ok(env)
    }

    pub fn get_step_number(&self) -> u32 {
        self.step_number.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn get_instance_id(&self) -> String {
        self.instance_id.clone()
    }

    pub fn get_read_log_name(&self) -> String {
        self.read_log_name.clone()
    }

    pub fn get_invoke_log_name(&self) -> String {
        self.invoke_log_name.clone()
    }

    pub fn get_intent_log_name(&self) -> String {
        self.intent_log_name.clone()
    }

    pub async fn create_intent_log(&self, aws_client: &AWSClient) -> Result<IntentLog> {
        let intent_log_name = self.intent_log_name.clone();

        let client = aws_client.dynamodb_client.clone().clone();

        let table_name = intent_log_name.to_string();

        let intent_log = IntentLog {
            client,
            table_name: table_name.clone(),
        };

        let attribute_definitions = Some(vec![
            AttributeDefinition::builder()
                .attribute_name(String::from(Self::INTENT_LOG_INSTANCE_ID_ATTR))
                .attribute_type(ScalarAttributeType::S)
                .build()?,
            // AttributeDefinition::builder()
            //     .attribute_name(String::from(Self::INTENT_LOG_DONE_ATTR))
            //     .attribute_type(ScalarAttributeType::S) // DynamoDB doesn't have a boolean type, store "true" or "false"
            //     .build()?,
            // AttributeDefinition::builder()
            //     .attribute_name(String::from(Self::INTENT_LOG_ASYNC_ATTR))
            //     .attribute_type(ScalarAttributeType::S) // DynamoDB doesn't have a boolean type, store "true" or "false"
            //     .build()?,
            // AttributeDefinition::builder()
            //     .attribute_name(String::from(Self::INTENT_LOG_INPUT_ATTR))
            //     .attribute_type(ScalarAttributeType::S)
            //     .build()?,
            // AttributeDefinition::builder()
            //     .attribute_name(String::from(Self::INTENT_LOG_RETURN_ATTR))
            //     .attribute_type(ScalarAttributeType::S)
            //     .build()?,
            // AttributeDefinition::builder()
            //     .attribute_name(String::from(Self::INTENT_LOG_START_TIMESTAMP_ATTR))
            //     .attribute_type(ScalarAttributeType::N)
            //     .build()?,
            // AttributeDefinition::builder()
            //     .attribute_name(String::from(Self::INTENT_LOG_END_TIMESTAMP_ATTR))
            //     .attribute_type(ScalarAttributeType::N)
            //     .build()?,
        ]);

        let key_schema = Some(vec![KeySchemaElement::builder()
            .attribute_name(String::from(Self::INTENT_LOG_INSTANCE_ID_ATTR))
            .key_type(KeyType::Hash)
            .build()?]);

        let provisioned_throughput = ProvisionedThroughput::builder()
            .read_capacity_units(5)
            .write_capacity_units(5)
            .build()?;

        intent_log
            .client
            .create_table()
            .table_name(intent_log.table_name.clone())
            .set_attribute_definitions(attribute_definitions)
            .set_key_schema(key_schema)
            .provisioned_throughput(provisioned_throughput)
            .send()
            .await?;

        self.wait_for_table_creation(aws_client, &self.intent_log_name)
            .await?;

        Ok(intent_log)
    }

    pub async fn create_invoke_log(&self, aws_client: &AWSClient) -> Result<InvokeLog> {
        let invoke_log_name = self.invoke_log_name.clone();

        let client = aws_client.dynamodb_client.clone().clone();

        let table_name = invoke_log_name.to_string();

        let invoke_log = InvokeLog {
            client,
            table_name: table_name.clone(),
        };

        let attribute_definitions = Some(vec![
            AttributeDefinition::builder()
                .attribute_name(String::from("CallerId"))
                .attribute_type(ScalarAttributeType::S)
                .build()?,
            AttributeDefinition::builder()
                .attribute_name(String::from("CallerStep"))
                .attribute_type(ScalarAttributeType::N)
                .build()?,
            // brian: I don't think we include these since otherwise we'll get an error since
            // the number of attribute definitions needs to match the number of key schema elements
            // AttributeDefinition::builder()
            //     .attribute_name(String::from("CalleeId"))
            //     .attribute_type(ScalarAttributeType::S)
            //     .build()?,
            // AttributeDefinition::builder()
            //     .attribute_name(String::from("Result"))
            //     .attribute_type(ScalarAttributeType::S)
            //     .build()?,
        ]);

        let key_schema = Some(vec![
            KeySchemaElement::builder()
                .attribute_name(String::from("CallerId"))
                .key_type(KeyType::Hash)
                .build()?,
            KeySchemaElement::builder()
                .attribute_name(String::from("CallerStep"))
                .key_type(KeyType::Range)
                .build()?,
        ]);

        let provisioned_throughput = ProvisionedThroughput::builder()
            .read_capacity_units(5)
            .write_capacity_units(5)
            .build()?;

        invoke_log
            .client
            .create_table()
            .table_name(invoke_log.table_name.clone())
            .set_attribute_definitions(attribute_definitions)
            .set_key_schema(key_schema)
            .provisioned_throughput(provisioned_throughput)
            .send()
            .await?;

        self.wait_for_table_creation(aws_client, &self.invoke_log_name)
            .await?;

        Ok(invoke_log)
    }

    pub async fn create_read_log(&self, aws_client: &AWSClient) -> Result<ReadLog> {
        let read_log_name = self.read_log_name.clone();

        let client = aws_client.dynamodb_client.clone().clone();

        let table_name = read_log_name.to_string();

        let read_log = ReadLog {
            client,
            table_name: table_name.clone(),
        };

        let attribute_definitions = Some(vec![
            AttributeDefinition::builder()
                .attribute_name(String::from("InstanceId"))
                .attribute_type(ScalarAttributeType::S)
                .build()?,
            AttributeDefinition::builder()
                .attribute_name(String::from("StepNumber"))
                .attribute_type(ScalarAttributeType::N)
                .build()?,
        ]);

        let key_schema = Some(vec![
            KeySchemaElement::builder()
                .attribute_name(String::from("InstanceId"))
                .key_type(KeyType::Hash)
                .build()?,
            KeySchemaElement::builder()
                .attribute_name(String::from("StepNumber"))
                .key_type(KeyType::Range)
                .build()?,
        ]);

        let provisioned_throughput = ProvisionedThroughput::builder()
            .read_capacity_units(5)
            .write_capacity_units(5)
            .build()?;

        read_log
            .client
            .create_table()
            .table_name(read_log.table_name.clone())
            .set_attribute_definitions(attribute_definitions)
            .set_key_schema(key_schema)
            .provisioned_throughput(provisioned_throughput)
            .send()
            .await?;

        self.wait_for_table_creation(aws_client, &self.read_log_name)
            .await?;

        Ok(read_log)
    }

    async fn wait_for_table_creation(
        &self,
        aws_client: &AWSClient,
        table_name: &str,
    ) -> Result<()> {
        loop {
            let table = aws_client
                .dynamodb_client
                .describe_table()
                .table_name(table_name)
                .send()
                .await?;

            match table.table.and_then(|t| t.table_status) {
                Some(TableStatus::Active) => return Ok(()),
                Some(_) => {
                    println!("Table {} is being created. Waiting...", table_name);
                    sleep(Duration::from_secs(5)).await;
                }
                None => {
                    return Err(anyhow!("Failed to create table {}", table_name));
                }
            }
        }
    }

    pub fn as_key(&self) -> String {
        format!("{},{}", self.instance_id, self.get_step_number())
    }

    pub fn increment_step(&self) -> u32 {
        self.step_number
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let new_step = self.step_number.load(std::sync::atomic::Ordering::SeqCst);
        new_step
    }

    pub fn decrement_step(&self) -> u32 {
        self.step_number
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        let new_step = self.step_number.load(std::sync::atomic::Ordering::SeqCst);
        new_step
    }

    pub fn get_lambda_id(&self) -> String {
        self.lambda_id.clone()
    }

    pub fn set_instance_id(&mut self, id: String) {
        self.instance_id = id;
    }

    pub fn set_step(&mut self, step: u32) {
        self.step_number
            .store(step, std::sync::atomic::Ordering::SeqCst);
    }
}
