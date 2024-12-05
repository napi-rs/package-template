use anyhow::Result;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, KeySchemaElement, KeyType, ProvisionedThroughput, ScalarAttributeType,
    TableStatus,
};
use std::time::Duration;
use tokio::time::sleep;

use crate::aws_client::AWSClient;
use crate::utils::{get_log_table_name, LogType};

pub struct FunctionRegistration {
    pub function_name: String,
    pub read_log_name: String,
    pub invoke_log_name: String,
    pub intent_log_name: String,
}

impl FunctionRegistration {
    pub fn new(function_name: &str) -> Self {
        FunctionRegistration {
            function_name: function_name.to_string(),
            read_log_name: get_log_table_name(function_name, LogType::Read),
            invoke_log_name: get_log_table_name(function_name, LogType::Invoke),
            intent_log_name: get_log_table_name(function_name, LogType::Intent),
        }
    }

    pub async fn create_tables(&self, aws_client: &AWSClient) -> Result<()> {
        self.create_read_log_table(aws_client).await?;
        self.create_invoke_log_table(aws_client).await?;
        self.create_intent_log_table(aws_client).await?;
        Ok(())
    }

    async fn create_read_log_table(&self, aws_client: &AWSClient) -> Result<()> {
        let attribute_definitions = vec![
            AttributeDefinition::builder()
                .attribute_name("InstanceId")
                .attribute_type(ScalarAttributeType::S)
                .build()?,
            AttributeDefinition::builder()
                .attribute_name("StepNumber")
                .attribute_type(ScalarAttributeType::N)
                .build()?,
        ];

        let key_schema = vec![
            KeySchemaElement::builder()
                .attribute_name("InstanceId")
                .key_type(KeyType::Hash)
                .build()?,
            KeySchemaElement::builder()
                .attribute_name("StepNumber")
                .key_type(KeyType::Range)
                .build()?,
        ];

        create_table(
            aws_client,
            &self.read_log_name,
            attribute_definitions,
            key_schema,
        )
        .await
    }

    async fn create_invoke_log_table(&self, aws_client: &AWSClient) -> Result<()> {
        let attribute_definitions = vec![
            AttributeDefinition::builder()
                .attribute_name("CallerId")
                .attribute_type(ScalarAttributeType::S)
                .build()?,
            AttributeDefinition::builder()
                .attribute_name("CallerStep")
                .attribute_type(ScalarAttributeType::N)
                .build()?,
        ];

        let key_schema = vec![
            KeySchemaElement::builder()
                .attribute_name("CallerId")
                .key_type(KeyType::Hash)
                .build()?,
            KeySchemaElement::builder()
                .attribute_name("CallerStep")
                .key_type(KeyType::Range)
                .build()?,
        ];

        create_table(
            aws_client,
            &self.invoke_log_name,
            attribute_definitions,
            key_schema,
        )
        .await
    }

    async fn create_intent_log_table(&self, aws_client: &AWSClient) -> Result<()> {
        let attribute_definitions = vec![AttributeDefinition::builder()
            .attribute_name("InstanceId")
            .attribute_type(ScalarAttributeType::S)
            .build()?];

        let key_schema = vec![KeySchemaElement::builder()
            .attribute_name("InstanceId")
            .key_type(KeyType::Hash)
            .build()?];

        create_table(
            aws_client,
            &self.intent_log_name,
            attribute_definitions,
            key_schema,
        )
        .await
    }
}

async fn create_table(
    aws_client: &AWSClient,
    table_name: &str,
    attribute_definitions: Vec<AttributeDefinition>,
    key_schema: Vec<KeySchemaElement>,
) -> Result<()> {
    let provisioned_throughput = ProvisionedThroughput::builder()
        .read_capacity_units(5)
        .write_capacity_units(5)
        .build()?;

    aws_client
        .dynamodb_client
        .create_table()
        .table_name(table_name)
        .set_attribute_definitions(Some(attribute_definitions))
        .set_key_schema(Some(key_schema))
        .provisioned_throughput(provisioned_throughput)
        .send()
        .await?;

    // Wait for table creation
    loop {
        let table = aws_client
            .dynamodb_client
            .describe_table()
            .table_name(table_name)
            .send()
            .await?;

        match table.table.and_then(|t| t.table_status) {
            Some(TableStatus::Active) => break,
            Some(_) => {
                println!("Table {} is being created. Waiting...", table_name);
                sleep(Duration::from_secs(5)).await;
            }
            None => return Err(anyhow::anyhow!("Failed to create table {}", table_name)),
        }
    }

    Ok(())
}

pub async fn register_function(aws_client: &AWSClient, function_name: &str) -> Result<()> {
    let registration = FunctionRegistration::new(function_name);
    registration.create_tables(aws_client).await?;
    Ok(())
}
