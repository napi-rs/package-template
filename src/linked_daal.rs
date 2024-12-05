use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Error, Result};
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ProvisionedThroughput,
    ScalarAttributeType, TableStatus,
};
use aws_sdk_dynamodb::Client;
use serde_json::json;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

use crate::aws_client::AWSClient;
use crate::flowstate_env::FlowstateEnv;

pub enum ConditionalWriteResult {
    Success,
    Failure,
}

pub struct LinkedDAAL {
    client: Arc<Client>,
    table_name: String,
    attribute_map: HashMap<String, String>,
}

impl Clone for LinkedDAAL {
    fn clone(&self) -> Self {
        LinkedDAAL {
            client: Arc::clone(&self.client),
            table_name: self.table_name.clone(),
            attribute_map: self.attribute_map.clone(),
        }
    }
}

// type Result<T> = std::result::Result<T, LinkedDAALError>;

impl LinkedDAAL {
    pub const KEY_ATTRIBUTE: &str = "K";
    pub const ROW_HASH_ATTRIBUTE: &str = "RowHash";
    pub const NEXT_ROW_HASH_ATTRIBUTE: &str = "NextRowHash";
    pub const LOGS_ATTRIBUTE: &str = "Logs";
    pub const LOG_SIZE_ATTRIBUTE: &str = "LogSize";
    pub const GC_SIZE_ATTRIBUTE: &str = "GCSize";
    pub const VALUE_ATTRIBUTE: &str = "V";

    pub fn get_client(&self) -> Arc<Client> {
        Arc::clone(&self.client)
    }

    fn create_default_attribute_map() -> HashMap<String, String> {
        HashMap::from([
            (Self::ROW_HASH_ATTRIBUTE.to_string(), "#RH".to_string()),
            (
                Self::NEXT_ROW_HASH_ATTRIBUTE.to_string(),
                "#NRH".to_string(),
            ),
            (Self::LOGS_ATTRIBUTE.to_string(), "#LOGS".to_string()),
            (Self::LOG_SIZE_ATTRIBUTE.to_string(), "#LSZ".to_string()),
            (Self::GC_SIZE_ATTRIBUTE.to_string(), "#GCSZ".to_string()),
        ])
    }

    pub async fn create_new(aws_client: &AWSClient, table_name: &str) -> Result<Self> {
        /*

        PSEUDOCODE:
        Goal: create a table with a LinkedDAAL-friendly interface
        - Inputs: table name,

        1. Connect to the provider
        2. Create a table with the following properties:
           - The primary key will consist of a partition and a sort key
             - Partition (hash) key is the normal input key itself
             - Sort key should be the row id or row hash
           - Other attributes (I don't think we need to describe these for aws when the table is created)
             - Value (could be any type?)
             - Various logs: read log, write log, invoke log (these are collocated in the same row)
             - Next row (string)
             - Lock owner (when we do transactions)
         */

        let client = aws_client.dynamodb_client.clone().clone();

        let linked_daal = LinkedDAAL {
            client,
            table_name: table_name.to_string(),
            attribute_map: LinkedDAAL::create_default_attribute_map(),
        };

        let attribute_definitions = Some(vec![
            AttributeDefinition::builder()
                .attribute_name(Self::KEY_ATTRIBUTE)
                .attribute_type(ScalarAttributeType::S)
                .build()?,
            AttributeDefinition::builder()
                .attribute_name(Self::ROW_HASH_ATTRIBUTE)
                .attribute_type(ScalarAttributeType::S)
                .build()?,
        ]);

        let key_schemas = Some(vec![
            KeySchemaElement::builder()
                .attribute_name(Self::KEY_ATTRIBUTE)
                .key_type(KeyType::Hash)
                .build()?,
            KeySchemaElement::builder()
                .attribute_name(Self::ROW_HASH_ATTRIBUTE)
                .key_type(KeyType::Range)
                .build()?,
        ]);

        let provisioned_throughput = ProvisionedThroughput::builder()
            .read_capacity_units(5)
            .write_capacity_units(5)
            .build()?;

        // Create the table
        linked_daal
            .client
            .create_table()
            .table_name(table_name)
            .set_attribute_definitions(attribute_definitions)
            .set_key_schema(key_schemas)
            .provisioned_throughput(provisioned_throughput)
            .send()
            .await?;
        // .map_err(|e| LinkedDAALError::AwsSdkError(e.into()))?;

        // Wait for the table to be created
        linked_daal.wait_for_table_creation().await?;

        Ok(linked_daal)
    }

    // TODO (brian): we could make this fail if the table doesn't exist, if we prefer
    fn from_existing_table(aws_client: &AWSClient, table_name: &str) -> Self {
        let client = aws_client.dynamodb_client.clone();

        LinkedDAAL {
            client,
            table_name: table_name.to_string(),
            attribute_map: LinkedDAAL::create_default_attribute_map(),
        }
    }

    async fn table_exists(aws_client: &AWSClient, table_name: &str) -> bool {
        let table = aws_client
            .dynamodb_client
            .describe_table()
            .table_name(table_name)
            .send()
            .await;
        table.is_ok()
    }

    pub async fn use_linked_daal(aws_client: &AWSClient, table_name: &str) -> Self {
        if Self::table_exists(aws_client, table_name).await {
            Self::from_existing_table(aws_client, table_name)
        } else {
            Self::create_new(aws_client, table_name).await.unwrap()
        }
    }

    async fn wait_for_table_creation(&self) -> Result<()> {
        loop {
            let table = self
                .client
                .describe_table()
                .table_name(&self.table_name)
                .send()
                .await?;
            // .map_err(|e| LinkedDAALError::AwsSdkError(e.into()))?;

            match table.table.and_then(|t| t.table_status) {
                Some(TableStatus::Active) => return Ok(()),
                Some(_) => {
                    println!("Table is being created. Waiting...");
                    sleep(Duration::from_secs(5)).await;
                }
                None => {
                    return Err(anyhow!("Failed to create table"));
                    // return Err(LinkedDAALError::TableStatusError);
                }
            }
        }
    }

    async fn get_tail_hash(&self) -> Result<String> {
        todo!()
    }

    pub async fn log_entry_exists_in_row(
        &self,
        key: &str,
        row_hash: &str,
        flowstate_env: &FlowstateEnv,
    ) -> Result<bool> {
        let row = self
            .lib_read(key, row_hash, Some(Self::LOGS_ATTRIBUTE))
            .await?
            .ok_or_else(|| anyhow!("Couldn't find row with this row hash"))?;

        if let Some(logs) = row.get(Self::LOGS_ATTRIBUTE) {
            let logs = logs
                .as_m()
                .map_err(|_| anyhow!("Couldn't convert logs attribute to hashmap"))?;

            let log_key = flowstate_env.as_key();
            let contains_key = logs.contains_key(&log_key);
            return Ok(contains_key);
        }

        Err(anyhow!("Logs attribute not found in row"))
    }

    pub async fn next_row_exists(&self, key: &str, row_hash: &str) -> Result<bool> {
        let row = self
            .lib_read(key, row_hash, Some(Self::NEXT_ROW_HASH_ATTRIBUTE))
            .await?
            .ok_or_else(|| anyhow!("Couldn't find row with this row hash"))?;
        if let Some(_) = row.get(Self::NEXT_ROW_HASH_ATTRIBUTE) {
            return Ok(true);
        }
        return Ok(false);
    }

    pub async fn get_next_row(&self, key: &str, row_hash: &str) -> Result<String> {
        let row = self
            .lib_read(key, row_hash, Some(Self::NEXT_ROW_HASH_ATTRIBUTE))
            .await?
            .ok_or_else(|| anyhow!("Couldn't find row with this row hash"))?;

        if let Some(next_row_hash) = row.get(Self::NEXT_ROW_HASH_ATTRIBUTE) {
            if let AttributeValue::S(hash) = next_row_hash {
                return Ok(hash.to_string());
            }
        }
        Err(anyhow!("Couldn't get the next row"))
    }

    async fn lib_read(
        &self,
        key: &str,
        row_hash: &str,
        projection_expression: Option<&str>,
    ) -> Result<Option<HashMap<String, AttributeValue>>> {
        let mut read_action = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key(Self::KEY_ATTRIBUTE, AttributeValue::S(key.to_string()))
            .key(
                Self::ROW_HASH_ATTRIBUTE,
                AttributeValue::S(row_hash.to_string()),
            );

        // Only add projection expression if one is provided
        if let Some(expr) = projection_expression {
            read_action = read_action.projection_expression(expr);
        }

        let read_row_action = read_action.send().await?;
        Ok(read_row_action.item().cloned())
    }

    async fn lib_conditional_write(
        &self,
        update_expression: &str,
        condition_expression: &str,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    ) -> Result<ConditionalWriteResult> {
        let conditional_write_output = self
            .client
            .update_item()
            .table_name(&self.table_name)
            .update_expression(update_expression)
            .condition_expression(condition_expression)
            .set_expression_attribute_values(expression_attribute_values)
            .send()
            .await;

        match conditional_write_output {
            Ok(_) => Ok(ConditionalWriteResult::Success),
            Err(sdk_error) => match sdk_error.into_service_error() {
                UpdateItemError::ConditionalCheckFailedException(_) => {
                    return Ok(ConditionalWriteResult::Failure)
                }
                err => Err(Error::from(err)),
            },
        }
    }

    pub async fn write_value_to_logs_if_space_in_logs(
        &self,
        value: &str,
        flowstate_env: &FlowstateEnv,
    ) -> Result<ConditionalWriteResult> {
        const MAX_LOG_SIZE: i32 = 10;

        let log_key = flowstate_env.as_key();

        let condition_expression = format!(
            "attribute_not_exists({logs}[{log_key}]) AND {log_size} < :n",
            logs = Self::LOGS_ATTRIBUTE,
            log_key = &log_key,
            log_size = Self::LOG_SIZE_ATTRIBUTE
        );

        let update_expression = format!(
            "{value} = :val, {log_size} = {log_size} + :inc, {logs}.{log_key} = :null",
            value = Self::VALUE_ATTRIBUTE,
            log_size = Self::LOG_SIZE_ATTRIBUTE,
            logs = Self::LOGS_ATTRIBUTE,
            log_key = &log_key
        );

        let mut expression_attribute_values = HashMap::new();
        expression_attribute_values
            .insert(":val".to_string(), AttributeValue::S(value.to_string()));
        expression_attribute_values.insert(":inc".to_string(), AttributeValue::N("1".to_string()));
        expression_attribute_values.insert(
            ":n".to_string(),
            AttributeValue::N(MAX_LOG_SIZE.to_string()),
        );
        expression_attribute_values.insert(":null".to_string(), AttributeValue::Null(true));

        self.lib_conditional_write(
            &update_expression,
            &condition_expression,
            Some(expression_attribute_values),
        )
        .await
    }

    pub async fn create_new_row(&self, key: &str, parent_row_hash: Option<&str>) -> Result<String> {
        let new_row_hash = Self::generate_new_row_hash();

        let empty_logs = json!({});
        let mut create_row_action = self
            .client
            .put_item()
            .table_name(&self.table_name)
            .item(Self::KEY_ATTRIBUTE, AttributeValue::S(key.to_string()))
            .item(
                Self::ROW_HASH_ATTRIBUTE,
                AttributeValue::S(new_row_hash.to_string()),
            )
            .item(
                Self::LOGS_ATTRIBUTE,
                AttributeValue::S(empty_logs.to_string()),
            )
            .item(Self::LOG_SIZE_ATTRIBUTE, AttributeValue::N("0".to_string()))
            .item(Self::GC_SIZE_ATTRIBUTE, AttributeValue::N("0".to_string()));

        if let Some(parent_row_hash) = parent_row_hash {
            let parent_row = self
                .lib_read(key, parent_row_hash, Some(Self::VALUE_ATTRIBUTE))
                .await?
                .ok_or_else(|| {
                    anyhow!("Was passed a parent_row_hash but could not find a row with this hash")
                })?;

            if let Some(parent_row_value) = parent_row.get(Self::VALUE_ATTRIBUTE) {
                let value_string = parent_row_value
                    .as_s()
                    .map_err(|_| anyhow!("Couldn't convert value attribute to string"))?
                    .to_string();

                create_row_action =
                    create_row_action.item(Self::VALUE_ATTRIBUTE, AttributeValue::S(value_string));
            }

            let get_already_created_row_action = self
                .client
                .get_item()
                .table_name(&self.table_name)
                .key(Self::KEY_ATTRIBUTE, AttributeValue::S(key.to_string()))
                .key(
                    Self::ROW_HASH_ATTRIBUTE,
                    AttributeValue::S(new_row_hash.to_string()),
                );

            let get_already_created_row_output = get_already_created_row_action.send().await?;
            // .map_err(|e| LinkedDAALError::AwsSdkError(e.into()))?;

            if get_already_created_row_output.item().is_some() {
                return Ok(new_row_hash);
            }
        }

        // Create the row if it doesn't exist
        create_row_action.send().await?;
        // .map_err(|e| LinkedDAALError::AwsSdkError(e.into()))?;

        // Set the next row hash of the parent row
        if let Some(parent_row_hash) = parent_row_hash {
            let update_expression =
                format!("SET {} = :next_row_hash", Self::NEXT_ROW_HASH_ATTRIBUTE);
            let set_next_row_hash_action = self
                .client
                .update_item()
                .table_name(&self.table_name)
                .key(Self::KEY_ATTRIBUTE, AttributeValue::S(key.to_string()))
                .key(
                    Self::ROW_HASH_ATTRIBUTE,
                    AttributeValue::S(parent_row_hash.to_string()),
                )
                .update_expression(update_expression)
                .expression_attribute_values(
                    ":next_row_hash",
                    AttributeValue::S(new_row_hash.clone()),
                );
            set_next_row_hash_action.send().await?;

            // TODO: handle case where the next row hash already exists. if it does, then we need to delete the row
            // we just created (rare case, but possible)
        }

        // TODO: potentially think about error cases
        // - We should match the awssdk errors we get and figure out if it means something really went wrong (in which case we should return the error)
        //   Or if we should handle the error (e.g. we try to create the row and it already exists)

        /*
         * pk, key = Get primary key from key and row (to get current row)
         * new_pk, new_key = Get keys of new row to create (generate new row hash for the next row)
         * for new row in table, add the following attributes:
         *      "LOGS" = empty hashmap
         *      "LOGSIZE" = 0
         *      "GCSIZE" = 0
         * if the table does not contain the new key and rowhash (if new row doesn't exist in the table), add this row
         *      can use UpdateExpression to define updates that will occur if the condition passes
         * if the current row does not have NEXTROW attribute, sets NEXTROW field in current row to rowhash of new row
         * if success, return the new row hash
         * otherwise, check the error:
         *      delete the new row we just created
         *      if the error was because the NEXTROW field already exists (ie. there's already a row linked to the curr row),
         *          then return the rowhash of the next row
         *      otherwise, idk this is an unexpected error
         */

        Ok(new_row_hash)
    }

    fn generate_new_row_hash() -> String {
        let id = Uuid::new_v4();
        id.to_string()
    }

    /// Retrieves a minimal representation (skeleton) of the linked DAAL structure for a given key.
    ///
    /// The skeleton consists of a vector of tuples, where each tuple contains:
    /// - A row hash (String)
    /// - An optional next row hash (Option<String>)
    ///
    /// This implementation optimizes DynamoDB access by using scan with projection to minimize
    /// data transfer, only fetching the row hashes and next row pointers needed to traverse the structure.
    ///
    /// # Arguments
    /// * `key` - The key identifying the linked DAAL chain in the table
    ///
    /// # Returns
    /// * `Result<Vec<(String, Option<String>)>>` - A vector of (row_hash, next_row_hash) pairs
    ///   representing the structure of the linked DAAL
    ///
    /// # Errors
    /// * Returns `Err` if the DynamoDB scan operation fails
    pub async fn get_skeleton(
        &self,
        key: &str,
        attributes: Vec<&str>,
    ) -> Result<Vec<HashMap<String, AttributeValue>>> {
        let scan_action_builder_partial = self
            .client
            .scan()
            .table_name(&self.table_name)
            .filter_expression("#K = :key")
            .projection_expression(
                attributes
                    .iter()
                    .map(|&attribute| self.attribute_map[attribute].clone())
                    .collect::<Vec<_>>()
                    .join(","),
            )
            .expression_attribute_names("#K", Self::KEY_ATTRIBUTE)
            .expression_attribute_values(":key", AttributeValue::S(key.to_string()));

        let scan_action_builder = attributes
            .iter()
            .fold(scan_action_builder_partial, |acc, &x| {
                acc.expression_attribute_names(self.attribute_map[x].clone(), x)
            });

        let scan_action = scan_action_builder.send().await?;

        let items = scan_action.items();

        let skeleton: Vec<HashMap<String, AttributeValue>> = items
            .into_iter()
            // .map(|item| {
            //     let mut fetched_attributes: HashMap<String, AttributeValue> = HashMap::new();
            //     attributes.iter().for_each(
            //         |&attribute| {
            //         fetched_attributes.insert(attribute.to_string(), item.get(attribute).unwrap().clone());
            //     });
            //     return fetched_attributes;
            // })
            .cloned()
            .collect();

        Ok(skeleton)
    }

    pub async fn skeleton_contains_log_id(
        &self,
        skeleton: &Vec<HashMap<String, AttributeValue>>,
        log_id: &str,
    ) -> Result<bool> {
        Ok(skeleton.iter().any(|row| {
            row.get(Self::LOGS_ATTRIBUTE)
                .is_some_and(|logs| logs.as_m().is_ok_and(|logs| logs.contains_key(log_id)))
        }))
    }

    /// Retrieves the row hash for the tail of a linked DAAL chain using its skeleton.
    /// This expects a skeleton with ROW_HASH, NEXT_ROW_HASH attributes
    ///
    /// This function first identifies the tail node (the one with no next pointer) from the skeleton,
    /// then retrieves its complete data from DynamoDB. This two-step process optimizes performance
    /// by avoiding sequential reads through the entire chain.
    ///
    /// # Arguments
    /// * `key` - The key identifying the linked DAAL chain in the table
    /// * `skeleton` - A reference to the skeleton structure obtained from `get_skeleton()`
    ///
    /// # Returns
    /// * `Result<HashMap<String, AttributeValue>>` - The complete DynamoDB row data for the tail node
    ///
    /// # Errors
    /// * Returns `Err` if no tail can be found in the skeleton
    /// * Returns `Err` if the tail row doesn't exist in DynamoDB
    /// * Returns `Err` if the DynamoDB read operation fails
    ///     pub async fn get_tail_row_from_skeleton(
    pub async fn get_tail_hash_from_skeleton(
        &self,
        skeleton: Vec<HashMap<String, AttributeValue>>,
    ) -> Result<String> {
        // Make sure that the expected attributes were included in the skeleton
        if skeleton
            .iter()
            .any(|row| !row.contains_key(Self::ROW_HASH_ATTRIBUTE))
        {
            return Err(anyhow!(
                "Missing row hash attribute for a row in this skeleton"
            ));
        }

        let num_rows_without_next_row_hash = skeleton
            .iter()
            .filter(|row| !row.contains_key(Self::NEXT_ROW_HASH_ATTRIBUTE))
            .count();
        if num_rows_without_next_row_hash != 1 {
            return Err(anyhow!(
                "Found {:?} rows without a next row hash, expected to find exactly 1",
                num_rows_without_next_row_hash
            ));
        }

        // Find the tail by looking for the row with no next pointer
        let tail_row_from_skeleton = skeleton
            .iter()
            .find(|&fetched_attributes| {
                // TODO for brian: I added this extra logic to see if the column is there but empty, though
                // I don't think this is necessary anymore, so maybe we can remove it
                !fetched_attributes
                    .get(Self::NEXT_ROW_HASH_ATTRIBUTE)
                    .is_some_and(|next_row_hash| !next_row_hash.as_s().unwrap().is_empty())
            })
            .ok_or(anyhow!("Could not find tail row in skeleton"))?;

        let row_hash_attribute = tail_row_from_skeleton
            .get(Self::ROW_HASH_ATTRIBUTE)
            .ok_or(anyhow!("Could not find row hash for tail row"))?;
        row_hash_attribute
            .as_s()
            .map_err(|_| anyhow!("Couldn't convert row hash to string"))
            .map(|s| s.to_string())
    }

    /// Retrieves the value stored in the tail node of a linked DAAL chain.
    ///
    /// This is a high-level convenience function that combines `get_skeleton()` and
    /// `get_tail_row_from_skeleton()` to retrieve just the value from the tail node.
    /// The function performs two DynamoDB operations:
    /// 1. A scan to get the skeleton structure
    /// 2. A get_item to retrieve the tail row's data
    ///
    /// # Arguments
    /// * `key` - The key identifying the linked DAAL chain in the table
    ///
    /// # Returns
    /// * `Result<Option<String>>` - The value stored in the tail node, or None if no value exists
    ///
    /// # Errors
    /// * Returns `Err` if getting the skeleton fails
    /// * Returns `Err` if getting the tail row fails
    /// * Returns `Err` if the value attribute cannot be parsed as a string
    pub async fn get_tail_value(&self, key: &str) -> Result<Option<String>> {
        let skeleton: Vec<HashMap<String, AttributeValue>> = self
            .get_skeleton(
                key,
                vec![Self::NEXT_ROW_HASH_ATTRIBUTE, Self::ROW_HASH_ATTRIBUTE],
            )
            .await?;
        // TODO: Ask sebastian about why we do two calls to DynamoDB

        let tail_hash = self.get_tail_hash_from_skeleton(skeleton).await?;
        // Get the full tail row from DynamoDB
        let tail_row = self
            .lib_read(key, &tail_hash, None)
            .await?
            .ok_or_else(|| anyhow!("Tail row not found in DynamoDB"))?;

        let tail_value = tail_row
            .get(Self::VALUE_ATTRIBUTE)
            .map(|v| v.as_s().unwrap().to_string());

        Ok(tail_value)
    }
}