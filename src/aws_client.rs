use std::sync::Arc;

use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_dynamodb::config::Region;

pub struct AWSClient {
    pub dynamodb_client: Arc<aws_sdk_dynamodb::Client>,
    pub lambda_client: Arc<aws_sdk_lambda::Client>,
}

impl AWSClient {
    pub async fn new(region_name: &str) -> Self {
        const AWS_FALLBACK_REGION: &str = "us-east-1";

        let region_provider = RegionProviderChain::first_try(Region::new(region_name.to_string()))
            .or_default_provider()
            .or_else(Region::new(AWS_FALLBACK_REGION));
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;

        let dynamodb_client = aws_sdk_dynamodb::Client::new(&config);
        let lambda_client = aws_sdk_lambda::Client::new(&config);

        AWSClient {
            dynamodb_client: Arc::new(dynamodb_client),
            lambda_client: Arc::new(lambda_client),
        }
    }
}
