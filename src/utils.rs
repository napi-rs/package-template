pub enum LogType {
    Read,
    Invoke,
    Intent,
}

pub fn get_log_table_name(function_name: &str, log_type: LogType) -> String {
    let prefix = match log_type {
        LogType::Read => "read_log",
        LogType::Invoke => "invoke_log",
        LogType::Intent => "intent_log",
    };

    format!("{}_{}", prefix, function_name.replace(":", "_"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_log_table_name_with_intent_log_type() {
        let function_name = "arn:aws:lambda:us-east-1:443370680529:function:setup_tables";
        let log_type = LogType::Intent;

        let expected_table_name =
            "intent_log_arn_aws_lambda_us-east-1_443370680529_function_setup_tables";
        let actual_table_name = get_log_table_name(function_name, log_type);

        assert_eq!(
            actual_table_name, expected_table_name,
            "Table name did not match the expected value"
        );
    }
}
