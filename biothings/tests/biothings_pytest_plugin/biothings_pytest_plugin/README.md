# Biothings Pytest Plugin

The `biothings_pytest_plugin` is a custom Pytest plugin designed to integrate Slack notifications and AWS S3 interactions into your testing workflow. It allows you to automate the process of sending test results to a Slack channel and managing application metadata stored in AWS S3.

## Key Features

### 1. Environment Variable Configuration
The plugin configures essential environment variables for AWS S3, Slack, and the application under test. These variables can be passed as command-line options:

- **AWS_ACCESS_KEY_ID**: AWS Access Key ID.
- **AWS_SECRET_ACCESS_KEY**: AWS Secret Access Key.
- **AWS_DEFAULT_REGION**: AWS Default Region.
- **AWS_S3_BUCKET**: The S3 bucket name for storing application metadata.
- **SLACK_WEBHOOK_URL**: The Slack webhook URL for sending notifications.
- **SLACK_CHANNEL**: The Slack channel where notifications will be posted.
- **APPLICATION_NAME**: The name of the application being tested. It will be displayed in the Slack message.
- **GITHUB_EVENT_NAME**: GitHub event name, used to trigger tests manually (`workflow_dispatch`) or based on specific conditions.
- **PYTEST_PATH**: Path to the Pytest test files.
- **APPLICATION_METADATA_PATH**: Path to the application metadata, typically a URL.
- **APPLICATION_METADATA_FIELD**: Notation to the build version field (e.g., `"metadata.build_version"`).

### 2. Slack Notification Integration
After running your tests, the plugin sends a summary of the results, including passed, failed, and skipped tests, to a specified Slack channel. If any tests fail, error details are included in the message to help you quickly identify and resolve issues.

### 3. Custom Pytest Hooks
The plugin defines several custom hooks to extend Pytest's functionality:

- **`pytest_addoption`**: Adds custom command-line options for configuring Slack notifications and environment variables.
- **`pytest_configure`**: Configures environment variables based on command-line options.
- **`pytest_terminal_summary`**: Customizes the terminal summary and sends test results to Slack.
- **`pytest_collection_modifyitems`**: Skips tests if the build version in AWS S3 matches the one in the Hub.

### 4. Step-by-Step Logging
Each significant step in the setup, configuration, and execution process is logged via print statements. This provides visibility into the internal workings of the plugin when running Pytest with the `-s` option.

## Usage

To run Pytest with the custom environment variables and see the print statements, use the following command:

```bash
pytest -s -vv \
       --aws-access-key-id=<value> \
       --aws-secret-access-key=<value> \
       --aws-region=<value> \
       --aws-s3-bucket=<value> \
       --slack-webhook-url=<value> \
       --slack-channel=<value> \
       --application-name=<value> \
       --github-event-name=<value> \
       --pytest-path=<value> \
       --application-metadata-path=<value> \
       --application-metadata-field=<value>
