# Biothings Pytest Plugin

The `slack_test_notification` is a custom Pytest plugin designed to integrate Slack notifications and AWS S3 interactions into your testing workflow. It allows you to automate the process of sending test results to a Slack channel and managing application metadata stored in AWS S3.

When running in a GitHub Action, the plugin checks the environment variable `GITHUB_EVENT_NAME` (present in the GitHub Action environment) to bypass the build version check if the value is `workflow_dispatch`.

## Key Features

### 1. Environment Variable Configuration

The plugin configures essential environment variables for AWS S3, Slack, and the application under test. These variables can be passed as command-line options:

- `--aws-access-key-id`: AWS Access Key ID (Environment Variable: `AWS_ACCESS_KEY_ID`).
Example: `--aws-access-key-id="AKIAIOSFODNN7EXAMPLE"`

- `--aws-secret-access-key`: AWS Secret Access Key (Environment Variable: `AWS_SECRET_ACCESS_KEY`).
Example: `--aws-secret-access-key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"`

- `--aws-default-region`: AWS Region (Environment Variable: `AWS_DEFAULT_REGION`).
Example: `--aws-default-region="us-east-1"`

- `--aws-s3-bucket`: AWS S3 Bucket for storing application metadata (Environment Variable: `AWS_S3_BUCKET`).
Example: `--aws-s3-bucket="my-app-bucket"`

- `--slack-webhook-url`: Slack webhook URL for sending notifications (Environment Variable: `SLACK_WEBHOOK_URL`).
Example: `--slack-webhook-url="https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX"`

- `--slack-channel`: Slack channel where notifications will be posted (Environment Variable: `SLACK_CHANNEL`).
Example: `--slack-channel="#ci-cd-notifications"`

- `--application-name`: The name of the application under test. Just to show in the Slack message. (Environment Variable: `APPLICATION_NAME`).
Example: `--application-name="my-app"`

- `--application-metadata-url`: URL to the application metadata, typically used to retrieve the build version (Environment Variable: `APPLICATION_METADATA_URL`).
Example: `--application-metadata-url="https://my-app.com/metadata"`

- `--application-metadata-field`: JSON field in the metadata to retrieve the build version (Environment Variable: `APPLICATION_METADATA_FIELD`).
Example: `--application-metadata-field="metadata.build_version"`

- `--bypass-version-check`: (Optional) Set to `False` to skip the build version check. Default is `True`. (Environment Variable: `BYPASS_VERSION_PASS`).
Example: `--bypass-version-check="False"`

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
pytest --aws-access-key-id="AKIAIOSFODNN7EXAMPLE" \
       --aws-secret-access-key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
       --aws-default-region="us-east-1" \
       --aws-s3-bucket="my-app-bucket" \
       --slack-webhook-url="https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX" \
       --slack-channel="#ci-cd-notifications" \
       --application-name="my-app" \
       --application-metadata-url="https://my-app.com/metadata" \
       --application-metadata-field="metadata.build_version" \
       --bypass-version-check="True"
