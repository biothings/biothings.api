import pytest
import boto3
import os
import requests
from _pytest.terminal import TerminalReporter


"""
README: conftest.py - Pytest Configuration for GitHub action and Slack Integration

This `conftest.py` script is designed to enhance the Pytest testing framework with custom hooks and environment
configurations tailored for Slack notifications and AWS S3 interactions. 

Key Features:
--------------

1. **Environment Variable Configuration:**
   The script configures essential environment variables for AWS S3, Slack, and the application under test.
   The following variables can be passed as command-line options:
   
   - `AWS_ACCESS_KEY_ID`: AWS Access Key ID.
   - `AWS_SECRET_ACCESS_KEY`: AWS Secret Access Key.
   - `AWS_DEFAULT_REGION`: AWS Default Region.
   - `AWS_S3_BUCKET`: The S3 bucket name for storing application metadata.
   - `SLACK_WEBHOOK_URL`: The Slack webhook URL for sending notifications.
   - `SLACK_CHANNEL`: The Slack channel where notifications will be posted.
   - `APPLICATION_NAME`: The name of the application being tested. It will be displayed in the Slack message.
   - `GITHUB_EVENT_NAME`: GitHub event name, used to trigger tests manually (workflow_dispatch) or based on specific conditions.
   - `PYTEST_PATH`: Path to the Pytest test files.
   - `APPLICATION_METADATA_PATH`: Path to the application metadata, typically a URL.
   - `APPLICATION_METADATA_FIELD`: Notation to the build version field. Ex.: "metadata.build_version"

2. **Slack Notification Integration:**
   The script integrates Slack notifications into the Pytest workflow. After tests are executed, a summary of the 
   test results, including the number of passed, failed, and skipped tests, is sent to a specified Slack channel.
   Error details are included if any tests fail, helping to quickly identify and resolve issues.

3. **Custom Pytest Hooks:**
   - `pytest_addoption`: Adds custom command-line options for configuring Slack notifications.
   - `pytest_configure`: Sets up environment variables based on the provided command-line options, allowing for 
     flexible test execution.
   - `pytest_terminal_summary`: Customizes the terminal output summary and sends the test results to Slack.

4. **Step-by-Step Logging:**
   Each significant step in the setup, configuration, and execution process is logged via print statements. This 
   provides visibility into the internal workings of the script when running Pytest with the `-s` option.

Usage:
------

To run Pytest with the custom environment variables and see the print statements, use the following command:

```bash
pytest -s -vv \
       --aws-access-key-id=<value> \
       --aws-secret-access-key=<value> \
       --aws-default-region=<value> \
       --aws-s3-bucket=<value> \
       --slack-webhook-url=<value> \
       --slack-channel=<value> \
       --application-name=<value> \
       --github-event-name=<value> \
       --pytest-path=<value> \
       --application-metadata-path=<value> \
       --application-metadata-field=<value>
"""

# Function to fetch build version from S3
def fetch_build_version_s3():
    print("Fetching build version from the S3...")
    build_version_file = f"{os.getenv('APPLICATION_NAME')}.txt"
    s3 = boto3.client('s3')
    try:
        s3.download_file(os.getenv('AWS_S3_BUCKET'), build_version_file, build_version_file)
        with open(build_version_file, 'r') as file:
            build_version_s3 = file.read().strip()
        print(f" └─ BUILD_VERSION_S3={build_version_s3}")
        return build_version_s3
    except Exception as e:
        print(f" └─ No {build_version_file} found in S3, assuming first run. Error: {str(e)}")
        return ""

def get_nested_build_version_field_value(d, notation):
    keys = notation.split('.')
    value = d
    try:
        for key in keys:
            value = value[key]
        return value
    except KeyError:
        raise KeyError(f" └─ Key '{key}' not found in dictionary")
    except TypeError:
        raise TypeError(" └─ Invalid path or non-dictionary value encountered")

# Function to fetch build version from the Hub
def fetch_build_version_hub():
    print("Fetching build version from the Hub...")
    try:
        metadata_path = os.getenv('APPLICATION_METADATA_PATH')
        response = requests.get(metadata_path)
        response.raise_for_status()
        build_version_field = os.getenv('APPLICATION_METADATA_FIELD')
        build_version_hub = get_nested_build_version_field_value(response.json(), build_version_field)
        print(f" └─ BUILD_VERSION_HUB={build_version_hub}")
        return build_version_hub
    except requests.exceptions.RequestException as e:
        print(f" └─ Failed to fetch build version from HUB. Error: {str(e)}")
        return ""

# Function to store new build version to S3
def store_build_version_s3(build_version_hub):
    print("Storing build version to the S3...")
    build_version_file = f"{os.getenv('APPLICATION_NAME')}.txt"
    if build_version_hub:
        with open(build_version_file, 'w') as file:
            file.write(build_version_hub)
        
        s3 = boto3.client('s3')
        try:
            s3.upload_file(build_version_file, os.getenv('AWS_S3_BUCKET'), build_version_file)
            print(" └─ Stored new build version to S3.")
        except Exception as e:
            print(f" └─ Failed to store build version in S3. Error: {str(e)}")
    else:
        print(" └─ No valid build version found, not storing to S3.")

# Hook to conditionally skip pytest tests
@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session):
    build_version_s3 = fetch_build_version_s3()
    build_version_hub = fetch_build_version_hub()
    github_event_name = os.getenv("GITHUB_EVENT_NAME", "")

    # Check if pytest should run
    if build_version_hub == build_version_s3 and github_event_name != "workflow_dispatch":
        pytest.skip("No need to run the tests. The S3 and Hub build versions are the same.")
    
    # Store new build version if tests are going to run
    store_build_version_s3(build_version_hub)

# Hook to run pytest and send Slack notification
@pytest.hookimpl(tryfirst=True)
def pytest_terminal_summary(terminalreporter: TerminalReporter, exitstatus: int, config):
    """Customize pytest terminal summary and send to Slack."""
    
    SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
    SLACK_CHANNEL = os.getenv("SLACK_CHANNEL")
    SLACK_USERNAME = os.getenv("APPLICATION_NAME")

    # Collect test summary information
    total_tests = terminalreporter.stats.get('passed', []) + \
                  terminalreporter.stats.get('failed', []) + \
                  terminalreporter.stats.get('error', []) + \
                  terminalreporter.stats.get('skipped', [])

    passed_tests = len(terminalreporter.stats.get('passed', []))
    failed_tests = len(terminalreporter.stats.get('failed', []))
    error_tests = len(terminalreporter.stats.get('error', []))
    skipped_tests = len(terminalreporter.stats.get('skipped', []))

    # Prepare error details
    error_details = ""
    for test in terminalreporter.stats.get('failed', []) + terminalreporter.stats.get('error', []):
        error_details += f"• *Test*: `{test.nodeid}`\n"
        error_details += f"  _Details:_\n```\n{''.join(test.longreprtext.splitlines(keepends=True)[-10:])}```\n\n"
    error_details = "*Error Details:*\n" + error_details if error_details else ""

    # Determine status emoji and color
    status_emoji = ":thumbsup:" if failed_tests == 0 and error_tests == 0 else ":thumbsdown:"
    bug_emoji = "" if failed_tests == 0 and error_tests == 0 else ":bug-happy:"
    status_color = "good" if failed_tests == 0 and error_tests == 0 else "danger"

    # Check if there's anything to report
    if len(total_tests) == 0:
        terminalreporter.write("No tests were run, skipping Slack notification.\n")
        return

    # Create the payload for Slack
    slack_data = {
        "channel": SLACK_CHANNEL,
        "username": SLACK_USERNAME,
        "icon_emoji": f"{status_emoji}",
        "attachments": [
            {
                "color": status_color,
                "title": f"{bug_emoji} Pytest Summary",
                "text": f"Total Tests: *{len(total_tests)}*\n"
                        f"Passed: *{passed_tests}* :white_check_mark:\n"
                        f"Failed: *{failed_tests}* :x:\n"
                        f"Errors: *{error_tests}* :exclamation:\n"
                        f"Skipped: *{skipped_tests}* :fast_forward:\n\n"
                        f"{error_details}"
            }
        ]
    }

    # Send to Slack
    if SLACK_WEBHOOK_URL:
        response = requests.post(SLACK_WEBHOOK_URL, json=slack_data)
        if response.status_code == 200:
            terminalreporter.write("Slack notification sent successfully.\n")
        else:
            terminalreporter.write(f"Failed to send message to Slack: {response.status_code}, {response.text}\n")
    else:
        terminalreporter.write("Slack webhook URL not provided, skipping notification.\n")

@pytest.hookimpl(tryfirst=True)
def pytest_addoption(parser):
    """Add command-line options for Slack integration and environment variables."""
    parser.addoption("--slack-webhook-url", action="store", default=None, help="Slack webhook URL to send messages")
    parser.addoption("--slack-channel", action="store", default="#general", help="Slack channel to send messages")
    parser.addoption("--slack-username", action="store", default="Biothings (Default)", help="Slack username to send messages as")

    parser.addoption("--aws-access-key-id", action="store", help="AWS Access Key ID")
    parser.addoption("--aws-secret-access-key", action="store", help="AWS Secret Access Key")
    parser.addoption("--aws-region", action="store", help="AWS Region")
    parser.addoption("--aws-s3-bucket", action="store", help="AWS S3 Bucket")
    parser.addoption("--application-name", action="store", help="Application Name")
    parser.addoption("--github-event-name", action="store", help="GitHub Event Name")
    parser.addoption("--pytest-path", action="store", help="Pytest Path")
    parser.addoption("--application-metadata-path", action="store", help="Application Metadata Path")
    parser.addoption("--application-metadata-field", action="store", help="Application Metadata Field")

@pytest.hookimpl(tryfirst=True)
def pytest_configure(config):
    """Configure environment variables from command-line options, fallback to existing environment variables."""

    def set_env_var(env_var_name, option_name):
        option_value = config.getoption(option_name)
        if option_value is not None:
            os.environ[env_var_name] = option_value

    # Set environment variables using command-line options or fallback to existing environment variables
    set_env_var("AWS_ACCESS_KEY_ID", '--aws-access-key-id')
    set_env_var("AWS_SECRET_ACCESS_KEY", '--aws-secret-access-key')
    set_env_var("AWS_DEFAULT_REGION", '--aws-region')
    set_env_var("AWS_S3_BUCKET", '--aws-s3-bucket')
    set_env_var("SLACK_WEBHOOK_URL", '--slack-webhook-url')
    set_env_var("SLACK_CHANNEL", '--slack-channel')
    set_env_var("APPLICATION_NAME", '--application-name')
    set_env_var("GITHUB_EVENT_NAME", '--github-event-name')
    set_env_var("PYTEST_PATH", '--pytest-path')
    set_env_var("APPLICATION_METADATA_PATH", '--application-metadata-path')
    set_env_var("APPLICATION_METADATA_FIELD", '--application-metadata-field')
