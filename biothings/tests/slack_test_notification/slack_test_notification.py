import pytest
import boto3
import os
import requests
from _pytest.terminal import TerminalReporter


# Function to fetch build version from S3
def fetch_build_version_s3():
    print("")
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
        metadata_path = os.getenv('APPLICATION_METADATA_URL')
        response = requests.get(metadata_path, timeout=30)
        response.raise_for_status()
        build_version_field = os.getenv('APPLICATION_METADATA_FIELD')
        build_version_hub = get_nested_build_version_field_value(response.json(), build_version_field)
        print(f" └─ BUILD_VERSION_HUB={build_version_hub}")
        return build_version_hub
    except requests.exceptions.Timeout:
        print(f" └─ Request timed out: {metadata_path}")
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

@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config, items):
    """Skip all tests if certain conditions are met."""
    build_version_s3 = fetch_build_version_s3()
    build_version_hub = fetch_build_version_hub()
    github_event_name = os.getenv("GITHUB_EVENT_NAME", "")
    bypass_version_check = os.getenv("BYPASS_VERSION_PASS", "False")
    os.environ["SEND_SLACK_NOTIFICATION?"] = "True"

    if build_version_hub != build_version_s3 or github_event_name == "workflow_dispatch" or bypass_version_check == "True":
        # Store new build version if tests are going to run
        store_build_version_s3(build_version_hub)
    else:
        os.environ["SEND_SLACK_NOTIFICATION?"] = "False"
        print("No need to run the tests.")
        print(" └─ The S3 and Hub build versions are the same.")
        # Skip all tests
        for item in items:
            item.add_marker(pytest.mark.skip(reason="Skipped due to matching build versions"))

# Hook to run pytest and send Slack notification
@pytest.hookimpl(tryfirst=True)
def pytest_terminal_summary(terminalreporter: TerminalReporter, exitstatus: int, config):
    """Customize pytest terminal summary and send to Slack."""

    if os.getenv("SEND_SLACK_NOTIFICATION?") == "True":
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
        print("Sending Slack notification...")
        if SLACK_WEBHOOK_URL:
            try:
                response = requests.post(SLACK_WEBHOOK_URL, json=slack_data, timeout=10)
                if response.status_code == 200:
                    print(" └─ Slack notification sent successfully.")
                else:
                    print(f" └─ Failed to send message to Slack: {response.status_code}, {response.text}")
            except requests.exceptions.Timeout:
                print(" └─ Request timed out to Slack WebHook URL.")
            except requests.exceptions.RequestException as e:
                print(f" └─ Failed to send Slack notification. Error: {str(e)}")
        else:
            print(" └─ Slack webhook URL not provided, skipping notification.")

@pytest.hookimpl(tryfirst=True)
def pytest_addoption(parser):
    """Add command-line options for Slack integration and environment variables."""
    parser.addoption("--aws-access-key-id", action="store", help="AWS Access Key ID")
    parser.addoption("--aws-secret-access-key", action="store", help="AWS Secret Access Key")
    parser.addoption("--aws-default-region", action="store", help="AWS Region (e.g., us-east-1")
    parser.addoption("--aws-s3-bucket", action="store", help="AWS S3 Bucket (e.g., test-bucket")
    parser.addoption("--slack-webhook-url", action="store", help="Slack webhook URL to send messages (e.g., https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX")
    parser.addoption("--slack-channel", action="store", help="Slack channel to send messages (e.g., #general")
    parser.addoption("--application-name", action="store", help="Application Name (e.g. mygene.info")
    parser.addoption("--bypass-version-check", action="store", help="(Optional) Use the value `False` to force a run with no build version check. Default is `True`.")
    parser.addoption("--application-metadata-url", action="store", help="Application Metadata URL (e.g., https://mygene.info/metadata")
    parser.addoption("--application-metadata-field", action="store", help="Application Metadata Field (e.g., build_version")

@pytest.hookimpl(tryfirst=True)
def pytest_configure(config):
    """Configure environment variables from command-line options, fallback to existing environment variables."""

    def set_env_var(env_var_name, option_name):
        option_value = config.getoption(option_name)
        if option_value is not None:
            os.environ[env_var_name] = option_value

    def check_env_var(env_var_name, option_name, required=True):
        value = os.getenv(env_var_name)
        if value is None or value == "":
            if required:
                return env_var_name
        return None

    # List of required environment variables
    required_env_vars = [
        ("AWS_ACCESS_KEY_ID", '--aws-access-key-id'),
        ("AWS_SECRET_ACCESS_KEY", '--aws-secret-access-key'),
        ("AWS_DEFAULT_REGION", '--aws-default-region'),
        ("AWS_S3_BUCKET", '--aws-s3-bucket'),
        ("SLACK_WEBHOOK_URL", '--slack-webhook-url'),
        ("SLACK_CHANNEL", '--slack-channel'),
        ("APPLICATION_NAME", '--application-name'),
        ("APPLICATION_METADATA_URL", '--application-metadata-url'),
        ("APPLICATION_METADATA_FIELD", '--application-metadata-field')
    ]

    # List of optional environment variables
    optional_env_vars = [
        ("BYPASS_VERSION_PASS", '--bypass-version-check')
    ]

    missing_vars = []

    # Check for missing required environment variables
    for env_var_name, option_name in required_env_vars:
        set_env_var(env_var_name, option_name)
        missing_var = check_env_var(env_var_name, option_name, required=True)
        if missing_var:
            missing_vars.append(missing_var)

    # Check optional environment variables (does not add to missing_vars)
    for env_var_name, option_name in optional_env_vars:
        set_env_var(env_var_name, option_name)
        check_env_var(env_var_name, option_name, required=False)

    # If any required variables are missing, skip the tests with an error
    if missing_vars:
        missing_vars_str = ", ".join(missing_vars)
        example_usage = (
            "\n\nExample usage:\n\n"
            "    pytest --aws-access-key-id=\"AKIAIOSFODNN7EXAMPLE\" \\\n"
            "           --aws-secret-access-key=\"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\" \\\n"
            "           --aws-default-region=\"us-east-1\" \\\n"
            "           --aws-s3-bucket=\"my-app-bucket\" \\\n"
            "           --slack-webhook-url=\"https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX\" \\\n"
            "           --slack-channel=\"#ci-cd-notifications\" \\\n"
            "           --application-name=\"my-app\" \\\n"
            "           --application-metadata-url=\"https://my-app.com/metadata\" \\\n"
            "           --application-metadata-field=\"metadata.build_version\" \\\n"
            "           --bypass-version-check=\"True\""
        )

        pytest.exit(f"Error: The following environment variables are not set: {missing_vars_str}{example_usage}", returncode=1)
