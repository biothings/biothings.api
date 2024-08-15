import pytest
import requests
from _pytest.terminal import TerminalReporter

@pytest.hookimpl(tryfirst=True)
def pytest_terminal_summary(terminalreporter: TerminalReporter, exitstatus: int, config):
    """Customize pytest terminal summary and send to Slack."""

    SLACK_WEBHOOK_URL = config.getoption('--slack-webhook-url')
    SLACK_CHANNEL = config.getoption('--slack-channel')
    SLACK_USERNAME = config.getoption('--slack-username')

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
                "title": f"{bug_emoji} Mygeneset Pytest Summary",
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
    webhook_url = SLACK_WEBHOOK_URL
    if webhook_url:
        response = requests.post(webhook_url, json=slack_data)
        if response.status_code == 200:
            terminalreporter.write("Slack notification sent successfully.\n")
        else:
            terminalreporter.write(f"Failed to send message to Slack: {response.status_code}, {response.text}\n")
    else:
        terminalreporter.write("Slack webhook URL not provided, skipping notification.\n")

@pytest.hookimpl(tryfirst=True)
def pytest_addoption(parser):
    """Add command-line options for Slack integration."""
    parser.addoption("--slack-webhook-url", action="store", default=None, help="Slack webhook URL to send messages")
    parser.addoption("--slack-channel", action="store", default="#general", help="Slack channel to send messages")
    parser.addoption("--slack-username", action="store", default="Biothings (Default)", help="Slack username to send messages as")
