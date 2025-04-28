import logging
import os

import pytest

from biothings.utils.loggers import SlackHandler, SlackMentionPolicy


@pytest.mark.skipif(
    os.environ.get("SLACK_WEBHOOK", None) is None,
    reason="SLACK_WEBHOOK environment variable must be set to run slack tests",
)
def test_msg_builder():

    URL = os.environ["SLACK_WEBHOOK"]
    SlackHandler.send(URL, "This is a test. Level 5.", 5, ())
    SlackHandler.send(URL, "This is a test. Level DEBUG.", logging.DEBUG, ())
    SlackHandler.send(URL, "This is a test. Level 15.", 15, ("@Jerry",))
    SlackHandler.send(URL, "This is a test. Level INFO.", logging.INFO, ("@Jerry",))
    SlackHandler.send(URL, "This is a test. Level 25.", 25, ("@Jerry",))
    SlackHandler.send(URL, "This is a test. Level WARNING.", logging.WARNING, ("@yaoyao", "@Jerry"))
    SlackHandler.send(URL, "This is a test. Level 35.", 35, ("@yaoyao", "@Jerry"))
    SlackHandler.send(URL, "This is a test. Level ERROR.", logging.ERROR, ("@Jerry", "@chunleiwu"))
    SlackHandler.send(URL, "This is a test. Level 45.", 45, ("@Jerry", "@chunleiwu"))
    SlackHandler.send(URL, "This is a test. Level CRITICAL.", logging.CRITICAL, ("@channel",))
    SlackHandler.send(URL, "This is a test. Level 55.", logging.CRITICAL, ("@channel",))


@pytest.mark.skipif(
    os.environ.get("SLACK_WEBHOOK", None) is None,
    reason="SLACK_WEBHOOK environment variable must be set to run slack tests",
)
def test_slack_policy_1():
    # []
    # []
    # ['@developerA', '@developerB']
    # ['@developerA', '@developerB']
    policy = SlackMentionPolicy(["@developerA", "@developerB"])
    print(policy.mentions(logging.INFO))
    print(policy.mentions(logging.WARNING))
    print(policy.mentions(logging.ERROR))
    print(policy.mentions(logging.CRITICAL))


@pytest.mark.skipif(
    os.environ.get("SLACK_WEBHOOK", None) is None,
    reason="SLACK_WEBHOOK environment variable must be set to run slack tests",
)
def test_slack_policy_2():
    # []
    # []
    # ['@channel']
    # ['@channel']
    policy = SlackMentionPolicy("@channel")
    print(policy.mentions(logging.INFO))
    print(policy.mentions(logging.WARNING))
    print(policy.mentions(logging.ERROR))
    print(policy.mentions(logging.CRITICAL))


@pytest.mark.skipif(
    os.environ.get("SLACK_WEBHOOK", None) is None,
    reason="SLACK_WEBHOOK environment variable must be set to run slack tests",
)
def test_slack_policy_3():
    # []
    # []
    # []
    policy = SlackMentionPolicy([])
    print(policy.mentions(logging.WARNING))
    print(policy.mentions(logging.ERROR))
    print(policy.mentions(logging.CRITICAL))


@pytest.mark.skipif(
    os.environ.get("SLACK_WEBHOOK", None) is None,
    reason="SLACK_WEBHOOK environment variable must be set to run slack tests",
)
def test_slack_policy_4():
    # []
    # []
    # ['@devops1', '@devops2']
    # ['@devops1', '@devops2']
    # ['@developer', '@devops1', '@devops2']
    # ['@developer', '@devops1', '@devops2']
    # ['@manager', '@developer', '@devops1', '@devops2']
    # ['@manager', '@developer', '@devops1', '@devops2']
    # ['@vp', '@manager', '@developer', '@devops1', '@devops2']
    policy = SlackMentionPolicy(
        {
            logging.CRITICAL: ["@vp"],
            logging.ERROR: ["@manager"],
            logging.WARNING: ["@developer"],
            logging.INFO: ["@devops1", "@devops2"],
        }
    )
    print(policy.mentions(logging.DEBUG))  # 10
    print(policy.mentions(15))
    print(policy.mentions(logging.INFO))  # 20
    print(policy.mentions(25))
    print(policy.mentions(logging.WARNING))  # 30
    print(policy.mentions(35))
    print(policy.mentions(logging.ERROR))  # 40
    print(policy.mentions(45))
    print(policy.mentions(logging.CRITICAL))  # 50


@pytest.mark.skipif(
    os.environ.get("SLACK_WEBHOOK", None) is None,
    reason="SLACK_WEBHOOK environment variable must be set to run slack tests",
)
def test_slack_policy_5():
    # []
    # []
    # ['@devops1', '@devops2']
    # ['@devops1', '@devops2']
    # ['@devops1', '@devops2']
    # ['@devops1', '@devops2']
    # ['@devops1', '@devops2']
    # ['@devops1', '@devops2']
    # ['@vp', '@devops1', '@devops2']
    policy = SlackMentionPolicy({logging.INFO: ["@devops1", "@devops2"], logging.CRITICAL: ["@vp"]})
    print(policy.mentions(logging.DEBUG))  # 10
    print(policy.mentions(15))
    print(policy.mentions(logging.INFO))  # 20
    print(policy.mentions(25))
    print(policy.mentions(logging.WARNING))  # 30
    print(policy.mentions(35))
    print(policy.mentions(logging.ERROR))  # 40
    print(policy.mentions(45))
    print(policy.mentions(logging.CRITICAL))  # 50
