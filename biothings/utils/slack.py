import requests, json, os, re
from functools import wraps


def slack_msg(webhook, msg, color='warning'):
    headers = {'content-type': 'application/json'}
    _msg = msg.lower()
    for keyword in ['fail', 'error']:
        if _msg.find(keyword) != -1:
            color = 'danger'
            break
    # we use attachments so we can add colors
    params = {
            "attachments": [
                {
                    "text": msg,
                    "color" : color,
                    }
                ]
            }
    res = requests.post(webhook,json.dumps(params), headers=headers)
    res.raise_for_status()

