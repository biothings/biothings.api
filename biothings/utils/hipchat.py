import requests, json

try:
    from biothings import config
except ImportError:
    raise Exception("call biothings.config_for_app() first")

def hipchat_msg(msg, color='yellow', message_format='text'):
    if not config.HIPCHAT_CONFIG or not config.HIPCHAT_CONFIG.get("token"):
        return

    url = 'https://sulab.hipchat.com/v2/room/{roomid}/notification?auth_token={token}'.format(**config.HIPCHAT_CONFIG)
    headers = {'content-type': 'application/json'}
    _msg = msg.lower()
    for keyword in ['fail', 'error']:
        if _msg.find(keyword) != -1:
            color = 'red'
            break
    params = {"from" : config.HIPCHAT_CONFIG['from'], "message" : msg,
              "color" : color, "message_format" : message_format}
    res = requests.post(url,json.dumps(params), headers=headers)
    # hipchat replis with "no content"
    assert res.status_code == 200 or res.status_code == 204, (str(res), res.text)


