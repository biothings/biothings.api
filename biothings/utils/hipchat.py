import requests, json, os, re
from functools import wraps
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
import mimetypes


config = None

def requires_config(func):
    @wraps(func)
    def func_wrapper(*args,**kwargs):
        global config
        if not config:
            try:
                from biothings import config as config_mod
                config = config_mod
            except ImportError:
                raise Exception("call biothings.config_for_app() first")
        return func(*args,**kwargs)
    return func_wrapper

@requires_config
def hipchat_msg(msg, color='yellow', message_format='text'):
    if not hasattr(config,"HIPCHAT_CONFIG") or not config.HIPCHAT_CONFIG or not config.HIPCHAT_CONFIG.get("token"):
        return

    url = 'https://{host}/v2/room/{roomid}/notification?auth_token={token}'.format(**config.HIPCHAT_CONFIG)
    headers = {'content-type': 'application/json'}
    _msg = msg.lower()
    for keyword in ['fail', 'error']:
        if _msg.find(keyword) != -1:
            color = 'red'
            break
    params = {"from" : config.HIPCHAT_CONFIG['from'], "message" : msg,
              "color" : color, "message_format" : message_format}
    res = requests.post(url,json.dumps(params), headers=headers)
    res.raise_for_status()

# from https://gist.github.com/bdclark/0cbadce5816b6ab10eb2
@requires_config
def hipchat_file(filepath, message=""):
    """Send file to a HipChat room via API version 2"""
    if not hasattr(config,"HIPCHAT_CONFIG") or not config.HIPCHAT_CONFIG or not config.HIPCHAT_CONFIG.get("usertoken"):
        return

    if not os.path.isfile(filepath):
        raise ValueError("File '{0}' does not exist".format(filepath))

    host = config.HIPCHAT_CONFIG["host"]
    room = config.HIPCHAT_CONFIG["roomid"]
    token = config.HIPCHAT_CONFIG["usertoken"]
    url = "https://{0}/v2/room/{1}/share/file".format(host, room)
    headers = {
        'Authorization': 'Bearer {}'.format(token),
        'Accept-Charset': 'UTF-8',
        'Content-Type': 'multipart/related',
    }
    raw_body = MIMEMultipart('related')
    msg = json.dumps({"message" : str(message)})
    mmsg = MIMEBase("application","json",charset="UTF-8")
    mmsg.set_payload(msg)
    mmsg.add_header(
            'Content-Disposition',
            'attachment', name="metadata")
    raw_body.attach(mmsg)
    main,sub = mimetypes.guess_type(filepath)[0].split("/")
    with open(filepath, 'rb') as fin:
        img = MIMEBase(main,sub)
        img.set_payload(fin.read())
        img.add_header(
            'Content-Disposition',
            'attachment',
            name = 'file',
            filename = filepath.split('/')[-1]
        )
        raw_body.attach(img)

    raw_headers, body = raw_body.as_string().split('\n\n', 1)
    boundary = re.search('boundary="([^"]*)"', raw_headers).group(1)
    headers['Content-Type'] = 'multipart/related; boundary="{}"'.format(boundary)
    r = requests.post(url, data = body, headers = headers)
    r.raise_for_status()

