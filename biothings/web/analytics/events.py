import hashlib
# import smtplib
import uuid
from collections import UserDict
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from ipaddress import IPv4Address, IPv6Address, ip_address
from random import randint
from typing import Union
from urllib.parse import urlencode
from pprint import pformat


class Event(UserDict):

    # HTTP PageView
    # Fields under __request__:
    # user_agent, user_ip, host, path, referer

    def __getattr__(self, name):
        return self["__request__"][name] or ""

    def _cid_v1(self):
        # Author: Cyrus Afrasiabi
        # Reference: https://github.com/kra3/py-ga-mob
        # More at: pyga.utils.generate_hash

        hash_val = 1

        if self.user_agent:
            hash_val = 0
            for ordinal in map(ord, self.user_agent[::-1]):
                hash_val = ((hash_val << 6) & 0xfffffff) + ordinal + (ordinal << 14)
                left_most_7 = hash_val & 0xfe00000
                if left_most_7 != 0:
                    hash_val ^= left_most_7 >> 21

        return ((randint(0, 0x7fffffff) ^ hash_val) & 0x7fffffff)

    def _cid_v2(self):
        # Author: Zhongchao Qian
        # More at: https://github.com/biothings/biothings.api/pull/124
        """
        Generates a unique user ID

        Using the remote IP and client user agent, to produce a somewhat
        unique identifier for users. A UUID version 4 conforming to RFC 4122
        is produced which is generally acceptable. It is not entirely random,
        but looks random enough to the outside. Using a hash func. the user
        info is completely anonymized.

        Args:
            remote_ip: Client IP address as a string, IPv4 or IPv6
            user_agent: User agent string of the client
        """

        try:
            rip: Union[IPv4Address, IPv6Address] = ip_address(self.user_ip)
            ip_packed = rip.packed
        except ValueError:  # in the weird case I don't get an IP
            ip_packed = randint(0, 0xffffffff).to_bytes(4, 'big')  # nosec

        h = hashlib.blake2b(digest_size=16, salt=b'biothings')
        h.update(ip_packed)
        h.update(self.user_agent.encode('utf-8', errors='replace'))

        d = bytearray(h.digest())
        # truncating hash is not that bad, fixing some bits should be okay, too
        d[6] = 0x40 | (d[6] & 0x0f)  # set version
        d[8] = 0x80 | (d[8] & 0x3f)  # set variant
        u = str(uuid.UUID(bytes=bytes(d)))
        return u

    def _cid(self, version):

        if version == 1:
            return self._cid_v1()
        elif version == 2:
            return self._cid_v2()

        # this is a required GA field
        raise ValueError("CID Version.")

    def to_GA_payload(self, tracking_id, cid_version=1):

        # by default implements
        # a GA PageView hit-type

        # In the future, consider adding additional
        # keys as cutomized dimensions or metrics.

        payload = {
            "v": 1,  # protocol version
            "t": "pageview",
            "tid": tracking_id,
            "cid": self._cid(cid_version),
            "uip": self.user_ip,
            "dh": self.host,
            "dp": self.path
        }

        # add document referer
        if isinstance(self.referer, str):
            if len(self.referer) <= 2048:  # GA Limit
                payload["dr"] = self.referer

        # add user_agent
        if self.user_agent:
            payload["ua"] = self.user_agent

        # this also escapes payload vals
        return [urlencode(payload)]

    def to_GA4_payload(self, measurement_id, cid_version=1):
        payload = {
            "name": "page_view",
            "params": _clean({
                "client_id": self._cid(cid_version),
                "user_ip": self.user_ip,
                "page_location": f'{self.host}{self.path}',
                "page_title": self.path.strip('/').replace('/', '-') if self.path.strip('/') else 'index',
                "page_path": self.path
            })
        }

        # add document referer
        if isinstance(self.referer, str):
            # Parameter values (including item parameter values) must be 100 character or fewer.
            if len(self.referer) <= 100:
                payload["params"]["page_referrer"] = self.referer

        # add user_agent
        if self.user_agent:
            payload["params"]["user_agent"] = self.user_agent[:100]

        return [payload]

    def __str__(self):  # to facilitate logging
        return f"{type(self).__name__}({pformat(self)})"


def _clean(dict):
    return {k: v for k, v in dict.items() if v}


class GAEvent(Event):

    # GA Event
    # {
    #   "category": "video",
    #   "action": "play",
    #   "label": "sample.mpg",
    #   "value": "60"
    # }

    def to_GA_payload(self, tracking_id, cid_version=1):

        payloads = super().to_GA_payload(tracking_id, cid_version)
        if self.get("category") and self.get("action"):
            payloads.append(urlencode(_clean({
                "v": 1,  # protocol version
                "t": "event",
                "tid": tracking_id,
                "cid": self._cid(cid_version),
                "ec": self["category"],
                "ea": self["action"],
                "el": self.get("label", ""),
                "ev": self.get("value", "")
            })))
        for event in self.get("__secondary__", []):
            event["__request__"] = self["__request__"]
            payloads.extend(event.to_GA_payload(tracking_id, cid_version)[1:])
            # ignore the first event (pageview)
            # which is already generated once
        return payloads

    def to_GA4_payload(self, measurement_id, cid_version=1):

        payloads = super().to_GA4_payload(measurement_id, cid_version)
        if self.get("category") and self.get("action"):
            payloads.append({
                "name": self["action"],  # Event hit type
                "params": _clean({
                    "client_id": str(self._cid(cid_version)),  # Anonymous Client ID.
                    "event_category": self["category"],  # Event Category. Required.
                    "event_label": self.get("label", ""),  # Event label.
                    "value": self.get("value", ""),  # Event value.
                })
            })
        for event in self.get("__secondary__", []):
            event["__request__"] = self["__request__"]
            payloads.extend(event.to_GA4_payload(measurement_id, cid_version)[1:])
            # ignore the first event (pageview)
            # which is already generated once
        return payloads


class Message(Event):
    """
    Logical document that can be sent through services.
    Processable fields: title, body, url, url_text, image, image_altext
    Optionally define default field values below.
    """
    DEFAULTS = {
        "title": "Notification Message",
        "url_text": "View Details",
        "image_altext": "<IMAGE>"
    }

    def __getattr__(self, attr):
        # virtual attributes
        if attr in ('title', 'body', 'url', 'url_text', 'image', 'image_altext'):
            if attr in self:
                return self[attr]
            if attr in self.DEFAULTS:
                return self.DEFAULTS[attr]
            return ""
        raise AttributeError()

    def to_ADF(self):
        """
        Generate ADF for Atlassian Jira payload. Overwrite this to build differently.
        https://developer.atlassian.com/cloud/jira/platform/apis/document/playground/
        """
        adf = {
            "version": 1,
            "type": "doc",
            "content": []
        }
        if self.body:
            adf["content"].append({
                "type": "paragraph",
                "content": [{"type": "text", "text": self.body}]
            })
        if self.url:
            adf["content"].append({
                "type": "paragraph",
                "content": [{
                    "type": "text", "text": self.url_text,
                    "marks": [{"type": "link", "attrs": {"href": self.url}}]
                }]
            })
        return adf

    def to_slack_payload(self):
        """
        Generate slack webhook notification payload.
        https://api.slack.com/messaging/composing/layouts
        """
        blocks = []
        if self.title:
            blocks.append({
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": ":sparkles: " + self.title,
                    "emoji": True
                }
            })
            blocks.append({
                "type": "divider"
            })
        if self.body:
            body = {
                "type": "section",
                "text": {"type": "mrkdwn", "text": self.body},
            }
            if self.image:
                body["accessory"] = {
                    "type": "image",
                    "image_url": self.image,
                    "alt_text": self.image_altext
                }
            blocks.append(body)
        if self.url:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*<{self.url}|{self.url_text}>*"}
            })
        return {
            "attachments": [{
                "blocks": blocks
            }]
        }

    def to_jira_payload(self, profile):
        """
        Combine notification message with project profile to
        genereate jira issue tracking ticket request payload.
        """
        return {
            "fields": {
                "project": {"id": profile.project_id},
                "summary": self.title,
                "issuetype": {"id": profile.issuetype_id},
                "assignee": {"id": profile.assignee_id},
                "reporter": {"id": profile.reporter_id},
                "priority": {"id": "3"},
                "labels": [profile.label],
                "description": self.to_ADF()
            }
        }

    def to_email_payload(self, sendfrom, sendto):
        """
        Build a MIMEMultipart message that can be sent as an email.
        https://docs.aws.amazon.com/ses/latest/DeveloperGuide/examples-send-using-smtp.html
        """
        # Create message container - the correct MIME type is multipart/alternative.
        msg = MIMEMultipart('alternative')
        msg['Subject'] = self.title
        # msg['From'] = email.utils.formataddr((SENDERNAME, SENDER))
        msg['From'] = sendfrom
        msg['To'] = sendto

        # Comment or delete the next line if you are not using a configuration set
        # msg.add_header('X-SES-CONFIGURATION-SET',CONFIGURATION_SET)

        # Record the MIME types of both parts - text/plain and text/html.
        part1 = MIMEText(self.body, 'plain')
        part2 = MIMEText(f"""
        <!DOCTYPE html>
        <html>
            <head>
                <title>{self.title}</title>
            </head>
            <body>
                <p>{self.body}</p>
            </body>
        </html>""", 'html')

        # Attach parts into message container.
        # According to RFC 2046, the last part of a multipart message, in this case
        # the HTML message, is best and preferred.
        msg.attach(part1)
        msg.attach(part2)

        return msg
