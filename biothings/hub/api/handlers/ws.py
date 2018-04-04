import logging
import asyncio
import json

import biothings.hub.api.handlers.base as base
from biothings.utils.hub_db import ChangeListener

import tornado.websocket
import sockjs.tornado


class WebSocketConnection(sockjs.tornado.SockJSConnection):
    """Listen to Hub DB through a listener object, and publish
    events to any client connected"""

    clients = set()

    def __init__(self,session,listener):
        """
        SockJSConnection.__init__() takes only a session as argument, and there's
        no way to pass custom settings. In order to use that class, we need to use partial
        to partially init the instance with 'listener' and let the rest use the 'session'
        parameter:
          pconn = partial(WebSocketConnection,listener=listener)
          ws_router = sockjs.tornado.SockJSRouter(pconn,"/path")
        """
        self.listener = listener
        # propagate connection so listener can access it and trigger message sending
        self.listener.socket = self
        super(WebSocketConnection,self).__init__(session)

    def publish(self, message):
        self.broadcast(self.clients, message)

    def on_open(self, info):
        # Send that someone joined
        self.broadcast(self.clients, "Someone joined. %s" % info)
        # Add client to the clients list
        self.clients.add(self)

    def on_message(self, message):
        err = None
        strerr = None
        try:
            message = json.loads(message)
            if message["op"] == "ping":
                self.send({"op":"pong"})
        except json.JSONDecodeError:
            strerr = "malformed json message: %s" % message
            err = json.JSONDecodeError(strerr)
        except KeyError:
            strerr = "malformed socket message: %s" % message
            err = KeyError(strerr)
        except Exception as e:
            strerr = "Unable to process message '%s': %s" % (message,e)
            err = Exception(strerr)
        if err:
            self.send({"error" : strerr})
            raise err

    def on_close(self):
        # Remove client from the clients list and broadcast leave message
        self.clients.remove(self)
        self.broadcast(self.clients, "Someone left.")


class HubDBListener(ChangeListener):
    """
    Get events from Hub DB and propagate them through the 
    websocket instance
    """

    def read(self, event):
        # self.socket is set while initalizing the websocket connection
        self.socket.publish(event)

