import logging

import biothings.hub.api.handlers.base as base

import tornado.websocket
import sockjs.tornado

class WebSocketConnection(sockjs.tornado.SockJSConnection):
    """Chat connection implementation"""
    # Class level variable
    participants = set()

    def on_open(self, info):
        # Send that someone joined
        self.broadcast(self.participants, "Someone joined. %s" % info)

        # Add client to the clients list
        self.participants.add(self)
        logging.error("now participants: %s" % self.participants)

    def on_message(self, message):
        # Broadcast message
        logging.error("got message: %s" % message)
        self.broadcast(self.participants, message)

    def on_close(self):
        # Remove client from the clients list and broadcast leave message
        self.participants.remove(self)
        self.broadcast(self.participants, "Someone left.")
        logging.error("now participants: %s" % self.participants)

