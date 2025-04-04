import asyncio
import json
import logging

import sockjs.tornado

from biothings.utils.hub_db import ChangeListener


class WebSocketConnection(sockjs.tornado.SockJSConnection):
    """
    Listen to Hub DB through a listener object, and publish
    events to any client connected
    """

    clients = set()

    def __init__(self, session, listeners):
        """
        SockJSConnection.__init__() takes only a session as argument, and there's
        no way to pass custom settings. In order to use that class, we need to use partial
        to partially init the instance with 'listeners' and let the rest use the 'session'

        parameter:
          pconn = partial(WebSocketConnection,listeners=listeners)
          ws_router = sockjs.tornado.SockJSRouter(pconn,"/path")
        """
        if not isinstance(listeners, list):
            listeners = [listeners]
        self.listeners = listeners
        # propagate connection so listeners can access it and trigger message sending
        for listener in self.listeners:
            listener.socket = self
        super(WebSocketConnection, self).__init__(session)

    def publish(self, message):
        self.broadcast(self.__class__.clients, message)

    def on_open(self, info):
        # Send that someone joined
        self.broadcast(self.__class__.clients, "Someone joined. %s" % info)
        # Add client to the clients list
        self.__class__.clients.add(self)

    def on_message(self, message):
        err_to_raise = None
        err_note = None
        try:
            message = json.loads(message)
            if message["op"] == "ping":
                self.send({"op": "pong"})
        except json.JSONDecodeError as err:
            err_note = "malformed json message: %s" % message
            err.add_note(err_note)
            err_to_raise = err  # we need to assign it here, otherwise it's lost outside of this block
        except KeyError as err:
            err_note = "malformed socket message: %s" % message
            err.add_note(err_note)
            err_to_raise = err
        except Exception as err:
            err_note = "Unable to process message '%s': %s" % (message, err)
            err.add_note(err_note)
            err_to_raise = err
        if err_to_raise:
            self.send({"error": err_note})
            raise err_to_raise

    def on_close(self):
        # Remove client from the clients list and broadcast leave message
        self.__class__.clients.remove(self)
        self.broadcast(self.__class__.clients, "Someone left.")


class HubDBListener(ChangeListener):
    """
    Get events from Hub DB and propagate them through the
    websocket instance
    """

    def read(self, event):
        # self.socket is set while initalizing the websocket connection
        self.socket.publish(event)


class LogListener(ChangeListener):
    # IMPORTANT: no logging calls here, or infinite loop
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.socket = None

    def read(self, event):
        if self.socket:
            # make sure there's a loop in current thread
            try:
                logging.disable(logging.CRITICAL)
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            finally:
                logging.disable(logging.NOTSET)

            try:
                self.socket.publish(event)
            except Exception as e:
                # can't log anything there, but we don't want a problem with
                # issuing the log statements through the websocket to cause
                # any error in the caller
                print(e)
                pass


class ShellListener(LogListener):
    pass
