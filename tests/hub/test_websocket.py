import asyncio
import threading
from types import SimpleNamespace

import pytest
from tornado.ioloop import IOLoop

from biothings.hub.api.handlers.ws import WebSocketConnection


@pytest.mark.asyncio
async def test_publish_sends_from_the_ioloop_thread(monkeypatch):
    sent = []
    monkeypatch.setattr(
        WebSocketConnection, "broadcast", lambda self, clients, msg: sent.append((threading.current_thread(), msg))
    )
    session = SimpleNamespace(server=SimpleNamespace(io_loop=IOLoop.current()))
    conn = WebSocketConnection(session, listeners=[])
    conn.publish({"op": "log", "msg": "from the loop"})
    # eg. a log statement from a job running in a thread
    await asyncio.to_thread(conn.publish, {"op": "log", "msg": "from a thread"})
    await asyncio.sleep(0)
    loop_thread = threading.current_thread()
    assert sent == [
        (loop_thread, {"op": "log", "msg": "from the loop"}),
        (loop_thread, {"op": "log", "msg": "from a thread"}),
    ]
