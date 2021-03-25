import asyncio
import logging

import aiohttp
from elasticsearch.transport import Transport, TransportError
from elasticsearch_async.transport import AsyncTransport

from .es_connection import BiothingsAIOHttpConnection

logger = logging.getLogger(__package__)

class BiothingsAsyncTransport(AsyncTransport):
    """
    Use seed connections in case of invalid sniff results.
    """

    def __init__(self, *args, connection_class=BiothingsAIOHttpConnection, **kwargs):
        super().__init__(*args, connection_class=connection_class, **kwargs)

    @property
    def info(self):
        """
        Report hosts in connection pool.
        """
        return {
            "seed": self.hosts,
            "active": [conn.host for conn in self.connection_pool.connections]
        }

    @asyncio.coroutine
    def sniff_hosts(self, initial=False):
        """
        Obtain a list of nodes from the cluster and create a new connection
        pool using the information retrieved.

        To extract the node connection parameters use the ``nodes_to_host_callback``.

        :arg initial: flag indicating if this is during startup
            (``sniff_on_start``), ignore the ``sniff_timeout`` if ``True``
        """
        ### OVERRIDE START
        #
        try:
            node_info = yield from self._get_sniff_data(initial)
            hosts = list(filter(None, (self._get_host_info(n) for n in node_info)))
        except TransportError as exc:
            logger.error("Unable to sniff hosts.")
            logger.debug(str(exc))
            hosts = []
        else:
            if hosts:
                logger.info("Found hosts: %s", hosts)
            else:
                # we weren't able to get any nodes, maybe using an incompatible
                # transport_schema or host_info_callback blocked all - raise error.
                logger.error("No viable hosts found.")

        valid_hosts = []
        for host in hosts:
            timeout = aiohttp.ClientTimeout(total=3, connect=1.5)
            session = aiohttp.ClientSession(timeout=timeout)
            try:
                url = f"http://{host['host']}:{host['port']}"
                # yield from session.head(url, raise_for_status=True) TODO
                yield from session.head(url, raise_for_status=True, timeout=5)
                valid_hosts.append(host)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                logger.warning(str(exc))
            finally:
                yield from session.close()

        if valid_hosts:
            logger.info("Valid hosts: %s", valid_hosts)
            self.set_connections(valid_hosts)
        else:  # no published address is connectable
            logger.info("Fallback to seed address: %s", self.hosts)
            self.set_connections(self.hosts)

        # close those connections that are not in use any more
        # for c in orig_connections:
        #     if c not in self.connection_pool.connections:
        #         yield from c.close()
        #
        ### OVERRIDE END

class BiothingsTransport(Transport):
    """
    Always retain seed connections.
    """

    def sniff_hosts(self, initial=False):
        """
        Obtain a list of nodes from the cluster and create a new connection
        pool using the information retrieved.

        To extract the node connection parameters use the ``nodes_to_host_callback``.

        :arg initial: flag indicating if this is during startup
            (``sniff_on_start``), ignore the ``sniff_timeout`` if ``True``
        """
        ### OVERRIDE START
        #
        try:
            node_info = yield from self._get_sniff_data(initial)
        except TransportError:
            node_info = []
        #
        ### OVERRIDE END

        hosts = list(filter(None, (self._get_host_info(n) for n in node_info)))

        # we weren't able to get any nodes or host_info_callback blocked all -
        # raise error.

        ### OVERRIDE START
        #
        # if not hosts:
        #     raise TransportError("N/A", "Unable to sniff hosts - no viable hosts found.")
        #
        ### OVERRIDE END

        ### OVERRIDE START
        #
        self.set_connections(self.hosts + hosts)
        #
        ### OVERRIDE END
