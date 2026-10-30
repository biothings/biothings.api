Hub deployment and access control
=================================

BioThings Hub and Studio are administrative services: they prepare data, run builds and
publish releases. Only trusted administrators and authorized automation should reach them,
and neither they nor their databases should be exposed to the public internet.

.. warning::

   Unless it requires a login, anyone who can reach the Hub API can run commands on the Hub:
   consider :ref:`requiring a login <hub-login>`.

Choose who can access each service
----------------------------------

The BioThings query API serves data to end users and can be public. The Hub manages that data
and its releases. Give them separate access rules, even when they run on the same host or in
the same container. Common container ports are listed below; images and host-port mappings
may differ.

.. list-table::
   :header-rows: 1
   :widths: 35 20 45

   * - Service
     - Common ports
     - Intended access
   * - Studio, Hub API and its websocket
     - 8080, 7080
     - Trusted administrators and authorized automation
   * - Read-only Hub API
     - 7081
     - Authorized viewers: it still exposes operational information
   * - Elasticsearch, MongoDB, Cerebro and Code-Server
     - 9200, 27017, 9000, 60080
     - Required backend services and trusted administrators
   * - BioThings query API
     - 8000, or 80 in standalone images
     - Its intended data consumers

An internal network or VPN is an appropriate boundary only if its rules limit these services
to their intended users: being on a broader network doesn't make someone a Hub administrator.
Restrict access with firewall rules, network access controls or an authenticating gateway, for
both IPv4 and IPv6. A gateway must cover the Hub API and its websocket, not only Studio, and
direct access to the Hub's ports must be blocked. CORS headers are not access control.

.. _hub-login:

Require a login to run commands
-------------------------------

When users are defined in the Hub's configuration, requests to the Hub API running commands or
changing data (any method other than ``GET``, ``HEAD`` and ``OPTIONS``) require a login. That
covers Studio's terminal, the buttons running commands, and uploads. Reading data stays open.
Network rules alone don't stop a web page opened in a browser on an allowed network from sending
requests to the Hub: a login protects the commands from those too.

1. Make a password hash for each user (the password is asked twice)::

     python -m biothings.utils.passwords

2. Add the users to the Hub's configuration, and restart the Hub::

     HUB_API_USERS = {"alice": "pbkdf2_sha256$600000$..."}

   The Hub logs ``Hub API: running commands requires a login, for users: alice``, and doesn't
   start if a password isn't a hash.

In BioThings Studio, the user icon next to the Hub's name logs in and out, and Studio asks for
a login when the Hub refuses a command. The command line interface logs in with ``biothings-cli
hub login`` (see :doc:`cli`). Scripts log in with ``POST /login``, and send the token they get in
the ``X-Biothings-Access-Token`` header:

.. code-block:: bash

   $ curl -X POST -d '{"username": "alice", "password": "..."}' http://localhost:7080/login
   {"result":{"username":"alice","token":"eyJhbGciOi...","expires":"..."},"status":"ok"}
   $ curl -X PUT -H "X-Biothings-Access-Token: eyJhbGciOi..." -d '{"cmd": "status()"}' http://localhost:7080/shell

A login lasts 12 hours (``HUB_API_LOGIN_EXPIRY``, in seconds). Changing a user's password, or
removing the user, ends their logins. After 5 failed logins in a row, a user can only try again
once a minute. The Hub logs each login, each failed login with the client's address, and each
request running a command with the user's name.

.. note:: The Hub no longer runs an SSH console: ``HUB_SSH_PORT`` and ``HUB_PASSWD`` are
   ignored, and the Hub's commands run from BioThings Studio's terminal.

Use HTTPS
---------

Over plain HTTP, passwords and login tokens cross the network unencrypted. The Hub serves
HTTPS when its configuration gives it a certificate:

.. code-block:: python

   TORNADO_SETTINGS = {"ssl_options": {"certfile": "/path/to/cert.pem", "keyfile": "/path/to/key.pem"}}

An HTTPS reverse proxy, such as nginx, can also serve the Hub, as long as direct access to the
Hub's ports is blocked. Clients then use ``https://`` addresses, and browsers and scripts must
trust the certificate.

Configure port publishing before startup
----------------------------------------

Publish only the services that must be reached from outside the container network. For access
from the Docker host only, bind each mapping to the loopback address, eg.
``-p 127.0.0.1:7080:7080`` with ``docker run``, or in a Compose file:

.. code-block:: yaml

   ports:
     - "127.0.0.1:7080:7080"

Replace the existing broader mapping rather than adding one, and check the result with
``docker compose config``. For a remote Docker host, use SSH port forwarding or a restricted
gateway. Loopback publishing doesn't authenticate local clients, nor isolate containers sharing
a Docker network. Use Docker 28.0.0 or later: in older releases, other hosts on the same network
segment could reach ports published to the loopback address (see `port publishing
<https://docs.docker.com/engine/network/port-publishing/>`_).

Verify the deployment
---------------------

After startup, and after any change to port mappings, firewall rules, gateways or images, list
the published ports (``docker compose ps`` or ``docker port <container>``), and check from an
authorized client and from one outside the permitted network that the rules work as intended.
For a Hub started directly on a host, check the addresses it listens on and the network
controls in front of them.
