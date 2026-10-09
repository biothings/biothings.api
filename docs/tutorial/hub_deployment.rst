Hub deployment and access control
=================================

BioThings Hub and Studio are administrative services for preparing data, running
builds, and publishing releases. Deploy them in a trusted administrative environment
and restrict access before starting the services.

.. warning::

   Treat access to the Hub administrative API as permission to administer the Hub.
   Only trusted administrators and authorized automation should be able to reach
   it. Do not expose Hub, Studio, databases, or management tools directly to the
   public internet.

Choose who can access each service
----------------------------------

The BioThings query API serves data to end users and can be deployed as a public
service with appropriate production controls. The Hub manages that data and its
release process. Give these services separate access rules, even when they run in
the same container or on the same host.

Common container ports are listed below; images and host-port mappings may differ.

.. list-table::
   :header-rows: 1
   :widths: 35 20 45

   * - Service
     - Common ports
     - Intended access
   * - Studio, Hub SSH, Hub HTTP API and event connections
     - 8080, 7022, 7080
     - Trusted administrators and authorized automation
   * - Read-only Hub API
     - 7081
     - Authorized viewers; operational information is still private
   * - Elasticsearch, MongoDB, Cerebro and Code-Server
     - 9200, 27017, 9000, 60080
     - Required backend services and trusted administrators
   * - BioThings query API
     - 8000, or 80 in standalone images
     - Intended data consumers, subject to the deployment's access policy

An internal network or VPN is an appropriate boundary only when its access rules
limit these services to their intended users. Membership in a broader organizational
network does not automatically grant permission to administer a Hub. Restrict
access with firewall rules, network access controls, or an authenticating gateway,
and verify the effective rules for both IPv4 and IPv6.

When using a gateway, protect the HTTP API and event connections as well as the
Studio UI, and block direct access to the backend that would bypass the gateway.
For browser access, configure the gateway's authentication and request-origin/CSRF
protections for the intended Studio origin. CORS headers alone are not access
control. A browser on an authorized network can also load content from untrusted
sites, so network location alone does not establish the origin of a request.

Configure port publishing before startup
----------------------------------------

Review the port mappings in the image's deployment instructions and Compose file.
Publish only the services that need to be reached from outside the container
network. For access from the Docker host, include its loopback address explicitly,
for example ``-p 127.0.0.1:7080:7080`` with ``docker run``.

For Compose, replace the existing mapping for each required administrative service
with a loopback mapping. For example, the Hub API entry in that service's ``ports``
list becomes:

.. code-block:: yaml

   ports:
     - "127.0.0.1:7080:7080"

This is a mapping example, not a complete Compose file. Remove any existing broader
mapping for the same service rather than leaving both mappings active. Check the
effective configuration with ``docker compose config`` before starting containers.
For a remote Docker host, use SSH forwarding or an appropriately restricted gateway
to reach host-local ports. ``127.0.0.1`` refers to the Docker host in a port mapping,
and to the user's own machine in a browser URL.

These mappings control publishing on the host; they do not change the listener
inside the container or isolate it from other containers on the same network.
Loopback publishing also does not authenticate local clients. Use a maintained
Docker release and review its `port-publishing behavior
<https://docs.docker.com/engine/network/port-publishing/>`_, including limitations
of localhost publishing in releases older than 28.0.0.

Verify the deployment
---------------------

After startup, inspect the published ports with ``docker compose ps`` or
``docker port <container>``. Check from both an authorized client and a client
outside the permitted network that the access rules work as intended. Repeat these
checks after changing port mappings, firewall rules, gateways, or deployment images.

The same access requirements apply to a Hub started directly on a host: verify its
listening addresses and effective network controls. Protect SSH with
deployment-specific credentials or authorized keys, and keep database access
restricted to the services and administrators that need it.
