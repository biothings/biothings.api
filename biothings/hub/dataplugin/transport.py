import urllib

from biothings.hub.dataload.dumper import (
    LastModifiedFTPDumper,
    LastModifiedHTTPDumper,
    DockerContainerDumper,
)
from biothings.hub.dataplugin.exceptions import PluginTransportException

METADUMPER_CONFIGURATION = {
    "http": {"class": LastModifiedHTTPDumper, "headers": {}},
    "https": {"class": LastModifiedHTTPDumper, "headers": {"VERIFY_CERT": False}},
    "ftp": {"class": LastModifiedFTPDumper, "headers": {}},
    "docker": {"class": DockerContainerDumper, "headers": {}},
}


class DumperTransport:

    def __init__(self, data_urls: str | list[str]):
        self.dumper_urls = data_urls
        if not isinstance(data_urls, list):
            self.dumper_urls = [data_urls]
        self.protocol = self.determine_protocol_scheme()
        self.dumper_class = METADUMPER_CONFIGURATION[self.protocol]["class"]
        self.headers = METADUMPER_CONFIGURATION[self.protocol]["headers"]

    def determine_protocol_scheme(self) -> str:
        """Derive the protocol from the url scheme provided by the data urls."""
        try:
            protocol_schemes = {urllib.parse.urlsplit(durl).scheme for durl in self.dumper_urls}
        except ValueError as value_error:
            raise PluginTransportException(
                "Unable to dump urls due to formatting issue. Please verify your data urls in the manifest"
            ) from value_error

        # https = http regarding dumper generation
        if len({sch.replace("https", "http") for sch in protocol_schemes}) > 1:
            raise PluginTransportException(
                "Manifest specifies URLs of different protocol types (%s), we require exactly one protocol type"
                % protocol_schemes
            )

        protocol = protocol_schemes.pop()
        if "docker" in protocol:
            protocol = "docker"

        if protocol not in METADUMPER_CONFIGURATION:
            protocol_message = (
                f"Unsupported protocol type {protocol}. "
                "Please update your data urls to support one of the following schemas: "
                f"{list(METADUMPER_CONFIGURATION.keys())}"
            )
            raise PluginTransportException(protocol_message)
        return protocol

    def override_transport_headers(self, additional_headers: dict = None) -> dict:
        """Allows for header overloading to allow for non-default customization of our transport protocol."""
        if additional_headers is None:
            additional_headers = {}
        self.headers.update(additional_headers)
        return self.headers
