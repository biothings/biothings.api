import abc
import json
from typing import Iterable, Optional, Tuple, Type

from biothings.web.handlers import BaseAPIHandler

__all__ = [
    "BioThingsAuthenticationProviderInterface",
    "BioThingsAuthnMixin",
]


class BioThingsAuthenticationProviderInterface(abc.ABC):
    """
    Authentication Provider Interface for BioThings API Endpoints

    BioThingsAuthnMixin depends on this interface. Any authentication for
    the API endpoints is recommended to be implemented using this
    interface.

    A provider will be initialized with the API Handler calling it, so it will
    have access to the request, and other related BioThings facilities.

    Args:
        handler: the BaseAPIHandler that invoked this provider

    Attributes:
        WWW_AUTHENTICATE_HEADER: string used for the 'WWW-Authenticate' Header.
            If the handler returns a 401, this may be used to populate the
            'WWW-Authenticate' Header. It should be None or one from the list
            https://www.iana.org/assignments/http-authschemes/http-authschemes.xhtml
    """

    WWW_AUTHENTICATE_HEADER: Optional[str] = None

    def __init__(self, handler: BaseAPIHandler, **kwargs):
        # we use the BioThings BaseAPIHandler so that:
        #  - the interface has access to self.handler.biothings
        #  - it is something only intended for programmatic access
        super().__init__()
        self.handler = handler

    @abc.abstractmethod
    def get_current_user(self) -> Optional[dict]:
        """
        Get the current user.

        If authentication succeeds, return the current user.
        If authentication fails, return None
        """
        raise NotImplementedError


class BioThingsAuthnMixin(BaseAPIHandler):
    """
    MixIn Class to for Pluggable User Authentication

    Configuring `AUTHN_PROVIDERS` setting to a list of tuples, each tuple
    containing an authentication provider and its additional initialization
    arguments.

    The authentication providers will be processed in the order they are
    configured.

    Mixing in this class enables getting the current authenticated user through
    the `current_user` attribute of RequestHandler instances.

    The `get_www_authenticate_header` will offer a value to populate the
    WWW-Authenticate header, in the case that a 401 status code needs to be returned.

    Example:
        >>> from tornado.web import HTTPError
        >>> class ExampleGetUserInfoHandler(BioThingsAuthnMixin, BaseAPIHandler):
        >>>     def get(self):
        >>>         # if the current user is set, send it to output
        >>>         if self.current_user:
        >>>             self.write(self.current_user)
        >>>         else:
        >>>             # not authenticated, return 401 or 403
        >>>             header = self.get_www_authenticate_header()
        >>>             if header:
        >>>                 self.clear()
        >>>                 self.set_header('WWW-Authenticate', header)
        >>>                 self.set_status(401, "Unauthorized")
        >>>                 # raising HTTPError will cause headers to be emptied
        >>>                 self.finish()
        >>>             else:
        >>>                 raise HTTPError(403)

    Attributes:
        AUTHN_PROVIDERS: Overrides the global options for the specific handler.
            Do not set this attribute if not intending to override the default.

    Notes:
        Beware of MRO issues, see example for proper order of multi-inheritance.
    """

    def get_current_user(self):
        """
        Get the user from list of preconfigured authentication providers.
        """
        # Support pluggable authentication.
        # Compare to PAM in Linux. Sample logic below
        authenticators: Iterable[Tuple[Type[BioThingsAuthenticationProviderInterface], dict]] = getattr(
            self, "AUTHN_PROVIDERS", self.biothings.config.AUTHN_PROVIDERS
        )

        # loop through the list in order and initialize the provider using
        # self & configured options (like how handlers are configured for routing)
        # use the provider to check if the user can log in
        for authenticator_cls, init_kwargs in authenticators:
            authenticator = authenticator_cls(self, **init_kwargs)
            user = authenticator.get_current_user()
            if user:
                return user
        return None

    def get_www_authenticate_header(self) -> Optional[str]:
        """
        Get the most preferred header to populate WWW-Authenticate

        If this method returns None, it is better to set a 403 error status code.
        Otherwise, return a 401 error and use the value returned in the
        WWW-Authenticate header.

        According to RFC 7235 https://datatracker.ietf.org/doc/html/rfc7235#section-3.1
        "the server generating a 401 response MUST send a WWW-Authenticate header field".
        """
        authenticators: Iterable[Tuple[Type[BioThingsAuthenticationProviderInterface], dict], ...] = getattr(
            self, "AUTHN_PROVIDERS", self.biothings.config.AUTHN_PROVIDERS
        )
        for authenticator_cls, _ in authenticators:
            header = authenticator_cls.WWW_AUTHENTICATE_HEADER
            if header is not None:
                return header
        return None


class DefaultCookieAuthnProvider(BioThingsAuthenticationProviderInterface):
    WWW_AUTHENTICATE_HEADER = "None"

    def __init__(self, handler, cookie_name="user"):
        super(DefaultCookieAuthnProvider, self).__init__(handler)
        self.cookie_name = cookie_name

    def get_current_user(self):
        user = self.handler.get_secure_cookie(self.cookie_name)
        if not user:
            return None
        return json.loads(user.decode())
