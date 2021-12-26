import abc
from typing import Iterable, Optional, Tuple, Type

from biothings.web.handlers import BaseAPIHandler


__all__ = [
    'BioThingsAuthenticationProviderInterface',
    'BioThingsAuthnMixin'
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
    def get_current_user(self):
        """
        Get the user from list of preconfigured authentication providers.
        """
        # Support pluggable authentication.
        # Compare to PAM in Linux. Sample logic below
        authenticators: \
            Iterable[Tuple[Type[BioThingsAuthenticationProviderInterface], dict]] = \
            getattr(
                self,
                'AUTHN_PROVIDERS',
                self.biothings.config.AUTHN_PROVIDERS
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
        authenticators: \
            Iterable[Tuple[Type[BioThingsAuthenticationProviderInterface], dict], ...] = \
            getattr(
                self,
                'AUTHN_PROVIDERS',
                self.biothings.config.AUTHN_PROVIDERS
            )
        for authenticator_cls, _ in authenticators:
            header = authenticator_cls.WWW_AUTHENTICATE_HEADER
            if header is not None:
                return header
        return None
