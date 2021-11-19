import abc
from typing import Iterable, Optional, Tuple, Type

from biothings.web.handlers import BaseHandler


__all__ = [
    'BioThingsAuthenticationProviderInterface',
    'BioThingsAuthnMixin'
]


class BioThingsAuthenticationProviderInterface(abc.ABC):
    def __init__(self, handler: BaseHandler, **kwargs):
        # we use the BioThings BaseHandler so that
        # the interface has access to self.biothings
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

    def get_authenticate_header(self) -> Optional[str]:
        """
        Return a string used for the 'WWW-Authenticate' Header

        If the handler returns a 401, this is used
        """
        # FIXME: need to figure out a way to support both Authorization header
        #  and non-authorization header methods
        #  the former should return a 401 and will need WWW-Authenticate
        #  while the latter (say cookie based) gets more freedom.
        return None


class BioThingsAuthnMixin(BaseHandler):
    def get_current_user(self):
        """
        Get the user from list of preconfigured authentication providers.
        """
        # Support pluggable authentication.
        # Compare to PAM in Linux. Sample logic below
        authenticators: \
            Iterable[Tuple[Type[BioThingsAuthenticationProviderInterface], dict], ...] = \
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
