import urllib.parse
from typing import Any, Dict, Iterable, Optional

from tornado.auth import OAuth2Mixin
from tornado.escape import json_decode
from tornado.httputil import HTTPHeaders


__all__ = [
    'GithubOAuth2Mixin',
]


class GithubOAuth2Mixin(OAuth2Mixin):
    _OAUTH_AUTHORIZE_URL = 'https://github.com/login/oauth/authorize'
    _OAUTH_ACCESS_TOKEN_URL = 'https://github.com/login/oauth/access_token'

    async def get_oauth2_token(
            self,
            redirect_uri: str,
            client_id: str,
            client_secret: str,
            code: str,
    ) -> Optional[dict]:
        """
        Get Github OAuth2 Token

        Returns:
            Dictionary with keys access_token, scope, token_type. See https://git.io/J1ON4
            None if the operation failed
        """
        http = self.get_auth_http_client()
        args = {
            "redirect_uri": redirect_uri,
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
        }
        headers = HTTPHeaders()
        headers.add('Accept', 'application/json')
        response = await http.fetch(
            self._OAUTH_ACCESS_TOKEN_URL,
            raise_error=False,
            method='POST',
            body=urllib.parse.urlencode(args),
            headers=headers,
        )
        r = None
        if response.code == 200:
            try:
                r = json_decode(response.body)
            except ValueError:
                pass
        return r

    async def get_authenticated_user(self, token: str) -> Optional[dict]:
        """
        Get Github User Info


        """
        return


