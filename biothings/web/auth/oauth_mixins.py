import urllib.parse
from typing import Union

from tornado.auth import OAuth2Mixin
from tornado.escape import json_decode
from tornado.httputil import HTTPHeaders

__all__ = [
    'GithubOAuth2Mixin',
]


class GithubOAuth2Mixin(OAuth2Mixin):
    _OAUTH_AUTHORIZE_URL = 'https://github.com/login/oauth/authorize'
    _OAUTH_ACCESS_TOKEN_URL = 'https://github.com/login/oauth/access_token'

    _GITHUB_API_URL_BASE = 'https://api.github.com/'

    _GITHUB_API_ENDPOINTS = {
        'user': urllib.parse.urljoin(_GITHUB_API_URL_BASE, 'user')
    }

    async def get_oauth2_token(
            self,
            client_id: str,
            client_secret: str,
            code: str,
    ) -> dict:
        """
        Get Github OAuth2 Token

        Returns:
            Dictionary with keys access_token, scope, token_type. See https://git.io/J1ON4

        Raises:
            HTTPError: if request fails
            JSONDecodeError: if parsing the response fails
        """
        http = self.get_auth_http_client()
        args = {
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
        }
        headers = HTTPHeaders()
        headers.add('Accept', 'application/json')
        response = await http.fetch(
            self._OAUTH_ACCESS_TOKEN_URL,
            raise_error=True,
            method='POST',
            body=urllib.parse.urlencode(args),
            headers=headers,
        )
        ret = json_decode(response.body)
        return ret

    async def get_authenticated_user(self, token: Union[dict, str]) -> dict:
        """
        Get Github User Info

        Basically, only request the /user endpoint with the token provided.
        The token does not need special scopes to read basic user information
        (like Github username or user ID), but to get the user email in the
        response, a token with 'user:email' scope is needed.

        Caveats: unlike the torngithub package mixin or the Facebook mixin,
        this method has a different signature and accepts different parameters
        (just the token). Also unlike the Google mixin this actually returns the
        user, not the token. So, to replace the mixin, one method call becomes
        two calls, first get the token, then get the user.

        Another caveat is that the callback functions have been removed. We
        weren't using them anyways, and sane humans don't like callback hell.

        Args:
            token: token string or raw token dict obtained from get_oauth2_token in
            this mixin class

        Returns:
            Github user information in a dict. What you get depends on the scope
            of the token provided.

        Raises:
            HTTPError: if request fails
            JSONDecodeError: if parsing the response fails
            RuntimeError: when using an invalid token or unsupported token type

        Example:
            >>> token = await self.get_oauth2_token(
            >>>     'SOME_CLIENT_ID', 'SOME_CLIENT_SECRET',
            >>>     'THE_CODE_GITHUB_PROVIDED_IN_THE_REDIRECT'
            >>> )
            >>> user = await self.get_authenticated_user(token)
            >>> print(user['login'], user['id'])
            zcqian 7196478

        """
        http = self.get_auth_http_client()
        headers = HTTPHeaders()
        if isinstance(token, str):
            token_str = token
        elif isinstance(token, dict):
            if 'token_type' not in token \
                    or token['token_type'] != 'bearer' \
                    or 'access_token' not in token:
                raise RuntimeError("Token seems invalid")
            else:
                token_str = token['access_token']
        else:
            raise RuntimeError("Token seems invalid")
        headers.add('Authorization', f'token {token_str}')
        headers.add('Accept', 'Accept: application/vnd.github.v3+json')
        resp = await http.fetch(self._GITHUB_API_ENDPOINTS['user'], method='GET',
                                headers=headers)
        ret = json_decode(resp.body)
        return ret
