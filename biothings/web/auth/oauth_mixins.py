import urllib.parse
from typing import Union, Optional, Dict, Any

from tornado.auth import OAuth2Mixin
from tornado.escape import json_decode
from tornado.httputil import HTTPHeaders

__all__ = [
    'GithubOAuth2Mixin',
    'OrcidOAuth2Mixin'
]


class GithubOAuth2Mixin(OAuth2Mixin):
    _OAUTH_AUTHORIZE_URL = 'https://github.com/login/oauth/authorize'
    _OAUTH_ACCESS_TOKEN_URL = 'https://github.com/login/oauth/access_token'

    _GITHUB_API_URL_BASE = 'https://api.github.com/'

    _GITHUB_API_ENDPOINTS = {
        'user': urllib.parse.urljoin(_GITHUB_API_URL_BASE, 'user')
    }

    async def github_get_oauth2_token(
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

    async def github_get_authenticated_user(self, token: Union[dict, str]) -> dict:
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
            >>> token = await self.github_get_oauth2_token(
            >>>     'SOME_CLIENT_ID', 'SOME_CLIENT_SECRET',
            >>>     'THE_CODE_GITHUB_PROVIDED_IN_THE_REDIRECT'
            >>> )
            >>> user = await self.github_get_authenticated_user(token)
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
        headers.add('Accept', 'application/vnd.github.v3+json')
        resp = await http.fetch(self._GITHUB_API_ENDPOINTS['user'], method='GET',
                                headers=headers)
        ret = json_decode(resp.body)
        return ret


class OrcidOAuth2Mixin(OAuth2Mixin):
    """
    Mixin Class for using ORCID API with OAuth2

    Note:
        When redirecting the user to the authorization page, do NOT use
        the '/read-public' scope. Either get it using OAuth2 client
        credentials flow or just use '/authenticate' or 'openid' scopes,
        the token returned with these two scopes can be used to read
        public data.
    """
    _OAUTH_AUTHORIZE_URL = "https://orcid.org/oauth/authorize"
    _OAUTH_ACCESS_TOKEN_URL = "https://orcid.org/oauth/token"
    _ORCID_API_URL_BASE = 'https://pub.orcid.org/v2.0/'

    async def orcid_get_oauth2_token(
            self,
            client_id: str,
            client_secret: str,
            code: str,
    ) -> dict:
        """
        Get OAuth2 Token from ORCID

        Returns:
            Dictionary with access_token. If `openid` scope is used, key 'id_token'
                will also be present.

        Raises:
            HTTPError: if request fails
            JSONDecodeError: if parsing the response fails
        """
        http = self.get_auth_http_client()
        args = {
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "authorization_code",
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

    async def orcid_oauth2_request(
            self,
            url: str,
            access_token: Union[dict, str],
            method: str = 'GET',
            **kwargs: Any,
    ):
        """
        Make Request to ORCID API With OAuth2 Token

        Args:
            url: The full request URL in string
            access_token: the ORCID access token, either the entire object or the token
                in string. It is NOT the OpenID Connect ID Token.
            method: HTTP Method to use, string.
            **kwargs: Any additional keyword arguments are directly passed to the
                tornado.httpclient.AsyncHTTPClient.fetch method.
        Raises:
            ValueError: if the token looks obviously invalid
            HTTPError: if the request fails. Note Tornado seems to use 599 for timeouts
            JSONDecodeError: if JSON decoding fails. Check if you're making requests
                to the correct ORCID endpoint.
        """
        http = self.get_auth_http_client()
        headers = HTTPHeaders()
        if isinstance(access_token, str):
            token_str = access_token
        elif isinstance(access_token, dict):
            if 'token_type' not in access_token \
                    or access_token['token_type'] != 'bearer' \
                    or 'access_token' not in access_token:
                raise ValueError("Token seems invalid")
            else:
                token_str = access_token['access_token']
        else:
            raise ValueError("Token seems invalid")
        headers.add('Authorization', f'Bearer {token_str}')
        headers.add('Accept', 'application/json')
        resp = await http.fetch(url, method=method, headers=headers, **kwargs)
        ret = json_decode(resp.body)
        return ret

    async def orcid_get_authenticated_user_oidc(self, access_token: Union[dict, str])\
            -> dict:
        """
        Get ORCID User OpenID Connect information

        See ORCID Documentation https://git.io/JyY23 or its latest version.

        ORCID supports OpenID Connect and this provides the OpenID Connect user details
        format. This method obtains such information.

        Args:
             access_token: the ORCID access token, either the entire object or the token
                in string. It is NOT the OpenID Connect ID Token.

        Returns:
            Dictionary with OIDC User Details
        """
        ret = await self.orcid_oauth2_request(
            urllib.parse.urljoin(self._OAUTH_ACCESS_TOKEN_URL, 'userinfo'),
            access_token, 'GET'
        )
        return ret

    async def orcid_get_authenticated_user_record(
            self,
            access_token: Union[dict, str],
            orcid: str,
    ) -> dict:
        """
        Get ORCID User Record

        Get the full ORCID user record. Note, what you get depends on what the user
        allows you to read.

        Args:
            access_token: the ORCID access token, either the entire object or the token
                in string. It is NOT the OpenID Connect ID Token.
            orcid: the ORCID of the authenticated user. Typically you can find this
                in the token object under the 'orcid' field.
        Returns:
            Dictionary with ORCID Record
        """
        ret = await self.orcid_oauth2_request(
            urllib.parse.urljoin(self._ORCID_API_URL_BASE, f'{orcid}/record'),
            access_token, 'GET'
        )
        return ret
