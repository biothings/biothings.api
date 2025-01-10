class OAuth2Mixin:
    """
    Abstract implementation of OAuth 2.0.

    See `FacebookGraphMixin` or `GoogleOAuth2Mixin` below for example
    implementations.

    Class attributes:

    * ``_OAUTH_AUTHORIZE_URL``: The service's authorization url.
    * ``_OAUTH_ACCESS_TOKEN_URL``:  The service's access token url.
    """

    def authorize_redirect(
        self,
        redirect_uri: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        extra_params: Optional[Dict[str, Any]] = None,
        scope: Optional[List[str]] = None,
        response_type: str = "code",
    ) -> None:
        """Redirects the user to obtain OAuth authorization for this service.

        Some providers require that you register a redirect URL with
        your application instead of passing one via this method. You
        should call this method to log the user in, and then call
        ``get_authenticated_user`` in the handler for your
        redirect URL to complete the authorization process.

        .. versionchanged:: 6.0

           The ``callback`` argument and returned awaitable were removed;
           this is now an ordinary synchronous function.

        .. deprecated:: 6.4
           The ``client_secret`` argument (which has never had any effect)
           is deprecated and will be removed in Tornado 7.0.
        """
        if client_secret is not None:
            warnings.warn("client_secret argument is deprecated", DeprecationWarning)
        handler = cast(RequestHandler, self)
        args = {"response_type": response_type}
        if redirect_uri is not None:
            args["redirect_uri"] = redirect_uri
        if client_id is not None:
            args["client_id"] = client_id
        if extra_params:
            args.update(extra_params)
        if scope:
            args["scope"] = " ".join(scope)
        url = self._OAUTH_AUTHORIZE_URL  # type: ignore
        handler.redirect(url_concat(url, args))

    def _oauth_request_token_url(
        self,
        redirect_uri: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        code: Optional[str] = None,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> str:
        url = self._OAUTH_ACCESS_TOKEN_URL  # type: ignore
        args = {}  # type: Dict[str, str]
        if redirect_uri is not None:
            args["redirect_uri"] = redirect_uri
        if code is not None:
            args["code"] = code
        if client_id is not None:
            args["client_id"] = client_id
        if client_secret is not None:
            args["client_secret"] = client_secret
        if extra_params:
            args.update(extra_params)
        return url_concat(url, args)

    async def oauth2_request(
        self, url: str, access_token: Optional[str] = None, post_args: Optional[Dict[str, Any]] = None, **args: Any
    ) -> Any:
        """Fetches the given URL auth an OAuth2 access token.

        If the request is a POST, ``post_args`` should be provided. Query
        string arguments should be given as keyword arguments.

        Example usage:

        ..testcode::

            class MainHandler(tornado.web.RequestHandler,
                              tornado.auth.FacebookGraphMixin):
                @tornado.web.authenticated
                async def get(self):
                    new_entry = await self.oauth2_request(
                        "https://graph.facebook.com/me/feed",
                        post_args={"message": "I am posting from my Tornado application!"},
                        access_token=self.current_user["access_token"])

                    if not new_entry:
                        # Call failed; perhaps missing permission?
                        self.authorize_redirect()
                        return
                    self.finish("Posted a message!")

        .. testoutput::
           :hide:

        .. versionadded:: 4.3

        .. versionchanged::: 6.0

           The ``callback`` argument was removed. Use the returned awaitable object instead.
        """
        all_args = {}
        if access_token:
            all_args["access_token"] = access_token
            all_args.update(args)

        if all_args:
            url += "?" + urllib.parse.urlencode(all_args)
        http = self.get_auth_http_client()
        if post_args is not None:
            response = await http.fetch(url, method="POST", body=urllib.parse.urlencode(post_args))
        else:
            response = await http.fetch(url)
        return escape.json_decode(response.body)

    def get_auth_http_client(self) -> httpclient.AsyncHTTPClient:
        """Returns the `.AsyncHTTPClient` instance to be used for auth requests.

        May be overridden by subclasses to use an HTTP client other than
        the default.

        .. versionadded:: 4.3
        """
        return httpclient.AsyncHTTPClient()
