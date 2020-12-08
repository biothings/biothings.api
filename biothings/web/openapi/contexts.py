from typing import Optional, MutableMapping, Type


class _BaseMetaClass(type):
    subclasses: MutableMapping[str, Type['_ChildContext']] = {}

    def __new__(cls, clsname, bases, attrs):
        # produce the method that creates child contexts
        def mk_ch_context_meth(child_context, field):
            def fn(self, **kwargs):
                ch_context = cls.subclasses[child_context](self)
                for at, v in kwargs.items():
                    if not at.startswith('_') and at in dir(ch_context):
                        meth = getattr(ch_context, at)
                        if callable(meth):
                            meth(v)
                            continue
                    raise AttributeError(f"Child does not have {at}")
                self.document[field] = ch_context.document
                return ch_context
            return fn

        # produce the method that updates attributes
        def mk_attr_meth(field):
            def fn(self, v):
                self.document[field] = v
                return self
            return fn

        if 'CHILD_CONTEXTS' in attrs:
            for name, (ch_context, field) in attrs['CHILD_CONTEXTS'].items():
                # FIXME: Hopefully we want the docstrings for params here
                #  Which is kind of a problem because the "more root" types
                #  are defined earlier, and to obtain the possible params
                #  we need to look at the "less root" class which is yet to be
                #  defined.
                docstring = f"""Set {field}
                            Create {ch_context} and set {field}
                            """
                meth = mk_ch_context_meth(ch_context, field)
                meth.__doc__ = docstring
                meth.__name__ = name
                attrs[name] = meth
        if 'ATTRIBUTE_FIELDS' in attrs:
            for name, field in attrs['ATTRIBUTE_FIELDS'].items():
                docstring = f"""Set {field} field"""
                meth = mk_attr_meth(field)
                meth.__doc__ = docstring
                meth.__name__ = name
                attrs[name] = meth

        new_cls = type.__new__(cls, clsname, bases, attrs)
        cls.subclasses[clsname] = new_cls
        return new_cls


class _BaseContext(metaclass=_BaseMetaClass):
    # {'method_name': ('ContextClass', 'fieldName')}
    CHILD_CONTEXTS = {}
    # {'method_name: 'fieldName'}
    ATTRIBUTE_FIELDS = {}

    def __init__(self):
        self.document = {}


class _ChildContext(_BaseContext):
    def __init__(self, parent):
        super().__init__()
        self.parent = parent

    def end(self):
        """Explicitly return to the parent context

        Returns:
            Parent context
        """
        return self.parent

    def __getattr__(self, attr_name: str):
        multilevel_allow = ['path', 'get', 'put', 'post', 'delete', 'options',
                            'head', 'patch', 'trace']
        if not attr_name.startswith('_'):
            if attr_name in dir(self.parent) or (
                    attr_name in multilevel_allow and
                    hasattr(self.parent, attr_name)):
                return getattr(self.parent, attr_name)
        raise AttributeError()


class _HasParameters(_BaseContext):
    def parameter(self, name, in_, required):
        parameters = self.document.setdefault('parameters', [])
        param_context = OpenAPIParameterContext(self, name, in_, required)
        parameters.append(param_context.document)
        return param_context


class _HasSummary(_BaseContext):
    ATTRIBUTE_FIELDS = {
        'summary': 'summary'
    }


class _HasDescription(_BaseContext):
    ATTRIBUTE_FIELDS = {
        'description': 'description'
    }


class _HasExternalDocs(_BaseContext):
    CHILD_CONTEXTS = {
        'external_docs': ('OpenAPIExternalDocsContext', 'externalDocs'),
    }


class _HasTags(_BaseContext):
    def tag(self, t):
        tags = self.document.setdefault('tag', [])
        tags.append(t)
        return self


class OpenAPIContext(_HasExternalDocs):
    CHILD_CONTEXTS = {
        'info': ('OpenAPIInfoContext', 'info'),
    }

    def __init__(self):
        super(OpenAPIContext, self).__init__()
        self.document = {
            'openapi': '3.0.3',
            'paths': {},
        }

    def server(self, url: str, description: Optional[str] = None):
        servers = self.document.setdefault('servers', [])
        server = {
            'url': url
        }
        if description:
            server['description'] = description
        servers.append(server)
        return self

    def path(self, path: str,
             summary: Optional[str] = None,
             description: Optional[str] = None):
        path_item = OpenAPIPathContext(self)
        if summary:
            path_item.summary(summary)
        if description:
            path_item.description(description)
        self.document['paths'][path] = path_item.document
        return path_item


class OpenAPIInfoContext(_ChildContext, _HasDescription):
    ATTRIBUTE_FIELDS = {
        'title': 'title',
        'terms_of_service': 'termsOfService',
        'version': 'version',
    }
    CHILD_CONTEXTS = {
        'contact': ('OpenAPIContactContext', 'contact'),
        'license': ('OpenAPILicenseContext', 'license'),
    }


class OpenAPIContactContext(_ChildContext):
    ATTRIBUTE_FIELDS = {
        'name': 'name',
        'url': 'url',
        'email': 'email',
    }


class OpenAPILicenseContext(_ChildContext):
    ATTRIBUTE_FIELDS = {
        'name': 'name',
        'url': 'url',
    }


class OpenAPIPathContext(_ChildContext, _HasSummary, _HasDescription,
                         _HasParameters):
    CHILD_CONTEXTS = {}
    for http_method in ['get', 'put', 'post', 'delete', 'options', 'head',
                        'patch', 'trace']:
        CHILD_CONTEXTS[http_method] = ('OpenAPIOperation', http_method)


class OpenAPIOperation(_ChildContext, _HasSummary, _HasExternalDocs, _HasTags,
                       _HasDescription, _HasParameters):
    ATTRIBUTE_FIELDS = {
        'operation_id': 'operationId'
    }

    def __init__(self, parent):
        super(OpenAPIOperation, self).__init__(parent)
        self.document = {
            'responses': {
                '200': {
                    'description': "Success",
                }
            }
        }


class OpenAPIParameterContext(_ChildContext, _HasDescription):
    ATTRIBUTE_FIELDS = {
        'deprecated': 'deprecated',
        'allow_empty': 'allowEmptyValue',
        'style': 'style',
        'explode': 'explode',
        'allow_reserved': 'allowReserved',
        'schema': 'schema',
    }

    def __init__(self, parent, name: str, in_: str, required: bool):
        super(OpenAPIParameterContext, self).__init__(parent)
        self.document = {
            'name': name,
            'in': in_,
            'required': required,
        }

    def type(self, typ: str, **kwargs):
        schema = {
            'type': typ,
        }
        for k in ['minimum', 'maximum', 'default']:
            v = kwargs.get(k)
            if v is not None:
                schema[k] = v
        self.schema(schema)
        return self


class OpenAPIExternalDocsContext(_ChildContext, _HasDescription):
    ATTRIBUTE_FIELDS = {
        'url': 'url',
    }
