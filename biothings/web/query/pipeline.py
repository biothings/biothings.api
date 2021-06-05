

# here this module defines two types of operations supported in
# each query pipeline class, one called "search" which corresponds to
# a key-value pair search, with the sometimes optional parameter "scopes"
# being a list of keys to look for the value, named as parameter "q".
# the other type of query is called "fetch", that looks up documents
# basing on pre-defined fields that function as their mostly unique
# identifiers, it is a special type of "search" that ususally returns
# only one document. in this type of query, "scopes" are absolutely NOT
# provided, and the query term is called "id" in the parameter list.

# this module is called pipeline because
# a) it is a combination of individual query processing stages
# b) during async operations, it functions like the CPU pipeline,
#    more than 1 stage can be busy at a single point in time.

# TODO make sure assertions are for internal error checking
# and use ValueError or TypeError for external errors.

class QueryPipelineException(Exception):
    # use code here to indicate error types instead of
    # exception types, this reduces the number of potential
    # exception types this module needs to create.
    # furthermore, consider using a superset of HTTP codes,
    # so that error translation from the upper layers
    # is also convenient and straightforward.
    def __init__(self, code=500, data=None):
        self.code = code
        self.data = data

class QueryPipeline():
    def __init__(self, builder, backend, formatter):
        self.builder = builder
        self.backend = backend
        self.formatter = formatter

    def search(self, q, **options):
        query = self.builder.build(q, **options)
        result = self.backend.execute(query, **options)
        return self.formatter.transform(result, **options)

    def fetch(self, id, **options):
        assert options.get('scopes') is None
        result = self.search(id, **options)
        return result


class ESQueryPipeline(QueryPipeline):

    def search(self, q, **options):

        if isinstance(q, list):
            options['templates'] = (dict(query=_q) for _q in q)
            options['template_miss'] = dict(notfound=True)
            options['template_hit'] = dict()

        return super().search(q, **options)

    def fetch(self, id, **options):
        assert options.get('scopes') is None
        options['scopes'] = []
        options['version'] = True
        options['score'] = False
        options['one'] = True
        result = self.search(id, **options)
        return result

class AsyncESQueryPipeline(QueryPipeline):

    async def search(self, q, **options):

        if isinstance(q, list):
            options['templates'] = (dict(query=_q) for _q in q)
            options['template_miss'] = dict(notfound=True)
            options['template_hit'] = dict()

        query = self.builder.build(q, **options)
        response = await self.backend.execute(query, **options)
        result = self.formatter.transform(response, **options)
        return result

    async def fetch(self, id, **options):
        assert options.get('scopes') is None
        options['scopes'] = []
        options['version'] = True
        options['score'] = False
        options['one'] = True
        result = await self.search(id, **options)
        return result


class MongoQueryPipeline(QueryPipeline):
    pass

class SQLQueryPipeline(QueryPipeline):
    pass
