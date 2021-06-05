
class QueryPipelineException(Exception):
    def __init__(self, data, code=500):
        self.data = data
        self.code = code

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
