from biothings.web.query.builder import MongoQueryBuilder, SQLQueryBuilder


def test_sql():
    builder = SQLQueryBuilder({
        "album": "album",
        "track": "track"
    })
    print(builder.build(
        'term', scopes=['fieldA', 'fieldB'],
        biothing_type='track'
    ))
    print(builder.build(
        'term', scopes=['fieldA'],
        _source=['id', 'fieldA']
    ))
    print(builder.build(
        'term', size=10, from_=10
    ))
    print(builder.build(
        'fieldA:termB'
    ))

def test_mongo():
    builder = MongoQueryBuilder()
    print(builder.build('term'))
    print(builder.build('fieldA:term'))
    print(builder.build('term', scopes=['fieldA', 'fieldB']))
    print(builder.build('term', scopes=['fieldA'], _source=['_id', 'fieldA']))


if __name__ == '__main__':
    test_sql()
