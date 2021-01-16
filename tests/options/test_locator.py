from biothings.web.options import ReqArgs

def test_01():
    args = ReqArgs(
        query={"format": "json"},
        form={"q": "1017"}
    )
    assert args.lookup("format") == "json"
    assert args.lookup("q") == "1017"

def test_02():
    args = ReqArgs(
        ReqArgs.Path(
            args=("gene", "1017"),
            kwargs={
                "host": "mygene.info",
                "version": "v3",
            }
        ),
        query={"format": "json"},
        form={"format": "html"}
    )
    assert args.lookup({
        "keyword": "biothing_type",
        "path": 0
    }) == "gene"
    assert args.lookup({
        "keyword": "_id",
        "path": 1
    }) == "1017"
    assert args.lookup({
        "keyword": "unknown",
        "path": 2
    }) is None
    assert args.lookup({
        "keyword": "host",
        "path": "host"
    }) == "mygene.info"
    assert args.lookup({
        "keyword": "version",
        "path": "version"
    }) == "v3"
    assert args.lookup({
        "keyword": "unknown",
        "path": "unknown"
    }) is None
    assert args.lookup("format") == "json"
    assert args.lookup("format", "form") == "html"
    assert args.lookup("format", ("form", "query")) == "html"

def test_03():
    args = ReqArgs(
        query={
            "format": "yaml",
            "size": "1002"},
        json_={
            "q": "cdk2",
            "scopes": ["ensembl", "entrez"],
            "format": "json"}
    )
    assert args.lookup("scopes") == ["ensembl", "entrez"]
    assert args.lookup("format") == "yaml"
    assert args.lookup("size") == "1002"
    assert args.lookup("q") == "cdk2"

def test_04():
    locator = {
        "keyword": "_source",
        'alias': ['fields', 'field', 'filter']
    }
    assert ReqArgs(query={
        "q": "cdk2",
        "_source": "taxid"
    }).lookup(locator) == "taxid"
    assert ReqArgs(query={
        "q": "cdk2",
        "fields": "taxid"
    }).lookup(locator) == "taxid"
    assert ReqArgs(query={
        "q": "cdk2",
        "field": "taxid"
    }).lookup(locator) == "taxid"
    assert ReqArgs(query={
        "q": "cdk2",
        "filter": "taxid"
    }).lookup(locator) == "taxid"

def test_05():
    args = ReqArgs(
        query={
            "q": "cdk2",
            "fields": "taxid"
        },
        form={
            "_source": "_id"
        }
    )
    locator = {
        "keyword": "_source",
        'alias': ['fields', 'field', 'filter']
    }

    assert args.lookup(locator) == "taxid"
    assert args.lookup(locator, ("form", "query")) == "_id"
