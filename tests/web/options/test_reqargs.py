from biothings.web.options import ReqArgs


def test_01():
    assert str(ReqArgs(
        query={
            "format": "yaml",
            "size": "1002"},
        json_={
            "q": "cdk2",
            "scopes": ["ensembl", "entrez"],
            "format": "json"}
    )).replace(" ", "") == """
        ReqArgs(query={'format': 'yaml', 'size': '1002'},
        json={'q': 'cdk2', 'scopes': ['ensembl', 'entrez'], 'format': 'json'})
    """.replace(" ", "")[1:-1]

def test_02():
    assert str(ReqArgs(
        ReqArgs.Path(
            args=("gene", "1017"),
            kwargs={"version": "v3"}),
        query={"format": "html"}
    )).replace(" ", "") == """
        ReqArgs(path=Path(args=('gene', '1017'), kwargs={'version': 'v3'}),
        query={'format': 'html'})
    """.replace(" ", "")[1:-1]

def test_03():
    assert str(ReqArgs(ReqArgs.Path())) == """
        ReqArgs()
    """.replace(" ", "")[1:-1]
