from biothings.web.query import ESResultFormatter


def test_es_1():
    formatter = ESResultFormatter()
    print(formatter.transform({"hits": {"total": {}, "hits": []}}))


def test_es_2():
    formatter = ESResultFormatter()
    print(
        formatter.transform(
            [
                {"hits": {"total": {}, "hits": [{"_source": {"_id": "1"}}, {"_source": {"_id": "2"}}]}},
                {"hits": {"total": {}, "hits": [{"_source": {"_id": "1"}}]}},
                {"hits": {"total": {}, "hits": []}},
            ]
        )
    )


def test_es_3():
    formatter = ESResultFormatter()
    print(
        formatter.transform(
            {"hits": {"total": {}, "hits": []}},
            one=True,
        )
    )
    print(
        formatter.transform(
            {
                "hits": {
                    "total": {},
                    "hits": [
                        {"_source": {"_id": "1"}},
                    ],
                }
            },
            one=True,
        )
    )
    print(
        formatter.transform(
            {
                "hits": {
                    "total": {},
                    "hits": [
                        {"_source": {"_id": "1"}},
                        {"_source": {"_id": "2"}},
                    ],
                }
            },
            one=True,
        )
    )


if __name__ == "__main__":
    test_es_3()
