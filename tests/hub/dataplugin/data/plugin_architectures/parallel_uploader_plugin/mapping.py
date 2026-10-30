def size_mapping(cls):
    """
    Mock elasticsearch mapping for the size loader
    """
    elasticsearch_mapping = {
        "associatedWith": {"properties": {"name": {"type": "keyword"}, "size": {"type": "keyword"}}}
    }
    return elasticsearch_mapping
