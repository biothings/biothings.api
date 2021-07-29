KWARG_DESCRIPTIONS = {
    '_source': {
        'name': 'fields',
        'text_template': 'a comma-separated list of fields (in dotfield notation) used to limit the fields returned from the matching {biothing_object} hit(s). The supported field names can be found from any {biothing_object} object or from the /metadata/fields endpoint. If "fields=all", all available fields will be returned.{param_type}{param_default_value}{param_max}'},
    'size': {
        'name': 'size',
        'text_template': 'the maximum number of matching {biothing_object} hits to return per batch.{param_type}{param_default_value}{param_max}'},
    'from': {
        'name': 'from',
        'text_template': 'the number of matching {biothing_object} hits to skip, starting from 0.  This can be useful for paging in combination with the "size" parameter.{param_type}{param_default_value}{param_max}'},
    'sort': {
        'name': 'sort',
        'text_template': 'the comma-separated list of fields to sort on. Prefix each with "-" for descending order, otherwise in ascending order. Default: sort by descending score.'},
    'dotfield': {
        'name': 'dotfield',
        'text_template': 'control the format of the returned {biothing_object} object. If "true" or "1", all fields will be collapsed into a single level deep object (all nested objects will be a single level deep, using dotfield notation to signify the nested structure){param_type}{param_default_value}{param_max}'},
    'email': {
        'name': 'email',
        'text_template': 'If you are regular users of our services, we encourage you to provide us with an email, so that we can better track the usage or follow up with you.'},
    'format': {
        'name': 'format',
        'text_template': 'controls output format of server response, currently supports: "json", "jsonld", "html".{param_type}{param_default_value}{param_max}'},
    'aggs': {
        'name': 'facets',
        'text_template': 'a comma-separated list of fields to return facets on.  In addition to query hits, the fields notated in "facets" will be aggregated by value and bucklet counts will be displayed in the "facets" field of the response object.{param_type}{param_default_value}{param_max}'},
    'facet_size': {
        'name': 'facet_size',
        'text_template': 'the number of facet buckets to return in the response.{param_type}{param_default_value}{param_max}'},
    'ids': {
        'name': 'ids',
        'text_template': 'multiple {biothing_object} ids separated by comma. Note that currently we only take the input ids up to 1000 maximum, the rest will be omitted.{param_type}{param_default_value}{param_max}'},
    'q': {
        'name': 'q',
        'text_template': 'Query string.  The detailed query syntax can be found from our [docs]{doc_query_syntax_url}'},
    'scopes': {
        'name': 'scopes',
        'text_template': 'a comma-separated list of fields as the search "scopes" (fields to search through for query term). The available "fields" that can be passed to the "scopes" parameter are listed in the **/metadata/fields** endpoint.{param_type} Default: "scopes=_id".{param_max}'},
    'search': {
        'name': 'search',
        'text_template': 'Pass a search term to filter the available fields.{param_type}{param_default_value}{param_max}'},
    'prefix': {
        'name': 'prefix',
        'text_template': 'Pass a prefix string to filter the available fields.{param_type}{param_default_value}{param_max}'}}
