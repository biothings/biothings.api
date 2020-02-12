all: unittest

test_config:
	nosetests config
test_jsondiff:
	cd jsondiff && nosetests
test_jsonschema:
	nosetests jsonschema
test_utils:
	nosetests utils

unittest: test_config test_jsondiff test_jsonschema test_utils


