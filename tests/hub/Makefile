all: unittest

test_jsondiff:
	cd jsondiff && nosetests
test_jsonschema:
	nosetests jsonschema
test_utils:
	nosetests utils
test_inspector:
	nosetests inspector

test: test_jsondiff test_jsonschema test_utils test_inspector


