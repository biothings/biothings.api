test_config:
	nosetests config
test_jsondiff:
	cd jsondiff && nosetests
test_jsonschema:
	nosetests jsonschema
test_utils:
	nosetests utils

docker_pull:
	docker pull biothings/biothings-studio
docker_run:
	-docker kill test_studio
	sleep 5
	while docker ps | grep test_studio; do sleep 1; done
	docker run --rm --name test_studio -p 8080:8080 -p 7080:7080 -p 8000:8000 -d biothings/biothings-studio
	while ! curl localhost:7080; do echo waiting for hub api to run; sleep 5; done
test_api_mvcgi: docker_pull docker_run
	HUB_API_HOST=http://localhost:7080 py.test api/mvcgi


unittest: test_config test_jsondiff test_jsonschema test_utils
apitest: test_api_mvcgi
all: unittest apitest
