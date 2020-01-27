all: unittest apitest webapptest

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
npm_install:
	cd webapp/pharmgkb && npm install


test_api_mvcgi: docker_pull docker_run
	HUB_API_HOST=http://localhost:7080 py.test api/mvcgi
test_webapp_pharmgkb: docker_pull docker_run npm_install
	# npx is the launcher, not installed globally (no sudo) so use local path explicitely
	cd webapp/pharmgkb && ./node_modules/.bin/npx codeceptjs run --steps -o '{ "helpers": {"Puppeteer": {"show": false}}}'

unittest: test_config test_jsondiff test_jsonschema test_utils
apitest: test_api_mvcgi
webapptest: test_webapp_pharmgkb

