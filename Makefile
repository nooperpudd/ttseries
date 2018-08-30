install:
	pip3 install -r requirements.txt --upgrade
	pip3 install -r requirements-dev.txt --upgrade
test:
	pytest -v -s

benchmark-init:
	pip3 install -r benchmark/requirements.txt --upgrade

benchmark-test:
	pytest benchmark/benchmark.py -v -s --benchmark-disable-gc