install:
	pip3 install -r requirements.txt --upgrade
	pip3 install -r requirements-dev.txt --upgrade
test:
	pytest -v -s
