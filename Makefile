all: clean test

dev:
	pip install -r requirements.txt -r requirements-dev.txt

# auto correct indentation issues
fix:
	autopep8 dedupe.py --recursive --in-place

lint:
	flake8 dedupe.py

test:
	pytest --pep8 --cov -s

clean:
	find . -name '*.pyc' -exec rm --force {} +
	find . -name '*.pyo' -exec rm --force {} +
	find . -name '*~' -exec rm --force  {} +

.PHONY: clean test
