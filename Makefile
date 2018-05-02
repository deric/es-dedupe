NAME ?=es-dedupe
REGISTRY ?= deric

all: clean test

build:
	docker pull `head -n 1 Dockerfile | awk '{ print $$2 }'`
	docker build -t $(NAME) .

define RELEASE
	git tag "v$(1)"
	git push
	git push --tags
  docker tag $(NAME) $(REGISTRY)/$(NAME):v$(1)
	docker tag $(NAME) $(REGISTRY)/$(NAME):latest
	docker push $(REGISTRY)/$(NAME)
endef

shell: build
	docker run --entrypoint /bin/bash -it $(NAME)

release: build
	$(call RELEASE,$(v))

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
