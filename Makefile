NAME ?=es-dedupe
REGISTRY ?= deric

all: clean test

build:
	grep "^FROM" Dockerfile | awk '{ print $$2 }' | uniq | xargs -P2 -n1 docker pull
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
	pip3 install -r requirements.txt -r requirements-dev.txt

# auto correct indentation issues
fix:
	ruff check --fix .

lint:
	ruff check .

package:
	python3 setup.py sdist bdist_wheel

test:
	docker build -f Dockerfile.test -t esdedupe-test .
	docker run -v $(shell pwd):/app --entrypoint /bin/bash -it esdedupe-test

clean:
	find . -name '*.pyc' -exec rm --force {} +
	find . -name '*.pyo' -exec rm --force {} +
	find . -name '*~' -exec rm --force  {} +
	rm -rf esdedupe.egg-info dist build

.PHONY: clean test build
