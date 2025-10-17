.PHONY: clean lint style black test spell

clean:
	rm tests/*.pkl
	rm tests/*.nc

lint:
	python -m pylint -rn one_pass tests

style:
	python -m black --check one_pass tests docs

black:
	python -m black one_pass tests docs

test:
	pytest tests/

spell:
	python -m pylint -rn --disable=all --enable=spelling --spelling-dict=en_US docs one_pass tests tools

