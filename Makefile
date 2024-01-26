MYPY_OPTIONS = --ignore-missing-imports --disallow-untyped-calls --disallow-untyped-defs --disallow-incomplete-defs

.PHONY: tests
tests:
	pytest tests/integration/test_movie_transform.py
