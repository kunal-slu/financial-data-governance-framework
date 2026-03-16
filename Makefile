PYTHON ?= python3

install:
	$(PYTHON) -m pip install -r requirements.txt

install-full:
	$(PYTHON) -m pip install -r requirements-full.txt

test:
	pytest

demo:
	$(PYTHON) examples/run_lightweight_demo.py

package-check:
	$(PYTHON) -m pip install build >/dev/null 2>&1 || true
	$(PYTHON) -m build
