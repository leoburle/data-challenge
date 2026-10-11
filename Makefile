.PHONY: install test run

PYTHON = python3
VENV = .venv
BIN = $(VENV)/bin

install:
	$(PYTHON) -m venv $(VENV)
	$(BIN)/pip install -r requirements.txt

test:
	$(BIN)/pytest -v

run:
	$(BIN)/python ./load_subscriptions.py
