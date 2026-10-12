.PHONY: install test run

PYTHON = python3
VENV = .venv
BIN = $(VENV)/bin

local-setup: venv requirements

venv:
	$(PYTHON) -m venv $(VENV)

requirements:
	$(BIN)/pip install -r requirements.txt

extract-subscriptions:
	$(BIN)/python ./extract_subscriptions.py

data-normalization:
	$(BIN)/python ./data_normalization.py
