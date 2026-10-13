.PHONY: install test run

PYTHON = python3
VENV = .venv
BIN = $(VENV)/bin

local-setup: venv requirements

venv:
	$(PYTHON) -m venv $(VENV)

requirements:
	$(BIN)/pip install -r requirements.txt

setup-api:
	docker compose up --build

extract-subscriptions:
	$(BIN)/python ./data_pipeline/extract_subscriptions.py

data-normalization:
	$(BIN)/python ./data_pipeline/data_normalization.py

load-dw-gold-layer:
	$(BIN)/python ./data_pipeline/load_dw_gold_layer.py

run-pipeline: extract-subscriptions data-normalization load-dw-gold-layer
	@echo "Pipeline executed successfully"
