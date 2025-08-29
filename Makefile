.PHONY: venv install seed app eval test fmt clean help

# Default target
help:
	@echo "Available targets:"
	@echo "  venv     - Create and activate virtual environment"
	@echo "  install  - Install dependencies"
	@echo "  seed     - Bootstrap DuckDB with sample data"
	@echo "  app      - Run Streamlit application"
	@echo "  eval     - Run evaluation harness"
	@echo "  test     - Run test suite"
	@echo "  fmt      - Format code with black, isort, and ruff"
	@echo "  clean    - Clean temporary files and caches"

venv:
	@echo "Creating virtual environment..."
	python -m venv .venv
	@echo "Activate with: source .venv/bin/activate (Linux/Mac) or .venv\\Scripts\\activate (Windows)"

install:
	pip install -r requirements.txt
	pre-commit install

seed:
	python scripts/bootstrap_duckdb.py

app:
	streamlit run app/streamlit_app.py --server.port ${APP_PORT:-8501}

eval:
	python eval/evaluator.py

test:
	pytest -v --cov=analytics --cov=app tests/

fmt:
	black .
	isort .
	ruff --fix .

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .data/vector/*

# Docker targets
docker-build:
	docker-compose build

docker-up:
	docker-compose --profile local up -d

docker-down:
	docker-compose down

docker-app:
	docker-compose --profile app up --build