.PHONY: help setup venv install lint test clean

# Default target executed when no arguments are given to make.
help:
	@echo "setup     - set up python interpreter environment"
	@echo "venv      - create a virtual environment for the project"
	@echo "install   - install all dependencies"
	@echo "lint      - run linter on the project"
	@echo "test      - run pytests"
	@echo "clean     - remove all temporary files"

# Set up python interpreter environment.
setup:
	python3 -m venv venv
	@echo ">> virtual environment created."

# Create a virtual environment.
venv:
	test -d venv || python3 -m venv venv

# Install dependencies.
install: venv
	. venv/bin/activate; pip install -U pip setuptools wheel
	. venv/bin/activate; pip install -r requirements.txt
	@echo ">> dependencies installed."

# Run linter.
lint: venv
	. venv/bin/activate; flake8 src tests

# Run tests, none yet though
test: venv
	. venv/bin/activate; pytest pytests/

# Remove all temporary files.
clean:
	find . -type f -name '*.pyc' -delete
	find . -type d -name '__pycache__' -delete
	find . -type d -name '*.egg-info' -exec rm -rf {} +
	rm -rf .pytest_cache
	@echo ">> clean."