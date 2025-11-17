# Agent Guidelines for Pebble

This document is for Codex agents contributing to the project. It captures coding style, libraries, and best practices.

## Language & Tools

- **Python** (3.12+ preferred)
- **NumPy** for numerical operations
- **Pandas** for tabular data management
- **Redis** for lightweight queues and caching -- always configured and available locally.
- **MongoDB** for long-term data storage -- always configured and available locally.

## Services

- **Warcraft Logs (WCL)** for nightly data about raid encounters.
- **Google Sheets**

## Coding Conventions

- Follow PEP 8 with **Black** auto-formatting and **isort** for imports.
- Prefer type annotations and **mypy** checks.
- Write **docstrings** using **NumPy style** (preferred in scientific Python).
- Configuration via YAML/JSON; secrets in `.env` with `python-dotenv`.

## Design Philosophy

- KISS -- Keep it super simple.
- YANGI -- You aren't gonna need it.
- Strive for simplicity in design and coding.
- Do not add test-only paths to the main codebase unless absolutely necessary, and mark them clearly. Modify the tests
  to adapt to the main codebase whenever possible.
- Minimize reads/writes to external services like Google Sheets and Warcraft logs. Use Redis for caching when
  appropriate (WCL).
- This is a V1 project: do not worry about backwards compatibility with prior versions. Assume all components are
  in-sync, and all datastores start fresh.
