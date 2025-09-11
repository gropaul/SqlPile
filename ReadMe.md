
# Setup and Running
This project is managed with Poetry. 

## Getting Started
To set it up, follow these steps:
1. Install Poetry if you haven't already. You can find the installation instructions at [Poetry's official documentation](https://python-poetry.org/docs/#installation).
2. Clone the repository:
   ```bash
   git clone
3. Navigate to the project directory:
   ```bash
   cd your-project-name
   ```
4. Install the dependencies:
   ```bash
    poetry install
    ```
5. To run the project, you can use:


## Running the Project

```bash
poetry run python main.py
```

## Build the Custom DuckDB

```shell
cd  ./external/duckdb-sqlpile/
BUILD_PYTHON=1 PIP_BREAK_SYSTEM_PACKAGES=1 make release
cd ../..
poetry install
```
1. In the DuckDB repo:
2. In poetry `poetry install`