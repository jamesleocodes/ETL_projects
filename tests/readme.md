# Simple ETL Project Test Suite

The Simple ETL Project test suite validates the ETL pipeline's extract, transform, and load processes for car and person data.

## Test Suites

### `test_etl_car.py`

- **Extract**: Validates data extraction from CSV, JSON, and XML formats, handling missing car_model fields.
- **Transform**: Rounds prices to 2 decimal places and replaces missing car_model values with "Unknown".
- **Load**: Ensures data is saved correctly to CSV.

### `test_etl_person.py`

- **Extract**: Validates data extraction from CSV, JSON, and XML formats.
- **Transform**: Converts height to meters and weight to kilograms.
- **Load**: Verifies transformed data is saved to CSV.

## Continuous Integration (CI)

CI is automated with GitHub Actions (`.github/workflows/ci.yaml`) and:

- Sets up a Python 3.10 environment.
- Installs dependencies (pytest, pylint).
- Runs linting (pylint) and tests for both suites.

The CI pipeline ensures reliability by running on every push or pull request.

## Running the Tests

To run all tests:

```bash
python -m unittest discover tests
```

To run a specific test file:

```bash
python -m unittest tests/test_etl_car.py
# or
python -m unittest tests/test_etl_person.py
```

To run a specific test case:

```bash
python -m unittest tests.test_etl_car.TestETL.test_transform
```

## Test Requirements

The tests require the following dependencies:
- unittest (Python standard library)
- pandas
- mock (included in unittest.mock)

Make sure the project's `src` directory is in your Python path when running the tests.


