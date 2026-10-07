# AGENTS.md

Guidance for AI coding agents working in this repository. The project is a collection of
introductory **Apache Spark 4.2.0** (PySpark) examples written in **Python 3.11+**.

The detailed rules live in [CODING_GUIDELINES.md](CODING_GUIDELINES.md) and
[GIT_GUIDELINES.md](GIT_GUIDELINES.md). Both are mandatory; this file summarizes them.

## Language

All code, identifiers, comments, docstrings, commit messages and Markdown documentation
must be written in **English**.

## Environment

- Use the Conda environment **`spark420`**: `conda activate spark420`, or run commands with
  `conda run -n spark420 <command>`.
- Do not install packages in the base environment or in any other environment. If a
  dependency is missing, add it to `spark420` and declare it in `pyproject.toml`.
- Java (a JDK supported by Spark 4.2) is required to run PySpark.

## Project layout

```text
src/rdd/             Examples based on the RDD API (top-level package `rdd`)
src/dataframes/      Planned: examples based on the DataFrame API
tests/               pytest tests, mirroring the layout of src/
data/                Small input files used by the examples
pyproject.toml       Package metadata, dependencies, ruff and pytest configuration
Makefile             install / test / lint / format targets
```

## Commands

| Task                         | Command                           |
| ---------------------------- | --------------------------------- |
| Install in editable mode     | `make install`                    |
| Run tests                    | `make test` (`pytest tests/ -x`)  |
| Lint                         | `make lint` (`ruff check`)        |
| Format                       | `make format` (`ruff format`)     |
| Run an example               | `python -m rdd.add_numbers` |
| Submit an example            | `spark-submit src/rdd/<file>.py` |

Run `make test` and `make lint` before proposing a commit.

## Code conventions

- Annotate parameters and return types on new or modified code; use `|` for unions.
- Google-style docstrings (Args / Returns / Raises).
- One `return` per function (except validation guards), functions of at most ~20 lines,
  cognitive complexity of at most 10, no nested conditionals.
- Raise specific exceptions (`ValueError`, `TypeError`, ...) for invalid input; use `with`
  for resources.
- Formatting and linting with **ruff** (line length 100).

## Spark conventions

- Create the `SparkSession` / `SparkContext` in a single entry point (`main`) and pass it as a
  parameter. Never use it as a global variable.
- Prefer the DataFrame / SQL API over RDDs, and native `pyspark.sql.functions` over Python
  UDFs. RDD code is acceptable in the `rdd/` examples, whose purpose is to teach that API.
- Keep transformations as pure functions (`DataFrame -> DataFrame` or `RDD -> RDD`) so they
  can be tested with a local session (`master("local[*]")`).
- Stop the session when the application ends (`stop()`), preferably in a `finally` block.

## Testing

- Framework: **pytest**, following the Arrange–Act–Assert pattern.
- Name tests `test_should_<behavior>`; group related tests in `class Test<Subject>:`.
- Use `@pytest.mark.parametrize` for scenario variants and `pytest.raises` for errors.
- Cover both success and failure paths.
- Use the shared session-scoped `spark_context` fixture from
  [tests/conftest.py](tests/conftest.py); do not create a new context per test.

## Git workflow

- Commits follow **Conventional Commits**: `<type>[(scope)][!]: <imperative description>`,
  in English, subject of at most 72 characters, no trailing period.
- Allowed types: `feat`, `fix`, `perf`, `test`, `refactor`, `style`, `docs`, `build`, `ci`,
  `chore`, `revert`.
- Commits are atomic: one logical change each. Never mix production code with tests, or code
  with documentation.
- **Do not commit or push unless explicitly asked.**
- The "Branches and releases" section of GIT_GUIDELINES.md describes the Evolver-Studio
  release flow; apply it only if this repository adopts that flow.
