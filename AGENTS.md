# AGENTS.md

Guidance for AI coding agents working in this repository. The project is a collection of
introductory **Apache Spark 4.2.0** (PySpark) examples written in **Python 3.11+**.

The detailed rules live in [CODING_GUIDELINES.md](CODING_GUIDELINES.md) and
[GIT_GUIDELINES.md](GIT_GUIDELINES.md). Both are mandatory; this file summarizes them.

## Language

All code, identifiers, comments, docstrings, commit messages and Markdown documentation
must be written in **English**.

## Environment

- Use the project's virtual environment, named **`spark420`** by default (Conda:
  `conda activate spark420`, or `conda run -n spark420 <command>`). If the environment has
  another name, use that one. See [environment.yml](environment.yml) and the README.
- Do not install packages in the base environment or in any other environment. If a
  dependency is missing, add it to the project environment and declare it in `pyproject.toml`.
- Java 17 or later is required to run PySpark.

## Project layout

```text
src/rdd/             Examples based on the RDD API (top-level package `rdd`)
src/dataframes/      Examples based on the DataFrame API (top-level package `dataframes`)
src/streaming/       Examples based on Structured Streaming (top-level package `streaming`)
tests/               pytest tests, mirroring the layout of src/
data/                Input files used by the examples (`output/` is git-ignored)
pyproject.toml       Package metadata, dependencies, ruff and pytest configuration
Makefile             install / test / lint / format / check / clean targets
environment.yml      Conda environment definition
```

## Commands

| Task                         | Command                                     |
| ---------------------------- | ------------------------------------------- |
| Install in editable mode     | `make install`                              |
| Run tests                    | `make test` (`pytest tests/ -x`)            |
| Run tests, skipping smoke    | `make test-fast`                            |
| Lint                         | `make lint` (`ruff check`)                  |
| Format                       | `make format` (`ruff format`)               |
| Lint + format check + tests  | `make check`                                |
| Run an example               | `python -m rdd.add_numbers`                 |
| Submit an example            | `spark-submit src/rdd/<file>.py`            |

Run `make check` before proposing a commit.

## Focus on Spark

The examples are teaching material about Spark, so they contain only the code needed to show
the Spark concepts. Python code that is not related to Spark can hide what matters and must be
avoided, even if it would be good practice in another context:

- No command line parsing (`argparse`, `sys.argv`): the inputs are module constants used as
  default values of the parameters of `main`.
- No input validation that Spark already does or that is not the point of the example; let
  Spark raise its own errors (e.g. `AnalysisException` for a missing file).
- No auxiliary helpers (file downloads, path handling, custom logging, ...) unless the example
  is about them.

This rule takes precedence over the general guidelines below when they conflict.

## Code conventions

- Annotate parameters and return types; use `|` for unions.
- Google-style docstrings (Args / Returns / Raises).
- One `return` per function (except validation guards), short and simple functions (at most
  ~20 lines), no nested conditionals.
- Raise specific exceptions (`ValueError`, `TypeError`, ...) for invalid input; use `with`
  for resources.
- Formatting and linting with **ruff** (line length 100).
- Chained Spark calls: one call per line, starting with the dot and aligned, inside `# fmt: off` /
  `# fmt: on` (see "Chained Spark calls" in [CODING_GUIDELINES.md](CODING_GUIDELINES.md)).

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
- Use the shared session-scoped fixtures from [tests/conftest.py](tests/conftest.py):
  `spark_context` for RDD tests and `spark_session` (built on the same context) for DataFrame
  tests. Do not create a new context or session per test.
- Every example also has a smoke test in `tests/rdd/test_examples_smoke.py` (marker `smoke`)
  that runs it as a separate process; register new examples there.

## Documentation

Every package under `src/` has a `README.md` with a table of its examples (what each one shows
and what to observe). Register every new example in the README of its package, not in the
main `README.md`, which only links to them.

## Git workflow

- Commits follow **Conventional Commits**: `<type>[(scope)][!]: <imperative description>`,
  in English, subject of at most 72 characters, no trailing period.
- Allowed types: `feat`, `fix`, `perf`, `test`, `refactor`, `style`, `docs`, `build`, `ci`,
  `chore`, `revert`.
- Commits are atomic: one logical change each. Never mix production code with tests, or code
  with documentation.
- Work happens on `main`; use topic branches and squash-merge for larger changes.
- **Do not commit or push unless explicitly asked.**

