# Spark Introduction Codes

A collection of introductory code examples for **Apache Spark 4.2.0**, written in
**Python** (PySpark). They are meant as learning material, and progress from the basic RDD
API towards more advanced topics.

## Requirements

- Python 3.11 or later
- A Java JDK supported by Spark 4.2 (required by PySpark)
- **PySpark 4.2.0** (`pyspark==4.2.0`), installed in a dedicated virtual environment
- A virtual environment manager: [Conda](https://docs.conda.io/) or Python's built-in `venv`

The project assumes that a virtual environment for PySpark already exists. This
documentation calls it `spark420`, but you can name it whatever you like; just replace the
name in the commands below.

## Setup

Create and activate the environment with **one** of the following options, then install the
project.

### Option 1: Conda

```bash
conda create -n spark420 python=3.11
conda activate spark420
```

### Option 2: venv

```bash
python3.11 -m venv spark420
source spark420/bin/activate        # Windows: spark420\Scripts\activate
```

### Install the project

With the environment active:

```bash
make install        # pip install -e ".[dev]"
```

This installs the package in editable mode, together with `pyspark==4.2.0` (declared in
[pyproject.toml](pyproject.toml)), `pytest` and `ruff`. To install only PySpark, run
`pip install pyspark==4.2.0`.

## Project structure

```text
src/rdd/            Examples based on the RDD API
tests/                pytest tests
data/                 Input files used by the examples
```

## Examples

All the examples below live in [src/rdd/](src/rdd/) and run on a local
Spark master. Run them from the project root.

| Example | What it shows |
| ------- | ------------- |
| [add_numbers.py](src/rdd/add_numbers.py) | Create an RDD with `parallelize` and sum its elements with `reduce`. |
| [add_numbers_from_file.py](src/rdd/add_numbers_from_file.py) | Read numbers from a text file (`data/numbers.txt`), sum them and measure the computing time. |
| [count_lines_containing_word.py](src/rdd/count_lines_containing_word.py) | Filter and count lines of a text file, reusing an RDD with `persist()` and saving results with `saveAsTextFile`. |
| [closure_driver_variable.py](src/rdd/closure_driver_variable.py) | Closures: a driver variable is serialized and copied to the workers. |
| [closure_shared_generator.py](src/rdd/closure_shared_generator.py) | Closures pitfall: a captured object with state (a random generator) is copied to every partition. |
| [closure_reproducible_random.py](src/rdd/closure_reproducible_random.py) | Closures fix: reproducible results independent of the data distribution. |

Each example has its tests in [tests/rdd/](tests/rdd/).

### Running an example

```bash
python -m rdd.add_numbers
python -m rdd.add_numbers_from_file
```

Or with `spark-submit`:

```bash
spark-submit src/rdd/count_lines_containing_word.py data/quijote.txt
```

`count_lines_containing_word.py` expects the path of a text file; use
`data/quijote.txt` (the text of *Don Quijote*). It writes the lines containing "Dulcinea" to
a `Dulcinea.txt` output directory, which Spark refuses to overwrite if it already exists.

## Development

| Task | Command |
| ---- | ------- |
| Run tests | `make test` |
| Lint | `make lint` |
| Format | `make format` |

Tests use a local, session-scoped `SparkContext` defined in
[tests/conftest.py](tests/conftest.py).

## Contributing

The project follows the rules in [CODING_GUIDELINES.md](CODING_GUIDELINES.md) and
[GIT_GUIDELINES.md](GIT_GUIDELINES.md) (Conventional Commits, atomic commits). AI coding
agents should read [AGENTS.md](AGENTS.md); Claude Code gets it through
[CLAUDE.md](CLAUDE.md), which simply imports it.
