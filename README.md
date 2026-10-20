# Spark Examples

A collection of introductory code examples for **Apache Spark 4.2.0**, written in
**Python** (PySpark). They are meant as learning material, and progress from the basic RDD
API towards more advanced topics.

## AI-assisted development

This project is used for teaching. Its code derives from the original code written by the
project authors, and it has been developed with the assistance of AI agents (such as
[Claude Code](https://claude.com/claude-code)). The agents are used for code review,
documentation, bug finding, proposals for improvements, refactoring to follow the style
guidelines, and the addition of unit tests. The authors review and decide which changes are
accepted. To keep the work of the agents consistent and transparent, the repository includes
the instructions that they must follow:

- [AGENTS.md](AGENTS.md): guidance shared by all the agents ([CLAUDE.md](CLAUDE.md) points to
  it for Claude Code).
- [CODING_GUIDELINES.md](CODING_GUIDELINES.md): style and testing rules for the code.
- [GIT_GUIDELINES.md](GIT_GUIDELINES.md): rules for commits and branches.

The commits made with an AI agent are marked with a `Co-Authored-By` trailer. As with any other
source, check what you learn from the examples against the
[Spark documentation](https://spark.apache.org/docs/latest/).

## Requirements

- Python 3.11 or later
- Java 17 or later (a JDK supported by Spark 4.2; required by PySpark)
- **PySpark 4.2.0** (`pyspark==4.2.0`), installed in a dedicated virtual environment
- A virtual environment manager: [Conda](https://docs.conda.io/) or Python's built-in `venv`

The project assumes that a virtual environment for PySpark exists. This documentation calls
it `spark420`, but you can name it whatever you like; just replace the name in the commands
below.

## Setup

Create and activate the environment with **one** of the following options, then install the
project.

### Option 1: Conda

The [environment.yml](environment.yml) file creates the environment (Python, a JDK and the
project with its dependencies) in one step:

```bash
conda env create -f environment.yml
conda activate spark420
```

To use another name, run `conda env create -n <name> -f environment.yml`.

### Option 2: venv

```bash
python3.11 -m venv spark420
source spark420/bin/activate        # Windows: spark420\Scripts\activate
make install                        # pip install -e ".[dev]"
```

`venv` needs a JDK to be already installed on your system.

`make install` installs the package in editable mode, together with `pyspark==4.2.0`
(declared in [pyproject.toml](pyproject.toml)), `pytest` and `ruff`. To install only PySpark,
run `pip install pyspark==4.2.0`.

## Project structure

```text
src/rdd/          Examples based on the RDD API
src/dataframes/   Examples based on the DataFrame API
src/streaming/    Examples based on Structured Streaming
tests/            pytest tests (one test module per example)
data/             Input files used by the examples
```

The examples take no command line arguments: their inputs (file names, words, ...) are
constants at the top of each module, so they can be changed by editing them.

## Examples

The examples run on a local Spark master and must be run from the project root. They are
listed in the suggested order of study; each package has its own README with a table that
describes every example and what to observe in it:

1. [RDD API](src/rdd/README.md): `reduce`, files, `reduceByKey`, closures and accumulators.
2. [DataFrame API](src/dataframes/README.md): reading and writing files, aggregations, SQL,
   Parquet and joins.
3. [Structured Streaming](src/streaming/README.md): processing files as they arrive in a
   directory.

### Running an example

```bash
python -m rdd.add_numbers
python -m dataframes.basic_operations
```

Or with `spark-submit`:

```bash
spark-submit src/rdd/max_number.py
```

## Development

| Task | Command |
| ---- | ------- |
| Run all tests | `make test` |
| Run tests, skipping the slow smoke tests | `make test-fast` |
| Lint | `make lint` |
| Format | `make format` (`make format-check` only checks) |
| Lint, format check and tests | `make check` |
| Remove generated files | `make clean` |

Tests use a local, session-scoped `SparkContext`, and a `SparkSession` built on it, defined in
[tests/conftest.py](tests/conftest.py). The smoke tests in
[tests/rdd/test_examples_smoke.py](tests/rdd/test_examples_smoke.py) run every example as a
separate process; the airports examples are skipped if their data files have not been
downloaded.

## Contributing

The project follows the rules in [CODING_GUIDELINES.md](CODING_GUIDELINES.md) and
[GIT_GUIDELINES.md](GIT_GUIDELINES.md) (Conventional Commits, atomic commits). AI coding
agents should read [AGENTS.md](AGENTS.md); Claude Code gets it through
[CLAUDE.md](CLAUDE.md), which simply imports it.

## License

The code is distributed under the [GNU General Public License v3.0 or later](LICENSE). The
text in `data/quijote.txt` is *Don Quijote* by Miguel de Cervantes Saavedra, taken from
[Project Gutenberg](https://www.gutenberg.org/) (eBook #2000), and is covered by the Project
Gutenberg license included in the file, not by the GPL.
