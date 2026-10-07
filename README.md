# Spark Introduction Codes

A collection of introductory code examples for **Apache Spark 4.2.0**, written in
**Python** (PySpark). They are meant as learning material, and progress from the basic RDD
API towards more advanced topics.

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
src/rdd/       Examples based on the RDD API
tests/         pytest tests (one test module per example)
data/          Input files used by the examples
```

## Examples

All the examples live in [src/rdd/](src/rdd/) and run on a local Spark master. Run them from
the project root. They are listed in the suggested order of study; each has its tests in
[tests/rdd/](tests/rdd/).

| Example | What it shows | What to observe |
| ------- | ------------- | --------------- |
| [add_numbers.py](src/rdd/add_numbers.py) | Create an RDD with `parallelize` and sum its elements with `reduce`. | The result of an action is returned to the driver. |
| [max_number.py](src/rdd/max_number.py) | Find the maximum of a list with `reduce`. | `reduce` works with any associative and commutative function. |
| [add_numbers_from_file.py](src/rdd/add_numbers_from_file.py) | Read numbers from a text file (`data/numbers.txt`), sum them and measure the computing time. | Transformations are lazy: the work happens in the action. |
| [count_lines_containing_word.py](src/rdd/count_lines_containing_word.py) | Filter and count lines of a text file, reusing an RDD with `persist()` and saving results with `saveAsTextFile`. | The file is read once thanks to `persist()`. |
| [closure_driver_variable.py](src/rdd/closure_driver_variable.py) | Closures: a driver variable is serialized and copied to the workers. | Changes made to a captured variable in the workers are not seen by the driver. |
| [closure_shared_generator.py](src/rdd/closure_shared_generator.py) | Closures pitfall: a captured object with state (a random generator) is copied to every partition. | The same sequence is repeated in each partition. |
| [closure_reproducible_random.py](src/rdd/closure_reproducible_random.py) | Closures fix: reproducible results independent of the data distribution. | The result does not change with the number of partitions. |
| [add_numbers_with_accumulator.py](src/rdd/add_numbers_with_accumulator.py) | Accumulators: count how many additions a `reduce` performs using a shared variable. | Accumulators are the right tool to send values from the workers to the driver. |

### Running an example

```bash
python -m rdd.add_numbers
python -m rdd.add_numbers_from_file
```

Or with `spark-submit`:

```bash
spark-submit src/rdd/max_number.py
```

### Counting lines of a text file

`count_lines_containing_word.py` takes the path of a text file; use `data/quijote.txt` (the
text of *Don Quijote*). By default it counts the lines containing "Quijote" and "Sancho", and
writes the lines containing "Dulcinea" to the `output/lines_with_word` directory, which
Spark refuses to overwrite if it already exists (`make clean` removes it):

```bash
python -m rdd.count_lines_containing_word data/quijote.txt
python -m rdd.count_lines_containing_word data/quijote.txt \
    --count-words molino gigante --extract-word Rocinante --output output/rocinante
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

Tests use a local, session-scoped `SparkContext` defined in
[tests/conftest.py](tests/conftest.py). The smoke tests in
[tests/rdd/test_examples_smoke.py](tests/rdd/test_examples_smoke.py) run every example as a
separate process.

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
