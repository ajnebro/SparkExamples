# Spark Examples

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
src/rdd/          Examples based on the RDD API
src/dataframes/   Examples based on the DataFrame API
tests/            pytest tests (one test module per example)
data/             Input files used by the examples
```

The examples take no command line arguments: their inputs (file names, words, ...) are
constants at the top of each module, so they can be changed by editing them.

## Examples

The examples run on a local Spark master and must be run from the project root. They are
listed in the suggested order of study: first the RDD API, then the DataFrame API.

### RDD API

The RDD examples live in [src/rdd/](src/rdd/); each has its tests in [tests/rdd/](tests/rdd/).

| Example | What it shows | What to observe |
| ------- | ------------- | --------------- |
| [add_numbers.py](src/rdd/add_numbers.py) | Create an RDD with `parallelize` and sum its elements with `reduce`. | The result of an action is returned to the driver. |
| [max_number.py](src/rdd/max_number.py) | Find the maximum of a list with `reduce`. | `reduce` works with any associative and commutative function. |
| [add_numbers_from_file.py](src/rdd/add_numbers_from_file.py) | Read numbers from a text file (`data/numbers.txt`), sum them and measure the computing time. | Transformations are lazy: the work happens in the action. |
| [count_lines_containing_word.py](src/rdd/count_lines_containing_word.py) | Filter and count lines of a text file, reusing an RDD with `persist()` and saving results with `saveAsTextFile`. | The file is read once thanks to `persist()`. |
| [word_count.py](src/rdd/word_count.py) | The classic word count: split lines into words with `flatMap`, count them with `map` and `reduceByKey`, and get the most frequent ones with `takeOrdered`. | `reduceByKey` combines the counts of each partition before the shuffle, and `takeOrdered` avoids sorting the whole RDD. |
| [closure_driver_variable.py](src/rdd/closure_driver_variable.py) | Closures: a driver variable is serialized and copied to the workers. | Changes made to a captured variable in the workers are not seen by the driver. |
| [closure_shared_generator.py](src/rdd/closure_shared_generator.py) | Closures pitfall: a captured object with state (a random generator) is copied to every partition. | The same sequence is repeated in each partition. |
| [closure_reproducible_random.py](src/rdd/closure_reproducible_random.py) | Closures fix: reproducible results independent of the data distribution. | The result does not change with the number of partitions. |
| [add_numbers_with_accumulator.py](src/rdd/add_numbers_with_accumulator.py) | Accumulators: count how many additions a `reduce` performs using a shared variable. | Accumulators are the right tool to send values from the workers to the driver. |

### DataFrame API

The DataFrame examples live in [src/dataframes/](src/dataframes/); each has its tests in
[tests/dataframes/](tests/dataframes/).

| Example | What it shows | What to observe |
| ------- | ------------- | --------------- |
| [basic_operations.py](src/dataframes/basic_operations.py) | Read a CSV file (`data/personalData.csv`) inferring its schema; select, add, rename and drop columns; filter rows; create columns with `when`/`otherwise`; sort; aggregate; and write the result as JSON and CSV. | The same aggregations (sum and mean) are computed with the DataFrame API and with the underlying RDD. |
| [count_airports_by_type.py](src/dataframes/count_airports_by_type.py) | Count the airports of a country (Spain by default) by type, using the [OurAirports](https://ourairports.com/data/) data (more than 80,000 rows). | A `filter`, `groupBy`, `count` and `orderBy` pipeline on a real dataset. |
| [count_airports_by_type_with_schema.py](src/dataframes/count_airports_by_type_with_schema.py) | The same count, reading the CSV file with an explicit schema (`StructType`) instead of inferring it. | The file is read only once, and `enforceSchema=false` makes Spark fail if the header does not match the schema instead of silently shifting the columns. |
| [count_airports_by_type_with_sql.py](src/dataframes/count_airports_by_type_with_sql.py) | The same count as a Spark SQL query over a temporary view, with a named parameter (`:country`). | SQL and the DataFrame API produce the same result. |
| [count_airports_by_type_with_parquet.py](src/dataframes/count_airports_by_type_with_parquet.py) | Convert the CSV file to Parquet, a columnar format that stores the schema with the data, partitioned by country (`partitionBy`), and run the same count on it. | The physical plan (`explain()`): Spark reads only the `type` column and only the `iso_country=ES` directory. |
| [count_airports_by_country.py](src/dataframes/count_airports_by_country.py) | Join the airports with the countries (`data/countries.csv`) to get the countries with the most airports. | The small countries table is broadcast: the plan shows a `BroadcastHashJoin` instead of a `SortMergeJoin`. |

### Running an example

```bash
python -m rdd.add_numbers
python -m rdd.add_numbers_from_file
python -m dataframes.basic_operations
python -m dataframes.count_airports_by_type
python -m dataframes.count_airports_by_type_with_schema
python -m dataframes.count_airports_by_type_with_sql
python -m dataframes.count_airports_by_type_with_parquet
python -m dataframes.count_airports_by_country
```

Or with `spark-submit`:

```bash
spark-submit src/rdd/max_number.py
```

### Output directories

`count_lines_containing_word.py` writes the lines containing "Dulcinea" to the
`output/lines_with_word` directory, which Spark refuses to overwrite if it already exists
(`make clean` removes it). `basic_operations.py` writes the data in JSON and CSV formats to
`output/basic_operations`, replacing any previous output, and
`count_airports_by_type_with_parquet.py` writes the airports in Parquet format to
`output/airports_parquet`, also replacing any previous output.

### Downloading the airports data

The airports examples read `data/airports.csv` (and `count_airports_by_country.py` also reads
`data/countries.csv`). They are not included in the repository because they are updated daily.
Spark cannot read a CSV file directly from an HTTP(S) URL, so download them first:

```bash
curl -o data/airports.csv https://davidmegginson.github.io/ourairports-data/airports.csv
curl -o data/countries.csv https://davidmegginson.github.io/ourairports-data/countries.csv
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
