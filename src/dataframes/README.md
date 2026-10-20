# DataFrame examples

Examples based on the DataFrame API. Each has its tests in
[tests/dataframes/](../../tests/dataframes/). They must be run from the project root (see the
[main README](../../README.md)).

| Example | What it shows | What to observe |
| ------- | ------------- | --------------- |
| [basic_operations.py](basic_operations.py) | Read a CSV file (`data/personalData.csv`) inferring its schema; select, add, rename and drop columns; filter rows; create columns with `when`/`otherwise`; sort; aggregate; and write the result as JSON and CSV. | The same aggregations (sum and mean) are computed with the DataFrame API and with the underlying RDD. |
| [count_airports_by_type.py](count_airports_by_type.py) | Count the airports of a country (Spain by default) by type, using the [OurAirports](https://ourairports.com/data/) data (more than 80,000 rows). | A `filter`, `groupBy`, `count` and `orderBy` pipeline on a real dataset. |
| [count_airports_by_type_with_schema.py](count_airports_by_type_with_schema.py) | The same count, reading the CSV file with an explicit schema (`StructType`) instead of inferring it. | The file is read only once, and `enforceSchema=false` makes Spark fail if the header does not match the schema instead of silently shifting the columns. |
| [count_airports_by_type_with_sql.py](count_airports_by_type_with_sql.py) | The same count as a Spark SQL query over a temporary view, with a named parameter (`:country`). | SQL and the DataFrame API produce the same result. |
| [count_airports_by_type_with_parquet.py](count_airports_by_type_with_parquet.py) | Convert the CSV file to Parquet, a columnar format that stores the schema with the data, partitioned by country (`partitionBy`), and run the same count on it. | The physical plan (`explain()`): Spark reads only the `type` column and only the `iso_country=ES` directory. |
| [count_airports_by_country.py](count_airports_by_country.py) | Join the airports with the countries (`data/countries.csv`) to get the countries with the most airports. | The small countries table is broadcast: the plan shows a `BroadcastHashJoin` instead of a `SortMergeJoin`. |
| [word_count_with_dataframe.py](word_count_with_dataframe.py) | The word count of `data/quijote.txt` with the DataFrame API: `regexp_extract_all` and `explode` to get the words, `groupBy`, `count` and `orderBy`. It is the version of [rdd/word_count.py](../rdd/word_count.py) with DataFrames. | Both examples print exactly the same words and counts (a test checks it), so their code and their computing times can be compared. `groupBy` shuffles the data in `spark.sql.shuffle.partitions` partitions, 200 by default: the example sets 4, which is enough for a book (see the docstring of the module). |

## Output directories

`basic_operations.py` writes the data in JSON and CSV formats to `output/basic_operations`,
replacing any previous output, and `count_airports_by_type_with_parquet.py` writes the airports
in Parquet format to `output/airports_parquet`, also replacing any previous output.

## Downloading the airports data

The airports examples read `data/airports.csv` (and `count_airports_by_country.py` also reads
`data/countries.csv`). They are not included in the repository because they are updated daily.
Spark cannot read a CSV file directly from an HTTP(S) URL, so download them first:

```bash
curl -o data/airports.csv https://davidmegginson.github.io/ourairports-data/airports.csv
curl -o data/countries.csv https://davidmegginson.github.io/ourairports-data/countries.csv
```
