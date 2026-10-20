# Streaming examples

Examples based on Spark Structured Streaming. They must be run from the project root (see the
[main README](../../README.md)).

| Example | What it shows | What to observe |
| ------- | ------------- | --------------- |
| [temperature_sensor_generator.py](temperature_sensor_generator.py) | Not a Spark program: it simulates a sensor that writes a random temperature in a new file of `output/streaming` every 5 seconds. It is the data source of the streaming examples; run it in a separate terminal and stop it with Ctrl+C. | Each file is written to a hidden temporary file and then renamed, so Spark never reads a partially written file. |
| [fetch_file_by_interval.py](fetch_file_by_interval.py) | Not a Spark program: it downloads a URL (e.g. a CSV that a web site updates periodically) every few seconds, each time into a new file of a directory. It is a data source for streaming examples and, unlike the others, it takes command-line arguments: `python -m streaming.fetch_file_by_interval <url> <directory> <seconds> [--downloads N]`. Stop it with Ctrl+C. | Like the sensor generator, each file is written to a hidden temporary file and then renamed, so Spark never reads a partially downloaded file. |
| [streaming_average_temperature.py](streaming_average_temperature.py) | Read the directory of the sensor as a stream (`readStream`), compute the average temperature with `avg` and print it on the console after each micro-batch. Run the generator first, in another terminal. | An aggregation without a window needs the `complete` output mode, and the average covers all the files received since the query started, including the ones already in the directory (`make clean` removes them). |
| [streaming_find_high_temperature.py](streaming_find_high_temperature.py) | Read the directory of the sensor as a stream, keep only the temperatures higher than a threshold (`where`) and print them on the console. Run the generator first, in another terminal. | The values are text, so they must be cast to `double` before comparing; a query without aggregation uses the `append` mode, which writes each row once. |
