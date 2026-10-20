# Streaming examples

Examples based on Spark Structured Streaming. They must be run from the project root (see the
[main README](../../README.md)).

| Example | What it shows | What to observe |
| ------- | ------------- | --------------- |
| [temperature_sensor_generator.py](temperature_sensor_generator.py) | Not a Spark program: it simulates a sensor that writes a random temperature in a new file of `output/streaming` every 5 seconds. It is the data source of the streaming examples; run it in a separate terminal and stop it with Ctrl+C. | Each file is written to a hidden temporary file and then renamed, so Spark never reads a partially written file. |
| [streaming_average_temperature.py](streaming_average_temperature.py) | Read the directory of the sensor as a stream (`readStream`), compute the average temperature with `avg` and print it on the console after each micro-batch. Run the generator first, in another terminal. | An aggregation without a window needs the `complete` output mode, and the average covers all the files received since the query started, including the ones already in the directory (`make clean` removes them). |
