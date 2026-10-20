# RDD examples

Examples based on the RDD API. Each has its tests in [tests/rdd/](../../tests/rdd/). They must
be run from the project root (see the [main README](../../README.md)).

| Example | What it shows | What to observe |
| ------- | ------------- | --------------- |
| [add_numbers.py](add_numbers.py) | Create an RDD with `parallelize` and sum its elements with `reduce`. | The result of an action is returned to the driver. |
| [max_number.py](max_number.py) | Find the maximum of a list with `reduce`. | `reduce` works with any associative and commutative function. |
| [add_numbers_from_file.py](add_numbers_from_file.py) | Read numbers from a text file (`data/numbers.txt`), sum them and measure the computing time. | Transformations are lazy: the work happens in the action. |
| [count_lines_containing_word.py](count_lines_containing_word.py) | Filter and count lines of a text file, reusing an RDD with `persist()` and saving results with `saveAsTextFile`. | The file is read once thanks to `persist()`. |
| [word_count.py](word_count.py) | The classic word count: split lines into words with `flatMap`, count them with `map` and `reduceByKey`, and get the most frequent ones with `takeOrdered`. | `reduceByKey` combines the counts of each partition before the shuffle, and `takeOrdered` avoids sorting the whole RDD. |
| [closure_driver_variable.py](closure_driver_variable.py) | Closures: a driver variable is serialized and copied to the workers. | Changes made to a captured variable in the workers are not seen by the driver. |
| [closure_shared_generator.py](closure_shared_generator.py) | Closures pitfall: a captured object with state (a random generator) is copied to every partition. | The same sequence is repeated in each partition. |
| [closure_reproducible_random.py](closure_reproducible_random.py) | Closures fix: reproducible results independent of the data distribution. | The result does not change with the number of partitions. |
| [add_numbers_with_accumulator.py](add_numbers_with_accumulator.py) | Accumulators: count how many additions a `reduce` performs using a shared variable. | Accumulators are the right tool to send values from the workers to the driver. |

## Output directories

`count_lines_containing_word.py` writes the lines containing "Dulcinea" to the
`output/lines_with_word` directory, which Spark refuses to overwrite if it already exists
(`make clean` removes it).
