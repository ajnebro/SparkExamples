# Machine learning examples

Examples based on Spark ML (`pyspark.ml`), the machine learning library of Spark built on
DataFrames. Each has its tests in [tests/ml/](../../tests/ml/). They must be run from the project
root (see the [main README](../../README.md)).

| Example | What it shows | What to observe |
| ------- | ------------- | --------------- |
| [normalizer_example.py](normalizer_example.py) | Divide each vector of a column by its norm with `Normalizer`, using the L1 norm and the infinity norm (`p=inf`). | A transformer adds a column to the DataFrame (`transform`) and each row is processed on its own, so there is no shuffle. A zero vector stays as it is. |
| [read_libsvm.py](read_libsvm.py) | Read a file in LIBSVM format (`data/iris.libsvm`, the Iris data set) with the `libsvm` data source. | The result has the columns `label` (double) and `features` (vector), and the vectors are sparse: `(4,[0,1,2,3],[...])` holds the size, the indices and the non-zero values. |
| [remove_stop_words.py](remove_stop_words.py) | Remove the stop words of some sentences with `StopWordsRemover` and the default list for English that Spark brings. | It works on a column of arrays of words, and the comparison ignores the case. [dataframes/word_count_without_stop_words.py](../dataframes/word_count_without_stop_words.py) uses it to count the words of a book. |

## About the data

`data/iris.libsvm` is the Iris data set (150 flowers of 3 classes, with 4 features: sepal and
petal length and width, in centimeters) in LIBSVM format. It was generated from the original
data, published by the UCI Machine Learning Repository, replacing the class names by the numbers
0 (setosa), 1 (versicolor) and 2 (virginica), and the feature values are not changed.

- Fisher, R. A. (1936). The use of multiple measurements in taxonomic problems. *Annals of
  Eugenics*, 7(2), 179-188.
- Dua, D. and Graff, C. [Iris](https://archive.ics.uci.edu/dataset/53/iris), UCI Machine Learning
  Repository. DOI: [10.24432/C56C76](https://doi.org/10.24432/C56C76). Licensed under
  [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
