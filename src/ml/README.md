# Machine learning examples

Examples based on Spark ML (`pyspark.ml`), the machine learning library of Spark built on
DataFrames. Each has its tests in [tests/ml/](../../tests/ml/). They must be run from the project
root (see the [main README](../../README.md)).

| Example | What it shows | What to observe |
| ------- | ------------- | --------------- |
| [normalizer_example.py](normalizer_example.py) | Divide each vector of a column by its norm with `Normalizer`, using the L1 norm and the infinity norm (`p=inf`). | A transformer adds a column to the DataFrame (`transform`) and each row is processed on its own, so there is no shuffle. A zero vector stays as it is. |
| [read_libsvm.py](read_libsvm.py) | Read a file in LIBSVM format (`data/iris.libsvm`, the Iris data set) with the `libsvm` data source. | The result has the columns `label` (double) and `features` (vector), and the vectors are sparse: `(4,[0,1,2,3],[...])` holds the size, the indices and the non-zero values. |
| [remove_stop_words.py](remove_stop_words.py) | Remove the stop words of some sentences with `StopWordsRemover` and the default list for English that Spark brings. | It works on a column of arrays of words, and the comparison ignores the case. [dataframes/word_count_without_stop_words.py](../dataframes/word_count_without_stop_words.py) uses it to count the words of a book. |
| [svm_training.py](svm_training.py) | Train a linear SVM (`LinearSVC`) with `fit` on a small two-class data set (`data/classificationDataLibsvm.txt`) and save the model in `output/svm_model`. Run it first. | An estimator learns from the data (`fit`) and returns a model, which is a transformer. The model has one coefficient per feature and an intercept, and it is saved to be used by other programs. |
| [svm_evaluation.py](svm_evaluation.py) | Load the saved model, predict labeled test points and measure the accuracy with `MulticlassClassificationEvaluator`. | The test points are not in the training data, so the accuracy measures how well the model generalizes. `transform` adds the columns `rawPrediction` and `prediction`. |
| [svm_prediction.py](svm_prediction.py) | Load the saved model and classify new points that have no label, only an identifier. | It is how a model is used once trained: only `transform` is needed, no `fit` and no labels. |
| [svm_classification_workflow.py](svm_classification_workflow.py) | The complete workflow on the Iris data: `randomSplit` into training and test sets, a `Pipeline` with `StandardScaler` and `LogisticRegression`, `fit`, accuracy on the test set, and saving and loading the `PipelineModel`. | The pipeline is fitted with the training set only, to avoid leaking information from the test set. The seed makes the split reproducible. |
| [kmeans_training.py](kmeans_training.py) | Group the points of `data/classificationDataLibsvm.txt` in 2 clusters with `KMeans`, print the centers and save the model in `output/kmeans_model`. Run it first. | K-means is unsupervised: it ignores the label. The output shows the cluster of each point next to its real label, and both match. `fit` returns a model whose `transform` adds the column `prediction`. |
| [kmeans_prediction.py](kmeans_prediction.py) | Load the saved k-means model, assign new points to a cluster and measure the clustering with the silhouette (`ClusteringEvaluator`). | There are no real classes to compare with, so the quality is measured with the silhouette, between -1 and 1: the closer to 1, the better separated the clusters are. |
| [kmeans_choose_k.py](kmeans_choose_k.py) | Train k-means for several numbers of clusters (2 to 5) and compare their silhouettes to choose `k`. | K-means needs `k` in advance. The silhouette is highest for `k=2` and decreases afterwards, because the data has two groups. |
| [decision_tree.py](decision_tree.py) | Classify Iris with a `DecisionTreeClassifier` (`maxDepth=3`) trained with 70% of the data (`randomSplit` with a seed) and evaluated with the other 30%. | `toDebugString` prints the questions of the tree, and `featureImportances` shows that only the petal measurements are used. A tree can be read, unlike most models. |
| [logistic_regression.py](logistic_regression.py) | Classify Iris with a `LogisticRegression`: print its parameters with `explainParams`, the coefficients of the model and the probability of each class. | With three classes the model is multinomial: one row of coefficients per class. The predicted class is the one with the highest probability. |
| [linear_regression.py](linear_regression.py) | Predict the petal width of Iris from the other three measurements with `LinearRegression`, preparing the data with `VectorSlicer` and `vector_to_array`. | A regression predicts a number. RMSE (in centimeters) and R2 are computed on the training and the test sets, and are similar: the model does not overfit. |
| [preprocess_iris.py](preprocess_iris.py) | Prepare the original Iris file of UCI (`data/iris.data`, CSV without header) for Spark ML: read it with an explicit schema, and use a `Pipeline` with `StringIndexer` (species to number) and `VectorAssembler` (measurements to a vector). | The result has the columns `label` and `features`, like the LIBSVM file. `StringIndexer` numbers by frequency unless `stringOrderType` is set; here it is alphabetical, because the three species are equally frequent. |
| [preprocess_wine.py](preprocess_wine.py) | The same preparation for the Wine data (`data/wine.data`): the first column is the class (1 to 3) and the other 13 are chemical measurements. | The class is cast to string and indexed so that the labels start at 0. The measurements have very different scales (proline up to 1680, hue below 2), which matters for the algorithms that use distances or penalties. |

## About the data

`data/classificationDataLibsvm.txt` is a small set of 13 points with two features and two
classes (0 and 1), used by the SVM and k-means examples.

`data/iris.libsvm` is the Iris data set (150 flowers of 3 classes, with 4 features: sepal and
petal length and width, in centimeters) in LIBSVM format. It was generated from the original
data, published by the UCI Machine Learning Repository, replacing the class names by the numbers
0 (setosa), 1 (versicolor) and 2 (virginica), and the feature values are not changed.

- Fisher, R. A. (1936). The use of multiple measurements in taxonomic problems. *Annals of
  Eugenics*, 7(2), 179-188.
- Dua, D. and Graff, C. [Iris](https://archive.ics.uci.edu/dataset/53/iris), UCI Machine Learning
  Repository. DOI: [10.24432/C56C76](https://doi.org/10.24432/C56C76). Licensed under
  [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

`data/iris.data` is the original file of the same data set, as published by UCI (150 lines and an
empty last line, which Spark ignores).

`data/wine.data` is the Wine data set (178 wines from three grape varieties, with 13 chemical
measurements), as published by the UCI Machine Learning Repository
([Wine](https://archive.ics.uci.edu/dataset/109/wine)). The original source is Forina, M. et al.,
*PARVUS - An Extendible Package for Data Exploration, Classification and Correlation*, Institute
of Pharmaceutical and Food Analysis and Technologies, Genoa, Italy.
