# Chapter 1: Introduction to Data Algorithms

This chapter introduces fundamental PySpark concepts:

- Resilient Distributed Datasets (RDDs) - A set of elements of the same type `RDD[T] (each element has type T)` immutable
- DataFrames - A table of rows with named columns, also immutable
- Common transformations.

## Examples

| Example | Description |
| --------- | ------------- |
| `rdd_map_transformation.py` | Demonstrates `map()` and `mapValues()` transformations |
| `rdd_transformations_overview.py` | Overview of `filter`, `flatMap`, `groupByKey`, `reduceByKey`, `sortBy`, `cartesian` |
| `average_by_key_reducebykey.py` | Calculate averages per key using the (sum, count) pattern with `reduceByKey()` |
| `dataframe_basics.py` | DataFrame creation, filtering, selecting, and aggregations |

## Running Examples

From the project root:

```bash
# Run with Python
make run CHAPTER=chapter_01 EXAMPLE=rdd_map_transformation

# Run with spark-submit (for full Spark context)
make run-spark CHAPTER=chapter_01 EXAMPLE=dataframe_basics
```

## Key Concepts

- The **SparkSession** instance is represented as a spark object. Reading input creates a new RDD as an
RDD[String]: each input record is converted to an RDD element of the type String (if your input path has N records, then the number of RDD elements is N), gives you access to an instance of SparkContext.

- The **SparkContext** holds a connection to the Spark cluster manager and can be used to create RDDs and broadcast variables in the cluster. A shell (such as the PySpark shell) or PySpark driver program cannot create more than one instance of SparkContext.

- **Driver** All Spark applications (including the PySpark shell and standalone Python programs) run as independent sets of processes. These processes are coordinated by a SparkContext in a driver program. To submit a standalone Python program to Spark, you need to write a driver program with the PySpark API (or Java or Scala). This program is in charge of the process of running the main() function of the application and creating the SparkContext. It can also be used to create RDDs and DataFrames.

- **Worker** In a Spark cluster environment, there are two types of nodes: one (or two, for high availability) master and a set of workers. A worker is any node that can run programs in the cluster.

- When start a PySpark shell (by executing `<spark-installed-dir>/bin/pyspark`), you automatically get two variables/objects defined, `spark` An instance of SparkSession, which is ideal for creating DataFrames and `sc` An instance of SparkContext, which is ideal for creating RDDs

- **Actions** Spark actions are RDD operations or functions that produce non-RDD values. Infor‐
mally, we can express an action as: `action: RDD => non-RDD value`

- Avoid `collect()` on large datasets, use the `take()` or `takeSample()` actions instead. For example `RDD.take(N)` returns the first N elements of the RDD and `DataFrame.take(N)` returns the first N rows of the DataFrame as a list of Row objects.

- In a **Nutshell**, to solve a data analytics problem in PySpark, you read data and represent it as an RDD or DataFrame (depending on the nature of the data format), then write a set of transformations to convert your data into the desired output. Spark automatically partitions your DataFrames and RDDs and distributes the partitions across different cluster nodes. Partitions are the basic units of parallel‐ism in Spark. Parallelism is what allows developers to perform tasks on hundreds of computer servers in a cluster in parallel and independently. A partition in Spark is a chunk (a logical division) of data stored on a node in the cluster. DataFrames and RDDs are collections of partitions. Spark has a default data partitioner for RDDs and DataFrames, but you may override that partitioning with your own custom programming.

### RDD Transformations

Support two types of operations: transformations, which transform the source RDD(s) into one or more new RDDs, and actions, which transform the source RDD(s) into a non-RDD object such as a dictionary or array.
RDDs are not evaluated until an action is performed on them: this means that transformations are lazily evaluated. If an RDD fails during a transformation, the data lineage of transformations rebuilds the RDD.

- **map()**: 1-to-1 transformation, applies function to each element
- **flatMap()**: 1-to-many transformation, flattens results
- **filter()**: Selects elements matching a predicate
- **reduceByKey()**: Aggregates values by key (more efficient than groupByKey)
- **groupByKey()**: Groups values by key (use sparingly, memory intensive)

### DataFrames

- Higher-level abstraction over RDDs
- SQL-like operations: `filter()`, `select()`, `groupBy()`, `join()`
- Better optimization through Catalyst query optimizer
