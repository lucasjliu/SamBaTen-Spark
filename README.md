# SamBaTen

This is a re-implementation of [SamBaTen](https://arxiv.org/abs/1709.00668) in Spark, an online batch incremental tensor decomposition algorithm. It also includes an implementation of CP (PARAFAC) decomposition by ALS (Alternating Least Squares) in Spark.

## Introduction


## Installation
Requirements: Scala 2.11.11, Spark 2.2.0, sbt 0.1.

Or use Docker:
```
# Create an image using the provided Dockerfile.
docker rmi spark-node

# Create a container on master machine:
docker run -dit --name master -v your_project_path:/root/spark/sambaten -p 8080:8080 -p 7077:7077 -p 4040:4040 spark-node

# Create containers on worker machines:
docker run -dit --name worker spark-node
```

## Usage Instruction

To run the tests in local environment without setting up a cluster, simply run:
```
sbt package
spark-submit --class "edu.ucr.sambaten.App" --master local[*] ./target/scala-2.11/sambaten_2.11-0.1.jar
```
Here is an example of setting up a built-in cluster. See [Spark Docs](https://spark.apache.org/docs/latest/cluster-overview.html) for other cluster manager options.

```
# On the master machine:
$SPARK_HOME/sbin/start-master.sh -h your_master_host

# On every worker machine:
$SPARK_HOME/sbin/start-slave.sh spark://your_master_host:7077
```
Now master’s web UI (http://localhost:8080 by default) should show the master URL and worker nodes.
Then compile and submit the application:
```
sbt package
spark-submit --class "edu.ucr.sambaten.App" --master your_spark_url ./target/scala-2.11/sambaten_2.11-0.1.jar
```


## References

- [SamBaTen: Sampling-based Batch Incremental Tensor Decomposition](https://arxiv.org/abs/1709.00668).
  Gujral, Ekta, Ravdeep Pasricha, and Evangelos E. Papalexakis. arXiv, 2017.
- [Haten2: Billion-scale tensor decompositions](https://ieeexplore.ieee.org/abstract/document/7113355/).
  Jeon, Inah, Evangelos E. Papalexakis, U. Kang, and Christos Faloutsos. Data Engineering (ICDE), 2015 IEEE 31st International Conference on. IEEE, 2015.
- [Tensors for Data Mining and Data Fusion: Models, Applications, and Scalable Algorithms](https://dl.acm.org/citation.cfm?id=2915921).
  Papalexakis, Evangelos E., Christos Faloutsos, and Nicholas D. Sidiropoulos. ACM Transactions on Intelligent Systems and Technology (TIST), 2017.