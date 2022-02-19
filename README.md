# spark_dockerfiles
Holds tar and gzipped files for Spark
The following are included in the dockerfile names

1.      The version of Spark
2.      The Scala version
3.      The Java version
4.      The OS version

So your dockerfile will be in this format

docker images
No alt text provided for this image

REPOSITORY                             TAG                                                    IMAGE ID       CREATED          SIZE
spark/spark-py                         3.1.1-scala_2.12-8-jre-slim-buster                     f74d60205fe9   31 minutes ago   952MB
spark/spark                            3.1.1-scala_2.12-8-jre-slim-buster                     4d903c6cf0de   2 months ago     599MB

The tarred ang gzipped dockerfiles are created as below

 docker save spark/spark-py:3.1.1-scala_2.12-8-jre-slim-buster|gzip > spark-spark-py_3.1.1-scala_2.12-8-jre-slim-buster.tar.gz
 docker save spark/spark:3.1.1-scala_2.12-8-jre-slim-buster | gzip > spark-spark_3.1.1-scala_2.12-8-jre-slim-buster.tar.gz

Jusyt download them gunzip and untar them, so you would have it
