# Scala-Spark various exercices

## Sbt setup
Custom javaOptions are used to fix problems caused by running Spark with Java 17

## Dockerfile usage (for workers)
Dockerfile is intended to build a simple container for worker nodes, only spark version, address of the master 
and memory available are configurable.

Default build is intented to take a previously downloaded Spark distribution and copy it within the container. 
For this to work, Spark must be dowloaded as a tar.gz, the file renamed only to "spark-<spark-verion-numer>.tgz" 
(where <spark-version-number> is the full version, such as 3.5.1) and kept in a "software" folder within the 
project root

> [!IMPORTANT]
The downloaded Spark version must match the sbt Spark dependencies versions and be compiled for the target Scala
version

Spark distribution can also be dowloaded on image build, for that, uncomment the lines starting with "ARG" and 
comment out the previous stage starting with the "WORKDIR" command. In this case, download url must be provided
via the downloadurl build arg
