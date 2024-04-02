FROM openjdk:17

RUN microdnf install -y git unzip procps 
    
ENV MASTER=The-Prestigeous. \
    SPARK_VER=3.5.1
ENV SPARK_HOME=/usr/spark-$SPARK_VER-bin-hadoop3-scala2.13     

WORKDIR /usr
COPY software/spark-$SPARK_VER-bin-hadoop3-scala2.13.tgz .
RUN tar -xzf spark-$SPARK_VER-bin-hadoop3-scala2.13.tgz \
    rm ./spark-$SPARK_VER-bin-hadoop3-scala2.13.tgz \
    rm -rf $SPARK_HOME/examples

#RUN curl -sL \
#    https://dlcdn.apache.org/spark/spark-$SPARK_VER/spark-$SPARK_VER-bin-hadoop3-scala2.13.tgz |\
#    gunzip |\
#    tar -x -C /usr/ &&\
#    rm -rf $SPARK_HOME/examples

WORKDIR ${SPARK_HOME}

RUN chmod +x sbin/*.sh
CMD bin/spark-class org.apache.spark.deploy.worker.Worker spark://${MASTER}:7077
    