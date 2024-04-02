FROM openjdk:17

RUN microdnf install -y git unzip procps 
    
ENV MASTER=The-Prestigeous. \
    SPARK_VER=3.5.1
ENV SPARK_HOME=/usr/spark-$SPARK_VER     

WORKDIR /usr
COPY software/spark-$SPARK_VER.tgz .
RUN tar -xzf spark-$SPARK_VER.tgz \
    rm ./spark-$SPARK_VER.tgz \
    rm -rf $SPARK_HOME/examples

#ARG downloadurl
#RUN curl -sL \
#    $downloadurl |\
#    gunzip |\
#    tar -x -C /usr/ &&\
#    rm -rf $SPARK_HOME/examples

WORKDIR ${SPARK_HOME}

RUN chmod +x sbin/*.sh
CMD bin/spark-class org.apache.spark.deploy.worker.Worker spark://${MASTER}:7077
    