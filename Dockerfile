FROM openjdk:17

RUN microdnf install -y git unzip procps 
    
ENV MASTER=The-Prestigeous. \
    SPARK_HOME=/usr/spark-3.5.1-bin-hadoop3-scala2.13     

WORKDIR /usr
COPY software/spark-3.5.1-bin-hadoop3-scala2.13.tgz .
RUN tar -xzf spark-3.5.1-bin-hadoop3-scala2.13.tgz
RUN rm ./spark-3.5.1-bin-hadoop3-scala2.13.tgz \
    rm -rf $SPARK_HOME/examples

#RUN curl -sL \
#    https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3-scala2.13.tgz |\
#    gunzip |\
#    tar -x -C /usr/ &&\
#    rm -rf $SPARK_HOME/examples

WORKDIR ${SPARK_HOME}

RUN chmod +x sbin/*.sh
#CMD sbin/start-worker.sh spark://${MASTER}:7077 -m ${WORKER_RAM}
CMD bin/spark-class org.apache.spark.deploy.worker.Worker spark://${MASTER}:7077
    