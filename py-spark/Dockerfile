FROM ubuntu:latest
WORKDIR /py-spark

COPY . .

RUN apt-get update
RUN apt-get install openjdk-8-jdk -y
RUN apt-get install python3
RUN apt-get install python3-pip -y
RUN pip3 install pyspark
RUN pip3 install cassandra-driver

CMD ["bash", "./scripts/run-seeds.sh"]