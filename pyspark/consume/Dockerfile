FROM python:3-alpine
USER root
RUN adduser -D muser
RUN apk update && apk add bash && apk add openjdk11
RUN pip install pyspark
ADD consume.py /home/muser
RUN wget https://dlcdn.apache.org/spark/spark-3.3.3/spark-3.3.3-bin-hadoop3.tgz -P /home/muser
RUN cd /home/muser && tar -xzvf /home/muser/spark-3.3.3-bin-hadoop3.tgz
RUN chmod a+x /home/muser/spark-3.3.3-bin-hadoop3/bin/spark-submit
USER muser
ENTRYPOINT ["/home/muser/spark-3.3.3-bin-hadoop3/bin/spark-submit","--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0","/home/muser/consume.py"]