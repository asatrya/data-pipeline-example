from gcr.io/datamechanics/spark:platform-3.1-dm14

ENV PYSPARK_MAJOR_PYTHON_VERSION=3
WORKDIR /opt/application/

COPY jars/mysql-connector-java-8.0.28.jar /opt/spark/jars/

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY main.py .