# The Background

The Data Engineering team is collaborating with the Marketing Team on the important project: "Churning
Customer Reactivation".

The idea behind the project is to reactivate customers who left the platform and resume their
order frequency. The Marketing Team approaches the problem by sending vouchers to
customers based on specific rules and customer attributes.

The data provided in the assignment is the historical data of voucher assignments for
customers. Each row is a voucher assigned to a customer.

# Architecture

![data_pipeline_diagram](https://user-images.githubusercontent.com/284098/159368723-bbc4753f-3c1f-41a0-9677-2870753e33ef.png)

       
## Component Explanation

1. Data Pipeline/Processing, implemented in PySpark, consist of 4 steps:
   - **Ingestion**: not implemented. The data is assumed have been got from source and stored in Raw zone.
   - **Curation**: process of having a standardized data. This includes cleaning, customer segment selection, and type-assigning.
   - **Aggregation**: the calculate most-used voucher amount for a particular customer segment.
   - **Publish**: make data available for external.
3. Data Lake Zones, consist of 4 zones:
   - **Raw zone**: store as-is data, no data processing at all. Implemented using Minio object storage.
   - **Curated zone**: store clean and standard data. Implemented using Minio object storage.
   - **Provision zone**: store aggregated and integrated data. Implemented using Minio object storage.
   - **Consumption zone**: provide access to data for external. Implemented using MySQL database.
5. REST API: Implement an endpoint that will expose the most-used voucher amount for a particular customer segment. The endpoint is implemented using FastAPI.
6. Minio Console: web-based console to administer Minio object storage.

# How to Run

## Prerequisite

1. Docker Engine
2. Docker Compose

## 1) Build & run Minio (including NGINX), MySQL, API

Run this command
```
cd ./data-pipeline-example
docker-compose up --build
```

Check if Minio is already up by going to your browser and open http://host-ip:9000, it will show a login screen (username=minioadmin, password: minioadmin).

Check if MySQL is already up by running this command
```
# in host's terminal
docker exec -it mysql bash

# in mysql docker's terminal
mysql -u user -ppassword db

# in mysql CLI
mysql> show tables;
+----------------------------+
| Tables_in_db               |
+----------------------------+
| voucher_frequent_most_used |
| voucher_recency_most_used  |
+----------------------------+
```

Check if API is already up by running curl
```
curl http://host-ip:81/

# response
{"Hello":"World!!"}
```

## 2) Build PySpark image & Run Pipeline

Build pyspark image
```
cd ./data-pipeline-example
docker build -t pyspark-example:dev ./pyspark
```

Run data pipeline
```
docker run --mount type=bind,source="$(pwd)/pyspark",target=/opt/application \
        --network=my_network \
        -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin \
        pyspark-example:dev driver local:///opt/application/main.py -e '2020-05-29T00:00:00'
```

Please note that the pipeline is parameterized with `execution_date` (`-e`). In the above example, the parameter value is `'2020-05-29T00:00:00'`.

After the pipeline run, check if the MySQL database tables are loaded with data from pipeline.
```
# in host's terminal
docker exec -it mysql bash

# in mysql docker's terminal
mysql -u user -ppassword db

# in mysql CLI, for example we select data for country_code=Peru
mysql> select * from voucher_frequent_most_used where country_code='Peru';
+--------------+------------------+----------------+-------------------+
| country_code | frequent_segment | voucher_amount | execution_date    |
+--------------+------------------+----------------+-------------------+
| Peru         | 5-13             |           2640 | 2020-05-29T000000 |
| Peru         | 0-4              |           2640 | 2020-05-29T000000 |
| Peru         | 37+              |           2640 | 2020-05-29T000000 |
| Peru         | 13-37            |           2640 | 2020-05-29T000000 |
+--------------+------------------+----------------+-------------------+

# in mysql CLI, for example we select data for country_code=Peru
mysql> select * from voucher_recency_most_used where country_code='Peru';
+-------------------+--------------+-----------------+----------------+
| execution_date    | country_code | recency_segment | voucher_amount |
+-------------------+--------------+-----------------+----------------+
| 2020-05-29T000000 | Peru         | 0-30            |           2640 |
| 2020-05-29T000000 | Peru         | 30-60           |           2640 |
| 2020-05-29T000000 | Peru         | 180+            |           2640 |
| 2020-05-29T000000 | Peru         | 120-180         |           2640 |
| 2020-05-29T000000 | Peru         | 90-120          |           2640 |
| 2020-05-29T000000 | Peru         | 60-90           |           2640 |
+-------------------+--------------+-----------------+----------------+
```

Check if the API give well response.

Run this curl command to test with customer object `{"customer_id": 123,"country_code": "Peru","last_order_ts": "2022-03-01 00:00:00","first_order_ts": "2017-05-03 00:00:00","total_orders": 15,"segment_name": "recency_segment"}`
```
curl http://34.124.180.131:81/voucher/most-used?customer=%7B%22customer_id%22%3A%20123%2C%22country_code%22%3A%20%22Peru%22%2C%22last_order_ts%22%3A%20%222022-03-01%2000%3A00%3A00%22%2C%22first_order_ts%22%3A%20%222017-05-03%2000%3A00%3A00%22%2C%22total_orders%22%3A%2015%2C%22segment_name%22%3A%20%22recency_segment%22%7D

# expected response
{"voucher_amount":2640}
```

# Development & Test

To run spark-shell
```
docker run -it \
    --mount type=bind,source="$(pwd)/pyspark",target=/opt/application \
    -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin \
    --network=my_network \
    pyspark-example:dev /opt/spark/bin/pyspark --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
                        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
                        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
                        --conf spark.hadoop.fs.s3a.path.style.access=True \
                        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
                        --conf fs.s3a.connection.ssl.enabled=False \
                        --conf fs.s3a.signing-algorithm=S3SignerType \
                        --conf spark.sql.sources.partitionOverwriteMode=dynamic
```

Then it will promt you spark shell
```
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.1.1
      /_/

Using Python version 3.8.10 (default, Jun  4 2021 15:09:15)
Spark context Web UI available at http://e7f710273094:4040
Spark context available as 'sc' (master = local[*], app id = local-1647898254446).
SparkSession available as 'spark'.
>>> 
```

# API Test

To run API test, use this command
```
docker exec fastapi pytest

# response
============================= test session starts ==============================
platform linux -- Python 3.9.11, pytest-7.1.1, pluggy-1.0.0
rootdir: /code
collected 2 items

tests/test_main.py ..                                                    [100%]

============================== 2 passed in 0.24s ===============================
```

# Best Practices Implementation

1. Idempotent and deterministic data processing
   - Idempotent: run multiple times, the result will be the same.
   - Deterministic: the result of data pipeline only determined by input parameters. In this case, we implement by parameterizing the `execution_date`.
   - These properties make the pipeline robust when run by orchestrators (ie: Airflow).
1. Rest data between data processing jobs (ideally each steps is implemented in separate python files and run by orchestrator. But for simplicity of POC, we put it in a single file and run as a job)
   - Each data processing step writes data to storage, and next step will read from storage
   - Good foundation for scalability
1. Validating data after each step (for simplicity of this POC, we only validate that all required columns are exist)
   - Prevent silent errors
   - Never publish wrong data
1. Functional paradigmn in data pipeline
   - Each step will read and write (overwrite) in specific partition based on execution_date
   - Good for maintainability (issue tracing, backfilling, retry, etc)


# Assumptions

* Batch daily processing
* Voucher calculation is done per country_code basis
* Add new variant for frequent_segment: "37+"
* Add new variant for recency_segment: "0-30"
* In determining segment variants, the left edge of the range is assumed as exclusive
* Any time calculation always assumes in the same timezone
