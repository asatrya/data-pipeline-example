docker build -t pyspark-example:dev ./pyspark

docker run --mount type=bind,source="$(pwd)/pyspark",target=/opt/application \
        --network=my_network \
        -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin \
        pyspark-example:dev driver local:///opt/application/main.py -e '2020-05-29T00:00:00'

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

curl http://34.124.180.131:81/


{"customer_id": 123,"country_code": "Peru","last_order_ts": "2022-03-01 00:00:00","first_order_ts": "2017-05-03 00:00:00","total_orders": 15,"segment_name": "recency_segment"}

curl http://34.124.180.131:81/voucher/most-used?customer=%7B%22customer_id%22%3A%20123%2C%22country_code%22%3A%20%22Peru%22%2C%22last_order_ts%22%3A%20%222022-03-01%2000%3A00%3A00%22%2C%22first_order_ts%22%3A%20%222017-05-03%2000%3A00%3A00%22%2C%22total_orders%22%3A%2015%2C%22segment_name%22%3A%20%22recency_segment%22%7D

{"customer_id": 123,"country_code": "Peru","last_order_ts": "2022-03-01 00:00:00","first_order_ts": "2017-05-03 00:00:00","total_orders": 15,"segment_name": "recency_segment"}
curl http://34.124.180.131:81/voucher/most-used?customer=%7B%22customer_id%22%3A%20123%2C%22country_code%22%3A%20%22Peru%22%2C%22last_order_ts%22%3A%20%222022-03-01%2000%3A00%3A00%22%2C%22first_order_ts%22%3A%20%222017-05-03%2000%3A00%3A00%22%2C%22total_orders%22%3A%2015%2C%22segment_name%22%3A%20%22recency_segment%22%7D

curl http://34.124.180.131:81/voucher/most-used?customer=%7B%22customer_id%22%3A%20123%2C%22country_code%22%3A%20%22Peru%22%2C%22last_order_ts%22%3A%20%222022-03-01%2000%3A00%3A00%22%2C%22first_order_ts%22%3A%20%222017-05-03%2000%3A00%3A00%22%2C%22total_orders%22%3A%2015%2C%22segment_name%22%3A%20%22frequent_segment%22%7D


# Test

docker exec fastapi pytest

# Assumptions & Questions

* batch daily processing
* segment is per country_code
* add new variant for frequent_segment: "37+"
* add new variant for recency_segment: "0-30"
* In determining segment variants, the left edge of the range is assumed as exclusive
* any time calculation always assumes in the same timezone