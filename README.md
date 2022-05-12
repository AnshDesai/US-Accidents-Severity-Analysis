US Accidents Severity Analysis
============

Stream producer for time series data using Kafka.

The producer and consumer Python scripts use [Confluent's Kafka client for Python](https://github.com/confluentinc/confluent-kafka-python), which is installed in the Docker image built with the accompanying Dockerfile, if you choose to use it.

Requires Docker and Docker Compose.

Usage
-------------------

Clone repo and cd into directory.

```
git clone https://github.com/AnshDesai/US-Accidents-Severity-Analysis.git
cd time-series-kafka-demo
```

**Start the Kafka broker**

```
docker compose up --build
```

**Produce a time series stream**

Send time series from data/data.csv to topic “my-stream”, and speed it up by a factor of 10.

```
python sendstream.py data/testdata.csv accidents-stream --speed 10
```

or with Docker:

```
docker run -it --rm \
      -v $PWD:/home \
      --network=host \
      kafkacsv python bin/sendstream.py data/testdata.csv accidents-stream --speed 10
```

When complete:
**Shut down and clean up**

Stop the consumer with Return and Ctrl+C.

Shutdown Kafka broker system:

```
docker compose down
```
