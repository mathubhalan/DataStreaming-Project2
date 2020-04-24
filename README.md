# DataStreaming-Nanodegree-project2
Project work for Data Streaming nanodegree at Udacity.

# Project2 Udacity Nanodegree:

> 1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

Without tuning  `processedRowsPerSecond` was way too low and one batch took more than 23 seconds to complete, so if `.trigger(processingTime="20 seconds")` set less than 20 sec - I cannot see any results - screen refreshes too often.

> 2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

With this settings I was able to achieve throughput of 120 rows per second, when I varied it - throughput declined. 

```
        .config("spark.sql.shuffle.partitions", 9) \
        .config("spark.streaming.kafka.maxRatePerPartition", 1000) \
        .config("spark.default.parallelism", 300) \
```
