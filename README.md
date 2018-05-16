<h1>Spark Streaming and Structured Streaming with Java and Kafka 10</h1>

<h3>Background:</h3>
This repository contains two applications CryptoStream.java and
StructCryptoStream.java. The former is using Spark's Structured Streaming API
while the latter is using the Spark Streaming API.

<h3>To Do:</h3>

1. Insert logging for debugging issues
2. Create an ML model for VaR
3. Create a layer of persistence with Pheonix/HBase


<h3>Sample Results:</h3>

+--------------+-----------------+---------+---------+
|cryptocurrency|    average_price|max_price|min_price|
+--------------+-----------------+---------+---------+
|           BTC|8311.699999999999|   8390.1|   8270.0|
|           ETH|697.9666666666666|   704.82|   693.71|
|           LTC|           137.97|    138.1|   137.84|
+--------------+-----------------+---------+---------+