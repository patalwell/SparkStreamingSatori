/**
 * Spark Streaming application that pulls JSON from Kafka and iterates
 * through each series of DStream objects and conducts DF operations.
 *
 * To DO Usage: CryptoStream <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.JavaDirectKafkaWordCount broker1-host:port,broker2-host:port \
 *      topic1,topic2
 */
package com.palwell;


import java.util.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.Durations;
import org.apache.kafka.common.serialization.StringDeserializer;
//import org.apache.spark.sql.types.StructType;

public class CryptoStream {

    //Kafka Parameters
    private static final String KAFKA_BROKERS =
            "pathdf3.field.hortonworks.com:6667";
    private static final String TOPICS = "cryptocurrency-nifi-data";
    private static final String APP_NAME = "CryptoStream";
    private static final int DURATION = 10;

    //  private void startStream() throws Exception {
    public static void main(String[] args) throws Exception {

        // Create context with a 10 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName(APP_NAME).setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations
                .seconds(DURATION));

        //Setting kafka topics and brokers
        Set<String> topicsSet = new HashSet<>(Arrays.asList(TOPICS));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", KAFKA_BROKERS);
        kafkaParams.put("key.deserializer", ByteArrayDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "ID-1");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> kafka_messages =
                KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String,String>Subscribe(topicsSet,
                        kafkaParams));

        // Filter for values from our Kafka Stream of (key,value)
        JavaDStream<String> lines = kafka_messages.map(ConsumerRecord::value);

        // lines.print();

        // Iterate over our JavaDStream object line and apply function to each
        // series
        lines.foreachRDD((rdd, time) -> {

            // Get the singleton instance of SparkSession
            SparkSession spark = SparkSession.builder()
                    .config(rdd.context().getConf())
                    .getOrCreate();

            // Create our DataFrame with JavaClass and drop nulls
            Dataset data = spark.read().json(rdd).as(Encoders.bean(PayloadSchema
                    .class));
//            data.na();
            data.createOrReplaceTempView("CryptoCurrency");

            // Conduct SQL on our Stream
            spark.sql(
                    "SELECT cryptocurrency ,avg(price) as average_price,max" +
                            "(price) as max_price,min(price) as min_price,std" +
                            "(price) as 1_std,std(price) * 2 as 2_std,std" +
                            "(price)/avg(price)*100 as normalized_stnd_dev " +
                            "FROM CryptoCurrency WHERE (cryptocurrency " +
                            "=='ADX' OR cryptocurrency == 'BTC' OR " +
                            "cryptocurrency == 'LTC' OR cryptocurrency == " +
                            "'ETH') AND basecurrency == 'USD' GROUP BY " +
                            "cryptocurrency ORDER BY cryptocurrency").show();

        });
        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}

/**
 *
 * Including creation of dataFrame with StructType Class
 *
 *             StructType schema = new StructType()
 *                  .add("exchange","string")
 *                  .add("cryptocurrency","string")
 *                  .add("basecurrency","string")
 *                  .add("type","string")
 *                  .add("price","double")
 *                  .add("size","double")
 *                  .add("bid","double")
 *                  .add("ask","double")
 *                  .add("open","double")
 *                  .add("high","double")
 *                  .add("low","double")
 *                  .add("volume","double")
 *                  .add("timestamp","long");
 *
 * To Create our DataFrame with Schema use Dataset<Row> class
 *
 *   Dataset<Row> dataDF = spark.read().schema(schema).json(rdd);
 *   dataDF.na();
 *   dataDF.createOrReplaceTempView("CryptoCurrency");
 *
 */

