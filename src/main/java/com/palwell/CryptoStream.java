package com.palwell;

import java.sql.Date;
import java.text.DateFormat;
import java.util.*;
import java.util.regex.Pattern;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;


import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.*;
import scala.Tuple2;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.Durations;
import org.apache.kafka.common.serialization.StringDeserializer;

import javax.swing.*;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaDirectKafkaWordCount <brokers> <topics>
 *   <brokers> is a list of one or more Kafka brokers
 *   <topics> is a list of one or more kafka topics to consume from
 *
 * Example:
 *    $ bin/run-example streaming.JavaDirectKafkaWordCount broker1-host:port,broker2-host:port \
 *      topic1,topic2
 */

public class CryptoStream {

    //Kafka Parameters
    private static final String BROKERS = "pathdp3.field.hortonworks.com:6667";
    private static String TOPICS = "cryptocurrency-nifi-data";
    private static String APP_NAME = "CryptoStream";

    private void startStream() throws Exception {

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName(APP_NAME).setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        //Setting kafka topics and brokers
        Set<String> topicsSet = new HashSet<>(Arrays.asList(TOPICS));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", BROKERS);
        kafkaParams.put("key.deserializer", ByteArrayDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "ID-1");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> kafka_messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topicsSet, kafkaParams));

        kafka_messages.foreachRDD(rdd ->
        {
            // Get the singleton instance of SparkSession
            SparkSession spark = SparkSession
                    .builder()
                    .getOrCreate();

            // Convert RDD[String] to RDD[case class] to DataFrame
            Dataset<Row> satoriDF= spark.createDataFrame(rdd,JavaRow.class);
            satoriDF.createOrReplaceTempView("CryptoStream");

            Dataset<Row> final_df =
                    spark.sql("Select cryptocurrency, exchange from CryptoStream limit 5");
            final_df.show();
        });

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

    public static void main(String[] args) throws Exception {
        new CryptoStream().startStream();
    }
}