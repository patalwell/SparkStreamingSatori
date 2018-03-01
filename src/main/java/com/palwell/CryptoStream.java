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
//    private static final Pattern SPACE = Pattern.compile(" ");

//    public static void main(String[] args) throws Exception {
//        if (args.length < 2) {
//            System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n" +
//                    "  <brokers> is a list of one or more Kafka brokers\n" +
//                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
//            System.exit(1);
//        }

    public static void main(String[] args) throws Exception {

        //Kafka Parameters
        String brokers = "pathdp3.field.hortonworks.com:6667";
        String topics = "cryptocurrency-nifi-data";

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("CryptoStream").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        //Setting kafka topics and brokers
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);
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


//        lines.foreachRDD(rdd, time ) --> {
//            SparkSession spark = SparkSession.builder().config(rdd.sparkConetxt().getConf()).getOrCreate();
//
//
//        // Convert RDD[String] to RDD[case class] to DataFrame
//        JavaRDD<JavaRow> rowRDD = rdd.map(line -> {
//            String [] fields = line.split(",");
//            JavaRow record = new JavaRow();
//            record.setExchange(fields[0]);
//            record.setCryptocurrency(fields[1]);
//            record.setBasecurrency(fields[2]);
//            record.setType(fields[3]);
//            record.setPrice(Double.parseDouble(fields[4].trim()));
//            record.setSize(fields[5]);
//            record.setBid(Double.parseDouble(fields[6].trim()));
//            record.setAsk(Double.parseDouble(fields[7].trim()));
//            record.setOpen(Double.parseDouble(fields[8].trim()));
//            record.setHigh(Double.parseDouble(fields[9].trim()));
//            record.setLow(Double.parseDouble(fields[10].trim()));
//            record.setVolume(Double.parseDouble(fields[11].trim()));
//            record.setTimestamp(Date.valueOf(fields[12].trim()));
//            return record;
//        });
//
//        Datset<Row> wordsDataFrame = spark.createDataFrame(rowRDD, JavaRow.class);
//
//        // Creates a temporary view using the DataFrame
//        wordsDataFrame.createOrReplaceTempView("crypto");
//
//        DataFrame wordCountsDataFrame =
//                spark.sql("select count(cryptocurrency) from crypto group by exchange limit 5");
//        wordCountsDataFrame.show();
//    }

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}