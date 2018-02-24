package com.palwell;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.deser.std.NullifyingDeserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import scala.Tuple2;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.Durations;
import org.apache.kafka.common.serialization.StringDeserializer;

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
        kafkaParams.put("bootstrap.servers",brokers);
        kafkaParams.put("key.deserializer", ByteArrayDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "ID-1");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String,String>Subscribe(topicsSet, kafkaParams));

        //Print Messages to console for now
        JavaDStream<String> lines = messages.map(ConsumerRecord::value);
        lines.print();

        ObjectMapper mapper = new ObjectMapper();

        //JSON from file to Object
        User user = mapper.readValue(new File("c:\\user.json"), User.class);

        //JSON from String to Object
        User user = mapper.readValue(jsonInString, User.class);

        // Convert RDD[String] to RDD[case class] to DataFrame
        JavaRDD<JavaRow> rowRDD = lines.map(word -> {
            JavaRow record = new JavaRow();
            record.getExchange();
            return record;
        });

        DataFrame wordsDataFrame = spark.createDataFrame(rowRDD, JavaRow.class);

        // Creates a temporary view using the DataFrame
        wordsDataFrame.createOrReplaceTempView("words");



//        // Get the lines, split them into words, count the words and print
//        JavaDStream<String> lines = messages.map(ConsumerRecord::value);
//        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
//        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
//                .reduceByKey((i1, i2) -> i1 + i2);
//        wordCounts.print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}