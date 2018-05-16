package com.palwell;

import java.util.*;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.Durations;
import org.apache.kafka.common.serialization.StringDeserializer;


public class StructCryptoStream {

    private static final String KAFKA_BROKERS = "pathdf3.field.hortonworks" +
            ".com:6667";
    private static String TOPICS = "cryptocurrency-nifi-data";
    private static String APP_NAME = "CryptoStream";
    private static int DURATION = 10;

    public static void main(String[] args) throws Exception {


        SparkSession spark = SparkSession
                .builder()
                .appName(APP_NAME)
                .master("local[2]")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to kafka broker
        Dataset<Row> messages = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BROKERS)
                .option("subscribe", TOPICS)
                .option("auto.offset.reset", "earliest")
                .load();

        Dataset<Row> df = messages.selectExpr("CAST (value AS STRING)");
        JavaRDD<Row> RDD = df.toJavaRDD();

        StructType schema = new StructType()
                .add("exchange","string")
                .add("cryptocurrency","string")
                .add("basecurrency","string")
                .add("type","string")
                .add("price","double")
                .add("size","double")
                .add("bid","double")
                .add("ask","double")
                .add("open","double")
                .add("high","double")
                .add("low","double")
                .add("volume","double")
                .add("timestamp","long");

        Dataset<Row> data_frame = spark.createDataFrame(RDD,schema);

         data_frame.writeStream()
                 .queryName("CryptoCurrency")
                .outputMode("complete")
                .format("memory")
                .start();

         spark.sql("Select * from CryptoCurrency WHERE basecurrency == 'USD'");
    }
}