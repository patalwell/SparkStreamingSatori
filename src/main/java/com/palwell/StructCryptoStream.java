package com.palwell;

import org.apache.spark.sql.*;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.javalang.typed;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;


public class StructCryptoStream {

    //Satori Payload
//    public class DeviceData {
//        private String device;
//        private String deviceType;
//        private Double signal;
//        private java.sql.Date time;
//        // Getter and setter methods for each field
//    }

    public static void main(String[] args) throws Exception {

        String broker = "pathdp3.field.hortonworks.com:6667";
        String topic = "cryptocurrency-nifi-data";


        SparkSession spark = SparkSession
                .builder()
                .appName("CryptoCurrencyStreaming")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to kafka broker
        Dataset<Row> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", broker)
                .option("subscribe", topic)
                .load();

        lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

        lines.printSchema();



//        // Split the lines into words
//        Dataset<String> words = lines
//                .as(Encoders.STRING())
//                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());
//
//        // Generate running word count
//        Dataset<Row> wordCounts = words.groupBy("value").count();


    }
}