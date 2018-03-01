package com.palwell;

import jdk.nashorn.internal.runtime.regexp.joni.constants.StringType;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
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
                .master("local[2]")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to kafka broker
        // Streaming DataFrame from Satori API schema
        // { exchange: String, cryptocurrency: String, basecurrency: String, type: String, price: Double,
        // size: String, bid: Double, ask: Double, open: Double, high: Double, low: Double, volume: Double,
        // timestamp : Date }

        Dataset<Row> messages = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", broker)
                .option("subscribe", topic)
                .load();

        JavaRow schema = new JavaRow();

//         messages.select(col("key").cast("String"), from_json


//
//        messages.createOrReplaceTempView("CryptoCurrency");
//
//        spark.sql("Select count(*) from CryptoCurrency");





    }
}