package com.virtualpairprogrammers.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class LogStreamAnalysis {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("startingSpark");
        //durations->batch is generated after every 1 sec... smaller the time you will be nearer to real-time processing
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<String> inputDStream = sc.socketTextStream("localhost", 8989);
        //We can inputDStream as if we were working on batch RDDs
        //Following a single RDD
        JavaDStream<String> results = inputDStream.map(item -> item);

//        results.print();
        JavaPairDStream<String, Long> pairRDD = results.mapToPair(msg -> new Tuple2<>(msg.split(",")[0], 1L));
        pairRDD = pairRDD.reduceByKeyAndWindow((x, y)-> x+y, Durations.seconds(30));
        // we see only output for that particular batch  ie, data is not aggregated!
        pairRDD.print();

        //run spark streaming in infinite loop
        sc.start();
        //await until termination of JVM
        sc.awaitTermination();

        //If you make any changes to teh spark job we will stop and restart the LoggingServer, coz LoggingServer can only take one
        // connection and if the streaming job ie. this class stops LoggingServer has no way of knowing teh connection has been dropped
        //If you see any issues running the code, run on Java 1.8

    }
}
