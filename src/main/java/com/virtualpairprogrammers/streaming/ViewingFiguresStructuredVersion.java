package com.virtualpairprogrammers.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class ViewingFiguresStructuredVersion {
    public static void main(String[] args) throws StreamingQueryException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder().master("local[*]").appName("structuredViewingReport").getOrCreate();

        //stream is going to be kafka-> reference is spark stremaing official website from Apache
        //option values below populated from https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
        // At this point following pom artifact was added spark-sql-kafka-0-10_2.11
        Dataset<Row> df = session.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "viewrecords").load();

        df.createOrReplaceTempView("viewing_figures");

        //Kafka stream will return us key , value and timestamp
        //kafka deserializer needed so cast is needed below
//        Dataset<Row> results = session.sql("select cast (value as string) as course_name from viewing_figures");

        //in addition to above, sum(5) group by course name in chunks of 5 seconds
        Dataset<Row> results = session.sql("select cast (value as string) as course_name, sum(5) from viewing_figures group by course_name");


        //Different values of sink! https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-sinks
        //start() equivalent to start() in dstream
        StreamingQuery console = results.writeStream()
                .format("console")
                .outputMode(OutputMode.Complete()) // when sql was without chunk of 5 sec, OutputMode.Append()
                .start();
        //equivalent to await termination previously

        /* output with OutputMode.Complete()
            -------------------------------------------
Batch: 1
-------------------------------------------
+--------------------+------+
|         course_name|sum(5)|
+--------------------+------+
|   Hibernate and JPA|   110|
|    Java Build Tools|    70|
|Java Web Developm...|   110|
|Spring Security M...|    20|
|           Thymeleaf|    15|
|         Spring Boot|   245|
|Spring MVC and We...|    45|
|      Securing a VPC|    20|
|Spring Framework ...|   305|
|Spring Remoting a...|    90|
|Java Web Developm...|    90|
|Java Advanced Topics|   150|
|              JavaEE|    85|
+--------------------+------+

-------------------------------------------
Batch: 2
-------------------------------------------
+--------------------+------+
|         course_name|sum(5)|
+--------------------+------+
|   Hibernate and JPA|   155|
|    Java Build Tools|   125|
|Java Web Developm...|   170|
|Spring Security M...|    30|
|           Thymeleaf|    70|
|         Spring Boot|   375|
|Spring MVC and We...|    65|
|      Securing a VPC|    30|
|Spring Framework ...|   515|
|Spring Remoting a...|   140|
|Java Web Developm...|   170|
|Java Advanced Topics|   240|
|              JavaEE|   160|
+--------------------+------+

IMPORTANT THING TO NOTE : IN STRUCTURED STREAMING THERE IS NO CONCEPT OF WINDOWS, IT TREATS AS IF THERE AN INFINITE TABLE AND DATA IS APPENDED TO IT AND AGGREGATION IS PERFORMED ON THE BATCHES
WITHOUT WORRYING ABOUT WINDOWS AS IN DSTREAMS, IF YOU SEE ABOVE OUTPUT DATA IS INCREASING/  AGGREGATED ACROSS BATCHES
         */
        console.awaitTermination();
    }
}
