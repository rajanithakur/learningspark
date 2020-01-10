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
        /*
            OutputMode.Complete() ... all the results of the sql query should show up as output and written to table (not allowed without aggregation..
            order by in sql mode can happen complete mode only)
             Dataset<Row> results = session.sql("select cast (value as string) as course_name, sum(5) as sec_watched from viewing_figures group by course_name ordery by sec_watched desc");

             OutputMode.update().... update any rows of the table that might have changed since the last batch ... see same output in evernote
             ( order by above sql cannot happen with update ....error: sorting is not supported unless agreggated by output.complete mode())
             OutputMode.append().... only output any new rows to output table
         */

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

        /* DEMO WINDOWING **********************

        //Timestamp  below is event generation timestamp and not the timestamp when the records  were processed
        Dataset<Row> results = session.sql("select window,  cast (value as string) as course_name, sum(5) as sec_watched from viewing_figures group by window(timestamp, '2 minutes'), course_name");
        StreamingQuery console = results.writeStream()
        .format("console")
        .outputMode(OutputMode.Update()) // when sql was without chunk of 5 sec, OutputMode.Append(
                .option("truncate", false) // so that 'window' column is not truncated
                .option("numRows", 50) // so that I see more rows  in output from batch processed, from default output row size of 20
                .start();

                 OUTPUT
                -------------------------------------------
Batch: 7
-------------------------------------------
+------------------------------------------+---------------------------------------------+-----------+
|window                                    |course_name                                  |sec_watched|
+------------------------------------------+---------------------------------------------+-----------+
|[2020-01-10 09:20:00, 2020-01-10 09:22:00]|Hadoop for Java Developers                   |15         |
|[2020-01-10 09:18:00, 2020-01-10 09:20:00]|Test Driven Development                      |230        |
|[2020-01-10 09:20:00, 2020-01-10 09:22:00]|Test Driven Development                      |10         |
|[2020-01-10 09:18:00, 2020-01-10 09:20:00]|Hadoop for Java Developers                   |760        |
|[2020-01-10 09:18:00, 2020-01-10 09:20:00]|Spring Framework Fundamentals                |900        |
|[2020-01-10 09:18:00, 2020-01-10 09:20:00]|Groovy Programming                           |530        |
|[2020-01-10 09:20:00, 2020-01-10 09:22:00]|Spring Framework Fundamentals                |20         |
|[2020-01-10 09:20:00, 2020-01-10 09:22:00]|Java Web Development Second Edition: Module 2|5          |
|[2020-01-10 09:18:00, 2020-01-10 09:20:00]|Java Web Development Second Edition: Module 1|1060       |
|[2020-01-10 09:18:00, 2020-01-10 09:20:00]|Hibernate and JPA                            |1315       |
|[2020-01-10 09:20:00, 2020-01-10 09:22:00]|Groovy Programming                           |5          |
|[2020-01-10 09:20:00, 2020-01-10 09:22:00]|Java Fundamentals                            |65         |
|[2020-01-10 09:18:00, 2020-01-10 09:20:00]|Java Fundamentals                            |1940       |
|[2020-01-10 09:20:00, 2020-01-10 09:22:00]|Hibernate and JPA                            |25         |
|[2020-01-10 09:18:00, 2020-01-10 09:20:00]|Java Web Development Second Edition: Module 2|120        |
|[2020-01-10 09:18:00, 2020-01-10 09:20:00]|Java Build Tools                             |60         |
|[2020-01-10 09:20:00, 2020-01-10 09:22:00]|Java Web Development Second Edition: Module 1|50         |

                'window' values above is the timestamp put on that event by kafka when it was received by kafka
                There can be 2 rows  of same course... just coz their windows are different
                Spark Structured Streaming is so powerful, if the report for rows is  generated based on event generation timestamp and  events arrived late
                due to delays on kafka... spark goes back and updates the record  event though report generation was complete for that window
                Handling LATE data and watermarking in Ache online doc... .withWaterMark
                */
        console.awaitTermination();
    }
}
