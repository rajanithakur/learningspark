package com.virtualpairprogrammers.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

public class ViewingFiguresDStreamVersion {

    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("viewingFigures");

        //durations->batch is generated after every 1 sec... smaller the time you will be nearer to real-time processing
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(1));
        Collection<String> topics = Arrays.asList("viewrecords");

        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", "localhost:9092");

        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", StringDeserializer.class);

        /*
            FOLLOWING IS USE-FUL FOR LOAD BALANCING
            Group ID : used to group consumers , use-case : if we can say one consumer in that group consumed a message, we can say its consumed by all consumers in that group
         */
        props.put("group.id","spark-group");

        /*
            we have capability to commit the message programmatically over the network... more control but time-consuming, we pass false below
            but commit messages may not come through over the network... so you can tell consumer to automatically commit ( in 5 sec for example by passing 'true' below)
            but it can also mean that commit happened  but we were only able to partially process the batch, so once server restarts in this case... we may end up replaying
            same message more than once, we dont need it in course below
         */


        //props.put("enable.auto.commit",false);

        //if there is no commit made at all-either to start from beginning or latest
        props.put("auto.offset.reset","latest");


        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(sc, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, props));
        /*
            GOAL : Following will output course names processed in each second
               JavaDStream<String> map = directStream.map(item -> item.value());
               map.print();
         */

         /*
         *** GOAL *** : we want to maintain the view-count for each course
         5l= no. of seconds consumed for that video

        *** CODE ***
        directStream.mapToPair(item ->new Tuple2<>(item.value(), 5L))
                .reduceByKey((x,y) ->x+y)
                .mapToPair(item->item.swap()) //for sort by key I need to swap, sort by key is not available directly so I do transform to get underlying rdd
                .transformToPair(rdd->rdd.sortByKey(false)).print(50);

        *** sample output ****
                     -------------------------------------------
                    Time: 1578169816000 ms
                    -------------------------------------------
                    (30,Hibernate and JPA)
                    (20,Hadoop for Java Developers)
                    (15,NoSQL Databases)
                    (10,Java Web Development Second Edition: Module 1)
                    (10,Java Web Development Second Edition: Module 2)
                    (10,Spring Boot Microservices)
                    (10,Cloud Deployment with AWS)
                    (10,Java Advanced Topics)
                    (10,JavaEE and WildFly Module 1 : Getting Started)
                    (5,Groovy Programming)
                    (5,Spring Framework Fundamentals)
                    (5,Java Fundamentals)
                    (5,HTML5 and Responsive CSS for Developers)

                    -------------------------------------------
                    Time: 1578169817000 ms
                    -------------------------------------------
                    (25,Spring Framework Fundamentals)
                    (25,Java Advanced Topics)
                    (25,Hibernate and JPA)
                    (15,Java Web Development Second Edition: Module 1)
                    (15,Spring Boot Microservices)
                    (10,Java Web Development Second Edition: Module 2)
                    (5,Spring MVC and WebFlow)
                    (5,Groovy Programming)
                    (5,Java Fundamentals)
                    (5,Java Build Tools)
                    (5,Hadoop for Java Developers)
         */

          /*
         *** GOAL *** : we want to maintain the view-count for each course per window of 30 seconds lets say
         5l= no. of seconds consumed for that video

        *** CODE ***
        directStream.mapToPair(item ->new Tuple2<>(item.value(), 5L))
                .reduceByKeyAndWindow((x,y) ->x+y, Durations.seconds(30))
                .mapToPair(item->item.swap()) //for sort by key I need to swap, sort by key is not available directly so I do transform to get underlying rdd
                .transformToPair(rdd->rdd.sortByKey(false)).print(50);

        *** sample output ****
                     -------------------------------------------
                    Time: 1578169816000 ms
                    -------------------------------------------
                    (30,Hibernate and JPA)
                    (20,Hadoop for Java Developers)
                    (15,NoSQL Databases)
                    (10,Java Web Development Second Edition: Module 1)
                    (10,Java Web Development Second Edition: Module 2)
                    (10,Spring Boot Microservices)
                    (10,Cloud Deployment with AWS)
                    (10,Java Advanced Topics)
                    (10,JavaEE and WildFly Module 1 : Getting Started)
                    (5,Groovy Programming)
                    (5,Spring Framework Fundamentals)
                    (5,Java Fundamentals)
                    (5,HTML5 and Responsive CSS for Developers)

                    -------------------------------------------
                    Time: 1578169817000 ms
                    -------------------------------------------
                    Higher values than above , aggregation will continue until 30 secs
         */
         /*
            *** GOAL *** :
            * we want to maintain the view-count for each course per window of 60 seconds but we dont to output record to console for every
            * second(even though batch is processed every second)
            * we provide 3rd  parameter on reduceBykEY called slide window ( value we should provide should be integer multiple of batch size
            * this requires all our things in pom to 2.4.0 due to bug in 2.3.2
        *** CODE *** */

        directStream.mapToPair(item ->new Tuple2<>(item.value(), 5L))
                .reduceByKeyAndWindow((x,y) ->x+y, Durations.seconds(60), Durations.minutes(1))
                .mapToPair(item->item.swap()) //for sort by key I need to swap, sort by key is not available directly so I do transform to get underlying rdd
                .transformToPair(rdd->rdd.sortByKey(false)).print(50);
        /*
        *** sample output ****
                     -------------------------------------------
                    Time: 1578169816000 ms
                    -------------------------------------------
                    (30,Hibernate and JPA)
                    (20,Hadoop for Java Developers)
                    (15,NoSQL Databases)
                    (10,Java Web Development Second Edition: Module 1)
                    (10,Java Web Development Second Edition: Module 2)
                    (10,Spring Boot Microservices)
                    (10,Cloud Deployment with AWS)
                    (10,Java Advanced Topics)
                    (10,JavaEE and WildFly Module 1 : Getting Started)
                    (5,Groovy Programming)
                    (5,Spring Framework Fundamentals)
                    (5,Java Fundamentals)
                    (5,HTML5 and Responsive CSS for Developers)

                    -------------------------------------------
                    Time: 1578169817000 ms
                    -------------------------------------------
                    (25,Spring Framework Fundamentals)
                    (25,Java Advanced Topics)
                    (25,Hibernate and JPA)
                    (15,Java Web Development Second Edition: Module 1)
                    (15,Spring Boot Microservices)
                    (10,Java Web Development Second Edition: Module 2)
                    (5,Spring MVC and WebFlow)
                    (5,Groovy Programming)
                    (5,Java Fundamentals)
                    (5,Java Build Tools)
                    (5,Hadoop for Java Developers)
         */
        sc.start();
        sc.awaitTermination();
    }
}
