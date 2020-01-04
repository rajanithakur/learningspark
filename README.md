# learningspark
Spark Streaming Kafka Messages

Source of streaming data can be Kafka
Kafka is DISTRIBUTED EVENT-LEDGER/LOG

By default Kafka messages are stored on disk not in memory 
Kafka vs RabitMQ/AMQ ( I think message broker = queue)
* Kafka is many-many publisher/subscriber model
* In RabitMQ/AMQ messages are deleted once consumed, but Kafka we have event-log / event-ledger (log of historic messages) for messages (makes fault-tolerant/resilient)



Use Kafka-Training notes
To start zookeeper , server and topic

Create topic
./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic viewrecords

List topics
./kafka-topics.sh --list --zookeeper localhost:2181

Start the Kafka producer project( run the main class)  ViewReportsSimulator in viewing -figures-generation project

Start the Kafka consumer on command line 

./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic viewrecords
You will stream of string flying in

CTRL+c on the consumer console
If you stop producer (ViewReportsSimulator.class)

Go to consumer console and do
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic viewrecords
You won’t see new messages produced

But if you do 
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic viewrecords --from-beginning

SEE HOW KAFKA IS RESILIENT!!!!
You will see list of messages processed till now

Stop kafka-server
Stop zookeeper
Start zookeeper
Start Kafka-server
Now do ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic viewrecords --from-beginning
We will still see messages consumed so far

Project on personal GitHub 
1. Streaming app on ‘learningspark'
2. Kafka producer on ‘viewing-figures-reporting'

To Run the Project

1)Start, Zookeeper, Kafka server and topic name with view records through command line
2)Kafka producer
3)Then start streaming app


POINTERS: Window size should be integer multiple of batch size

