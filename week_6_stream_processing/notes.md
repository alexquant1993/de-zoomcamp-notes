# Week 6: Stream processing

## What is stream processing?
It is the process of exchanging data between producers and consumers in real time, unlike batch processing where the data exchange can be delayed by hours or days. Real time, as of now, means that data is exchanged in a matter of seconds.

## What is Kafka?
Kafka is an intermediary between data producers and consumers managing topics, that can be used for a particular case/scenario. A topic is a continuous stream of events.

- A topic would be measuring the temperature of a room every 30 seconds.
- An event would be the temperature of the room at a given timestamp. The collection of events go into our topic which are read by our data consumers.
- Logs are the way an event (data) is actually stored into a topic.
- Each event will contain a message, in our example, the message will be the timestamp and the temperature of the room: key, value and timestamp.

**More on this:**

Kafka is a distributed streaming platform that is used to build real-time data pipelines and streaming applications. It was originally developed by LinkedIn and is now maintained as an open-source project by the Apache Software Foundation.

The core architecture of Kafka consists of several key components, including:

- Broker: The Kafka broker is a server that stores and manages messages in topics. Each broker is responsible for handling read and write requests for a subset of the topic partitions.

- Topic: A topic is a named stream of messages in Kafka. Topics are divided into partitions, which allow messages to be distributed across multiple brokers for scalability and fault tolerance.

- Partition: A partition is a subset of a topic that is stored on a single broker. Each partition is ordered and identified by a unique partition ID.

- Producer: A producer is a client that publishes messages to a Kafka topic. Producers can specify the topic and partition to which the message should be sent.

- Consumer: A consumer is a client that reads messages from a Kafka topic. Consumers subscribe to one or more topics and consume messages in parallel from one or more partitions.

- Consumer Group: A consumer group is a set of consumers that collectively consume messages from one or more topics. Each consumer in a group reads from a unique subset of the partitions in the topic, allowing for parallel processing of messages.

- Connectors: Kafka Connect is a framework for connecting Kafka to external systems such as databases, message queues, and Hadoop. Connectors allow data to be streamed into and out of Kafka without requiring custom code.

- Cluster: A Kafka cluster is a group of brokers that work together to manage the storage and distribution of messages. Kafka provides built-in support for replication and failover, so that messages can be stored redundantly across multiple brokers for high availability.

- Client: a client is a software library or tool that connects to a Kafka cluster to produce or consume messages. Kafka clients can be written in a variety of programming languages, including Java, Python, Ruby, and more.
    - Producers and consumers are the most common types of Kafka clients. Producers are responsible for publishing messages to Kafka topics, while consumers read messages from topics and process them as needed. Kafka also supports other types of clients, such as administrative clients for managing topics and configuring the Kafka cluster, and streaming clients for building real-time data pipelines.
    - Kafka clients interact with the Kafka cluster by communicating with Kafka brokers, which are responsible for storing and managing messages. Clients can connect to any broker in the cluster and can automatically discover the rest of the brokers through the use of ZooKeeper, a distributed coordination service that Kafka uses for cluster management.
    - Kafka clients can be designed to provide various levels of reliability, scalability, and performance depending on the requirements of the application. For example, a high-throughput producer might use batching and compression to send large volumes of data efficiently, while a consumer might use parallelism and checkpointing to process messages in a fault-tolerant manner.

## Why is Kafka special?

- Robustness and reliability: Kafka replicates data across different nodes to ensure that it will be available for users
- Flexibility: topics can be small/big, consumers can vary from one to many. Inter-integration of Kafka connect, KSQL DB. Flexibility on data storage (tier storage) to be used online/offline.
- Scalability: Kafka can handle scalation of events very quickly from 10-1000 events/second.

## Confluent Cloud

Confluent Platform is a full-scale data streaming platform that enables you to easily access, store, and manage data as continuous, real-time streams. Built by the original creators of Apache Kafka®, Confluent expands the benefits of Kafka with enterprise-grade features while removing the burden of Kafka management or monitoring. Today, over 80% of the Fortune 100 are powered by data streaming technology – and the majority of those leverage Confluent.

### Set up Confluent Cluster

- Create a Kafka free account and get 400$ in credits
- Create a basic cluster and use GCP and specify a given region
- Skip payment details given 400$ free credits
- Set up a cluster name and launch cluster
- Create an API key and choose global access. Download it.
- Create a topic
    - Choose a name and choose 2 partitions
    - Advance settings:
        - Cleaun policy: delete
        - Retention policy: 1 day
        - Save and create
- Produce a new message to the created topic
- Create a dummy connector: DataGen Source
    - Select the topic
    - Select global access
    - Output format: JSON and Template: Orders
    - Enter a name and create connector
- You now can see the metrics of the connector and in the topic messages tab, you will be able to check all the inbound messages.


## Setup Kafka Clients - Producer and Consumer

- Don't forget to create your API key and secret for your cluster
- Add those values to Secrets.java in the kafka_examples/src/java/org/example/Secrets.java
- Install the Java Extension pack and the Gradle Extension for VSCode

### Producer Client

```java
/* The code imports several Java and Kafka libraries that are used 
throughout the code, including the OpenCSV library for reading CSV files,
the Kafka StreamsConfig for configuring the Kafka cluster, the Kafka Producer
API for publishing messages, and the KafkaJsonSerializer for serializing
data in JSON format.*/
package org.example;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.streams.StreamsConfig;
import org.example.data.Ride;

import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class JsonProducer {
    private Properties props = new Properties();
    /*The JsonProducer class defines a constructor that initializes a set of
    Kafka properties, including the bootstrap servers, security settings,
    and serialization classes.*/
    public JsonProducer() {
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-4r297.europe-west1.gcp.confluent.cloud:9092");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='"+Secrets.KAFKA_CLUSTER_KEY+"' password='"+Secrets.KAFKA_CLUSTER_SECRET+"';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", "45000");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");
    }

    /*The getRides() method reads a CSV file that contains information about
    taxi rides (such as pickup and drop-off times, locations, and fare
    amounts) and converts each row of the CSV file into a Ride object.*/
    public List<Ride> getRides() throws IOException, CsvException {
        var ridesUrl = this.getClass().getResource("/rides.csv");
        var reader = new CSVReader(new InputStreamReader(ridesUrl.openStream()));
        reader.skip(1);
        return reader.readAll().stream().map(arr -> new Ride(arr))
                .collect(Collectors.toList());
    }

    /*The publishRides() method publishes a list of Ride objects to a Kafka
    topic named "rides". For each Ride object in the list, the method sets
    the pickup and drop-off times to the current time minus 20 minutes
    and the current time, respectively. It then creates a Kafka producer
    record with the Ride object as the value and the destination location
    ID as the key. Finally, the record is sent to the Kafka cluster, and 
    the method waits for a response from the broker before printing the 
    offset of the message.*/
    public void publishRides(List<Ride> rides) throws ExecutionException, InterruptedException {
        KafkaProducer<String, Ride> kafkaProducer = new KafkaProducer<String, Ride>(props);
        for(Ride ride: rides) {
            ride.tpep_pickup_datetime = LocalDateTime.now().minusMinutes(20);
            ride.tpep_dropoff_datetime = LocalDateTime.now();
            var record = kafkaProducer.send(new ProducerRecord<>("rides", String.valueOf(ride.DOLocationID), ride), (metadata, exception) -> {
                if(exception != null) {
                    System.out.println(exception.getMessage());
                }
            });
            System.out.println(record.get().offset());
            System.out.println(ride.DOLocationID);
            Thread.sleep(500);
        }
    }

    /*The main() method creates a new JsonProducer object, reads the rides
    from the CSV file using the getRides() method, and publishes the rides
    to the "rides" Kafka topic using the publishRides() method.*/
    public static void main(String[] args) throws IOException, CsvException, ExecutionException, InterruptedException {
        var producer = new JsonProducer();
        var rides = producer.getRides();
        producer.publishRides(rides);
    }
}
```


## Consumer Client

```java
/*This Java code defines a Kafka consumer that reads messages in JSON format
from a Kafka topic and prints the destination location ID of each ride */

/*The code imports several Java and Kafka libraries that are used throughout
the code, including the Kafka Consumer API for consuming messages,
the KafkaJsonDeserializer for deserializing data in JSON format, and the
Ride class, which is the data model for the messages. */
package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.example.data.Ride;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.Properties;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
public class JsonConsumer {

    private Properties props = new Properties();
    private KafkaConsumer<String, Ride> consumer;
    /*The JsonConsumer class defines a constructor that initializes a set of
    Kafka properties, including the bootstrap servers, security settings,
    and deserialization classes. It also creates a new KafkaConsumer
    instance and subscribes it to the "rides" topic.*/
    public JsonConsumer() {
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-4r297.europe-west1.gcp.confluent.cloud:9092");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='"+Secrets.KAFKA_CLUSTER_KEY+"' password='"+Secrets.KAFKA_CLUSTER_SECRET+"';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", "45000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka_tutorial_example.jsonconsumer.v2");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, Ride.class);
        consumer = new KafkaConsumer<String, Ride>(props);
        consumer.subscribe(List.of("rides"));
    }

    /*The consumeFromKafka() method polls for messages from the Kafka topic
    and processes them. It uses a do-while loop to poll for messages every
    second and prints the destination location ID of each ride message.
    It continues to poll for messages until there are no more messages or
    10 seconds have passed.*/
    public void consumeFromKafka() {
        System.out.println("Consuming form kafka started");
        var results = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
        var i = 0;
        do {

            for(ConsumerRecord<String, Ride> result: results) {
                System.out.println(result.value().DOLocationID);
            }
            results =  consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
            System.out.println("RESULTS:::" + results.count());
            i++;
        }
        while(!results.isEmpty() || i < 10);
    }

    /*The main() method creates a new JsonConsumer object and calls the
    consumeFromKafka() method to start consuming messages from the Kafka topic. */
    public static void main(String[] args) {
        JsonConsumer jsonConsumer = new JsonConsumer();
        jsonConsumer.consumeFromKafka();
    }
}
```

## Kafka configuration

### What is a Kafka Cluster

- A Kafka cluster is a distributed system that consists of multiple Kafka brokers responsible for storing and managing streams of data. Each broker in the cluster contains a subset of the data streams, ensuring that data is distributed and replicated for fault tolerance.
- Kafka clusters provide a scalable and fault-tolerant infrastructure for handling large amounts of data in real-time, making them ideal for processing real-time data streams and building event-driven applications.
- Kafka clients can interact with Kafka brokers in the cluster over the network using the Kafka protocol, which is a binary protocol optimized for high-throughput data transfer.

### How brokers communicate with each other?

- Brokers communicate with each other using the Kafka replication protocol, which is a binary protocol that is optimized for high-throughput data transfer.
- The replication protocol uses a leader-follower model, where one broker is designated as the leader for each partition, and the other brokers are designated as followers. The leader is responsible for handling all read and write requests for a partition, while the followers replicate the data stored on the leader.
- When a new broker is added to the cluster or an existing broker goes offline, the Kafka controller, which is responsible for managing the Kafka cluster metadata, assigns new leaders for the affected partitions and notifies the brokers of the changes.

#### __consumer_offsets topic

- Communication between brokers for replication purposes is performed using an internal topic called the "__consumer_offsets" topic.
- The "__consumer_offsets" topic stores the offsets for each partition of each consumer group in the Kafka cluster. An offset is a unique identifier for a message within a partition, and it allows consumers to track their progress through the stream of messages in the topic.
- Each broker in the Kafka cluster maintains a local copy of the "__consumer_offsets" topic, and the topic is replicated across the brokers using the Kafka replication protocol. This ensures that the offsets for each partition are replicated and available to all brokers in the cluster.
- When a consumer group reads messages from a Kafka topic, it stores the offset of the last consumed message in the "__consumer_offsets" topic. This allows the consumer group to resume reading from where it left off if it is restarted or if a new consumer joins the group.
- Typical structure: <consumer.group.id, topic, partition, offset>


### Topic

- A topic is a named feed or category to which messages are published. It represents a stream of data that is organized into partitions, which are an ordered, immutable sequence of messages.
- Kafka topics are highly scalable and fault-tolerant, and they are partitioned and replicated across multiple brokers in the cluster to ensure that data is distributed and available for consumption even if some brokers fail.
- Producers send messages to topics, specifying the topic name and the partition to which the message should be written, while consumers read messages from one or more partitions of a topic by subscribing to the topic and specifying the partition(s) to read from.
- A message is a unit of data that is published to a topic by a producer and stored in a partition. It is an immutable record consisting of a key, a value, and a timestamp. Messages in Kafka are identified by a unique offset within a partition, which allows consumers to track their progress through the stream of messages. Messages can be of any type or format, and they can be replicated and distributed across multiple brokers in the cluster for fault tolerance and high availability.

### Retention

- Data retention in Kafka refers to the mechanism that controls the amount of time that data is retained in a topic. Kafka topics can be configured with a retention period, which is the amount of time that data in the topic is retained before it is automatically deleted.
- Kafka uses a garbage collection mechanism called the log compaction process to delete messages that are no longer needed based on the retention period or the size limit. Log compaction ensures that the latest state of the data is always available for consumers by retaining the most recent message for each key in a topic. 
- Data retention ensures that the system can manage disk space usage, maintain data integrity, and handle large amounts of data in a scalable and fault-tolerant way.

### Partition

- A partition is an ordered and immutable sequence of messages in a topic. A topic can be divided into multiple partitions, and each partition can be stored on one or more Kafka brokers in the cluster. Messages in a partition are identified by a unique offset, which allows consumers to track their progress through the stream of messages.
- Kafka uses partitions to achieve scalability, fault-tolerance, and parallel processing of data. 
- By partitioning a topic, Kafka ensures that data is distributed across the cluster and that each partition can be processed independently and in parallel.
- Kafka partitions enable load balancing and parallel processing of data across multiple consumers in a consumer group. Each consumer in the group is assigned to one or more partitions of the topic, and Kafka ensures that each partition is assigned to only one consumer. Consumers can read messages from their assigned partitions in parallel, and Kafka provides a mechanism for rebalancing partitions among the consumers in the group in case of failures or changes in the group membership.

### Auto offset reset
`auto.offset.reset` is a configuration parameter that specifies what to do when a consumer group starts reading a partition for the first time or when the consumer's current offset for a partition is no longer available.

- auto.offset.reset can be set to "earliest" or "latest"
- When set to "earliest", the consumer will start reading messages from the earliest available offset in the partition
- When set to "latest", the consumer will start reading messages from the latest available offset in the partition
- The default value for auto.offset.reset is "latest"
- If auto.offset.reset is set to "earliest" and there are no available offsets in the partition, the consumer will start reading from the beginning of the partition
- auto.offset.reset only affects the initial offset of a consumer group or a partition that has lost its offset. Once a consumer has read messages from a partition, it will keep track of its offset and continue from where it left off the next time it reads from the partition.

### Acknowledgment all
"acknowledgment all" (also known as "acks=all") is a producer configuration parameter that controls the level of acknowledgement required from the Kafka broker after a message is sent.

- When "acknowledgment all" is set, the producer waits for acknowledgement from all in-sync replicas of the topic partition that the message was written to
- In-sync replicas are replicas that have fully caught up with the leader partition and are in sync with the latest messages
- If acknowledgement is not received from all in-sync replicas within a specified timeout period, the producer will retry sending the message to the broker
- Once acknowledgement is received from all in-sync replicas, the producer considers the message as successfully sent
- Setting "acknowledgment all" ensures that messages are not lost even in the event of a broker failure
- However, setting "acknowledgment all" may impact the performance of the producer, as it requires waiting for acknowledgement from multiple replicas before sending the next message
- `acks = 0`: Fire and forget. It doesn't care whether the message was actually delivered to the leader node or not.
- `acks = 1`: Leader succesfull. Wait for a message back from the leader node that the message was succsesfully delivered/written to the log.
- `acks = all`: Leader and followers. Success message back to the consumer just if it was written successfully to leader + followers.

## Kafka Streams Basics

Streaming refers to the process of continuously processing real-time data as it arrives in a Kafka topic. This process involves the following steps:

1. Data is produced by a producer and written to a Kafka topic.
2. Kafka Streams applications consume data from one or more Kafka topics and perform processing operations on it using the stream builder API.
3. The processed data is written to one or more output Kafka topics.
4. Consumers read data from the output Kafka topics and perform further processing or analysis.
5. The entire process repeats continuously as new data arrives in the input Kafka topics.

Example to create a Kafka streams builder:

- Create a custom serdes class to create custom serializers and deserializers for use with Kafka.
```java
/*This Java code defines a CustomSerdes class to create custom serializers
and deserializers for use with Apache Kafka. It provides methods to generate
JSON and Avro Serdes for the specified classes. The getSerde method creates
a JSON Serde, while the getAvroSerde method creates an Avro Serde.*/
package org.example.customserdes;

// Import required classes and interfaces for serialization and deserialization
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

// Import java utils
import java.util.HashMap;
import java.util.Map;

public class CustomSerdes {

    // Create a custom Serde for the specified class using KafkaJsonSerializer and KafkaJsonDeserializer
    public static <T> Serde<T> getSerde(Class<T> classOf) {
        // Prepare the properties for the serde
        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("json.value.type", classOf);
        
        // Create and configure the JSON serializer
        final Serializer<T> mySerializer = new KafkaJsonSerializer<>();
        mySerializer.configure(serdeProps, false);

        // Create and configure the JSON deserializer
        final Deserializer<T> myDeserializer = new KafkaJsonDeserializer<>();
        myDeserializer.configure(serdeProps, false);
        
        // Return a serde created from the serializer and deserializer
        return Serdes.serdeFrom(mySerializer, myDeserializer);
    }

    // Create a custom Avro Serde for the specified class using SpecificAvroSerde
    public static <T extends SpecificRecordBase> SpecificAvroSerde getAvroSerde(boolean isKey, String schemaRegistryUrl) {
        // Create a new SpecificAvroSerde instance
        var serde = new SpecificAvroSerde<T>();

        // Prepare the properties for the serde
        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        
        // Configure the Avro serde
        serde.configure(serdeProps, isKey);
        
        // Return the configured Avro serde
        return serde;
    }
}
```
- Create an output Kafka topic
- Create a Stream Kafka script that does the following:
    - Producer client is writing messages to the rides topic. When working with partitions, the producer writes data to the target topic using hash(key) to define which partition each data value goes to. In this way, it is ensured that each partition will have all data corresponding to a given key, ensuring that the groupby operations work properly.
    - Stream processing: group the rides by key (PU Location) and count it
    - Place the results it in a new topic: `rides-pulocation-count`
```java
/*The code defines a Kafka Streams application that reads ride data from a Kafka topic,
counts the number of rides by pickup location, and writes the results to another Kafka topic.
It uses custom serdes to serialize and deserialize Ride objects and runs in a Kafka cluster
hosted on Confluent Cloud.
*/

/*A serde (short for serializer/deserializer) is a component in Kafka that serializes data
from Java objects to byte arrays for efficient storage and transportation within Kafka,
and then deserializes it back into Java objects when it's retrieved. Kafka supports several
built-in serdes for common data types like strings and integers, and allows developers to
create their own custom serdes for complex objects.*/

// The package statement defines a Java package for the code
package org.example;

// The code imports several Kafka and Java libraries that are used throughout the code
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.example.customserdes.CustomSerdes;
import org.example.data.Ride;
import java.util.Properties;

// The code defines a Java class called "JsonKStream"
public class JsonKStream {
    
    // The "props" variable holds Kafka properties used throughout the class
    private Properties props = new Properties();
    
    // The constructor initializes the Kafka properties in the "props" variable
    public JsonKStream() {
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-4r297.europe-west1.gcp.confluent.cloud:9092");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='"+Secrets.KAFKA_CLUSTER_KEY+"' password='"+Secrets.KAFKA_CLUSTER_SECRET+"';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", "45000");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka_tutorial.kstream.count.plocation.v1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        /*Determines the maximum amount of memory (in bytes) that Kafka Streams can use to buffer 
        records in memory before writing them to a downstream Kafka topic*/
        // Value = 0: Disable buffering entirely
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    }
    
    public Topology createTopology() {
        // Create a new StreamsBuilder
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        
        // Define a stream that reads data from the "rides" topic, using the String serde for keys
        // and the CustomSerdes serde for Ride objects
        var ridesStream = streamsBuilder.stream("rides", Consumed.with(Serdes.String(), CustomSerdes.getSerde(Ride.class)));
        
        // Group the stream by pickup location (PUlocationID), count the number of rides per pickup location,
        // and store the result in a KTable
        var puLocationCount = ridesStream.groupByKey().count().toStream();
        
        // Write the KTable to the "rides-pulocation-count" topic, using the String serde for keys and the
        // Long serde for counts
        puLocationCount.to("rides-pulocation-count", Produced.with(Serdes.String(), Serdes.Long()));
        
        // Build and return the topology
        return streamsBuilder.build();
    }
    
    public void countPLocation() throws InterruptedException {
        // Create the topology
        var topology = createTopology();
        
        // Create a new KafkaStreams instance with the topology and properties
        var kStreams = new KafkaStreams(topology, props);
        
        // Start the KafkaStreams instance
        kStreams.start();
        
        // Wait for the instance to start running
        while (kStreams.state() != KafkaStreams.State.RUNNING) {
            System.out.println(kStreams.state());
            Thread.sleep(1000);
        }
        
        // Print the final state of the KafkaStreams instance
        System.out.println(kStreams.state());
        
        // Add a shutdown hook to close the KafkaStreams instance when the program exits
        Runtime.getRuntime().addShutdownHook(new Thread(kStreams::close));
    }
    
    public static void main(String[] args) throws InterruptedException {
        // Create a new JsonKStream object
        var object = new JsonKStream();
        
        // Call the countPLocation method to start processing the rides stream
        object.countPLocation();
    }
}
```

### Kafka Stream Joins

- Kafka stream joins are used to combine two or more streams of data based on a common key. When a join is performed, the Kafka Streams library will look for records in each input stream that have the same key value, and then combine those records into a single output record.
- In a Kafka stream join, the input streams are usually represented as KStreams, and the join operation is performed using the join() method. The join() method takes two KStreams as input, as well as a ValueJoiner function that defines how the records should be combined.
- The ValueJoiner function takes two input records (one from each stream) and produces a single output record. The output record can be a new object that combines data from both input records, or it can be one of the input records with additional data added to it.
- Kafka stream joins can be performed in different ways, depending on the type of join operation that is needed. The most common types of joins are inner joins, left joins, and outer joins. Inner joins only produce output records for key values that exist in both input streams, while left joins produce output records for all key values in the left input stream (even if there is no matching key value in the right input stream). Outer joins produce output records for all key values in both input streams, even if there is no matching key value in one of the streams.

Example setup:
- Create two additional topics: another input topic to perform the join `rides-location` and an output topic `vendor-info`.
- Execute the stream join script, once it is running, execute both producers that write in the topics to be joined: `rides` and `rides-location`.
- Remember that both topics to be joined have to have the same number of partitions, otherwise it won't work.

```java
// Importing required Kafka and custom classes
package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.example.customserdes.CustomSerdes;
import org.example.data.PickupLocation;
import org.example.data.Ride;
import org.example.data.VendorInfo;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
public class JsonKStreamJoins {
    // Creating a Properties object to hold configuration properties for the Kafka Streams application
    private Properties props = new Properties();
    
    // Constructor to set the properties for the Kafka Streams application
    public JsonKStreamJoins() {
        // Setting the Kafka cluster bootstrap server
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-4r297.europe-west1.gcp.confluent.cloud:9092");
        // Configuring the security protocol and credentials for the Kafka cluster
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='"+Secrets.KAFKA_CLUSTER_KEY+"' password='"+Secrets.KAFKA_CLUSTER_SECRET+"';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        // Configuring the Kafka Streams application ID
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka_tutorial.kstream.joined.rides.pickuplocation.v1");
        // Configuring the auto offset reset behavior
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // Disabling caching to ensure that all data is processed in real-time
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    }

    // Method to create the topology of the Kafka Streams application
    public Topology createTopology() {
        // Creating a StreamsBuilder object to define the processing topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        
        // Creating KStreams from the input topics, specifying their key and value Serdes
        KStream<String, Ride> rides = 
            streamsBuilder.stream(Topics.INPUT_RIDE_TOPIC, Consumed.with(Serdes.String(), CustomSerdes.getSerde(Ride.class)));
        KStream<String, PickupLocation> pickupLocations = 
        streamsBuilder.stream(Topics.INPUT_RIDE_LOCATION_TOPIC, Consumed.with(Serdes.String(), CustomSerdes.getSerde(PickupLocation.class)));

        // Selecting the key for the pickupLocations KStream, and creating a new KStream with the same key
        var pickupLocationsKeyedOnPUId = pickupLocations.selectKey((key, value) -> String.valueOf(value.PULocationID));

        // Joining the rides and pickupLocations KStreams based on their keys, specifying a ValueJoiner to define the join operation
        var joined = rides.join(pickupLocationsKeyedOnPUId, (ValueJoiner<Ride, PickupLocation, Optional<VendorInfo>>) (ride, pickupLocation) -> {
                    var period = Duration.between(ride.tpep_dropoff_datetime, pickupLocation.tpep_pickup_datetime);
                    if (period.abs().toMinutes() > 10) return Optional.empty();
                    else return Optional.of(new VendorInfo(ride.VendorID, pickupLocation.PULocationID, pickupLocation.tpep_pickup_datetime, ride.tpep_dropoff_datetime));
                }, JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(20), Duration.ofMinutes(5)),
                StreamJoined.with(Serdes.String(), CustomSerdes.getSerde(Ride.class), CustomSerdes.getSerde(PickupLocation.class)));

        // Filtering out empty values from the joined KStream, mapping the non-empty values to the VendorInfo object, and writing the results to the output topic
        joined.filter(((key, value) -> value.isPresent())).mapValues(Optional::get)
                .to(Topics.OUTPUT_TOPIC, Produced.with(Serdes.String(), CustomSerdes.getSerde(VendorInfo.class)));

        // Building and returning the topology
        return streamsBuilder.build();
    }

    // Method to start the Kafka Streams application
    public void joinRidesPickupLocation() throws InterruptedException {
        // Creating the Kafka Streams topology and client properties
        var topology = createTopology();
        var kStreams = new KafkaStreams(topology, props);

        // Setting an uncaught exception handler to handle any exceptions that occur during processing
        kStreams.setUncaughtExceptionHandler(exception -> {
            System.out.println(exception.getMessage());
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });

        // Starting the Kafka Streams client
        kStreams.start();

        // Waiting for the Kafka Streams client to enter the RUNNING state
        while (kStreams.state() != KafkaStreams.State.RUNNING) {
            System.out.println(kStreams.state());
            Thread.sleep(1000);
        }

        // Printing the Kafka Streams client state to the console
        System.out.println(kStreams.state());

        // Adding a shutdown hook to gracefully stop the Kafka Streams client when the application is terminated
        Runtime.getRuntime().addShutdownHook(new Thread(kStreams::close));
    }

    // Main method to start the Kafka Streams application
    public static void main(String[] args) throws InterruptedException {
        var object = new JsonKStreamJoins();
        object.joinRidesPickupLocation();
    }
}
```

### Kafka Streams Testing

#### Kafka Streams Topology

In Kafka, a topology is a blueprint of how data is processed in a Kafka Streams application. It defines how data is read from input sources (such as Kafka topics), processed using different steps (such as filters, aggregations, and joins), and written to output sinks (such as Kafka topics or external systems). The topology is defined using the StreamsBuilder class and executed using the KafkaStreams object. Topologies can be customized and optimized for specific use cases, and provide built-in support for fault tolerance and scalability.

#### Testing in Kafka

Testing in Kafka involves verifying that the processing logic implemented in Kafka Streams applications is correct. Kafka provides a TestUtils library and the TopologyTestDriver class that allow developers to write unit tests for Kafka Streams applications. The TestInputTopic and TestOutputTopic classes can be used to simulate input and output Kafka topics respectively. By using these tools, developers can write tests that check the behavior of their Kafka Streams application in a controlled and isolated environment. They can also verify that the expected results are produced in the output topics for various inputs. Overall, testing Kafka Streams applications involves setting up a topology test driver with input and output topics, running the test cases, and asserting that the output topics contain the expected records.

```java
// Import required Libraries
package org.example;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.example.customserdes.CustomSerdes;
import org.example.data.Ride;
import org.example.helper.DataGeneratorHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.util.Properties;

// Class to test a Kafka Stream Json KStream
class JsonKStreamTest {
    // Define class variables
    private Properties props;
    private static TopologyTestDriver testDriver;
    private TestInputTopic<String, Ride> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;
    private Topology topology = new JsonKStream().createTopology();
    
    // Setup method to initialize test variables and create topology test driver
    @BeforeEach
    public void setup() {
        // Set Properties for stream
        props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "testing_count_application");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        // Close existing stream driver, if any
        if (testDriver != null) {
            testDriver.close();
        }
        // Get stream driver
        testDriver = new TopologyTestDriver(topology, props);
        // Create Input Topic 
        inputTopic = testDriver.createInputTopic("rides", Serdes.String().serializer(), CustomSerdes.getSerde(Ride.class).serializer());
        // Create Output Topic 
        outputTopic = testDriver.createOutputTopic("rides-pulocation-count", Serdes.String().deserializer(), Serdes.Long().deserializer());
    }

    // Test 1: For single message, count should be 1
    @Test
    public void testIfOneMessageIsPassedToInputTopicWeGetCountOfOne() {
        // Generate a test ride object and pass it to the input topic
        Ride ride = DataGeneratorHelper.generateRide();
        inputTopic.pipeInput(String.valueOf(ride.DOLocationID), ride);

        // Check that the output topic contains one record with the correct key and value
        assertEquals(outputTopic.readKeyValue(), KeyValue.pair(String.valueOf(ride.DOLocationID), 1L));
        assertTrue(outputTopic.isEmpty());
    }

    // Test 2: For two messages with different keys, counts must be 1 for each message
    @Test
    public void testIfTwoMessageArePassedWithDifferentKey() {
        // Generate two test ride objects with different keys and pass them to the input topic
        Ride ride1 = DataGeneratorHelper.generateRide();
        ride1.DOLocationID = 100L;
        inputTopic.pipeInput(String.valueOf(ride1.DOLocationID), ride1);

        Ride ride2 = DataGeneratorHelper.generateRide();
        ride2.DOLocationID = 200L;
        inputTopic.pipeInput(String.valueOf(ride2.DOLocationID), ride2);

        // Check that the output topic contains two records with the correct keys and values
        assertEquals(outputTopic.readKeyValue(), KeyValue.pair(String.valueOf(ride1.DOLocationID), 1L));
        assertEquals(outputTopic.readKeyValue(), KeyValue.pair(String.valueOf(ride2.DOLocationID), 1L));
        assertTrue(outputTopic.isEmpty());
    }

    // Test 3: For two messages with same key, counts must be 1 for first and 2 for second message
    @Test
    public void testIfTwoMessageArePassedWithSameKey() {
        // Generate two test ride objects with the same key and pass them to the input topic
        Ride ride1 = DataGeneratorHelper.generateRide();
        ride1.DOLocationID = 100L;
        inputTopic.pipeInput(String.valueOf(ride1.DOLocationID), ride1);

        Ride ride2 = DataGeneratorHelper.generateRide();
        ride2.DOLocationID = 100L;
        inputTopic.pipeInput(String.valueOf(ride2.DOLocationID), ride2);

        // Check that the output topic contains two records with the same key and the correct values
        assertEquals(outputTopic.readKeyValue(), KeyValue.pair("100", 1L));
        assertEquals(outputTopic.readKeyValue(), KeyValue.pair("100", 2L));
        assertTrue(outputTopic.isEmpty());
    }

    // Close testDriver after all tests are executed
    @AfterAll
    public static void tearDown() {
        testDriver.close();
    }
}
```

### Other topics for Kafka Streaming

#### KTable
A KTable is an abstraction of a changelog stream that represents the latest state of a database table. A KTable is an immutable data structure that is continually updated as new data is added to the source Kafka topic. It can be thought of as a distributed, continuously updated, read-only table that can be queried using the Kafka Streams API. KTables are materialized views of a Kafka topic, where each record in the topic is treated as an update to the table. Each record in a KTable consists of a key-value pair, where the key is used to identify a specific row in the table, and the value is the current state of that row. KTables can be used to perform joins, aggregations, and filtering operations on streams of data. KTables are often used in combination with KStreams to process streaming data and to maintain the latest state of a database table in real-time.

#### Global KTable
A GlobalKTable is like a KTable but is available to all nodes in a Kafka cluster. It provides a global view of a table that can be accessed and updated from any node. It's useful for scenarios where a global view of data is needed, like reference or configuration data. It's updated through a special input topic and updates are automatically propagated to all nodes. The use of a GlobalKTable simplifies processing and can improve performance by reducing network traffic.

There are a few caveats to keep in mind when using GlobalKTables in Kafka Streams:

- Data size: Since GlobalKTables are available to all nodes in a Kafka cluster, they can potentially become quite large. It's important to consider the size of the data when using GlobalKTables to ensure that they can be efficiently processed and updated.
- Latency: Updates to a GlobalKTable are propagated to all nodes in the Kafka cluster, which can result in some latency. Depending on the size of the cluster and the frequency of updates, this latency can become significant.
- Fault tolerance: GlobalKTables are not fault-tolerant in the same way that Kafka topics are. If a node hosting a GlobalKTable fails, the table must be rebuilt from scratch on another node. This can result in some downtime and data inconsistencies.
- Serialization and deserialization: When using GlobalKTables, it's important to ensure that the data is properly serialized and deserialized to avoid errors or inconsistencies in the data.
- Memory usage: Since GlobalKTables are materialized across all nodes in a Kafka cluster, they can potentially consume a lot of memory. It's important to monitor memory usage and ensure that the cluster has enough resources to handle the size of the GlobalKTable.

#### Joins
Kafka Streams provides several types of joins for combining data from different streams or tables. These include:

- Inner Join: Combines two streams or tables based on matching keys in both streams. Only the records with matching keys in both streams are retained.
- Left Join: Combines two streams or tables based on matching keys in both streams. All records from the left stream are retained, and matching records from the right stream are included. If there are no matching records in the right stream, null values are used for the missing fields.
- Outer Join: Combines two streams or tables based on matching keys in both streams. All records from both streams are retained, and null values are used for any missing fields.
- GlobalKTable-Stream Join: Allows joining a stream with a GlobalKTable based on matching keys. The GlobalKTable is available to all nodes in the Kafka cluster and provides a global view of a table that can be accessed and updated from any node.
- GlobalKTable-GlobalKTable Join: Allows joining two GlobalKTables based on matching keys. This type of join is useful for scenarios where a global view of data is required, like reference or configuration data.
- Windowed Join: Allows joining two streams or tables based on matching keys and a specified window of time. Records from both streams that fall within the specified window are joined.
    - Tumbling: fixed size non overlapping
    - Hopping: fixed size and overlapping
    - Sliding: Fixed size, overlapping windows that work on differences between record timestamps
    - Session: Dynamically-sized, non-overlapping, data-driven windows.

Setup for the Stream Windowing example:
- Run the Kafka Producer
- Create the windowing stream process. Make sure that APPLICATION_ID_CONFIG is different from any previous one you might have used. I was having an issue because I was re-using the ID for a previous process. I changed it to v2 and it worked smoothly.
```java
package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.example.customserdes.CustomSerdes;
import org.example.data.Ride;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

public class JsonKStreamWindow {

    // Define Kafka configuration properties
    private Properties props = new Properties();

    public JsonKStreamWindow() {
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-4r297.europe-west1.gcp.confluent.cloud:9092");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='"+Secrets.KAFKA_CLUSTER_KEY+"' password='"+Secrets.KAFKA_CLUSTER_SECRET+"';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", "45000");
        // I updated the ID to make it work
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka_tutorial.kstream.count.plocation.v2");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    }

    public Topology createTopology() {

        // Create a new streams builder
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Define a Kafka stream for rides
        var ridesStream = streamsBuilder.stream("rides", Consumed.with(Serdes.String(), CustomSerdes.getSerde(Ride.class)));

        // Count the number of rides by pickup location within a time window of 10 seconds with a grace period of 5 seconds
        var puLocationCount = ridesStream.groupByKey()
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .count().toStream();

        // Define the window serde for the windowed output
        var windowSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, 10*1000);

        // Send the windowed output to the "rides-pulocation-window-count" topic
        puLocationCount.to("rides-pulocation-window-count", Produced.with(windowSerde, Serdes.Long()));

        // Return the built topology
        return streamsBuilder.build();
    }

    // Start the Kafka Streams application to count pickup locations within a time window
    public void countPLocationWindowed() throws InterruptedException {
        var topology = createTopology();
        var kStreams = new KafkaStreams(topology, props);
        
        // Add logging statements to track the application's state
        kStreams.setStateListener((newState, oldState) -> {
            System.out.println("Kafka Streams state changed from " + oldState + " to " + newState);
        });

        kStreams.start();

        // Wait for the instance to start running
        while (kStreams.state() != KafkaStreams.State.RUNNING) {
            System.out.println(kStreams.state());
            Thread.sleep(1000);
        }
        
        // Print the final state of the KafkaStreams instance
        System.out.println(kStreams.state());
        
        // Add a shutdown hook to close the KafkaStreams instance when the program exits
        Runtime.getRuntime().addShutdownHook(new Thread(kStreams::close));        
    }

    // Main method to run the Kafka Streams application
    public static void main(String[] args) throws InterruptedException {
        var object = new JsonKStreamWindow();
        object.countPLocationWindowed();
    }
}
```

## ksqlDB
ksqlDB is a stream processing platform built on top of Apache Kafka that enables you to process streaming data using SQL-like syntax. With ksqlDB, you can write SQL queries to filter, transform, aggregate, and join streaming data in real-time, and also create materialized views that can be queried like a regular table. It simplifies the development of real-time stream processing applications by providing a familiar SQL interface for data manipulation and a scalable, fault-tolerant architecture for processing high volumes of data.

Setup:
- Create a ksqlDB cluster. Choose global access. Choose default settings and launch it.
- Create a stream that selects three variables from the rides topic:
```sql
CREATE STREAM ride_streams (
    VendorId varchar, 
    trip_distance double,
    payment_type varchar
)  WITH (KAFKA_TOPIC='rides',
        VALUE_FORMAT='JSON');
```
- Query a stream with filters:
```sql
SELECT payment_type, count(*) FROM RIDE_STREAMS 
WHERE payment_type IN ('1', '2')
GROUP BY payment_type
EMIT CHANGES;
```
- Create a table (persistent query) by querying a stream with window functions. A persistent query is a continuously running query that is materialized as a stream or table in Kafka. It persists the results of the query in Kafka topics, which allows other applications to consume and process the data as needed. The persistent query is continuously updated as new data arrives in the input topics, ensuring that the results are always up-to-date:
```sql
CREATE TABLE payment_type_sessions AS
  SELECT payment_type,
         count(*)
  FROM  RIDE_STREAMS 
  WINDOW SESSION (60 SECONDS)
  GROUP BY payment_type
  EMIT CHANGES;
```
- Check data created in the persistent query:
```sql
SELECT * FROM payment_type_sessions EMIT CHANGES;
```
- Terminate persistent query to avoid charges.

**Advantages:**

- ksqlDB is easy to use, with a syntax similar to SQL
- It allows users to work with real-time streaming data in a simple and intuitive way
- ksqlDB supports a wide range of data sources and destinations, including Kafka, databases, and file systems
- It integrates well with other Kafka-based technologies
- It provides a quick and easy way to prototype and test Kafka streams.

**Disadvantages:**

- ksqlDB can be limited in terms of its data processing capabilities compared to other stream processing frameworks
- It may not be suitable for very complex data processing scenarios or for very high throughput use cases
- Its scalability may be limited, depending on the deployment architecture
- It requires its own cluster and resources, which may incur additional costs for certain use cases.

## Kafka Connectors

Kafka Connectors are plugins that enable data integration between Kafka and other systems, allowing data to be easily transferred from one system to another in a scalable and fault-tolerant manner. They are used to connect Kafka with various sources and sinks such as databases, file systems, messaging systems, and other data platforms. Kafka Connectors provide an easy and flexible way to build a data pipeline for streaming data in and out of Kafka without writing custom code.

- JDBC connector: Connects a Kafka topic to a database using Java Database Connectivity (JDBC) for reading or writing data.
- Elasticsearch connector: Connects a Kafka topic to an Elasticsearch index, allowing for data to be indexed and searched in real-time.
- S3 connector: Connects a Kafka topic to an Amazon S3 bucket for reading or writing data in near real-time.
- MQTT connector: Connects a Kafka topic to an MQTT broker, enabling the ingestion of IoT device data into a Kafka cluster.
- HDFS connector: Connects a Kafka topic to a Hadoop Distributed File System (HDFS) for writing data in real-time

## Kafka Schema registry

A schema refers to the structure of data being transmitted between producers and consumers. It defines the format, data types, and rules for interpreting the data, ensuring that it can be accurately processed by downstream applications. Schemas help ensure consistency and compatibility between different components of a Kafka ecosystem, especially when data is being exchanged between different programming languages or technologies. Kafka uses the Schema Registry service to manage and enforce schema compatibility across different versions of data producers and consumers.

**Steps when using a Kafka schema:**

- In a Kafka system, producers and consumers exchange data in the form of messages or events. These messages may contain different types of data such as JSON, Avro, or Protobuf. In order for producers and consumers to communicate effectively, they need to agree on a common schema or structure for the data they exchange.
    - Avro is a data serialization system that provides a compact and efficient way to represent data in a platform-independent manner. It defines a binary format for data, as well as a JSON-like schema language that allows you to define the structure of your data. Avro is often used in big data ecosystems, such as Apache Kafka and Hadoop, to facilitate the exchange of data between different systems and applications.
- The schema registry is a service that stores and manages the schemas used in a Kafka system. Producers and consumers can register their schemas with the registry, which provides a central location for storing and retrieving schemas.
- When a producer sends a message, it includes the schema identifier in the message header. Consumers can then use the schema identifier to retrieve the schema from the schema registry and deserialize the message. This ensures that the data sent by the producer conforms to a known schema and can be consumed by the consumer without errors.
- By using the schema registry, producers and consumers can maintain compatibility and consistency in their data formats, even as the schema evolves over time. This allows for flexibility and scalability in the Kafka system, as new producers and consumers can be added and data formats can be updated without causing errors or breaking the system.

**Types of compatibility in Kafka schemas**
1. Backward compatibility: A schema is backward compatible if a new producer can use it to write data that can be read by an old consumer that uses the previous schema version. In other words, a new schema version can be added while still allowing consumers using the old schema version to continue reading the data.

2. Forward compatibility: A schema is forward compatible if an old producer can use it to write data that can be read by a new consumer that uses the latest schema version. In other words, a new schema version can be added while still allowing producers using the old schema version to continue writing data.

3. Full compatibility: A schema is fully compatible if it is both backward and forward compatible. In other words, a new schema version can be added while still allowing both producers and consumers using the old schema version to continue writing and reading data.

### Exercise setup
- Create a new topic: `rides-avro`
- Build gradle in order to generate the Avro class with a given plugin set up in the `build.gradle` file:
    - ./gradlew clean
    - ./gradlew build
- You can check the autogenerated class in: `build/generated-main-avro-java/schemaregistry`. This is built using the .avsc files located in the src/main/avro folder.
```json
{
       "type": "record",
       "name":"RideRecord",
       "namespace": "schemaregistry",
       "fields":[
         {"name":"vendor_id","type":"string"},
         {"name":"passenger_count","type":"int"},
         {"name":"trip_distance","type":"double"}
       ]
}
```
- Place your API key and secret in the Secrets java class.
- Execute the java producer to send rides data based on the defined schema above:
```java
package org.example;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;
import org.example.data.Ride;
import schemaregistry.RideRecord;

import java.io.InputStreamReader;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class AvroProducer {
    
    // Define Kafka configuration properties
    private Properties props = new Properties();

    // Constructor to set up Kafka Producer properties
    public AvroProducer() {
        // Set Kafka broker configuration
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-4r297.europe-west1.gcp.confluent.cloud:9092");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='"+Secrets.KAFKA_CLUSTER_KEY+"' password='"+Secrets.KAFKA_CLUSTER_SECRET+"';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", "45000");
        
        // Set producer configuration
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        // Set schema registry configuration
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "https://psrc-y5q2k.europe-west3.gcp.confluent.cloud");
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", Secrets.SCHEMA_REGISTRY_KEY+":"+Secrets.SCHEMA_REGISTRY_SECRET);
    }

    // Read rides data from CSV and map them to RideRecord objects
    public List<RideRecord> getRides() throws IOException, CsvException {
        // Get the resource file path
        var ridesUrl = this.getClass().getResource("/rides.csv");
        
        // Create a CSV reader and skip the header row
        var reader = new CSVReader(new InputStreamReader(ridesUrl.openStream()));
        reader.skip(1);

        // Map each row to a RideRecord object
        return reader.readAll().stream().map(row ->
            RideRecord.newBuilder()
                    .setVendorId(row[0])
                    .setTripDistance(Double.parseDouble(row[4]))
                    .setPassengerCount(Integer.parseInt(row[3]))
                    .build()
                ).collect(Collectors.toList());
    }

    // Publish RideRecords to Kafka topic
    public void publishRides(List<RideRecord> rides) throws ExecutionException, InterruptedException {
        // Create a new Kafka producer instance
        KafkaProducer<String, RideRecord> kafkaProducer = new KafkaProducer<>(props);
        
        // Send each ride record to the Kafka topic "rides_avro"
        for (RideRecord ride : rides) {
            var record = kafkaProducer.send(new ProducerRecord<>("rides-avro", String.valueOf(ride.getVendorId()), ride), (metadata, exception) -> {
                if (exception != null) {
                    System.out.println(exception.getMessage());
                }
            });

            // Print the offset of the published message
            System.out.println(record.get().offset());
            Thread.sleep(500);
        }
    }

    // Main method to run the AvroProducer
    public static void main(String[] args) throws IOException, CsvException, ExecutionException, InterruptedException {
        var producer = new AvroProducer();
        var rideRecords = producer.getRides();
        producer.publishRides(rideRecords);
    }
}
```
- Check the incoming messages and the corresponding schema in the `rides-avro` topic.
- Set up compatibility to "Transitive full" to ensure full compatibility and avoid future errors.
- Change the schema using the .avsc files and gradlew clean and build again.
- Change the RideRecord mapping in the java producer file.
- Run the producer and check the compatibility and non-compatibility.
- If compatible, compare schema versions. Ensuring compatibility you make sure that you will never break your consumers' downstream.


