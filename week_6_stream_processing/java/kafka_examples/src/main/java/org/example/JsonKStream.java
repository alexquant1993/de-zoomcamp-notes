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