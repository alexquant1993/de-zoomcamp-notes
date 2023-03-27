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
