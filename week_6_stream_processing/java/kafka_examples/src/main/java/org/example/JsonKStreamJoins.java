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
