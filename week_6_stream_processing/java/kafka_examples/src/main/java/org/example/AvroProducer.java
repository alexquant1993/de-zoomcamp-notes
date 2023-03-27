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
