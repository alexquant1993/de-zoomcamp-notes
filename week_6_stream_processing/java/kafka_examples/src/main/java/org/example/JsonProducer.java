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