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
