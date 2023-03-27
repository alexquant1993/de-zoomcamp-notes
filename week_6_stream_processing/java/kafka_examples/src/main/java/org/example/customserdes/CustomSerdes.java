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
