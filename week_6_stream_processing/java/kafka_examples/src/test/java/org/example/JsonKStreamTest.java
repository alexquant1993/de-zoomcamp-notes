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
