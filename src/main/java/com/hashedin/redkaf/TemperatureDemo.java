package com.hashedin.redkaf;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

/**
 * Demonstrates, using the high-level KStream DSL, how to implement an IoT demo application
 * which ingests temperature value processing the maximum value in the latest TEMPERATURE_WINDOW_SIZE seconds (which
 * is 5 seconds) sending a new message if it exceeds the TEMPERATURE_THRESHOLD (which is 20)
 *
 * In this example, the input stream reads from a topic named "iot-temperature", where the values of messages
 * represent temperature values; using a TEMPERATURE_WINDOW_SIZE seconds "tumbling" window, the maximum value is processed and
 * sent to a topic named "iot-temperature-max" if it exceeds the TEMPERATURE_THRESHOLD.
 *
 * Before running this example you must create the input topic for temperature values in the following way :
 *
 * bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic iot-temperature
 *
 * and at same time the output topic for filtered values :
 *
 * bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic iot-temperature-max
 *
 * After that, a console consumer can be started in order to read filtered values from the "iot-temperature-max" topic :
 *
 * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic iot-temperature-max --from-beginning
 *
 * On the other side, a console producer can be used for sending temperature values (which needs to be integers)
 * to "iot-temperature" typing them on the console :
 *
 * bin/kafka-console-producer.sh --broker-list localhost:9092 --topic iot-temperature
 * > 10
 * > 15
 * > 22
 */
public class TemperatureDemo {

    // threshold used for filtering max temperature values
    private static final int TEMPERATURE_THRESHOLD = 35;
    // window size within which the filtering is applied
    private static final int TEMPERATURE_WINDOW_SIZE = 5;

    public static void main(final String[] args) {

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-temperature");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(new RedisStoreBuilder());
        final KStream<String, String> source = builder.stream("iot-temperature");

        final KStream<Windowed<String>, String> max = source
            .selectKey((key, value) -> "temp")
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofSeconds(TEMPERATURE_WINDOW_SIZE)))
            .reduce((value1, value2) -> {
                if (Integer.parseInt(value1) > Integer.parseInt(value2)) {
                    return value1;
                } else {
                    return value2;
                }
            })
            .toStream()
            .filter((key, value) -> Integer.parseInt(value) > TEMPERATURE_THRESHOLD);
            

        // need to override key serde to Windowed<String> type
        max.to("iot-temperature-max");

        String clusterId = "redkaf";
		Node node = new Node(1, "localhost", 6379);
		MockKafka kafka = new MockKafka(clusterId, Collections.singleton(node));
		
		createTopics(kafka);
		sendSomeTemperatures(kafka);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props, new RedKafkaClientSupplier());
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-temperature-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

	private static void createTopics(MockKafka kafka) {
		NewTopic source = new NewTopic("iot-temperature", 1, (short) 0);
		NewTopic dest = new NewTopic("iot-temperature-max", 1, (short) 0);
		
		kafka.createTopics(Arrays.asList(source, dest), null);
	}

	private static void sendSomeTemperatures(MockKafka kafka) {
		int temperatures[] = new int[]{30, 31, 45, 45, 20, 23, 38};
		for(int temperature : temperatures) {
			ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>("iot-temperature", 0, 
					System.currentTimeMillis(), new byte[0], String.valueOf(temperature).getBytes()); 
			kafka.send(record, 0);
		}
	}
}
