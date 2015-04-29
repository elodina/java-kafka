package ly.stealth.kafka.emitters;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class KafkaLogAppenderTest {
    private static String brokerList = "localhost:9092";
    private static String zookeeper = "localhost:2181";
    private static String schemaRegistry = "http://localhost:8081";
    private static String topic = "kafka-log-appender-test-" + System.currentTimeMillis();

    private static String loggerName = "logger-test";
    private static Logger logger;

    @BeforeClass
    public static void initialize() {
        logger = Logger.getLogger(loggerName);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put("schema.registry.url", schemaRegistry);
        logger.addAppender(new KafkaLogAppender(props, topic));
    }

    @Test
    public void testKafkaLogAppender() {
        Properties consumerProps = new Properties();
        consumerProps.put("zookeeper.connect", zookeeper);
        consumerProps.put("group.id", "kafka-log-appender-test");
        consumerProps.put("auto.offset.reset", "smallest");
        consumerProps.put("schema.registry.url", schemaRegistry);

        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(topic, 1);

        ConsumerIterator<String, Object> iterator = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProps))
                .createMessageStreams(topicMap, new StringDecoder(null), new KafkaAvroDecoder(new VerifiableProperties(consumerProps)))
                .get(topic).get(0).iterator();

        String testMessage = "I am a test message";
        logger.info(testMessage);

        MessageAndMetadata<String, Object> messageAndMetadata = iterator.next();
        GenericRecord logLine = (GenericRecord) messageAndMetadata.message();
        assertEquals(logLine.get("line").toString(), testMessage);
        assertEquals(logLine.get("logtypeid"), KafkaLogAppender.InfoLogTypeId);
        assertNotNull(logLine.get("source"));
        assertEquals(((Map<CharSequence, Object>) logLine.get("timings")).size(), 1);
        assertEquals(((Map<CharSequence, Object>) logLine.get("tag")).size(), 2);
    }
}
