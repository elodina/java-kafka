package ly.stealth.kafka.metrics.yammer;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricsRegistry;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;

public class KafkaReporterTest {
    private final String zkConnect = "localhost:2181";
    private final String kafkaConnect = "localhost:9092";
    private final String schemaRegistry = "http://localhost:8081";
    private final String topic = UUID.randomUUID().toString();

    @Test
    public void testTopicReporter() {
        MetricsRegistry registry = new MetricsRegistry();
        Counter counter = registry.newCounter(KafkaReporterTest.class, "test-counter");
        counter.inc();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnect);
        producerProps.put("schema.registry.url", schemaRegistry);

        KafkaReporter reporter = new KafkaReporter(registry, producerProps, topic);
        reporter.start(1, TimeUnit.SECONDS);

        Properties props = new Properties();
        props.put("zookeeper.connect", zkConnect);
        props.put("group.id", UUID.randomUUID().toString());
        props.put("auto.offset.reset", "smallest");
        props.put("zookeeper.session.timeout.ms", "30000");
        props.put("consumer.timeout.ms", "30000");
        props.put("schema.registry.url", schemaRegistry);

        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        KafkaStream<String, Object> messageStream = consumer.createMessageStreamsByFilter(new Whitelist(topic),
                1,
                new StringDecoder(null),
                new KafkaAvroDecoder(new VerifiableProperties(props))).get(0);

        GenericRecord message = (GenericRecord) messageStream.iterator().next().message();
        assertNotNull(message);

        reporter.shutdown();
    }
}
