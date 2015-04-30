/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ly.stealth.kafka.metrics.codahale;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;

public class KafkaReporterTest {
    private final String zkConnect = "localhost:2181";
    private final String kafkaConnect = "localhost:9092";
    private final String schemaRegistry = "http://localhost:8081";
    private final String topic = UUID.randomUUID().toString();
    private KafkaReporter kafkaReporter;
    private MetricRegistry registry;

    @Test
    public void testCodahaleKafkaMetricsReporter() {
        registry = new MetricRegistry();
        registry.counter("test_counter").inc();

        kafkaReporter = KafkaReporter.builder(registry,
                kafkaConnect,
                topic,
                schemaRegistry).build();

//        ObjectMapper mapper = new ObjectMapper().registerModule(new MetricsModule(TimeUnit.SECONDS,
//                TimeUnit.SECONDS,
//                false));
//        StringWriter r = new StringWriter();
//        try {
//            mapper.writeValue(r, registry);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        kafkaReporter.report();

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
    }
}
