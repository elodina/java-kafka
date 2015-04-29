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
package ly.stealth.kafka.metrics;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import ly.stealth.avro.JsonToAvroConverter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class KafkaReporter extends ScheduledReporter {
    private static final Logger log = LoggerFactory.getLogger(KafkaReporter.class);

    private final KafkaProducer<String, IndexedRecord> kafkaProducer;
    private final String kafkaTopic;
    private final ObjectMapper mapper;
    private final MetricRegistry registry;
    private final JsonToAvroConverter converter;

    private KafkaReporter(MetricRegistry registry,
                          String name,
                          MetricFilter filter,
                          TimeUnit rateUnit,
                          TimeUnit durationUnit,
                          String kafkaTopic,
                          Properties kafkaProperties) {
        super(registry, name, filter, rateUnit, durationUnit);
        this.registry = registry;
        this.converter = new JsonToAvroConverter();
        mapper = new ObjectMapper().registerModule(new MetricsModule(rateUnit,
                                                                     durationUnit,
                                                                     false));
        this.kafkaTopic = kafkaTopic;
        kafkaProducer = new KafkaProducer<String, IndexedRecord>(kafkaProperties);
    }

    @Override
    public synchronized void report(SortedMap<String, Gauge> gauges,
                                    SortedMap<String, Counter> counters,
                                    SortedMap<String, Histogram> histograms,
                                    SortedMap<String, Meter> meters,
                                    SortedMap<String, Timer> timers) {
        try {
            log.info("Trying to report metrics to Kafka kafkaTopic {}", kafkaTopic);
            StringWriter report = new StringWriter();
            mapper.writeValue(report, registry);
            log.debug("Created metrics report: {}", report);
            ProducerRecord<String, IndexedRecord> record = new ProducerRecord<String, IndexedRecord>(kafkaTopic, converter.convert(report.toString()));
            kafkaProducer.send(record);
            log.info("Metrics were successfully reported to Kafka kafkaTopic {}", kafkaTopic);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static Builder builder(MetricRegistry registry, String brokerList, String kafkaTopic, String schemaRegistryUrl) {
        return new Builder(registry, kafkaTopic, brokerList, schemaRegistryUrl);
    }

    public static class Builder {
        private String brokerList;
        private String schemaRegistryUrl;

        private MetricRegistry registry;
        private String kafkaTopic;
        private Properties producerProperties;

        private String name = "KafkaReporter";
        private MetricFilter filter = MetricFilter.ALL;
        private TimeUnit rateUnit = TimeUnit.SECONDS;
        private TimeUnit durationUnit = TimeUnit.SECONDS;

        public Builder(MetricRegistry registry, String topic, String brokerList, String schemaRegistryUrl) {
            this.registry = registry;
            this.kafkaTopic = topic;
            this.brokerList = brokerList;
            this.schemaRegistryUrl = schemaRegistryUrl;
        }

        public String getKafkaTopic() {
            return kafkaTopic;
        }

        public Builder setKafkaTopic(String kafkaTopic) {
            this.kafkaTopic = kafkaTopic;
            return this;
        }

        public Properties getProducerProperties() {
            return producerProperties;
        }

        public Builder setProducerProperties(Properties producerProperties) {
            this.producerProperties = producerProperties;
            return this;
        }

        public MetricRegistry getRegistry() {
            return registry;
        }

        public Builder setRegistry(MetricRegistry registry) {
            this.registry = registry;
            return this;
        }

        public String getName() {
            return name;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public MetricFilter getFilter() {
            return filter;
        }

        public Builder setFilter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public TimeUnit getRateUnit() {
            return rateUnit;
        }

        public Builder setRateUnit(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        public TimeUnit getDurationUnit() {
            return durationUnit;
        }

        public Builder setDurationUnit(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        public KafkaReporter build() {
            Properties props = producerProperties == null ? new Properties() : producerProperties;
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
            props.put("schema.registry.url", schemaRegistryUrl);

            return new KafkaReporter(registry, name, filter, rateUnit, durationUnit, kafkaTopic, props);
        }
    }
}
