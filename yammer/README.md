yammer
==========

Yammer Metrics Reporter implementation that produces all metrics data to a Kafka topic encoded as Avro messages.

Avro schema will be created dynamically to match JSON representation of the registry. `LogLine` field will also be added to contain simple metadata about metrics.

LogLine Avro schema is located here: [[avsc]](https://github.com/stealthly/java-kafka/blob/master/avro/src/main/avro/logline.avsc) [[generated java]](https://github.com/stealthly/java-kafka/blob/master/avro/src/main/java/ly/stealth/avro/LogLine.java)
      
**Usage**:

```
MetricsRegistry registry = new MetricsRegistry();
Counter counter = registry.newCounter(SomeClass.class, "test-counter");
counter.inc();

Properties producerProps = new Properties();
producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
producerProps.put("schema.registry.url", "http://localhost:8081");

KafkaReporter reporter = new KafkaReporter(registry, producerProps, "kafka-metrics");
reporter.start(1, TimeUnit.SECONDS);
```