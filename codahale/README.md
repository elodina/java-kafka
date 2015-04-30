codahale
==========

Codahale Metrics Reporter implementation that produces all metrics data to a Kafka topic encoded as Avro messages.

Avro schema will be created dynamically to match JSON representation of the registry. `LogLine` field will also be added to contain simple metadata about metrics.

LogLine Avro schema is located here: [[avsc]](https://github.com/stealthly/java-kafka/blob/master/avro/src/main/avro/logline.avsc) [[generated java]](https://github.com/stealthly/java-kafka/blob/master/avro/src/main/java/ly/stealth/avro/LogLine.java)
      
**Usage**:

```
registry = new MetricRegistry();
registry.counter("test_counter").inc();

KafkaReporter kafkaReporter = KafkaReporter.builder(registry, "localhost:9092", "kafka-metrics", "http://localhost:8081").build();

kafkaReporter.report();
```