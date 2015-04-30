log-emitter
==========

Log4j Appender implementation that produces all log events to a Kafka topic encoded as Avro messages.

Avro schema is located here: [[avsc]](https://github.com/stealthly/java-kafka/blob/master/avro/src/main/avro/logline.avsc) [[generated java]](https://github.com/stealthly/java-kafka/blob/master/avro/src/main/java/ly/stealth/avro/LogLine.java)

Avro schema fields explanation:

`line` - actual log message.

`logtypeid` - log level:    
    `1` - Trace    
    `2` - Debug    
    `3` - Info    
    `4` - Warn    
    `5` - Error    
    `6` - Fatal
    
 `source` - thread name that logged the message.
 `tag` will contain 2 entries:    
    `logger.name` - logger name, e.g. the one that is used in call `Logger.getLogger("someName")`    
    `location` - event location information, e.g. source code line that logged the message
 
 `timings` will contain 1 entry:    
      `emitted` - timestamp in milliseconds when the message was sent
      
**Usage**:

```
logger = Logger.getLogger("kafka-logger");

Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put("schema.registry.url", "http://localhost:8081");
logger.addAppender(new KafkaLogAppender(props, topic));

logger.info("hello world!");
```