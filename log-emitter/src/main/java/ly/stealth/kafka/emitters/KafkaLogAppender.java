package ly.stealth.kafka.emitters;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import ly.stealth.avro.LogLine;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * KafkaLogAppender produces all log events to Kafka topic encoded as Avro.
 * Avro schema is defined as follows:
 * {
 *     "type": "record",
 *     "name": "ly.stealth.avro.LogLine",
 *     "fields": [
 *         {
 *             "name": "line",
 *             "type": ["null", "string"],
 *             "default": null
 *         },
 *         {
 *             "name": "source",
 *             "type": ["null", "string"],
 *             "default": null
 *         },
 *         {
 *             "name": "tag",
 *             "type": [
 *                 "null",
 *                 {
 *                     "type": "map",
 *                     "values": "string"
 *                 }
 *             ],
 *             "default": null
 *         },
 *         {
 *             "name": "logtypeid",
 *             "type": ["null", "long"],
 *             "default": null
 *         },
 *         {
 *             "name": "timings",
 *             "type": [
 *                 "null",
 *                 {
 *                     "type": "map",
 *                     "values": "long"
 *                 }
 *             ],
 *             "default": null
 *         }
 *     ]
 * }
 *
 * "line" field will contain the actual log message.
 * "logtypeid" will be the log level:
 *      1 - Trace
 *      2 - Debug
 *      3 - Info
 *      4 - Warn
 *      5 - Error
 *      6 - Fatal
 * "source" will be the thread name that logged the message.
 * "tag" will contain 2 entries:
 *      "logger.name" - logger name, e.g. the one that is used in call Logger.getLogger()
 *      "location" - event location information, e.g. source code line that logged the message
 * "timings" will contain 1 entry:
 *      "emitted" - timestamp in milliseconds when the message was sent
 */
public class KafkaLogAppender extends AppenderSkeleton {
    public static final Long TraceLogTypeId = 1L;
    public static final Long DebugLogTypeId = 2L;
    public static final Long InfoLogTypeId = 3L;
    public static final Long WarnLogTypeId = 4L;
    public static final Long ErrorLogTypeId = 5L;
    public static final Long FatalLogTypeId = 6L;

    private static final Map<Level, Long> logLevels = new HashMap<Level, Long>();

    static {
        logLevels.put(Level.TRACE, TraceLogTypeId);
        logLevels.put(Level.DEBUG, DebugLogTypeId);
        logLevels.put(Level.INFO, InfoLogTypeId);
        logLevels.put(Level.WARN, WarnLogTypeId);
        logLevels.put(Level.ERROR, ErrorLogTypeId);
        logLevels.put(Level.FATAL, FatalLogTypeId);
    }

    private KafkaProducer<String, IndexedRecord> producer;

    private String topic;
    private String brokerList;
    private String schemaRegistryUrl;

    public KafkaLogAppender() {

    }

    public KafkaLogAppender(Properties props, String topic) {
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        this.producer = new KafkaProducer<String, IndexedRecord>(props);
        this.topic = topic;
    }

    @Override
    public void activateOptions() {
        if (this.brokerList == null)
            throw new ConfigException("The bootstrap servers property is required");

        if (this.schemaRegistryUrl == null)
            throw new ConfigException("The schema registry url is required");

        if (this.topic == null)
            throw new ConfigException("Topic is required");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerList);
        props.put("schema.registry.url", this.schemaRegistryUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        this.producer = new KafkaProducer<String, IndexedRecord>(props);
    }

    @Override
    protected void append(LoggingEvent event) {
        LogLine line = new LogLine();
        line.setLine(event.getMessage() == null ? null : event.getMessage().toString());
        line.setLogtypeid(logLevels.get(event.getLevel()));
        line.setSource(event.getThreadName());

        Map<CharSequence, Long> timings = new HashMap<CharSequence, Long>();
        timings.put("emitted", event.getTimeStamp());

        line.setTimings(timings);

        Map<CharSequence, CharSequence> tags = new HashMap<CharSequence, CharSequence>();
        tags.put("logger.name", event.getLoggerName());
        tags.put("location", event.getLocationInformation().fullInfo);

        line.setTag(tags);

        ProducerRecord<String, IndexedRecord> record = new ProducerRecord<String, IndexedRecord>(this.topic, line);
        producer.send(record);
    }

    @Override
    public void close() {
        this.producer.close();
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public void setSchemaRegistryUrl(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }
}
