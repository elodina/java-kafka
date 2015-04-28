package ly.stealth.kafka.emitters;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import ly.stealth.avro.LogLine;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

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

    public KafkaLogAppender(Properties props, String topic) {
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        this.producer = new KafkaProducer<String, IndexedRecord>(props);
        this.topic = topic;
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
}
