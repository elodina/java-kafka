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

package ly.stealth.kafka.metrics.yammer;

import com.yammer.metrics.core.*;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import ly.stealth.avro.JsonToAvroConverter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Map;
import java.util.Properties;

/**
 * KafkaReporter produces all metrics data from a provided registry to Kafka topic with a given reporting interval
 * encoded as Avro.
 * Avro schema will be created dynamically to match JSON representation of the registry. LogLine field will also
 * be added to contain simple metadata about metrics. LogLines logtypeid will be 7 (metrics data) and the source
 * will be "metrics".
 */
public class KafkaReporter extends AbstractPollingReporter implements MetricProcessor<JSONObject> {
    private final KafkaProducer<String, IndexedRecord> producer;
    private final MetricPredicate predicate = MetricPredicate.ALL;
    private final JsonToAvroConverter converter;
    private final String topic;

    public KafkaReporter(MetricsRegistry metricsRegistry, Properties producerProperties, String topic) {
        super(metricsRegistry, "kafka-topic-reporter");

        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        this.producer = new KafkaProducer<String, IndexedRecord>(producerProperties);
        this.converter = new JsonToAvroConverter();
        this.topic = topic;

    }

    public void run() {
        try {
            JSONObject object = new JSONObject();
            for (Map.Entry<MetricName, Metric> entry : getMetricsRegistry().allMetrics().entrySet()) {
                if (predicate.matches(entry.getKey(), entry.getValue())) {
                    entry.getValue().processWith(this, entry.getKey(), object);
                }
            }
            ProducerRecord<String, IndexedRecord> record = new ProducerRecord<String, IndexedRecord>(this.topic, converter.convert(object.toString()));
            this.producer.send(record);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, JSONObject context) {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put("name", getName(name));
            jsonObject.put("type", "gauge");
            jsonObject.put("value", gauge.value());
            context.put(name.getName(), jsonObject);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void processCounter(MetricName name, Counter counter, JSONObject context) {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put("name", getName(name));
            jsonObject.put("type", "counter");
            jsonObject.put("value", counter.count());
            context.put(name.getName(), jsonObject);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void processMeter(MetricName name, Metered meter, JSONObject context) {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put("name", getName(name));
            jsonObject.put("type", "meter");
            JSONObject meterJsonObject = new JSONObject();

            addMeterInfo(meter, meterJsonObject);

            jsonObject.put("value", meterJsonObject);

            context.put(name.getName(), jsonObject);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    private void addMeterInfo(Metered meter, JSONObject meterJsonObject) throws JSONException {
        meterJsonObject.put("rateUnit", meter.rateUnit());
        meterJsonObject.put("eventType", meter.eventType());
        meterJsonObject.put("count", meter.count());
        meterJsonObject.put("meanRate", meter.meanRate());
        meterJsonObject.put("oneMinuteRate", meter.oneMinuteRate());
        meterJsonObject.put("fiveMinuteRate", meter.fiveMinuteRate());
        meterJsonObject.put("fifteenMinuteRate", meter.fifteenMinuteRate());
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, JSONObject context) {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put("name", getName(name));
            jsonObject.put("type", "meter");
            JSONObject histogramJsonObject = new JSONObject();

            histogramJsonObject.put("min", histogram.min());
            histogramJsonObject.put("max", histogram.max());
            histogramJsonObject.put("mean", histogram.mean());
            histogramJsonObject.put("stdDev", histogram.stdDev());

            Snapshot snapshot = histogram.getSnapshot();
            JSONObject snapshotJsonObject = new JSONObject();
            snapshotJsonObject.put("median", snapshot.getMedian());
            snapshotJsonObject.put("75%", snapshot.get75thPercentile());
            snapshotJsonObject.put("95%", snapshot.get95thPercentile());
            snapshotJsonObject.put("98%", snapshot.get98thPercentile());
            snapshotJsonObject.put("99%", snapshot.get99thPercentile());
            snapshotJsonObject.put("99.9%", snapshot.get999thPercentile());

            histogramJsonObject.put("snapshot", snapshotJsonObject);

            jsonObject.put("value", histogramJsonObject);
            context.put(name.getName(), jsonObject);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void processTimer(MetricName name, Timer timer, JSONObject context) {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put("name", getName(name));
            jsonObject.put("type", "meter");
            JSONObject timerJsonObject = new JSONObject();

            timerJsonObject.put("unit", timer.durationUnit());
            timerJsonObject.put("min", timer.min());
            timerJsonObject.put("max", timer.max());
            timerJsonObject.put("mean", timer.mean());
            timerJsonObject.put("stdDev", timer.stdDev());
            addMeterInfo(timer, timerJsonObject);

            Snapshot snapshot = timer.getSnapshot();
            JSONObject snapshotJsonObject = new JSONObject();
            snapshotJsonObject.put("median", snapshot.getMedian());
            snapshotJsonObject.put("75%", snapshot.get75thPercentile());
            snapshotJsonObject.put("95%", snapshot.get95thPercentile());
            snapshotJsonObject.put("98%", snapshot.get98thPercentile());
            snapshotJsonObject.put("99%", snapshot.get99thPercentile());
            snapshotJsonObject.put("99.9%", snapshot.get999thPercentile());

            timerJsonObject.put("snapshot", snapshotJsonObject);

            jsonObject.put("value", timerJsonObject);

            context.put(name.getName(), jsonObject);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    private JSONObject getName(MetricName metricName) throws JSONException {
        String group = metricName.getGroup();
        String name = metricName.getName();
        String scope = metricName.getScope();
        String type = metricName.getType();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", name);
        jsonObject.put("group", group);
        jsonObject.put("scope", scope);
        jsonObject.put("type", type);
        return jsonObject;
    }

    @Override
    public void shutdown() {
        try {
            super.shutdown();
        } finally {
            this.producer.close();
        }
    }
}