package ly.stealth.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.*;

public class JsonToAvroConverter {
    public static final long MetricsLogTypeId = 7L;

    private static final Map<Class, Schema.Type> classToSchema = new HashMap<Class, Schema.Type>();

    static {
        classToSchema.put(Integer.class, Schema.Type.INT);
        classToSchema.put(Long.class, Schema.Type.LONG);
        classToSchema.put(Float.class, Schema.Type.FLOAT);
        classToSchema.put(Double.class, Schema.Type.DOUBLE);
        classToSchema.put(String.class, Schema.Type.STRING);
        classToSchema.put(ArrayList.class, Schema.Type.ARRAY);
        classToSchema.put(LinkedHashMap.class, Schema.Type.RECORD);
    }

    private final ObjectMapper mapper;
    private Map<String, String> nameMapping;

    public JsonToAvroConverter() {
        mapper = new ObjectMapper();
    }

    public synchronized GenericRecord convert(String json) throws IOException {
        Map<String, Object> metrics = mapper.readValue(json, LinkedHashMap.class);

        nameMapping = new HashMap<String, String>();
        Schema schema = parseSchema(metrics);
        GenericRecord record = new GenericData.Record(schema);
        fillRecord(record, schema, metrics);

        return record;
    }

    private void fillRecord(GenericRecord record, Schema schema, Map<String, Object> metrics) {
        for (Schema.Field field : schema.getFields()) {
            if (field.name().equals("logLine")) {
                GenericRecord logLine = new GenericData.Record(field.schema());
                logLine.put("source", "metrics");
                logLine.put("logtypeid", MetricsLogTypeId);
                record.put("logLine", logLine);
            } else {
                String name = nameMapping.get(field.name());
                Schema fieldSchema = field.schema().getTypes().get(1);
                if (fieldSchema.getType().equals(Schema.Type.RECORD)) {
                    GenericRecord fieldRecord = new GenericData.Record(fieldSchema);
                    fillRecord(fieldRecord, fieldSchema, (Map<String, Object>) metrics.get(name));
                    record.put(field.name(), fieldRecord);
                } else {
                    record.put(field.name(), metrics.get(name));
                }
            }
        }
    }

    private Schema parseSchema(Map<String, Object> metrics) {
        Schema schema = Schema.createRecord("Metrics", null, "ly.stealth", false);
        List<Schema.Field> fields = new ArrayList<Schema.Field>();

        for (Map.Entry<String, Object> entry : metrics.entrySet()) {
            String lookupName = getLookupName(entry.getKey());
            nameMapping.put(lookupName, entry.getKey());

            Schema.Field field = new Schema.Field(lookupName,
                    Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), parseRecordField(entry.getKey(), entry.getValue()))),
                    null,
                    Schema.parseJson("null"));

            fields.add(field);
        }

        fields.add(new Schema.Field("logLine", LogLine.getClassSchema(), null, null));
        schema.setFields(fields);

        return schema;
    }

    private String getLookupName(String name) {
        String lookupName = name;
        if (name.matches("[0-9]+")) {
            lookupName = "metric" + name;
        }

        lookupName = lookupName.replace(".", "").replace("%", "").replace("-", "");
        return lookupName;
    }

    public Schema parseRecordField(String name, Object value) {
        String lookupRecordName = getLookupName(name);
        nameMapping.put(lookupRecordName, name);

        if (value instanceof Integer) {
            return Schema.create(Schema.Type.INT);
        } else if (value instanceof Long) {
            return Schema.create(Schema.Type.LONG);
        } else if (value instanceof Float) {
            return Schema.create(Schema.Type.FLOAT);
        } else if (value instanceof Double) {
            return Schema.create(Schema.Type.DOUBLE);
        } else if (value instanceof String) {
            return Schema.create(Schema.Type.STRING);
        } else if (value instanceof LinkedHashMap) {
            Schema record = Schema.createRecord(lookupRecordName, null, "ly.stealth", false);
            List<Schema.Field> fields = new ArrayList<Schema.Field>();
            Map<String, Object> jsonField = (Map<String, Object>) value;

            for (Map.Entry<String, Object> e : jsonField.entrySet()) {
                String lookupName = getLookupName(e.getKey());
                nameMapping.put(lookupName, e.getKey());

                Schema valueSchema = parseRecordField(e.getKey(), e.getValue());
                Schema.Field field = new Schema.Field(lookupName,
                        Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), valueSchema)),
                        null,
                        Schema.parseJson("null"));
                fields.add(field);
            }

            record.setFields(fields);
            return record;
        } else if (value instanceof ArrayList) {
            ArrayList list = (ArrayList) value;
            Schema elementSchema = Schema.create(Schema.Type.NULL);
            if (!list.isEmpty()) {
                elementSchema = parseRecordField(name, list.get(0));
            }

            return Schema.createArray(elementSchema);
        }

        return null;
    }
}
