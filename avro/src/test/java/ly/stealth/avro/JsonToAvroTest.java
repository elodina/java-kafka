package ly.stealth.avro;

import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class JsonToAvroTest {
    @Test
    public void testJsonToAvro() {
        String json = "{\"value1\":\"asd\",\"value2\":1,\"value.3\":1.23,\"value%4\":{\"subvalue1\":\"zxc\",\"subvalue2\":23,\"subvalue.3\":4.456,\"subvalue%4\":{\"another.subvalue\":\"hello\"}},\"value5\":[1,2,3]}";

        JsonToAvroConverter json2avro = new JsonToAvroConverter();

        GenericRecord record = null;
        try {
            record = json2avro.convert(json);
        } catch (IOException e) {
            e.printStackTrace();
            fail("Unexpected exception: " + e.getMessage());
        }

        System.out.println(record);
        assertEquals(6, record.getSchema().getFields().size());
        assertEquals("asd", record.get("value1").toString());
        assertEquals(Integer.valueOf(1), record.get("value2"));
        assertEquals(Double.valueOf(1.23), record.get("value3"));
    }
}
