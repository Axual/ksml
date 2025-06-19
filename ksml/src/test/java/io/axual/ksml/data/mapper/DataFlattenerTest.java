package io.axual.ksml.data.util;

import io.axual.ksml.data.mapper.DataObjectFlattener;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ConvertUtilTest {
    @Test
    void testConvertWindowedStruct() {
        final var window = new TimeWindow(0, 1);
        final var key = new DataStruct();
        key.put("key", DataString.from("value"));
        final var windowedData = new Windowed<>(key, window);
        final var converted = new DataObjectFlattener().toDataObject(windowedData);
        assertNotNull(converted, "Conversion result should not be null");
        assertInstanceOf(DataStruct.class, converted, "Conversion should result in a data struct");
        final var struct = (DataStruct) converted;
        assertTrue(struct.containsKey("key"), "Converted struct should contain key field");
        assertInstanceOf(DataStruct.class, struct.get("key"), "Converted struct should contain key field of type data struct");
        assertEquals("value", ((DataStruct) struct.get("key")).get("key").toString(), "Converted struct does not contain correct key");
    }
}
