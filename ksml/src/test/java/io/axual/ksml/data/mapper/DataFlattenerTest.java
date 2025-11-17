package io.axual.ksml.data.mapper;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataFlattenerTest {
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
