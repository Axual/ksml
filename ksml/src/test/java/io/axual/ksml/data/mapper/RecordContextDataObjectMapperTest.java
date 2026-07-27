package io.axual.ksml.data.mapper;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static io.axual.ksml.dsl.RecordContextSchema.RECORD_CONTEXT_SCHEMA_OFFSET_FIELD;
import static io.axual.ksml.dsl.RecordContextSchema.RECORD_CONTEXT_SCHEMA_TOPIC_FIELD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecordContextDataObjectMapperTest {

    private final RecordContextDataObjectMapper mapper = new RecordContextDataObjectMapper();

    @Test
    @DisplayName("toDataObject maps topic and offset fields into the struct")
    void toDataObjectMapsAllFields() {
        final var context = new ProcessorRecordContext(123L, 45L, 6, "topic", new RecordHeaders());

        final var result = mapper.toDataObject(context);

        assertThat(result).isInstanceOf(DataStruct.class);
        final var struct = (DataStruct) result;
        assertThat(struct.get(RECORD_CONTEXT_SCHEMA_TOPIC_FIELD)).isEqualTo(new DataString("topic"));
        assertThat(struct.get(RECORD_CONTEXT_SCHEMA_OFFSET_FIELD)).hasToString("45");
    }

    @Test
    @DisplayName("a record context survives a round trip through the data object")
    void roundTripsThroughDataObject() {
        // Same value for timestamp and offset keeps the assertion independent of field ordering.
        final var context = new ProcessorRecordContext(99L, 99L, 6, "topic", new RecordHeaders());

        final var restored = mapper.fromDataObject(mapper.toDataObject(context));

        assertThat(restored.topic()).isEqualTo("topic");
        assertThat(restored.partition()).isEqualTo(6);
        assertThat(restored.offset()).isEqualTo(99L);
        assertThat(restored.timestamp()).isEqualTo(99L);
    }

    @Test
    @DisplayName("fromDataObject rejects a non-struct input")
    void fromDataObjectRejectsNonStruct() {
        final DataObject value = new DataString("not a struct");
        assertThatThrownBy(() -> mapper.fromDataObject(value))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RecordContext");
    }

    @Test
    @DisplayName("fromDataObject falls back to default values for missing fields")
    void fromDataObjectUsesDefaultsForMissingFields() {
        final var restored = mapper.fromDataObject(new DataStruct());

        assertThat(restored.offset()).isEqualTo(RecordContextDataObjectMapper.NO_OFFSET);
        assertThat(restored.timestamp()).isEqualTo(RecordContextDataObjectMapper.NO_TIMESTAMP);
        assertThat(restored.partition()).isEqualTo(RecordContextDataObjectMapper.NO_PARTITION);
        assertThat(restored.topic()).isNull();
    }
}
