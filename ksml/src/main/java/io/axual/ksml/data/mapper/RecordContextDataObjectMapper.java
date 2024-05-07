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

import io.axual.ksml.data.object.*;
import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;

import static io.axual.ksml.dsl.RecordContextSchema.*;

public class RecordContextDataObjectMapper implements DataObjectMapper<RecordContext> {
    public static final long NO_OFFSET = -1;
    public static final long NO_TIMESTAMP = -1;
    public static final int NO_PARTITION = -1;
    private static final HeaderDataObjectMapper HEADER_MAPPER = new HeaderDataObjectMapper();

    @Override
    public DataObject toDataObject(DataType expected, RecordContext value) {
        final var result = new DataStruct(RECORD_CONTEXT_SCHEMA);
        result.put(RECORD_CONTEXT_SCHEMA_OFFSET_FIELD, new DataLong(value.offset()));
        result.put(RECORD_CONTEXT_SCHEMA_TIMESTAMP_FIELD, new DataLong(value.timestamp()));
        result.put(RECORD_CONTEXT_SCHEMA_TOPIC_FIELD, new DataString(value.topic()));
        result.put(RECORD_CONTEXT_SCHEMA_PARTITION_FIELD, new DataInteger(value.partition()));
        result.put(RECORD_CONTEXT_SCHEMA_HEADERS_FIELD, HEADER_MAPPER.toDataObject(value.headers()));
        return result;
    }

    @Override
    public RecordContext fromDataObject(DataObject value) {
        if (!(value instanceof DataStruct valueStruct)) {
            throw new IllegalArgumentException("Can not convert to RecordContext from type " + value);
        }
        final var offset = valueStruct.getAs(RECORD_CONTEXT_SCHEMA_OFFSET_FIELD, DataLong.class);
        final var timestamp = valueStruct.getAs(RECORD_CONTEXT_SCHEMA_TIMESTAMP_FIELD, DataLong.class);
        final var partition = valueStruct.getAs(RECORD_CONTEXT_SCHEMA_PARTITION_FIELD, DataInteger.class);
        final var topic = valueStruct.getAs(RECORD_CONTEXT_SCHEMA_TOPIC_FIELD, DataString.class);
        final var headers = valueStruct.getAs(RECORD_CONTEXT_SCHEMA_HEADERS_FIELD, DataStruct.class);
        return new ProcessorRecordContext(
                offset != null ? offset.value() : NO_OFFSET,
                timestamp != null ? timestamp.value() : NO_TIMESTAMP,
                partition != null ? partition.value() : NO_PARTITION,
                topic != null ? topic.value() : null,
                headers != null ? HEADER_MAPPER.fromDataObject(headers) : new RecordHeaders()
        );
    }
}
