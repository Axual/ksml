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

import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;

import static io.axual.ksml.dsl.RecordContextSchema.RECORD_CONTEXT_SCHEMA_HEADERS_FIELD;
import static io.axual.ksml.dsl.RecordContextSchema.RECORD_CONTEXT_SCHEMA_TIMESTAMP_FIELD;
import static io.axual.ksml.dsl.RecordMetadataSchema.*;

public class RecordMetadataDataObjectMapper implements DataObjectMapper<RecordMetadata> {
    private static final HeaderDataObjectMapper HEADER_MAPPER = new HeaderDataObjectMapper();

    @Override
    public DataObject toDataObject(DataType expected, RecordMetadata value) {
        final var result = new DataStruct(RECORD_METADATA_SCHEMA);
        result.put(RECORD_METADATA_SCHEMA_TIMESTAMP_FIELD, new DataLong(value.timestamp()));
        result.put(RECORD_METADATA_SCHEMA_HEADERS_FIELD, HEADER_MAPPER.toDataObject(value.headers()));
        return result;
    }

    @Override
    public RecordMetadata fromDataObject(DataObject value) {
        if (!(value instanceof DataStruct valueStruct)) {
            throw new IllegalArgumentException("Can not convert to RecordMetadata from type " + value);
        }
        final var timestamp = valueStruct.getAs(RECORD_CONTEXT_SCHEMA_TIMESTAMP_FIELD, DataLong.class);
        final var headers = valueStruct.getAs(RECORD_CONTEXT_SCHEMA_HEADERS_FIELD, DataStruct.class);
        return new RecordMetadata(
                timestamp != null ? timestamp.value() : null,
                headers != null ? HEADER_MAPPER.fromDataObject(headers) : new RecordHeaders()
        );
    }
}
