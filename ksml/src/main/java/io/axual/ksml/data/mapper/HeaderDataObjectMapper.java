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
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import io.axual.ksml.data.serde.StringSerde;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.TupleType;

import static io.axual.ksml.dsl.HeaderSchema.*;

public class HeaderDataObjectMapper implements DataObjectMapper<Headers> {
    private static final StringSerde STRING_SERDE = new StringSerde();

    @Override
    public DataObject toDataObject(DataType expected, Headers value) {
        final var result = new DataList(HEADER_TYPE);
        value.forEach(header -> {
            final var element = new DataStruct(HEADER_SCHEMA);
            element.put(HEADER_SCHEMA_KEY_FIELD, new DataString(header.key()));
            element.put(HEADER_SCHEMA_VALUE_FIELD, convertHeaderValue(header.value()));
            result.add(element);
        });
        return result;
    }

    private DataObject convertHeaderValue(byte[] value) {
        try {
            var result = STRING_SERDE.deserializer().deserialize(null, value);
            return result != null ? new DataString(result.toString()) : DataNull.INSTANCE;
        } catch (Throwable t) {
            return new DataBytes(value);
        }
    }

    private byte[] convertHeaderValue(DataObject value) {
        if (value instanceof DataString val) {
            return STRING_SERDE.serializer().serialize(null, val);
        }
        if (value instanceof DataBytes val) {
            return val.value();
        }
        throw new IllegalArgumentException("Unsupported Kafka Header value type: " + value.type());
    }

    @Override
    public Headers fromDataObject(DataObject value) {
        final var result = new RecordHeaders();
        if (!(value instanceof DataList headers)) {
            throw new IllegalArgumentException("Invalid Kafka Headers type: " + value.type());
        }
        for (final var element : headers) {
            if (!(HEADER_TYPE.isAssignableFrom(element)) || !(element instanceof DataStruct header)) {
                throw new IllegalArgumentException("Invalid Kafka Header type: " + element.type());
            }
            if (header.size() != 2) {
                throw new IllegalArgumentException("Invalid Kafka Header: " + header);
            }
            final var hKey = header.get(HEADER_SCHEMA_KEY_FIELD);
            final var hValue = header.get(HEADER_SCHEMA_VALUE_FIELD);
            if (!(hKey instanceof DataString headerKey)) {
                throw new IllegalArgumentException("Invalid Kafka Header key type: " + hKey.type());
            }
            result.add(headerKey.value(), convertHeaderValue(hValue));
        }
        return result;
    }
}
