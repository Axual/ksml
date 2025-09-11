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

import com.google.common.base.CharMatcher;
import io.axual.ksml.data.object.DataByte;
import io.axual.ksml.data.object.DataBytes;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataShort;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.serde.StringSerde;
import io.axual.ksml.data.type.DataType;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import static io.axual.ksml.dsl.HeaderSchema.HEADER_SCHEMA;
import static io.axual.ksml.dsl.HeaderSchema.HEADER_SCHEMA_KEY_FIELD;
import static io.axual.ksml.dsl.HeaderSchema.HEADER_SCHEMA_VALUE_FIELD;
import static io.axual.ksml.dsl.HeaderSchema.HEADER_TYPE;

public class HeaderDataObjectMapper implements DataObjectMapper<Headers> {
    private static final StringSerde STRING_SERDE = new StringSerde(new NativeDataObjectMapper());

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
            final var result = STRING_SERDE.deserializer().deserialize(null, value);
            if (result == null) return DataNull.INSTANCE;
            return isRealString(result.toString()) ? new DataString(result.toString()) : new DataBytes(value);
        } catch (Exception e) {
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
        if (value instanceof DataList list) {
            final var bytes = new byte[list.size()];
            for (var index = 0; index < list.size(); index++) {
                final var element = list.get(index);
                switch (element) {
                    case DataByte val:
                        bytes[index] = val.value();
                        break;
                    case DataShort val:
                        bytes[index] = val.value().byteValue();
                        break;
                    case DataInteger val:
                        bytes[index] = val.value().byteValue();
                        break;
                    case DataLong val:
                        bytes[index] = val.value().byteValue();
                        break;
                    default:
                        throw new IllegalArgumentException("Can not convert binary header: " + value);
                }
            }
            return bytes;
        }
        throw new IllegalArgumentException("Unsupported Kafka Header value type: " + value.type());
    }

    private boolean isRealString(String value) {
        for (var index = 0; index < value.length(); index++) {
            final var ch = value.charAt(index);
            if (!Character.isLetterOrDigit(ch)
                    && !CharMatcher.breakingWhitespace().matches(ch)
                    && !CharMatcher.whitespace().matches(ch)) {
                return false;
            }
        }
        return true;
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
