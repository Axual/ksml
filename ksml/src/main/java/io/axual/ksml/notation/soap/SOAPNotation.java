package io.axual.ksml.notation.soap;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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

import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.schema.AnySchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.notation.string.StringNotation;
import org.apache.kafka.common.serialization.Serde;

import static io.axual.ksml.dsl.SOAPSchema.generateSOAPSchema;

public class SOAPNotation extends StringNotation {
    public static final String NOTATION_NAME = "SOAP";
    public static final DataType DEFAULT_TYPE = new StructType(generateSOAPSchema(AnySchema.INSTANCE));
    private static final SOAPDataObjectMapper DATA_OBJECT_MAPPER = new SOAPDataObjectMapper();
    private static final SOAPStringMapper STRING_MAPPER = new SOAPStringMapper();

    public SOAPNotation() {
        super(new DataObjectMapper<>() {
            @Override
            public DataObject toDataObject(DataType expected, String value) {
                return DATA_OBJECT_MAPPER.toDataObject(expected, STRING_MAPPER.fromString(value));
            }

            @Override
            public String fromDataObject(DataObject value) {
                return STRING_MAPPER.toString(DATA_OBJECT_MAPPER.fromDataObject(value));
            }
        });
    }

    @Override
    public String name() {
        return NOTATION_NAME;
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        // SOAP types should ways be Maps (or Structs)
        if (type instanceof MapType) return super.serde(type, isKey);
        // Other types can not be serialized as SOAP
        throw noSerdeFor(type);
    }
}
