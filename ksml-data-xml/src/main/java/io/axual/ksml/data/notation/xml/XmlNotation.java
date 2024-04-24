package io.axual.ksml.data.notation.xml;

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
import io.axual.ksml.data.notation.string.StringNotation;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import org.apache.kafka.common.serialization.Serde;

public class XmlNotation extends StringNotation {
    public static final String NOTATION_NAME = "XML";
    public static final DataType DEFAULT_TYPE = new StructType();
    private static final XmlDataObjectMapper MAPPER = new XmlDataObjectMapper();

    public XmlNotation() {
        super(new DataObjectMapper<>() {
            @Override
            public DataObject toDataObject(DataType expected, String value) {
                return MAPPER.toDataObject(expected, value);
            }

            @Override
            public String fromDataObject(DataObject value) {
                return MAPPER.fromDataObject(value);
            }
        });
    }

    @Override
    public String name() {
        return NOTATION_NAME;
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        // XML types should ways be Maps (or Structs)
        if (type instanceof MapType) return super.serde(type, isKey);
        // Other types can not be serialized as XML
        throw noSerdeFor(type);
    }
}
