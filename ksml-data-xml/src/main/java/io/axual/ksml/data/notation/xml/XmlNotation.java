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

import io.axual.ksml.data.loader.SchemaLoader;
import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.notation.NotationConverter;
import io.axual.ksml.data.notation.string.StringNotation;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;

public class XmlNotation extends StringNotation {
    public static final DataType DEFAULT_TYPE = new StructType();
    private static final DataObjectMapper<String> MAPPER = new XmlDataObjectMapper();
    @Getter
    private final NotationConverter converter = new XmlDataObjectConverter();
    @Getter
    private final SchemaLoader loader;

    public XmlNotation(NativeDataObjectMapper nativeMapper, SchemaLoader loader) {
        super(nativeMapper, MAPPER);
        this.loader = loader;
    }

    @Override
    public DataType defaultType() {
        return DEFAULT_TYPE;
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        // XML types should ways be Maps (or Structs)
        if (type instanceof MapType) return super.serde(type, isKey);
        // Other types can not be serialized as XML
        throw noSerdeFor("XML", type);
    }
}
