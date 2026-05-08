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
import io.axual.ksml.data.notation.NotationContext;
import io.axual.ksml.data.notation.string.StringNotation;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import org.apache.kafka.common.serialization.Serde;

public class XmlNotation extends StringNotation {
    public static final String NOTATION_NAME = "xml";
    public static final DataType DEFAULT_TYPE = new StructType();
    private static final XmlDataObjectConverter CONVERTER = new XmlDataObjectConverter();
    private static final XmlDataObjectMapper STRING_MAPPER = new XmlDataObjectMapper(false);

    private SchemaParser lazySchemaParser;

    public XmlNotation() {
        this(null);
    }

    public XmlNotation(NotationContext context) {
        super(context);
    }

    @Override
    public String notationName() {
        return NOTATION_NAME;
    }

    @Override
    public String filenameExtension() {
        return ".xsd";
    }

    @Override
    public SchemaUsage schemaUsage() {
        return SchemaUsage.SCHEMA_OPTIONAL;
    }

    @Override
    public DataType defaultType() {
        return DEFAULT_TYPE;
    }

    @Override
    public Converter converter() {
        return CONVERTER;
    }

    @Override
    public SchemaParser schemaParser() {
        if (lazySchemaParser == null) {
            lazySchemaParser = new XmlSchemaParser(
                    context().nativeDataObjectMapper(),
                    context().typeSchemaMapper());
        }
        return lazySchemaParser;
    }

    @Override
    protected DataObjectMapper<String> stringMapper() {
        return STRING_MAPPER;
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        // XML types should always be Maps (or Structs)
        if (type instanceof MapType || type instanceof StructType) return super.serde(type, isKey);
        // Other types cannot be serialized as XML
        throw noSerdeFor(type);
    }
}
