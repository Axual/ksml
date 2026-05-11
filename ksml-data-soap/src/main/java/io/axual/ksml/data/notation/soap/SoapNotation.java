package io.axual.ksml.data.notation.soap;

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
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.StructType;
import org.apache.kafka.common.serialization.Serde;

import static io.axual.ksml.data.notation.soap.SoapSchema.generateSOAPSchema;

@Deprecated(forRemoval = true, since = "1.3.0")
public class SoapNotation extends StringNotation {
    public static final String NOTATION_NAME = "soap";
    public static final DataType DEFAULT_TYPE = new StructType(generateSOAPSchema(DataSchema.ANY_SCHEMA));
    private static final SoapDataObjectMapper DATA_OBJECT_MAPPER = new SoapDataObjectMapper();
    private static final SoapStringMapper STRING_MAPPER = new SoapStringMapper();
    private static final SoapDataObjectConverter CONVERTER = new SoapDataObjectConverter();

    private static final DataObjectMapper<String> SOAP_STRING_MAPPER = new DataObjectMapper<>() {
        @Override
        public DataObject toDataObject(DataType expected, String value) {
            return DATA_OBJECT_MAPPER.toDataObject(expected, STRING_MAPPER.fromString(value));
        }

        @Override
        public String fromDataObject(DataObject value) {
            return STRING_MAPPER.toString(DATA_OBJECT_MAPPER.fromDataObject(value));
        }
    };

    public SoapNotation(NotationContext context) {
        super(context);
    }

    @Override
    public String notationName() {
        return NOTATION_NAME;
    }

    @Override
    public String filenameExtension() {
        return null;
    }

    @Override
    public SchemaUsage schemaUsage() {
        return SchemaUsage.SCHEMALESS_ONLY;
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
        return null;
    }

    @Override
    protected DataObjectMapper<String> stringMapper() {
        return SOAP_STRING_MAPPER;
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        // SOAP types should always be Maps (or Structs)
        if (type instanceof MapType) return super.serde(type, isKey);
        // Other types cannot be serialized as SOAP
        throw noSerdeFor(type);
    }
}
