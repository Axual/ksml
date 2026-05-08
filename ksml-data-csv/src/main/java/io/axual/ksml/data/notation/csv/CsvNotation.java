package io.axual.ksml.data.notation.csv;

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
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.UnionType;
import org.apache.kafka.common.serialization.Serde;

public class CsvNotation extends StringNotation {
    public static final String NOTATION_NAME = "csv";
    public static final DataType DEFAULT_TYPE = new UnionType(
            new UnionType.Member(new StructType()),
            new UnionType.Member(new ListType()));
    private static final CsvDataObjectConverter CONVERTER = new CsvDataObjectConverter();
    private static final CsvSchemaParser SCHEMA_PARSER = new CsvSchemaParser();
    private static final CsvDataObjectMapper STRING_MAPPER = new CsvDataObjectMapper();

    public CsvNotation() {
        this(null);
    }

    public CsvNotation(NotationContext context) {
        super(context);
    }

    @Override
    public String notationName() {
        return NOTATION_NAME;
    }

    @Override
    public String filenameExtension() {
        return ".csv";
    }

    @Override
    public SchemaUsage schemaUsage() {
        return SchemaUsage.SCHEMA_REQUIRED;
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
        return SCHEMA_PARSER;
    }

    @Override
    protected DataObjectMapper<String> stringMapper() {
        return STRING_MAPPER;
    }

    @Override
    public Serde<Object> serde(DataType type, boolean isKey) {
        // CSV types should always be Lists, Structs or the union of them both
        if (type instanceof ListType || type instanceof StructType || DEFAULT_TYPE.equals(type))
            return super.serde(type, isKey);
        // Other types cannot be serialized as CSV
        throw noSerdeFor(type);
    }
}
