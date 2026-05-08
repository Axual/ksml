package io.axual.ksml.data.notation.jsonschema;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import io.axual.ksml.data.notation.json.JsonDataObjectConverter;
import io.axual.ksml.data.notation.json.JsonSchemaLoader;
import io.axual.ksml.data.notation.vendor.VendorNotation;
import io.axual.ksml.data.notation.vendor.VendorNotationContext;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.StructType;
import io.axual.ksml.data.type.UnionType;

/**
 * JSON Schema notation implementation for KSML using vendor-backed serdes.
 */
public class JsonSchemaNotation extends VendorNotation {
    public static final String NOTATION_NAME = "jsonschema";
    public static final DataType DEFAULT_TYPE = new UnionType(
            new UnionType.Member(new StructType()),
            new UnionType.Member(new ListType()));
    private static final JsonDataObjectConverter CONVERTER = new JsonDataObjectConverter();
    private static final JsonSchemaLoader SCHEMA_PARSER = new JsonSchemaLoader();

    public JsonSchemaNotation(VendorNotationContext context) {
        super(context);
    }

    @Override
    public String notationName() {
        return NOTATION_NAME;
    }

    @Override
    public String filenameExtension() {
        return ".json";
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
}
