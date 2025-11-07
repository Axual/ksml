package io.axual.ksml.dsl;

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

import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.schema.StructField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.DataSchemaConstants;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.DataType;

import java.util.ArrayList;

public class HeaderSchema {
    private HeaderSchema() {
    }

    // Public constants are the fixed schemas and the field names
    public static final StructSchema HEADER_SCHEMA = generateHeaderSchema();
    public static final DataType HEADER_TYPE = new DataTypeDataSchemaMapper().fromDataSchema(HEADER_SCHEMA);
    public static final String HEADER_SCHEMA_NAME = "Header";
    public static final String HEADER_SCHEMA_KEY_FIELD = "key";
    public static final String HEADER_SCHEMA_VALUE_FIELD = "value";
    private static final String HEADER_SCHEMA_KEY_DOC = "Header key";
    private static final String HEADER_SCHEMA_VALUE_DOC = "Header value";
    private static final String KAFKA_PREFIX = "Kafka ";

    private static StructSchema generateHeaderSchema() {
        final var headerFields = new ArrayList<StructField>();
        headerFields.add(new StructField(HEADER_SCHEMA_KEY_FIELD, DataSchema.STRING_SCHEMA, HEADER_SCHEMA_KEY_DOC, 1));
        headerFields.add(new StructField(HEADER_SCHEMA_VALUE_FIELD, DataSchema.ANY_SCHEMA, HEADER_SCHEMA_VALUE_DOC, 2));
        return new StructSchema(DataSchemaConstants.DATA_SCHEMA_KSML_NAMESPACE, HEADER_SCHEMA_NAME, KAFKA_PREFIX + HEADER_SCHEMA_NAME, headerFields, false);
    }
}
