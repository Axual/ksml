package io.axual.ksml.data.parser.schema;

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

public class DataSchemaDSL {
    private DataSchemaDSL() {
    }

    public static final String DATA_SCHEMA_NAMESPACE = "io.axual.ksml.data";
    public static final String DATA_OBJECT_TYPE_NAME = "DataObject";
    public static final String DATA_SCHEMA_TYPE_FIELD = "type";
    public static final String NAMED_SCHEMA_NAMESPACE_FIELD = "namespace";
    public static final String NAMED_SCHEMA_NAME_FIELD = "name";
    public static final String NAMED_SCHEMA_DOC_FIELD = "doc";
    public static final String ENUM_SCHEMA_SYMBOLS_FIELD = "symbols";
    public static final String ENUM_SCHEMA_DEFAULTVALUE_FIELD = "defaultValue";
    public static final String FIXED_SCHEMA_SIZE_FIELD = "size";
    public static final String LIST_SCHEMA_VALUES_FIELD = "items";
    public static final String MAP_SCHEMA_VALUES_FIELD = "values";
    public static final String STRUCT_SCHEMA_FIELDS_FIELD = "fields";
    public static final String DATA_FIELD_NAME_FIELD = "name";
    public static final String DATA_FIELD_SCHEMA_FIELD = "type";
    public static final String DATA_FIELD_DOC_FIELD = "doc";
    public static final String DATA_FIELD_REQUIRED_FIELD = "required";
    public static final String DATA_FIELD_CONSTANT_FIELD = "constant";
    public static final String DATA_FIELD_INDEX_FIELD = "index";
    public static final String DATA_FIELD_DEFAULT_VALUE_FIELD = "defaultValue";
    public static final String DATA_FIELD_ORDER_FIELD = "order";
    public static final String UNKNOWN_TYPE = "?";
    public static final String ANY_TYPE = "any";
    public static final String NULL_TYPE = "null";
    public static final String NONE_TYPE = "none";
    public static final String BOOLEAN_TYPE = "boolean";
    public static final String BYTE_TYPE = "byte";
    public static final String SHORT_TYPE = "short";
    public static final String INTEGER_TYPE = "integer";
    public static final String INTEGER_TYPE_ALTERNATIVE = "int";
    public static final String LONG_TYPE = "long";
    public static final String DOUBLE_TYPE = "double";
    public static final String FLOAT_TYPE = "float";
    public static final String BYTES_TYPE = "bytes";
    public static final String FIXED_TYPE = "fixed";
    public static final String STRING_TYPE = "string";
    public static final String STRING_TYPE_ALTERNATIVE = "str";
    public static final String ENUM_TYPE = "enum";
    public static final String LIST_TYPE = "list";
    public static final String LIST_TYPE_ALTERNATIVE = "array";
    public static final String MAP_TYPE = "map";
    public static final String STRUCT_TYPE = "struct";
    public static final String STRUCT_TYPE_ALTERNATIVE = "record";
    public static final String UNION_TYPE = "union";
    public static final String WINDOWED_TYPE = "windowed";
}
