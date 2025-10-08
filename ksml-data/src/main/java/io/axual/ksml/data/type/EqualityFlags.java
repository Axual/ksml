package io.axual.ksml.data.type;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
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

public class EqualityFlags {
    private EqualityFlags() {
    }

    public static final String IGNORE_DATA_FIELD_CONSTANT = "DataField.constant";
    public static final String IGNORE_DATA_FIELD_DEFAULT_VALUE = "DataField.defaultValue";
    public static final String IGNORE_DATA_FIELD_DOC = "DataField.doc";
    public static final String IGNORE_DATA_FIELD_NAME = "DataField.name";
    public static final String IGNORE_DATA_FIELD_ORDER = "DataField.order";
    public static final String IGNORE_DATA_FIELD_REQUIRED = "DataField.required";
    public static final String IGNORE_DATA_FIELD_SCHEMA = "DataField.schema";
    public static final String IGNORE_DATA_FIELD_TAG = "DataField.tag";
    public static final String IGNORE_DATA_LIST_CONTENTS = "DataStruct.contents";
    public static final String IGNORE_DATA_LIST_TYPE = "DataStruct.type";
    public static final String IGNORE_DATA_MAP_CONTENTS = "DataMap.contents";
    public static final String IGNORE_DATA_MAP_TYPE = "DataMap.type";
    public static final String IGNORE_DATA_SCHEMA_TYPE = "DataSchema.type";
    public static final String IGNORE_DATA_STRUCT_CONTENTS = "DataStruct.contents";
    public static final String IGNORE_DATA_STRUCT_TYPE = "DataStruct.type";
    public static final String IGNORE_DATA_TUPLE_CONTENTS = "DataTuple.contents";
    public static final String IGNORE_DATA_TUPLE_TYPE = "DataTuple.type";
    public static final String IGNORE_DATA_TYPE_CONTAINER_CLASS = "DataType.containerClass";
    public static final String IGNORE_ENUM_SCHEMA_DEFAULT_VALUE = "EnumSchema.defaultValue";
    public static final String IGNORE_ENUM_SCHEMA_SYMBOLS = "EnumSchema.symbols";
    public static final String IGNORE_ENUM_SYMBOL_DOC = "EnumSymbol.doc";
    public static final String IGNORE_ENUM_SYMBOL_NAME = "EnumSymbol.name";
    public static final String IGNORE_ENUM_SYMBOL_TAG = "EnumSymbol.tag";
    public static final String IGNORE_FIXED_SCHEMA_SIZE = "FixedSchema.size";
    public static final String IGNORE_LIST_SCHEMA_NAME = "ListSchema.name";
    public static final String IGNORE_LIST_SCHEMA_VALUE_SCHEMA = "ListSchema.valueSchema";
    public static final String IGNORE_MAP_SCHEMA_VALUE_SCHEMA = "MapSchema.valueSchema";
    public static final String IGNORE_NAMED_SCHEMA_DOC = "NamedSchema.doc";
    public static final String IGNORE_NAMED_SCHEMA_NAME = "NamedSchema.name";
    public static final String IGNORE_NAMED_SCHEMA_NAMESPACE = "NamedSchema.namespace";
    public static final String IGNORE_STRUCT_SCHEMA_ADDITIONAL_FIELDS_ALLOWED = "StructSchema.additionalFieldsAllowed";
    public static final String IGNORE_STRUCT_SCHEMA_ADDITIONAL_FIELDS_SCHEMA = "StructSchema.additionalFieldsSchema";
    public static final String IGNORE_STRUCT_SCHEMA_FIELDS = "StructSchema.fields";
    public static final String IGNORE_STRUCT_TYPE_SCHEMA = "StructType.schema";
    public static final String IGNORE_UNION_SCHEMA_MEMBERS = "UnionSchema.members";
    public static final String IGNORE_UNION_SCHEMA_MEMBER_NAME = "UnionSchema.Member.name";
    public static final String IGNORE_UNION_SCHEMA_MEMBER_SCHEMA = "UnionSchema.Member.schema";
    public static final String IGNORE_UNION_SCHEMA_MEMBER_TAG = "UnionSchema.Member.tag";
    public static final String IGNORE_UNION_TYPE_MEMBERS = "UnionType.members";
    public static final String IGNORE_UNION_TYPE_MEMBER_NAME = "UnionType.Member.name";
    public static final String IGNORE_UNION_TYPE_MEMBER_TAG = "UnionType.Member.tag";
    public static final String IGNORE_UNION_TYPE_MEMBER_TYPE = "UnionType.Member.type";
}
