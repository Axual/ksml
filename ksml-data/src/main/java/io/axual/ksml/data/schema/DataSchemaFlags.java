package io.axual.ksml.data.schema;

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

/**
 * Flags to influence deep-equality comparisons of {@code DataSchema} graphs.
 *
 * <p>Provide these names through {@link io.axual.ksml.data.type.Flags} to selectively
 * ignore parts of schemas when comparing for structural equivalence.</p>
 */
public class DataSchemaFlags {
    private DataSchemaFlags() {
    }

    /** Ignore whether a DataField is constant */
    public static final String IGNORE_DATA_FIELD_CONSTANT = "DataField.constant";
    /** Ignore a DataField's default value */
    public static final String IGNORE_DATA_FIELD_DEFAULT_VALUE = "DataField.defaultValue";
    /** Ignore a DataField's documentation */
    public static final String IGNORE_DATA_FIELD_DOC = "DataField.doc";
    /** Ignore a DataField's name */
    public static final String IGNORE_DATA_FIELD_NAME = "DataField.name";
    /** Ignore a DataField's order */
    public static final String IGNORE_DATA_FIELD_ORDER = "DataField.order";
    /** Ignore whether a DataField is required */
    public static final String IGNORE_DATA_FIELD_REQUIRED = "DataField.required";
    /** Ignore a DataField's nested schema */
    public static final String IGNORE_DATA_FIELD_SCHEMA = "DataField.schema";
    /** Ignore a DataField's tag */
    public static final String IGNORE_DATA_FIELD_TAG = "DataField.tag";
    /** Ignore the top-level DataSchema type */
    public static final String IGNORE_DATA_SCHEMA_TYPE = "DataSchema.type";
    /** Ignore an EnumSchema's default value */
    public static final String IGNORE_ENUM_SCHEMA_DEFAULT_VALUE = "EnumSchema.defaultValue";
    /** Ignore the set of enum symbols */
    public static final String IGNORE_ENUM_SCHEMA_SYMBOLS = "EnumSchema.symbols";
    /** Ignore an enum symbol's documentation */
    public static final String IGNORE_ENUM_SCHEMA_SYMBOL_DOC = "EnumSchema.Symbol.doc";
    /** Ignore an enum symbol's name */
    public static final String IGNORE_ENUM_SCHEMA_SYMBOL_NAME = "EnumSchema.Symbol.name";
    /** Ignore an enum symbol's tag */
    public static final String IGNORE_ENUM_SCHEMA_SYMBOL_TAG = "EnumSchema.Symbol.tag";
    /** Ignore a FixedSchema's size */
    public static final String IGNORE_FIXED_SCHEMA_SIZE = "FixedSchema.size";
    /** Ignore a ListSchema's name */
    public static final String IGNORE_LIST_SCHEMA_NAME = "ListSchema.name";
    /** Ignore a ListSchema's value schema */
    public static final String IGNORE_LIST_SCHEMA_VALUE_SCHEMA = "ListSchema.valueSchema";
    /** Ignore a MapSchema's value schema */
    public static final String IGNORE_MAP_SCHEMA_VALUE_SCHEMA = "MapSchema.valueSchema";
    /** Ignore a NamedSchema's documentation */
    public static final String IGNORE_NAMED_SCHEMA_DOC = "NamedSchema.doc";
    /** Ignore a NamedSchema's name */
    public static final String IGNORE_NAMED_SCHEMA_NAME = "NamedSchema.name";
    /** Ignore a NamedSchema's namespace */
    public static final String IGNORE_NAMED_SCHEMA_NAMESPACE = "NamedSchema.namespace";
    /** Ignore whether StructSchema allows additional fields */
    public static final String IGNORE_STRUCT_SCHEMA_ADDITIONAL_FIELDS_ALLOWED = "StructSchema.additionalFieldsAllowed";
    /** Ignore StructSchema's additional fields schema */
    public static final String IGNORE_STRUCT_SCHEMA_ADDITIONAL_FIELDS_SCHEMA = "StructSchema.additionalFieldsSchema";
    /** Ignore StructSchema's defined fields */
    public static final String IGNORE_STRUCT_SCHEMA_FIELDS = "StructSchema.fields";
    /** Ignore UnionSchema's members */
    public static final String IGNORE_UNION_SCHEMA_MEMBERS = "UnionSchema.members";
    /** Ignore a UnionSchema member's name */
    public static final String IGNORE_UNION_SCHEMA_MEMBER_NAME = "UnionSchema.Member.name";
    /** Ignore a UnionSchema member's schema */
    public static final String IGNORE_UNION_SCHEMA_MEMBER_SCHEMA = "UnionSchema.Member.schema";
    /** Ignore a UnionSchema member's tag */
    public static final String IGNORE_UNION_SCHEMA_MEMBER_TAG = "UnionSchema.Member.tag";
}
