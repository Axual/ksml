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

import io.axual.ksml.data.compare.EqualityFlag;
import io.axual.ksml.data.compare.EqualityFlags;

/**
 * Flags to influence deep-equality comparisons of {@code DataSchema} graphs.
 *
 * <p>Provide these flags through {@link EqualityFlags} to selectively
 * ignore parts of schemas when comparing for structural equivalence.</p>
 */
public enum DataSchemaFlag implements EqualityFlag {
    // Ignore whether a DataField is constant
    IGNORE_DATA_FIELD_CONSTANT,
    // Ignore a DataField's default value
    IGNORE_DATA_FIELD_DEFAULT_VALUE,
    // Ignore a DataField's documentation
    IGNORE_DATA_FIELD_DOC,
    // Ignore a DataField's name
    IGNORE_DATA_FIELD_NAME,
    // Ignore a DataField's order
    IGNORE_DATA_FIELD_ORDER,
    // Ignore whether a DataField is required
    IGNORE_DATA_FIELD_REQUIRED,
    // Ignore a DataField's nested schema
    IGNORE_DATA_FIELD_SCHEMA,
    // Ignore a DataField's tag
    IGNORE_DATA_FIELD_TAG,
    // Ignore the top-level DataSchema type
    IGNORE_DATA_SCHEMA_TYPE,
    // Ignore an EnumSchema's default value
    IGNORE_ENUM_SCHEMA_DEFAULT_VALUE,
    // Ignore an EnumSchema's namespace
    IGNORE_ENUM_SCHEMA_NAMESPACE,
    // Ignore the set of enum symbols
    IGNORE_ENUM_SCHEMA_SYMBOLS,
    // Ignore an enum symbol's documentation
    IGNORE_ENUM_SCHEMA_SYMBOL_DOC,
    // Ignore an enum symbol's name
    IGNORE_ENUM_SCHEMA_SYMBOL_NAME,
    // Ignore an enum symbol's tag
    IGNORE_ENUM_SCHEMA_SYMBOL_TAG,
    // Ignore a FixedSchema's size
    IGNORE_FIXED_SCHEMA_SIZE,
    // Ignore a ListSchema's name
    IGNORE_LIST_SCHEMA_NAME,
    // Ignore a ListSchema's value schema
    IGNORE_LIST_SCHEMA_VALUE_SCHEMA,
    // Ignore a MapSchema's value schema
    IGNORE_MAP_SCHEMA_VALUE_SCHEMA,
    // Ignore a NamedSchema's documentation
    IGNORE_NAMED_SCHEMA_DOC,
    // Ignore a NamedSchema's name
    IGNORE_NAMED_SCHEMA_NAME,
    // Ignore a NamedSchema's namespace
    IGNORE_NAMED_SCHEMA_NAMESPACE,
    // Ignore whether StructSchema allows additional fields
    IGNORE_STRUCT_SCHEMA_ADDITIONAL_FIELDS_ALLOWED,
    // Ignore StructSchema's additional fields schema
    IGNORE_STRUCT_SCHEMA_ADDITIONAL_FIELDS_SCHEMA,
    // Ignore StructSchema's defined fields
    IGNORE_STRUCT_SCHEMA_FIELDS,
    // Ignore UnionSchema's members
    IGNORE_UNION_SCHEMA_MEMBERS,
    // Ignore a UnionSchema member's name
    IGNORE_UNION_SCHEMA_MEMBER_NAME,
    // Ignore a UnionSchema member's schema
    IGNORE_UNION_SCHEMA_MEMBER_SCHEMA,
    // Ignore a UnionSchema member's tag
    IGNORE_UNION_SCHEMA_MEMBER_TAG
}
