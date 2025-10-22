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

/**
 * Flags used to influence deep-equality comparisons for {@code DataType} implementations.
 *
 * <p>These string constants can be supplied via {@link io.axual.ksml.data.type.Flags}
 * to selectively ignore certain aspects of type metadata when comparing
 * {@code DataType} instances (for example in DataType.equals(Flags)–style logic).
 * This allows callers to relax comparisons in scenarios where full structural
 * equality is not required.</p>
 */
public class DataTypeFlags {
    private DataTypeFlags() {
    }

    /** Ignore differences in the container class that backs a DataType */
    public static final String IGNORE_DATA_TYPE_CONTAINER_CLASS = "DataType.containerClass";
    /** Ignore the schema attached to an {@code EnumType} when comparing */
    public static final String IGNORE_ENUM_TYPE_SCHEMA = "EnumType.schema";
    /** Ignore the schema attached to a {@code StructType} when comparing */
    public static final String IGNORE_STRUCT_TYPE_SCHEMA = "StructType.schema";
    /** Ignore membership differences between {@code UnionType}s */
    public static final String IGNORE_UNION_TYPE_MEMBERS = "UnionType.members";
    /** Ignore the name of a {@code UnionType.Member} when comparing */
    public static final String IGNORE_UNION_TYPE_MEMBER_NAME = "UnionType.Member.name";
    /** Ignore the tag of a {@code UnionType.Member} when comparing */
    public static final String IGNORE_UNION_TYPE_MEMBER_TAG = "UnionType.Member.tag";
    /** Ignore the type of {@code UnionType.Member} when comparing */
    public static final String IGNORE_UNION_TYPE_MEMBER_TYPE = "UnionType.Member.type";
}
