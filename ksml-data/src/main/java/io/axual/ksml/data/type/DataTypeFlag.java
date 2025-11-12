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

import io.axual.ksml.data.compare.EqualityFlag;
import io.axual.ksml.data.compare.EqualityFlags;

/**
 * Flags used to influence deep-equality comparisons for {@code DataType} implementations.
 *
 * <p>These flags can be supplied via {@link EqualityFlags}
 * to selectively ignore certain aspects of type metadata when comparing
 * {@code DataType} instances (for example in DataType.equals(Flags)–style logic).
 * This allows callers to relax comparisons in scenarios where full structural
 * equality is not required.</p>
 */
public enum DataTypeFlag implements EqualityFlag {
    // Ignore differences in the container class that backs a DataType
    IGNORE_DATA_TYPE_CONTAINER_CLASS,
    // Ignore the schema attached to an EnumType when comparing
    IGNORE_ENUM_TYPE_SCHEMA,
    // Ignore the schema attached to a StructType when comparing
    IGNORE_STRUCT_TYPE_SCHEMA,
    // Ignore membership differences between UnionTypes
    IGNORE_UNION_TYPE_MEMBERS,
    // Ignore the doc of a UnionType.Member when comparing
    IGNORE_UNION_TYPE_MEMBER_DOC,
    // Ignore the name of a UnionType.Member when comparing
    IGNORE_UNION_TYPE_MEMBER_NAME,
    // Ignore the tag of a UnionType.Member when comparing
    IGNORE_UNION_TYPE_MEMBER_TAG,
    // Ignore the type of UnionType.Member when comparing
    IGNORE_UNION_TYPE_MEMBER_TYPE
}
