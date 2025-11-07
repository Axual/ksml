package io.axual.ksml.data.object;

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
 * Flags used to influence deep-equality comparisons for {@code DataObject} implementations.
 *
 * <p>These flags can be provided via {@link EqualityFlags} to relax
 * comparisons for certain structural aspects of concrete {@code DataObject}s (lists, maps,
 * structs, tuples, and primitives). This is useful when callers want to focus equality on
 * values or on structure only.</p>
 */
public enum DataObjectFlag implements EqualityFlag {
    /** Ignore the contents (elements) of a {@code DataList}. */
    IGNORE_DATA_LIST_CONTENTS,
    /** Ignore the type of {@code DataList}. */
    IGNORE_DATA_LIST_TYPE,
    /** Ignore the entries (key/value pairs) of a {@code DataMap}. */
    IGNORE_DATA_MAP_CONTENTS,
    /** Ignore the type of {@code DataMap}. */
    IGNORE_DATA_MAP_TYPE,
    /** Ignore the type of {@code DataPrimitive}. */
    IGNORE_DATA_PRIMITIVE_TYPE,
    /** Ignore the value of a {@code DataPrimitive}. */
    IGNORE_DATA_PRIMITIVE_VALUE,
    /** Ignore the contents of a {@code DataStruct}. */
    IGNORE_DATA_STRUCT_CONTENTS,
    /** Ignore the type of {@code DataStruct}. */
    IGNORE_DATA_STRUCT_TYPE,
    /** Ignore the elements of a {@code DataTuple}. */
    IGNORE_DATA_TUPLE_CONTENTS,
    /** Ignore the type of {@code DataTuple}. */
    IGNORE_DATA_TUPLE_TYPE
}