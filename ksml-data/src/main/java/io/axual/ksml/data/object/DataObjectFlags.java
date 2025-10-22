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

/**
 * Flags used to influence deep-equality comparisons for {@code DataObject} implementations.
 *
 * <p>These constants can be provided via {@link io.axual.ksml.data.type.Flags} to relax
 * comparisons for certain structural aspects of concrete {@code DataObject}s (lists, maps,
 * structs, tuples, and primitives). This is useful when callers want to focus equality on
 * values or on structure only.</p>
 */
public class DataObjectFlags {
    private DataObjectFlags() {
    }

    /** Ignore the contents (elements) of a {@code DataList}. */
    public static final String IGNORE_DATA_LIST_CONTENTS = "DataStruct.contents";
    /** Ignore the type of {@code DataList}. */
    public static final String IGNORE_DATA_LIST_TYPE = "DataStruct.type";
    /** Ignore the entries (key/value pairs) of a {@code DataMap}. */
    public static final String IGNORE_DATA_MAP_CONTENTS = "DataMap.contents";
    /** Ignore the type of {@code DataMap}. */
    public static final String IGNORE_DATA_MAP_TYPE = "DataMap.type";
    /** Ignore the type of {@code DataPrimitive}. */
    public static final String IGNORE_DATA_PRIMITIVE_TYPE = "DataPrimitive.type";
    /** Ignore the value of a {@code DataPrimitive}. */
    public static final String IGNORE_DATA_PRIMITIVE_VALUE = "DataPrimitive.value";
    /** Ignore the contents of a {@code DataStruct}. */
    public static final String IGNORE_DATA_STRUCT_CONTENTS = "DataStruct.contents";
    /** Ignore the type of {@code DataStruct}. */
    public static final String IGNORE_DATA_STRUCT_TYPE = "DataStruct.type";
    /** Ignore the elements of a {@code DataTuple}. */
    public static final String IGNORE_DATA_TUPLE_CONTENTS = "DataTuple.contents";
    /** Ignore the type of {@code DataTuple}. */
    public static final String IGNORE_DATA_TUPLE_TYPE = "DataTuple.type";
}
