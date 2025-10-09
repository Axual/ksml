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

public class DataObjectFlags {
    private DataObjectFlags() {
    }

    public static final String IGNORE_DATA_LIST_CONTENTS = "DataStruct.contents";
    public static final String IGNORE_DATA_LIST_TYPE = "DataStruct.type";
    public static final String IGNORE_DATA_MAP_CONTENTS = "DataMap.contents";
    public static final String IGNORE_DATA_MAP_TYPE = "DataMap.type";
    public static final String IGNORE_DATA_PRIMITIVE_TYPE = "DataPrimitive.type";
    public static final String IGNORE_DATA_PRIMITIVE_VALUE = "DataPrimitive.value";
    public static final String IGNORE_DATA_STRUCT_CONTENTS = "DataStruct.contents";
    public static final String IGNORE_DATA_STRUCT_TYPE = "DataStruct.type";
    public static final String IGNORE_DATA_TUPLE_CONTENTS = "DataTuple.contents";
    public static final String IGNORE_DATA_TUPLE_TYPE = "DataTuple.type";
}
