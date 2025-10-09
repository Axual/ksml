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

public class DataTypeFlags {
    private DataTypeFlags() {
    }

    public static final String IGNORE_DATA_TYPE_CONTAINER_CLASS = "DataType.containerClass";
    public static final String IGNORE_ENUM_TYPE_SCHEMA = "EnumType.schema";
    public static final String IGNORE_STRUCT_TYPE_SCHEMA = "StructType.schema";
    public static final String IGNORE_UNION_TYPE_MEMBERS = "UnionType.members";
    public static final String IGNORE_UNION_TYPE_MEMBER_NAME = "UnionType.Member.name";
    public static final String IGNORE_UNION_TYPE_MEMBER_TAG = "UnionType.Member.tag";
    public static final String IGNORE_UNION_TYPE_MEMBER_TYPE = "UnionType.Member.type";
}
