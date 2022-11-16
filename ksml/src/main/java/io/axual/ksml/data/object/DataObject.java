package io.axual.ksml.data.object;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 Axual B.V.
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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.axual.ksml.data.type.DataType;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(name = "null", value = DataNull.class),
        @JsonSubTypes.Type(name = "boolean", value = DataBoolean.class),
        @JsonSubTypes.Type(name = "byte", value = DataByte.class),
        @JsonSubTypes.Type(name = "short", value = DataShort.class),
        @JsonSubTypes.Type(name = "int", value = DataInteger.class),
        @JsonSubTypes.Type(name = "long", value = DataLong.class),
        @JsonSubTypes.Type(name = "double", value = DataDouble.class),
        @JsonSubTypes.Type(name = "float", value = DataFloat.class),
        @JsonSubTypes.Type(name = "bytes", value = DataBytes.class),
        @JsonSubTypes.Type(name = "string", value = DataString.class),
        @JsonSubTypes.Type(name = "list", value = DataList.class),
        @JsonSubTypes.Type(name = "tuple", value = DataTuple.class),
        @JsonSubTypes.Type(name = "struct", value = DataStruct.class),
})
public interface DataObject {
    DataType type();
}
