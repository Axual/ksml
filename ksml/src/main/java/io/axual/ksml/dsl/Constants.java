package io.axual.ksml.dsl;

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



import io.axual.ksml.type.DataType;
import io.axual.ksml.type.SimpleType;

public class Constants {
    private Constants() {
    }

    public static final ParameterDefinition[] NO_PARAMETERS = new ParameterDefinition[]{};
    public static final ParameterDefinition[] KEY_AND_TWO_VALUE_PARAMETERS = new ParameterDefinition[]{new ParameterDefinition("key", DataType.UNKNOWN), new ParameterDefinition("value1", DataType.UNKNOWN), new ParameterDefinition("value2", DataType.UNKNOWN)};
    public static final ParameterDefinition[] KEY_VALUE_AGGREGATEDVALUE_PARAMETERS = new ParameterDefinition[]{new ParameterDefinition("key", DataType.UNKNOWN), new ParameterDefinition("value", DataType.UNKNOWN), new ParameterDefinition("aggregatedValue", DataType.UNKNOWN)};
    public static final ParameterDefinition[] KEY_VALUE_PARAMETERS = new ParameterDefinition[]{new ParameterDefinition("key", DataType.UNKNOWN), new ParameterDefinition("value", DataType.UNKNOWN)};
    public static final ParameterDefinition[] STREAM_PARTITIONER_PARAMETERS = new ParameterDefinition[]{new ParameterDefinition("topic", SimpleType.STRING), new ParameterDefinition("key", DataType.UNKNOWN), new ParameterDefinition("value", DataType.UNKNOWN), new ParameterDefinition("numPartitions", SimpleType.INTEGER)};
    public static final ParameterDefinition[] TWO_VALUE_PARAMETERS = new ParameterDefinition[]{new ParameterDefinition("value1", DataType.UNKNOWN), new ParameterDefinition("value2", DataType.UNKNOWN)};
}
