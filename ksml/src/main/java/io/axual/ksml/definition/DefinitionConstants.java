package io.axual.ksml.definition;

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


import io.axual.ksml.data.object.user.UserInteger;
import io.axual.ksml.data.object.user.UserString;
import io.axual.ksml.data.type.base.DataType;
import io.axual.ksml.data.type.base.MapType;
import io.axual.ksml.dsl.FunctionParameters;

public class DefinitionConstants {
    private DefinitionConstants() {
    }

    protected static final ParameterDefinition[] NO_PARAMETERS = new ParameterDefinition[]{};
    protected static final ParameterDefinition[] KEY_AND_TWO_VALUE_PARAMETERS = new ParameterDefinition[]{new ParameterDefinition(FunctionParameters.PARAM_KEY, DataType.UNKNOWN), new ParameterDefinition(FunctionParameters.PARAM_VALUE1, DataType.UNKNOWN), new ParameterDefinition(FunctionParameters.PARAM_VALUE2, DataType.UNKNOWN)};
    protected static final ParameterDefinition[] KEY_VALUE_AGGREGATEDVALUE_PARAMETERS = new ParameterDefinition[]{new ParameterDefinition(FunctionParameters.PARAM_KEY, DataType.UNKNOWN), new ParameterDefinition(FunctionParameters.PARAM_VALUE, DataType.UNKNOWN), new ParameterDefinition(FunctionParameters.PARAM_AGGREGATED_VALUE, DataType.UNKNOWN)};
    protected static final ParameterDefinition[] KEY_VALUE_PARAMETERS = new ParameterDefinition[]{new ParameterDefinition(FunctionParameters.PARAM_KEY, DataType.UNKNOWN), new ParameterDefinition(FunctionParameters.PARAM_VALUE, DataType.UNKNOWN)};
    protected static final ParameterDefinition[] STREAM_PARTITIONER_PARAMETERS = new ParameterDefinition[]{new ParameterDefinition(FunctionParameters.PARAM_TOPIC, UserString.DATATYPE), new ParameterDefinition(FunctionParameters.PARAM_KEY, DataType.UNKNOWN), new ParameterDefinition(FunctionParameters.PARAM_VALUE, DataType.UNKNOWN), new ParameterDefinition(FunctionParameters.PARAM_NUM_PARTITIONS, UserInteger.DATATYPE)};
    protected static final ParameterDefinition[] TOPIC_NAME_EXTRACTOR_PARAMETERS = new ParameterDefinition[]{new ParameterDefinition(FunctionParameters.PARAM_KEY, DataType.UNKNOWN), new ParameterDefinition(FunctionParameters.PARAM_VALUE, DataType.UNKNOWN), new ParameterDefinition(FunctionParameters.PARAM_RECORD_CONTEXT, new MapType(UserString.DATATYPE, DataType.UNKNOWN, null))};
    protected static final ParameterDefinition[] TWO_VALUE_PARAMETERS = new ParameterDefinition[]{new ParameterDefinition(FunctionParameters.PARAM_VALUE1, DataType.UNKNOWN), new ParameterDefinition(FunctionParameters.PARAM_VALUE2, DataType.UNKNOWN)};
}
