package io.axual.ksml;

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



import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Map;

import io.axual.ksml.generator.DefaultSerdeGenerator;
import io.axual.ksml.generator.SerdeGenerator;

@InterfaceStability.Evolving
public class KSMLConfig {
    public static final String PYTHON_INTERPRETER_ISOLATION = "python.interpreter.isolation";
    public static final String KSML_SOURCE_TYPE = "ksml.source.type";
    public static final String KSML_SOURCE = "ksml.source";
    public static final String KSML_WORKING_DIRECTORY = "ksml.dir";
    public static final String SERDE_GENERATOR = "serde.generator";

    public final boolean interpreterIsolation;
    public final String sourceType;
    public final String workingDirectory;
    public final Object source;
    public final SerdeGenerator serdeGenerator;

    public KSMLConfig(Map<String, ?> configs) {
        interpreterIsolation = Boolean.parseBoolean((String) configs.get(PYTHON_INTERPRETER_ISOLATION));
        sourceType = configs.containsKey(KSML_SOURCE_TYPE) ? (String) configs.get(KSML_SOURCE_TYPE) : "file";
        source = configs.get(KSMLConfig.KSML_SOURCE);
        workingDirectory = (String) configs.get(KSML_WORKING_DIRECTORY);
        serdeGenerator = configs.containsKey(SERDE_GENERATOR)
                ? (SerdeGenerator) configs.get(SERDE_GENERATOR)
                : new DefaultSerdeGenerator((Map<String, Object>) configs);
    }
}
