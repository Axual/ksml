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

import io.axual.ksml.notation.NotationLibrary;

/**
 * Configuration for generating and running KSML definitions.
 */
@InterfaceStability.Evolving
public class KSMLConfig {
    public static final String KSML_SOURCE_TYPE = "ksml.source.type";
    public static final String KSML_SOURCE = "ksml.source";
    public static final String KSML_WORKING_DIRECTORY = "ksml.dir";
    public static final String NOTATION_LIBRARY = "notation.library";

    public final String sourceType;
    public final String workingDirectory;
    public final Object source;
    public final NotationLibrary notationLibrary;

    public KSMLConfig(Map<String, ?> configs) {
        sourceType = configs.containsKey(KSML_SOURCE_TYPE) ? (String) configs.get(KSML_SOURCE_TYPE) : "file";
        source = configs.get(KSMLConfig.KSML_SOURCE);
        workingDirectory = (String) configs.get(KSML_WORKING_DIRECTORY);
        notationLibrary = configs.containsKey(NOTATION_LIBRARY)
                ? (NotationLibrary) configs.get(NOTATION_LIBRARY)
                : new NotationLibrary(configs);
    }
}
