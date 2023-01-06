package io.axual.ksml.exception;

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


import io.axual.ksml.parser.YamlNode;

public class KSMLParseException extends KSMLException {
    private static final String ACTIVITY = "parsing";

    public KSMLParseException(YamlNode node, String message) {
        this(node, message, null);
    }

    public KSMLParseException(YamlNode node, String message, Throwable cause) {
        super(ACTIVITY, "Error in YAML node " + (node != null ? node.toString() : "<root>") + ": " + message, cause);
    }

    public KSMLParseException(String message) {
        super(ACTIVITY, message);
    }
}
