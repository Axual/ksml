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


import io.axual.ksml.type.DataType;

public class KSMLTypeException extends RuntimeException {
    public KSMLTypeException(DataType type) {
        this(type.toString());
    }

    public KSMLTypeException(Class<?> type) {
        this(type.getSimpleName());
    }

    private KSMLTypeException(String type) {
        super("Unknown type: \"" + type + "\"");
    }

    public KSMLTypeException(DataType variableType, DataType valueType) {
        this(valueType.toString(), variableType.toString());
    }

    public KSMLTypeException(DataType variableType, Class<?> valueType) {
        this(valueType.getSimpleName(), variableType.toString());
    }

    public KSMLTypeException(Class<?> fromType, Class<?> toType) {
        this(fromType.getSimpleName(), toType.getSimpleName());
    }

    private KSMLTypeException(String fromType, String toType) {
        super("Can not convert object from type \"" + fromType + "\" to \"" + toType + "\"");
    }
}
