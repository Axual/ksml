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


import io.axual.ksml.generator.StreamDataType;
import io.axual.ksml.data.type.DataType;

public class KSMLTypeException extends RuntimeException {
    public static KSMLTypeException conversionFailed(DataType variableType, DataType valueType) {
        return conversionFailed(valueType.toString(), variableType.toString());
    }

    public static KSMLTypeException conversionFailed(DataType variableType, Class<?> valueType) {
        return conversionFailed(valueType.getSimpleName(), variableType.toString());
    }

    public static KSMLTypeException conversionFailed(Class<?> fromType, Class<?> toType) {
        return conversionFailed(fromType.getSimpleName(), toType.getSimpleName());
    }

    public static KSMLTypeException conversionFailed(String fromType, String toType) {
        return new KSMLTypeException("Can not convert object from type \"" + fromType + "\" to \"" + toType + "\"");
    }

    public static KSMLTypeException validationFailed(String key, Object value) {
        return new KSMLTypeException("Field validation failed for key \"" + key + "\": value=" + value);
    }

    public static KSMLTypeException unknownType(DataType type) {
        return unknownType(type.toString());
    }

    public static KSMLTypeException unknownType(Class<?> type) {
        return unknownType(type.getSimpleName());
    }

    public static KSMLTypeException unknownType(String type) {
        return new KSMLTypeException("Unknown type: \"" + type + "\"");
    }

    public static KSMLTypeException topicTypeMismatch(String topic, StreamDataType keyType, StreamDataType valueType, DataType expectedKeyType, DataType expectedValueType) {
        return new KSMLTypeException("Incompatible key/value types: " +
                "topic=" + topic +
                ", keyType=" + keyType +
                ", valueType=" + valueType +
                ", expected keyType=" + expectedKeyType +
                ", expected valueType=" + expectedValueType);
    }

    private KSMLTypeException(String message) {
        super(message);
    }
}
