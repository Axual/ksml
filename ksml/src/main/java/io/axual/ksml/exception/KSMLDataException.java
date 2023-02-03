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


import io.axual.ksml.data.type.DataType;

public class KSMLDataException extends KSMLExecutionException {
    public static KSMLDataException conversionFailed(DataType variableType, DataType valueType) {
        return conversionFailed(valueType.toString(), variableType.toString());
    }

    public static KSMLDataException conversionFailed(String fromType, String toType) {
        return new KSMLDataException("Can not convert object from dataType \"" + fromType + "\" to \"" + toType + "\"");
    }

    public static KSMLDataException validationFailed(String key, Object value) {
        return new KSMLDataException("Field validation failed for key \"" + key + "\": value=" + value);
    }

    public KSMLDataException(String message) {
        super(message);
    }

    public KSMLDataException(String message, Throwable cause) {
        super(message, cause);
    }
}
