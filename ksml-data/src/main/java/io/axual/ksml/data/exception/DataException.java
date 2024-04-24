package io.axual.ksml.data.exception;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2024 Axual B.V.
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

public class DataException extends BaseException {
    private static final String ACTIVITY = "Data";

    public DataException(String message) {
        super(ACTIVITY, message);
    }

    public DataException(String message, Throwable t) {
        super(ACTIVITY, message, t);
    }

    public static DataException conversionFailed(DataType variableType, DataType valueType) {
        return conversionFailed(valueType.toString(), variableType.toString());
    }

    public static DataException conversionFailed(String fromType, String toType) {
        return new DataException("Can not convert object from dataType \"" + fromType + "\" to \"" + toType + "\"");
    }

    public static DataException validationFailed(String key, Object value) {
        return new DataException("Field validation failed for key \"" + key + "\": value=" + value);
    }
}
