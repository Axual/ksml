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

/**
 * Unchecked exception indicating data conversion, validation, or mapping errors.
 *
 * <p>Messages are prefixed consistently via {@link BaseException} to aid troubleshooting in logs.</p>
 */
public class DataException extends BaseException {
    private static final String ACTIVITY = "data";

    /**
     * Create a DataException with a message.
     *
     * @param message human-readable details of the failure
     */
    public DataException(String message) {
        super(ACTIVITY, message);
    }

    /**
     * Create a DataException with a message and an underlying cause.
     *
     * @param message human-readable details of the failure
     * @param t       underlying cause
     */
    public DataException(String message, Throwable t) {
        super(ACTIVITY, message, t);
    }

    /**
     * Convenience constructor for a failed conversion between two DataTypes.
     */
    public static DataException conversionFailed(DataType variableType, DataType valueType) {
        return conversionFailed(valueType.toString(), variableType.toString());
    }

    /**
     * Convenience constructor for a failed conversion with string descriptors.
     */
    public static DataException conversionFailed(String fromType, String toType) {
        return new DataException("Can not convert object from dataType \"" + fromType + "\" to \"" + toType + "\"");
    }

    /**
     * Convenience constructor for field validation errors.
     */
    public static DataException validationFailed(String key, Object value) {
        return new DataException("Field validation failed for key \"" + key + "\": value=" + value);
    }
}
