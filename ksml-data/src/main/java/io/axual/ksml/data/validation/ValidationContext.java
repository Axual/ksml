package io.axual.ksml.data.validation;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.type.DataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class ValidationContext implements ValidationResult {
    private final List<ValidationError> errors;
    private final Supplier<String> thisContext;
    private final Supplier<String> thatContext;

    public ValidationContext() {
        this(null, () -> "", () -> "");
    }

    private ValidationContext(ValidationContext parent, Supplier<String> thisContext, Supplier<String> thatContext) {
        this.errors = parent != null ? parent.errors : new ArrayList<>();
        this.thisContext = thisContext;
        this.thatContext = thatContext;
    }

    public ValidationResult ok() {
        return this;
    }

    public void addError(ValidationError error) {
        errors.add(error);
    }

    public ValidationContext addError(String message) {
        addError(new ValidationError(message));
        return this;
    }

    public List<ValidationError> errors() {
        return Collections.unmodifiableList(errors);
    }

    public ValidationContext merge(ValidationResult other) {
        errors.addAll(other.errors());
        return this;
    }

    public String thatType(Class<?> clazz) {
        return thatType(clazz.getSimpleName());
    }

    public String thatType(DataSchema thatSchema) {
        return thatType(thatSchema.toString());
    }

    public String thatType(DataType thatType) {
        return thatType(thatType.toString());
    }

    public String thatType(Object thatType) {
        return thatType(thatType.getClass());
    }

    public String thatType(String name) {
        return thatContext.get() + name;
    }

    public String thisClass(Class<?> clazz) {
        return thisType(clazz.getSimpleName());
    }

    public String thisType(DataSchema thisSchema) {
        return thisType(thisSchema.toString());
    }

    public String thisType(DataType thisType) {
        return thisType(thisType.toString());
    }

    public String thisType(String name) {
        return thisContext.get() + name;
    }

    public ValidationContext sub(String thisPrefix, String thatPrefix) {
        return new ValidationContext(this, () -> thisPrefix + thisContext.get(), () -> thatPrefix + thatContext.get());
    }

    public ValidationContext schemaMismatch(DataSchema thisSchema, DataSchema thatSchema) {
        return mismatch("Schema", thisType(thisSchema), thatType(thatSchema));
    }

    public ValidationContext typeMismatch(DataType thisType, Class<?> thatType) {
        return mismatch("Type", thisType(thisType), thatType(thatType));
    }

    public ValidationContext typeMismatch(DataType thisType, Object thatType) {
        return mismatch("Type", thisType(thisType), thatType(thatType));
    }

    public ValidationContext typeMismatch(DataType thisType, DataType thatType) {
        return mismatch("Type", thisType(thisType), thatType(thatType));
    }

    public ValidationContext mismatch(String type, String thisType, String thatType) {
        addError(type + " mismatch: got \"" + thatType + "\" but expected \"" + thisType + "\"");
        return this;
    }
}
