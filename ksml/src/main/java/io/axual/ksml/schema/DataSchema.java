package io.axual.ksml.schema;

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

import java.util.Objects;

import io.axual.ksml.exception.KSMLExecutionException;

// Generic internal schema class
public class DataSchema {
    public enum Type {
        BOOLEAN,
        BYTE,
        BYTES,
        SHORT,
        DOUBLE,
        FLOAT,
        INTEGER,
        LONG,
        STRING,
        ARRAY,
        ENUM,
        MAP,
        RECORD,
        NULLABLE
    }

    private final Type type;

    public static DataSchema create(Type type) {
        return switch (type) {
            case BOOLEAN, BYTE, BYTES, SHORT, DOUBLE, FLOAT, INTEGER, LONG, STRING -> new DataSchema(type);
            default -> throw new KSMLExecutionException("Can not use 'create' to create a schema for type " + type);
        };
    }

    protected DataSchema(Type type) {
        this.type = type;
    }

    public Type type() {
        return type;
    }

    @Override
    public String toString() {
        return type.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        if (this == other) return true;
        if (other.getClass() != getClass()) return false;

        // Compare all schema relevant fields
        return Objects.equals(type, ((DataSchema) other).type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }
}
