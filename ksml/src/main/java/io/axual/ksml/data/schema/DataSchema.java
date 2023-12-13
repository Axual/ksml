package io.axual.ksml.data.schema;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2023 Axual B.V.
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

import io.axual.ksml.exception.KSMLExecutionException;
import lombok.EqualsAndHashCode;
import lombok.Getter;

// Generic internal schema class
@Getter
@EqualsAndHashCode
public class DataSchema {
    public enum Type {
        ANY,

        NULL,

        BOOLEAN,

        BYTE,
        SHORT,
        INTEGER,
        LONG,

        DOUBLE,
        FLOAT,

        BYTES,
        FIXED,

        STRING,

        ENUM,
        LIST,
        MAP,
        STRUCT,

        UNION,
    }

    private final Type type;

    public static DataSchema create(Type type) {
        return switch (type) {
            case NULL, BOOLEAN, BYTE, SHORT, INTEGER, LONG, DOUBLE, FLOAT, BYTES, STRING -> new DataSchema(type);
            default -> throw new KSMLExecutionException("Can not use 'create' to create a schema for dataType " + type);
        };
    }

    public static DataSchema nullSchema() {
        return create(Type.NULL);
    }

    public static DataSchema booleanSchema() {
        return create(Type.BOOLEAN);
    }

    public static DataSchema byteSchema() {
        return create(Type.BYTE);
    }

    public static DataSchema shortSchema() {
        return create(Type.SHORT);
    }

    public static DataSchema integerSchema() {
        return create(Type.INTEGER);
    }

    public static DataSchema longSchema() {
        return create(Type.LONG);
    }

    public static DataSchema doubleSchema() {
        return create(Type.DOUBLE);
    }

    public static DataSchema floatSchema() {
        return create(Type.FLOAT);
    }

    public static DataSchema bytesSchema() {
        return create(Type.BYTES);
    }

    public static DataSchema stringSchema() {
        return create(Type.STRING);
    }

    protected DataSchema(Type type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return type.toString();
    }

    public boolean isAssignableFrom(DataSchema otherSchema) {
        if (otherSchema == null) return false;

        // Assignable if non of the union's possible types conflict
        if (otherSchema instanceof UnionSchema otherUnion) {
            for (var possibleSchema : otherUnion.possibleSchemas()) {
                if (!this.isAssignableFrom(possibleSchema)) return false;
            }
            return true;
        }

        if (type == otherSchema.type) return true;
        if (type == Type.STRING && otherSchema.type == Type.NULL) return true; // Allow assigning from NULL values
        if (type == Type.STRING && otherSchema.type == Type.ENUM) return true; // ENUMs are convertable to String
        if (type == Type.ENUM && otherSchema.type == Type.STRING) return true; // Strings are convertable to ENUM
        return false;
    }
}
