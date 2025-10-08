package io.axual.ksml.data.compare;

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

import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.UnionType;
import lombok.Getter;

@Getter
public class Compared {
    private static final Compared OK = new Compared(null, null);
    private static final String OBJECT_STRING = "Object";
    private static final String SCHEMA_STRING = "Schema";
    private static final String TYPE_STRING = "Type";
    private final String errorMessage;
    private final Compared cause;

    private Compared(String errorMessage, Compared cause) {
        this.errorMessage = errorMessage;
        this.cause = cause != null && cause.isError() ? cause : null;
    }

    public static Compared ok() {
        return OK;
    }

    public static Compared error(String errorMessage) {
        return error(errorMessage, null);
    }

    public static Compared error(String errorMessage, Compared cause) {
        return errorMessage != null ? new Compared(errorMessage, cause) : ok();
    }

    public static Compared ksmlError() {
        return error("No type specified, this is a bug in KSML");
    }

    public static Compared otherIsNull(DataField thisField) {
        return error("Cannot compare " + thisField + " to null");
    }

    public static Compared otherIsNull(EnumSchema.Symbol thisSymbol) {
        return error("Cannot compare " + thisSymbol + " to null");
    }

    public static Compared otherIsNull(DataObject thisObject) {
        return error("Cannot compare " + thisObject + " to null");
    }

    public static Compared otherIsNull(UnionSchema.Member thisMember) {
        return error("Cannot compare " + thisMember + " to null");
    }

    public static Compared otherIsNull(UnionType.Member thisMember) {
        return error("Cannot compare " + thisMember + " to null");
    }

    public static Compared otherIsNull(DataSchema thisSchema) {
        return error("Cannot compare " + thisSchema + " to null");
    }

    public static Compared otherIsNull(DataType thisType) {
        return error("Cannot compare " + thisType + " to null");
    }

    public static Compared notEqual(Class<?> thisClass, Class<?> thatClass) {
        if (thisClass == null || thatClass == null) return ksmlError();
        return notEqual(SCHEMA_STRING, thisClass.getSimpleName(), thatClass.getSimpleName(), null);
    }

    public static Compared notEqual(DataSchema thisSchema, Object thatSchema) {
        return notEqual(SCHEMA_STRING, thisSchema, thatSchema, null);
    }

    public static Compared notEqual(DataSchema thisSchema, Object thatSchema, Compared cause) {
        return notEqual(SCHEMA_STRING, thisSchema, thatSchema, cause);
    }

    public static Compared notEqual(DataType thisType, Object thatType) {
        return notEqual(thisType, thatType, null);
    }

    public static Compared notEqual(DataObject thisObject, Object thatType) {
        return notEqual(OBJECT_STRING, thisObject, thatType, null);
    }

    public static Compared notEqual(DataObject thisObject, Object thatType, Compared cause) {
        return notEqual(OBJECT_STRING, thisObject, thatType, cause);
    }

    public static Compared notEqual(DataType thisType, Object thatType, Compared cause) {
        if (thisType == null || thatType == null) return ksmlError();
        return notEqual(TYPE_STRING, thisType, thatType, cause);
    }

    private static Compared notEqual(String type, Object thisType, Object thatType, Compared cause) {
        return Compared.error(type + " \"" + (thatType != null ? thatType : "null") + "\" is not equal to \"" + (thisType != null ? thisType : "null") + "\"", cause);
    }

    public static Compared fieldNotEqual(String fieldName, Object thisType, Object thisValue, Object thatType, Object thatValue) {
        return fieldNotEqual(fieldName, thisType, thisValue, thatType, thatValue, null);
    }

    public static Compared fieldNotEqual(String fieldName, Object thisType, Object thisValue, Object thatType, Object thatValue, Compared cause) {
        if (thisType == null || thatType == null) return ksmlError();
        return Compared.error(thisType + " differs from " + thatType + ": this." + fieldName + "=" + (thisValue != null ? thisValue : "null") + ", that." + fieldName + "=" + (thatValue != null ? thatValue : "null"), cause);
    }

    public static Compared schemaMismatch(DataSchema thisSchema, Object thatSchema) {
        return schemaMismatch(thisSchema, thatSchema, null);
    }

    public static Compared schemaMismatch(DataSchema thisSchema, Object thatSchema, Compared cause) {
        if (thisSchema == null || thatSchema == null) return ksmlError();
        return mismatch(SCHEMA_STRING, thisSchema.toString(), thatSchema.toString(), cause);
    }

    public static Compared typeMismatch(DataType thisType, Class<?> thatType) {
        if (thisType == null || thatType == null) return ksmlError();
        return mismatch(TYPE_STRING, thisType.toString(), thatType.getSimpleName(), null);
    }

    public static Compared typeMismatch(DataType thisType, Object thatType) {
        if (thisType == null || thatType == null) return ksmlError();
        return mismatch(TYPE_STRING, thisType.toString(), thatType.toString(), null);
    }

    public static Compared mismatch(String type, String thisType, String thatType, Compared cause) {
        return Compared.error(type + " mismatch: got \"" + thatType + "\" but expected \"" + thisType + "\"", cause);
    }

    public boolean isOK() {
        return errorMessage == null;
    }

    public boolean isError() {
        return errorMessage != null;
    }

    @Override
    public String toString() {
        return toString(false);
    }

    public String toString(boolean prefixFirstLine) {
        return toString("caused by: ", prefixFirstLine);
    }

    public String toString(String linePrefix, boolean prefixFirstLine) {
        final var builder = new StringBuilder();
        for (var i = this; i != null; i = i.cause) {
            if (i != this) builder.append("\n");
            if (i != this || prefixFirstLine) builder.append(linePrefix);
            builder.append(i.errorMessage);
        }
        return builder.toString();
    }
}
