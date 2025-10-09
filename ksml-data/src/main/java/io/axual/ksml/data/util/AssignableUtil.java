package io.axual.ksml.data.util;

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

import io.axual.ksml.data.compare.Assignable;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.UnionType;

import static io.axual.ksml.data.compare.Assignable.error;

public class AssignableUtil {
    private static final String SCHEMA_STRING = "Schema";
    private static final String TYPE_STRING = "Type";

    private AssignableUtil() {
    }

    public static Assignable fieldNotAssignable(String fieldName, Object thisType, Object thisValue, Object thatType, Object thatValue) {
        return fieldNotAssignable(fieldName, thisType, thisValue, thatType, thatValue, null);
    }

    public static Assignable fieldNotAssignable(String fieldName, Object thisType, Object thisValue, Object thatType, Object thatValue, Assignable cause) {
        return error(strOf(thisType) + " is not assignable from " + strOf(thatType) + ": this." + fieldName + "=" + strOf(thisValue) + ", that." + fieldName + "=" + strOf(thatValue), cause);
    }

    public static Assignable schemaMismatch(DataSchema thisSchema, DataSchema thatSchema) {
        return mismatch(SCHEMA_STRING, thisSchema, thatSchema);
    }

    public static Assignable typeMismatch(DataType thisType, Object thatType) {
        return mismatch(TYPE_STRING, thisType, thatType);
    }

    private static Assignable mismatch(String type, Object thisType, Object thatType) {
        return error(type + " mismatch: expected \"" + strOf(thisType) + "\", but got \"" + strOf(thatType) + "\"");
    }

    public static Assignable unionNotAssignableFromMember(UnionType thisType, UnionType.Member thatMember) {
        return unionNotAssignable(thisType, "other union's member", thatMember);
    }

    public static Assignable unionNotAssignableFromType(UnionType thisType, DataType thatType) {
        return unionNotAssignable(thisType, "type", thatType);
    }

    public static Assignable unionNotAssignableFromValue(UnionType thisType, Object thatValue) {
        return unionNotAssignable(thisType, "value", thatValue);
    }

    private static Assignable unionNotAssignable(Object thisObject, String thatType, Object thatValue) {
        return Assignable.error("Union \"" + strOf(thisObject) + "\" is not assignable from " + strOf(thatType) + " \"" + strOf(thatValue) + "\"", null);
    }

    private static String strOf(Object obj) {
        if (obj instanceof Class<?> classObj)
            return classObj.getSimpleName();
        if (obj instanceof DataObject dataObj)
            return dataObj.toString(DataObject.Printer.EXTERNAL_TOP_SCHEMA);
        return obj != null ? obj.toString() : "null";
    }
}
