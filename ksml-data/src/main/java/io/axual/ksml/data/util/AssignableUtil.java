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

import static io.axual.ksml.data.compare.Assignable.notAssignable;

/**
 * Helper utilities to construct uniform {@link Assignable} results for assignability checks.
 *
 * <p>These methods standardize human-readable messages produced by
 * isAssignableFrom-style validations across the KSML type and object model. They help
 * compose consistent diagnostics and support optional cause chaining to pinpoint why
 * a particular assignment is not valid.</p>
 */
public class AssignableUtil {
    private static final String SCHEMA_STRING = "Schema";
    private static final String TYPE_STRING = "Type";

    private AssignableUtil() {
        // Prevent instantiation.
    }

    /**
     * Create an {@link Assignable} indicating that a named field value cannot be assigned from the
     * right-hand side to the left-hand side, using a standardized message.
     *
     * @param fieldName the field being assigned
     * @param thisType  descriptor for the left-hand container/type
     * @param thisValue current left-hand field value
     * @param thatType  descriptor for the right-hand container/type
     * @param thatValue right-hand field value being assigned
     * @return a not-assignable result describing the field assignment mismatch
     */
    public static Assignable fieldNotAssignable(String fieldName, Object thisType, Object thisValue, Object thatType, Object thatValue) {
        return fieldNotAssignable(fieldName, thisType, thisValue, thatType, thatValue, null);
    }

    /**
     * Same as {@link #fieldNotAssignable(String, Object, Object, Object, Object)} but allows including
     * an optional cause explaining the deeper reason of the mismatch.
     *
     * @param fieldName the field being assigned
     * @param thisType  descriptor for the left-hand container/type
     * @param thisValue current left-hand field value
     * @param thatType  descriptor for the right-hand container/type
     * @param thatValue right-hand field value being assigned
     * @param cause     optional underlying reason providing more context; may be null
     * @return a not-assignable result describing the field assignment mismatch including the cause
     */
    public static Assignable fieldNotAssignable(String fieldName, Object thisType, Object thisValue, Object thatType, Object thatValue, Assignable cause) {
        return notAssignable(strOf(thisType) + " is not assignable from " + strOf(thatType) + ": this." + fieldName + "=" + strOf(thisValue) + ", that." + fieldName + "=" + strOf(thatValue), cause);
    }

    /**
     * Report that two {@link DataSchema} descriptors do not match during an assignment check.
     *
     * @param thisSchema the expected schema
     * @param thatSchema the encountered schema
     * @return a not-assignable result describing the schema mismatch
     */
    public static Assignable schemaMismatch(DataSchema thisSchema, DataSchema thatSchema) {
        return mismatch(SCHEMA_STRING, thisSchema, thatSchema);
    }

    /**
     * Report that two type descriptors do not match during an assignment check.
     *
     * @param thisType the expected {@link DataType}
     * @param thatType the encountered type descriptor (may be a DataType or other descriptor)
     * @return a not-assignable result describing the type mismatch
     */
    public static Assignable typeMismatch(DataType thisType, Object thatType) {
        return mismatch(TYPE_STRING, thisType, thatType);
    }

    /**
     * Create a not-assignable result describing a generic mismatch between expected and actual descriptors.
     */
    private static Assignable mismatch(String type, Object thisType, Object thatType) {
        return Assignable.notAssignable(type + " mismatch: expected \"" + strOf(thisType) + "\", but got \"" + strOf(thatType) + "\"");
    }

    /**
     * Report that a value of another union's member cannot be assigned to this union type.
     *
     * @param thisType   the target union type
     * @param thatMember the source union member
     * @return a not-assignable result describing the union-member assignment failure
     */
    public static Assignable unionNotAssignableFromMember(UnionType thisType, UnionType.Member thatMember) {
        return unionNotAssignable(thisType, "other union's member", thatMember);
    }

    /**
     * Report that a value of a specific DataType cannot be assigned to this union type.
     *
     * @param thisType the target union type
     * @param thatType the source data type
     * @return a not-assignable result describing the union assignment failure
     */
    public static Assignable unionNotAssignableFromType(UnionType thisType, DataType thatType) {
        return unionNotAssignable(thisType, "type", thatType);
    }

    /**
     * Report that a given value cannot be assigned to this union type.
     *
     * @param thisType  the target union type
     * @param thatValue the source value
     * @return a not-assignable result describing the union assignment failure
     */
    public static Assignable unionNotAssignableFromValue(UnionType thisType, Object thatValue) {
        return unionNotAssignable(thisType, "value", thatValue);
    }

    /**
     * Internal helper to create a standardized union not-assignable message.
     */
    private static Assignable unionNotAssignable(Object thisObject, String thatType, Object thatValue) {
        return Assignable.notAssignable("Union \"" + strOf(thisObject) + "\" is not assignable from " + strOf(thatType) + " \"" + strOf(thatValue) + "\"", null);
    }

    /**
     * Render objects to human-readable strings. For DataObject instances, uses external top-schema printing.
     */
    private static String strOf(Object obj) {
        if (obj instanceof Class<?> classObj)
            return classObj.getSimpleName();
        if (obj instanceof DataObject dataObj)
            return dataObj.toString(DataObject.Printer.EXTERNAL_TOP_SCHEMA);
        return obj != null ? obj.toString() : "null";
    }
}
