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

import io.axual.ksml.data.compare.Equality;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;

/**
 * Helper utilities to construct uniform {@link Equality} results.
 *
 * <p>These methods standardize human-readable difference messages for deep-equality checks
 * across the KSML data model. They produce consistent messages and allow chaining causes to
 * pinpoint where comparisons diverge.</p>
 */
public class EqualUtil {
    private static final String CONTAINER_CLASS_STRING = "Container class";
    private static final String OBJECT_STRING = "Object";
    private static final String TYPE_STRING = "Type";

    private EqualUtil() {
    }

    /**
     * Create an Equal result indicating the other object was null while this object was not.
     *
     * @param thisObject the non-null lhs object participating in the comparison
     * @return a non-equal result explaining the null mismatch
     * @throws io.axual.ksml.data.exception.DataException if thisObject is null (logic error)
     */
    public static Equality otherIsNull(Object thisObject) {
        if (thisObject == null)
            throw new DataException("Can not handle NULL object, this is a bug in KSML");
        return Equality.notEqual("Cannot compare " + strOf(thisObject) + " to null");
    }

    /**
     * Report a difference between container classes.
     *
     * @param thisClass the container class of the left-hand side
     * @param thatClass the container class of the right-hand side
     * @return an Equal indicating inequality with a standardized message
     */
    public static Equality containerClassNotEqual(Class<?> thisClass, Class<?> thatClass) {
        return notEqual(CONTAINER_CLASS_STRING, thisClass, thatClass, null);
    }

    /**
     * Report that two DataObject instances differ, without a nested cause.
     *
     * @param thisObject the left-hand side object
     * @param thatObject the right-hand side object
     * @return a non-equal result describing the difference
     */
    public static Equality objectNotEqual(DataObject thisObject, DataObject thatObject) {
        return objectNotEqual(thisObject, thatObject, null);
    }

    /**
     * Report that two DataObject instances differ, with an optional nested cause detailing the mismatch.
     *
     * @param thisObject the left-hand side object
     * @param thatObject the right-hand side object
     * @param cause      an underlying reason providing more context; may be null
     * @return a non-equal result describing the difference and including the cause
     */
    public static Equality objectNotEqual(DataObject thisObject, DataObject thatObject, Equality cause) {
        return notEqual(OBJECT_STRING, thisObject, thatObject, cause);
    }

    /**
     * Report a difference between two DataType descriptors, with an optional nested cause detailing the mismatch.
     *
     * @param thisType the left-hand side type
     * @param thatType the right-hand side type (may be a DataType or another descriptor)
     * @param cause    an underlying reason providing more context; may be null
     * @return a non-equal result describing the type mismatch
     */
    public static Equality typeNotEqual(DataType thisType, Object thatType, Equality cause) {
        return notEqual(TYPE_STRING, thisType, thatType, cause);
    }

    /**
     * Report that a named field differs between two structures.
     *
     * @param fieldName the field being compared
     * @param thisType  a descriptor for the left-hand container/type
     * @param thisValue the left-hand field value
     * @param thatType  a descriptor for the right-hand container/type
     * @param thatValue the right-hand field value
     * @return a non-equal result describing the field mismatch
     */
    public static Equality fieldNotEqual(String fieldName, Object thisType, Object thisValue, Object thatType, Object thatValue) {
        return fieldNotEqual(fieldName, thisType, thisValue, thatType, thatValue, null);
    }

    /**
     * Report that a named field differs between two structures, with an optional nested cause detailing the mismatch.
     *
     * @param fieldName the field being compared
     * @param thisType  a descriptor for the left-hand container/type
     * @param thisValue the left-hand field value
     * @param thatType  a descriptor for the right-hand container/type
     * @param thatValue the right-hand field value
     * @param cause     an underlying reason providing more context; may be null
     * @return a non-equal result describing the field mismatch with cause
     */
    public static Equality fieldNotEqual(String fieldName, Object thisType, Object thisValue, Object thatType, Object thatValue, Equality cause) {
        return Equality.notEqual(strOf(thisType) + " differs from " + strOf(thatType) + ": this." + fieldName + "=" + strOf(thisValue) + ", that." + fieldName + "=" + strOf(thatValue), cause);
    }

    private static Equality notEqual(String type, Object thisType, Object thatType, Equality cause) {
        return Equality.notEqual(type + " \"" + strOf(thisType) + "\" is not equal to \"" + strOf(thatType) + "\"", cause);
    }

    private static String strOf(Object obj) {
        if (obj instanceof Class<?> classObj)
            return classObj.getSimpleName();
        if (obj instanceof DataObject dataObj)
            return dataObj.toString(DataObject.Printer.EXTERNAL_TOP_SCHEMA);
        return obj != null ? obj.toString() : "null";
    }
}
