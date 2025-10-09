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

import io.axual.ksml.data.compare.Equal;
import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;

import static io.axual.ksml.data.compare.Equal.error;

public class EqualUtil {
    private static final String CONTAINER_CLASS_STRING = "Container class";
    private static final String OBJECT_STRING = "Object";
    private static final String TYPE_STRING = "Type";

    private EqualUtil() {
    }

    public static Equal otherIsNull(Object thisObject) {
        if (thisObject == null)
            throw new DataException("Can not handle NULL object, this is a bug in KSML");
        return error("Cannot compare " + strOf(thisObject) + " to null");
    }

    public static Equal containerClassNotEqual(Class<?> thisClass, Class<?> thatClass) {
        return notEqual(CONTAINER_CLASS_STRING, thisClass, thatClass, null);
    }

    public static Equal objectNotEqual(DataObject thisObject, DataObject thatObject) {
        return objectNotEqual(thisObject, thatObject, null);
    }

    public static Equal objectNotEqual(DataObject thisObject, DataObject thatObject, Equal cause) {
        return notEqual(OBJECT_STRING, thisObject, thatObject, cause);
    }

    public static Equal typeNotEqual(DataType thisType, Object thatType, Equal cause) {
        return notEqual(TYPE_STRING, thisType, thatType, cause);
    }

    public static Equal fieldNotEqual(String fieldName, Object thisType, Object thisValue, Object thatType, Object thatValue) {
        return fieldNotEqual(fieldName, thisType, thisValue, thatType, thatValue, null);
    }

    public static Equal fieldNotEqual(String fieldName, Object thisType, Object thisValue, Object thatType, Object thatValue, Equal cause) {
        return error(strOf(thisType) + " differs from " + strOf(thatType) + ": this." + fieldName + "=" + strOf(thisValue) + ", that." + fieldName + "=" + strOf(thatValue), cause);
    }

    private static Equal notEqual(String type, Object thisType, Object thatType, Equal cause) {
        return error(type + " \"" + strOf(thisType) + "\" is not equal to \"" + strOf(thatType) + "\"", cause);
    }

    private static String strOf(Object obj) {
        if (obj instanceof Class<?> classObj)
            return classObj.getSimpleName();
        if (obj instanceof DataObject dataObj)
            return dataObj.toString(DataObject.Printer.EXTERNAL_TOP_SCHEMA);
        return obj != null ? obj.toString() : "null";
    }
}
