package io.axual.ksml.data.object;

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

import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.exception.KSMLExecutionException;

import java.util.ArrayList;
import java.util.Iterator;

public class DataList implements DataObject, Iterable<DataObject> {
    private static final ListType LIST_OF_UNKNOWN = new ListType(DataType.UNKNOWN);
    private final ArrayList<DataObject> contents = new ArrayList<>();
    private final transient ListType type;

    public DataList() {
        this(DataType.UNKNOWN);
    }

    public DataList(DataType valueType) {
        type = valueType != null ? new ListType(valueType) : LIST_OF_UNKNOWN;
    }

    public void addIfNotNull(DataObject value) {
        if (value != null) add(value);
    }

    @Override
    public ListType type() {
        return type;
    }

    public DataType valueType() {
        return type.valueType();
    }

    private DataObject checkValueType(DataObject value) {
        if (!type.valueType().isAssignableFrom(value.type())) {
            throw new KSMLExecutionException("Can not cast value of dataType " + value.type() + " to " + type.valueType());
        }
        return value;
    }

    public DataObject set(int index, DataObject element) {
        return contents.set(index, checkValueType(element));
    }

    public boolean add(DataObject element) {
        return contents.add(checkValueType(element));
    }

    public void add(int index, DataObject element) {
        contents.add(index, checkValueType(element));
    }

    @Override
    public Iterator<DataObject> iterator() {
        return contents.iterator();
    }

    public int size() {
        return contents.size();
    }

    public DataObject get(int index) {
        return contents.get(index);
    }

    public boolean isEmpty() {
        return contents.isEmpty();
    }

    @Override
    public boolean equals(Object other) {
        if (!super.equals(other)) return false;
        if (!(other instanceof DataList)) return false;
        return type.equals(((DataList) other).type);
    }

    @Override
    public int hashCode() {
        return type.hashCode() + super.hashCode() * 31;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(type.toString()).append(": [");
        for (int index = 0; index < size(); index++) {
            if (index > 0) sb.append(", ");
            sb.append(get(index).toString());
        }
        sb.append("]");
        return sb.toString();
    }
}
