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

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.UnaryOperator;

import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.exception.KSMLExecutionException;

public class DataList extends ArrayList<DataObject> implements DataObject {
    private final transient ListType type;

    public DataList(DataType valueType) {
        type = new ListType(valueType);
    }

    public DataList(DataType valueType, int initialCapacity) {
        super(initialCapacity);
        type = new ListType(valueType);
    }

    public void addIfNotNull(DataObject value) {
        if (value != null) add(value);
    }

    @Override
    public DataType type() {
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

    private void checkValueTypes(Collection<? extends DataObject> values) {
        for (DataObject value : values) {
            checkValueType(value);
        }
    }

    @Override
    public DataObject set(int index, DataObject element) {
        return super.set(index, checkValueType(element));
    }

    @Override
    public boolean add(DataObject element) {
        return super.add(checkValueType(element));
    }

    @Override
    public void add(int index, DataObject element) {
        super.add(index, checkValueType(element));
    }

    @Override
    public boolean addAll(Collection<? extends DataObject> values) {
        checkValueTypes(values);
        return super.addAll(values);
    }

    @Override
    public boolean addAll(int index, Collection<? extends DataObject> values) {
        checkValueTypes(values);
        return super.addAll(index, values);
    }

    @Override
    public void replaceAll(UnaryOperator<DataObject> operator) {
        super.replaceAll(element -> checkValueType(operator.apply(element)));
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
}
