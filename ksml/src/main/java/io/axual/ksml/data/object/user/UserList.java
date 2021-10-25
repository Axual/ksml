package io.axual.ksml.data.object.user;

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

import io.axual.ksml.data.type.user.UserListType;
import io.axual.ksml.data.type.user.UserType;
import io.axual.ksml.exception.KSMLExecutionException;

public class UserList extends ArrayList<UserObject> implements UserObject {
    private final UserListType type;

    public UserList(String notation, UserType valueType) {
        type = new UserListType(notation, valueType);
    }

    public UserList(String notation, UserType valueType, int initialCapacity) {
        super(initialCapacity);
        type = new UserListType(notation, valueType);
    }

    @Override
    public UserType type() {
        return type;
    }

    public UserType valueType() {
        return type.valueType();
    }

    private UserObject checkValueType(UserObject value) {
        if (!type.valueType().isAssignableFrom(value.type())) {
            throw new KSMLExecutionException("Can not cast value of type " + value.type() + " to " + type.valueType());
        }
        return value;
    }

    private void checkValueTypes(Collection<? extends UserObject> values) {
        for (UserObject value : values) {
            checkValueType(value);
        }
    }

    @Override
    public UserObject set(int index, UserObject element) {
        return super.set(index, checkValueType(element));
    }

    @Override
    public boolean add(UserObject element) {
        return super.add(checkValueType(element));
    }

    @Override
    public void add(int index, UserObject element) {
        super.add(index, checkValueType(element));
    }

    @Override
    public boolean addAll(Collection<? extends UserObject> values) {
        checkValueTypes(values);
        return super.addAll(values);
    }

    @Override
    public boolean addAll(int index, Collection<? extends UserObject> values) {
        checkValueTypes(values);
        return super.addAll(index, values);
    }

    @Override
    public void replaceAll(UnaryOperator<UserObject> operator) {
        super.replaceAll(element -> checkValueType(operator.apply(element)));
    }
}
