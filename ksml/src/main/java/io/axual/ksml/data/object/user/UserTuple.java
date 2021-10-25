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

import io.axual.ksml.data.object.base.Tuple;
import io.axual.ksml.data.type.user.UserTupleType;
import io.axual.ksml.data.type.user.UserType;

public class UserTuple extends Tuple<UserObject> implements UserObject {
    private final UserType type;

    public UserTuple(String notation, UserObject... elements) {
        super(elements);
        this.type = new UserTupleType(notation, convertElements(elements));
    }

    private static UserType[] convertElements(UserObject... elements) {
        UserType[] result = new UserType[elements.length];
        for (int index = 0; index < elements.length; index++) {
            result[index] = elements[index].type();
        }
        return result;
    }

    @Override
    public UserType type() {
        return type;
    }
}
