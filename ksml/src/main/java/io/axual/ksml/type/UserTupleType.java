package io.axual.ksml.type;

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

import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.TupleType;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class UserTupleType extends TupleType {
    private final UserType[] userTypes;

    public UserTupleType(UserType... subTypes) {
        super("UserTuple", dataTypesOf(subTypes));
        userTypes = subTypes;
    }

    public UserType getUserType(int index) {
        return userTypes[index];
    }

    private static DataType[] dataTypesOf(UserType[] userTypes) {
        var result = new DataType[userTypes.length];
        for (int index = 0; index < userTypes.length; index++) {
            result[index] = userTypes[index].dataType();
        }
        return result;
    }
}
