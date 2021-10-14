package io.axual.ksml.data.mapper;

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

import io.axual.ksml.data.object.UserObject;
import io.axual.ksml.data.type.user.StaticUserType;
import io.axual.ksml.data.type.user.UserType;

public interface DataMapper<T> {
    default UserObject toDataObject(String notation, T value) {
        return toDataObject(new StaticUserType(null, notation), value);
    }

    UserObject toDataObject(UserType expected, T value);

    T fromDataObject(UserObject object);
}
