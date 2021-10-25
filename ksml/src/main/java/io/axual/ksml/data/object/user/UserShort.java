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

import io.axual.ksml.data.type.base.SimpleType;

import static io.axual.ksml.data.type.user.UserType.DEFAULT_NOTATION;

public class UserShort extends UserPrimitive<Short> {
    public static final SimpleType TYPE = new SimpleType(Short.class);

    public UserShort(Short value) {
        this(DEFAULT_NOTATION, value);
    }

    public UserShort(String notation, Short value) {
        super(TYPE, notation, value);
    }
}