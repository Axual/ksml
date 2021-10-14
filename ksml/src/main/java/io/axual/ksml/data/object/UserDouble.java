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

import io.axual.ksml.data.type.base.SimpleType;

import static io.axual.ksml.data.type.user.UserType.DEFAULT_NOTATION;

public class UserDouble extends UserPrimitive<Double> {
    public static final SimpleType TYPE = new SimpleType(Double.class);

    public UserDouble(Double value) {
        this(DEFAULT_NOTATION, value);
    }

    public UserDouble(String notation, Double value) {
        super(TYPE, notation, value);
    }
}
