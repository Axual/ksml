package io.axual.ksml.store;

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

import lombok.Getter;

import static io.axual.ksml.dsl.KSMLDSL.*;

@Getter
public enum StoreType {
    KEYVALUE_STORE(Stores.TYPE_KEY_VALUE),
    SESSION_STORE(Stores.TYPE_SESSION),
    WINDOW_STORE(Stores.TYPE_WINDOW);

    private final String externalName;

    StoreType(String externalName) {
        this.externalName = externalName;
    }
}
