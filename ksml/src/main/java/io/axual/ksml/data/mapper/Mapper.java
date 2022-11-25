package io.axual.ksml.data.mapper;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2022 Axual B.V.
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

import io.axual.ksml.exception.KSMLExecutionException;

public abstract class Mapper<S, T> {
    public final S to(T value) {
        try {
            return mapTo(value);
        } catch (Exception e) {
            var valueClass = value != null ? value.getClass().getSimpleName() : "null";
            throw new KSMLExecutionException("Exception caught in " + getClass().getSimpleName() + ".to(" + valueClass + ")", e);
        }
    }

    public final T from(S value) {
        try {
            return mapFrom(value);
        } catch (Exception e) {
            var valueClass = value != null ? value.getClass().getSimpleName() : "null";
            throw new KSMLExecutionException("Exception caught in " + getClass().getSimpleName() + ".from(" + valueClass + ")", e);
        }
    }

    protected abstract S mapTo(T value);

    protected abstract T mapFrom(S value);
}
