package io.axual.ksml.data.type.user;

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

import io.axual.ksml.data.type.base.DataType;
import io.axual.ksml.notation.BinaryNotation;

public interface UserType {
    String DEFAULT_NOTATION = BinaryNotation.NAME;

    DataType type();

    String notation();

    default String schemaName() {
        return type().schemaName();
    }

    default boolean isAssignableFrom(Class<?> type) {
        return type().isAssignableFrom(type);
    }

    default boolean isAssignableFrom(DataType type) {
        return type().isAssignableFrom(type);
    }

    default boolean isAssignableFrom(UserType type) {
        return type().isAssignableFrom(type.type());
    }

    default boolean isAssignableFrom(Object value) {
        return type().isAssignableFrom(value);
    }

    UserType UNKNOWN = new UserType() {
        @Override
        public String toString() {
            return "?";
        }

        @Override
        public DataType type() {
            return DataType.UNKNOWN;
        }

        @Override
        public String notation() {
            return DEFAULT_NOTATION;
        }
    };
}
