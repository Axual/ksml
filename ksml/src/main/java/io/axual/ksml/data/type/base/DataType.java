package io.axual.ksml.data.type.base;

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


public interface DataType {
    Class<?> containerClass();

    String schemaName();

    boolean isAssignableFrom(Class<?> type);

    boolean isAssignableFrom(DataType type);

    boolean isAssignableFrom(Object value);

    DataType UNKNOWN = new DataType() {
        @Override
        public String toString() {
            return "?";
        }

        @Override
        public Class<?> containerClass() {
            return Object.class;
        }

        public String schemaName() {
            return "Unknown";
        }

        @Override
        public boolean isAssignableFrom(Class<?> type) {
            return true;
        }

        @Override
        public boolean isAssignableFrom(DataType type) {
            return true;
        }

        @Override
        public boolean isAssignableFrom(Object value) {
            return true;
        }
    };
}
