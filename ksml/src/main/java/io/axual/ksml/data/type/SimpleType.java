package io.axual.ksml.data.type;

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


public class SimpleType implements DataType {
    private final Class<?> containerClass;

    public SimpleType(Class<?> containerClass) {
        this.containerClass = containerClass;
    }

    @Override
    public String toString() {
        return containerClass.getSimpleName();
    }

    public Class<?> containerClass() {
        return containerClass;
    }

    public String schemaName() {
        return containerClass.getSimpleName();
    }

    public boolean isAssignableFrom(Class<?> type) {
        return this.containerClass.isAssignableFrom(type);
    }

    @Override
    public boolean isAssignableFrom(DataType other) {
        if (other instanceof SimpleType) {
            return isAssignableFrom(((SimpleType) other).containerClass);
        }
        return false;
    }

    @Override
    public boolean isAssignableFrom(Object value) {
        return value == null || isAssignableFrom(value.getClass());
    }

    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        SimpleType other = (SimpleType) obj;
        return isAssignableFrom(other) && other.isAssignableFrom(this);
    }

    public int hashCode() {
        int result = super.hashCode();
        return result * 31 + containerClass.hashCode();
    }
}
