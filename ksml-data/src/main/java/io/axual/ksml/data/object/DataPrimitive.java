package io.axual.ksml.data.object;

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

import io.axual.ksml.data.exception.ExecutionException;
import io.axual.ksml.data.type.DataType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class DataPrimitive<T> implements DataObject {
    private final DataType type;
    private final T value;

    protected DataPrimitive(DataType type, T value) {
        this.type = type;
        this.value = value;
        checkValue();
    }

    private void checkValue() {
        boolean valid = value instanceof DataObject dataObject
                ? type.isAssignableFrom(dataObject)
                : type.isAssignableFrom(value);
        if (!valid) throw new ExecutionException("Value assigned to " + type + " can not be \"" + this + "\"");
    }

    @Override
    public String toString() {
        return value != null ? value.toString() : type + ": null";
    }
}
