package io.axual.ksml.data.compare;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library
 * %%
 * Copyright (C) 2021 - 2025 Axual B.V.
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

import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;

public interface IsAssignableFrom {
    Compared checkAssignableFrom(final DataType type);

    default Compared checkAssignableFrom(DataObject value) {
        // Always allow a null value to be assigned
        if (value == DataNull.INSTANCE) return Compared.ok();
        // If not NULL, check the value type for assignability
        return checkAssignableFrom(value.type());
    }

    Compared checkAssignableFrom(Class<?> type);

    default Compared checkAssignableFrom(Object value) {
        // Always allow a null value to be assigned
        if (value == null) return Compared.ok();
        // If not NULL, check the value class for assignability
        return checkAssignableFrom(value.getClass());
    }
}
