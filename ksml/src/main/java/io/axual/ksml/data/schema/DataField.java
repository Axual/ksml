package io.axual.ksml.data.schema;

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

@Getter
public class DataField {
    private final String name;
    private final DataSchema schema;
    private final String doc;
    private final DataValue defaultValue;
    private final Order order;

    public enum Order {
        ASCENDING, DESCENDING, IGNORE;
    }

    public DataField(String name, DataSchema schema, String doc, DataValue defaultValue, Order order) {
        this.name = name;
        this.schema = schema;
        this.doc = doc;
        this.defaultValue = defaultValue;
        this.order = order;
    }

    public DataField(String name, DataSchema schema, String doc) {
        this(name, schema, doc, null);
    }

    public DataField(String name, DataSchema schema, String doc, DataValue defaultValue) {
        this(name, schema, doc, defaultValue, Order.ASCENDING);
    }

    public boolean isAssignableFrom(DataField field) {
        return field != null && schema.isAssignableFrom(field.schema);
    }
}
