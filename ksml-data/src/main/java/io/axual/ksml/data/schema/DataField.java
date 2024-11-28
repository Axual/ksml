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

import io.axual.ksml.data.exception.ExecutionException;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Objects;

@Getter
@EqualsAndHashCode
public class DataField {
    public static final int NO_INDEX = -1;

    public enum Order {
        ASCENDING, DESCENDING, IGNORE
    }

    private final String name;
    private final DataSchema schema;
    private final String doc;
    private final boolean required;
    private final boolean constant;
    private final int index;
    private final DataValue defaultValue;
    private final Order order;

    public DataField(String name, DataSchema schema, String doc, int index, boolean required, boolean constant, DataValue defaultValue, Order order) {
        Objects.requireNonNull(schema);
        this.name = name;
        this.schema = schema;
        this.doc = doc;
        this.index = index;
        this.required = required;
        this.constant = constant;
        this.defaultValue = defaultValue;
        this.order = order;
        if (required && defaultValue != null && defaultValue.value() == null) {
            throw new ExecutionException("Default value for field \"" + name + "\" can not be null");
        }
    }

    public DataField(String name, DataSchema schema, String doc) {
        this(name, schema, doc, -1);
    }

    public DataField(String name, DataSchema schema, String doc, int index) {
        this(name, schema, doc, index, true, false, null);
    }

    public DataField(String name, DataSchema schema, String doc, int index, boolean required) {
        this(name, schema, doc, index, required, false, null);
    }

    public DataField(String name, DataSchema schema, String doc, int index, boolean required, boolean constant, DataValue defaultValue) {
        this(name, schema, doc, index, required, constant, defaultValue, Order.ASCENDING);
    }

    public boolean isAssignableFrom(DataField field) {
        return field != null && schema.isAssignableFrom(field.schema);
    }

    public DataField withDefaultValue(DataValue possibleValues) {
        return new DataField(name, schema, doc, index, required, constant, defaultValue, order);
    }

    public DataField withDoc(String doc) {
        return new DataField(name, schema, doc, index, required, constant, defaultValue, order);
    }
}
