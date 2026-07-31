package io.axual.ksml.data.schema.logical;

/*-
 * ========================LICENSE_START=================================
 * KSML
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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.exception.SchemaException;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.type.DataType;

import java.math.BigDecimal;
import java.math.RoundingMode;

/** The decimal logical type: an exact number with fixed precision and scale, carried as bytes and represented in KSML as a canonical string. */
public record DecimalLogicalType(int precision, int scale) implements LogicalType {
    public static final String LOGICAL_TYPE_NAME = "decimal";

    public DecimalLogicalType {
        if (precision <= 0)
            throw new SchemaException("Decimal precision must be positive, but was " + precision);
        if (scale < 0)
            throw new SchemaException("Decimal scale must not be negative, but was " + scale);
        if (scale > precision)
            throw new SchemaException("Decimal scale (" + scale + ") must not exceed precision (" + precision + ")");
    }

    @Override
    public String name() {
        return LOGICAL_TYPE_NAME;
    }

    @Override
    public DataSchema baseSchema() {
        return DataSchema.BYTES_SCHEMA;
    }

    @Override
    public DataType representationType() {
        return DataString.DATATYPE;
    }

    @Override
    public void validate(DataObject value) {
        if (value == null || value instanceof DataNull) return;
        if (!(value instanceof DataString stringValue))
            throw new DataException("Decimal value must be a string, but was " + value.getClass().getSimpleName());
        final var text = stringValue.value();
        if (text == null) return;

        final BigDecimal parsed;
        try {
            parsed = new BigDecimal(text);
        } catch (NumberFormatException _) {
            throw new DataException("Value \"" + text + "\" is not a valid decimal");
        }

        final BigDecimal scaled;
        try {
            scaled = parsed.setScale(scale, RoundingMode.UNNECESSARY);
        } catch (ArithmeticException _) {
            throw new DataException("Decimal value \"" + text + "\" has more fraction digits than the schema scale of " + scale);
        }

        if (scaled.precision() > precision)
            throw new DataException("Decimal value \"" + text + "\" needs " + scaled.precision()
                    + " digits of precision, but the schema allows only " + precision);
    }

    @Override
    public String toString() {
        return LOGICAL_TYPE_NAME + "(" + precision + "," + scale + ")";
    }
}
