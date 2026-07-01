package io.axual.ksml.data.util;

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

import io.axual.ksml.data.exception.DataException;
import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.mapper.NativeDataObjectMapper;
import io.axual.ksml.data.object.DataByte;
import io.axual.ksml.data.object.DataDouble;
import io.axual.ksml.data.object.DataFloat;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataBoolean;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataMap;
import io.axual.ksml.data.object.DataNull;
import io.axual.ksml.data.object.DataShort;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataTuple;
import io.axual.ksml.data.type.ListType;
import io.axual.ksml.data.type.MapType;
import io.axual.ksml.data.type.TupleType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class ConvertUtilTest {
    private final ConvertUtil converter = new ConvertUtil(new NativeDataObjectMapper(), new DataTypeDataSchemaMapper());

    @Test
    @DisplayName("convert: DataLong exceeding Integer range to DataInteger -> DataException")
    void convertDataLongOverflowToInteger() {
        final var aboveMax = new DataLong(3_000_000_001L);
        final var belowMin = new DataLong((long) Integer.MIN_VALUE - 1);
        assertThatCode(() -> converter.convert(DataInteger.DATATYPE, aboveMax))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("exceeds INT range");
        assertThatCode(() -> converter.convert(DataInteger.DATATYPE, belowMin))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("exceeds INT range");
    }

    @Test
    @DisplayName("convert: DataLong within Integer range to DataInteger -> DataInteger")
    void convertDataLongWithinIntegerRange() {
        assertThat(converter.convert(DataInteger.DATATYPE, new DataLong(42L))).isEqualTo(new DataInteger(42));
        assertThat(converter.convert(DataInteger.DATATYPE, new DataLong((long) Integer.MAX_VALUE)))
                .isEqualTo(new DataInteger(Integer.MAX_VALUE));
        assertThat(converter.convert(DataInteger.DATATYPE, new DataLong((long) Integer.MIN_VALUE)))
                .isEqualTo(new DataInteger(Integer.MIN_VALUE));
    }

    @Test
    @DisplayName("convert: DataLong exceeding Byte range to DataByte -> DataException")
    void convertDataLongOverflowToByte() {
        final var overflow = new DataLong(200L);
        assertThatCode(() -> converter.convert(DataByte.DATATYPE, overflow))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("exceeds BYTE range");
    }

    @Test
    @DisplayName("convert: DataLong exceeding Short range to DataShort -> DataException")
    void convertDataLongOverflowToShort() {
        final var overflow = new DataLong(40_000L);
        assertThatCode(() -> converter.convert(DataShort.DATATYPE, overflow))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("exceeds SHORT range");
    }

    @Test
    @DisplayName("convert: DataShort exceeding Byte range to DataByte -> DataException")
    void convertDataShortOverflowToByte() {
        final var overflow = new DataShort((short) 200);
        assertThatCode(() -> converter.convert(DataByte.DATATYPE, overflow))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("exceeds BYTE range");
    }

    @Test
    @DisplayName("convert: DataInteger exceeding Byte range to DataByte -> DataException")
    void convertDataIntegerOverflowToByte() {
        final var overflow = new DataInteger(200);
        assertThatCode(() -> converter.convert(DataByte.DATATYPE, overflow))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("exceeds BYTE range");
    }

    @Test
    @DisplayName("convert: DataShort/DataInteger/DataLong within Byte range -> DataByte accepted")
    void convertWithinByteRange() {
        assertThat(converter.convert(DataByte.DATATYPE, new DataShort((short) 42))).isEqualTo(new DataByte((byte) 42));
        assertThat(converter.convert(DataByte.DATATYPE, new DataInteger(42))).isEqualTo(new DataByte((byte) 42));
        assertThat(converter.convert(DataByte.DATATYPE, new DataLong(42L))).isEqualTo(new DataByte((byte) 42));
        assertThat(converter.convert(DataByte.DATATYPE, new DataLong((long) Byte.MAX_VALUE)))
                .isEqualTo(new DataByte(Byte.MAX_VALUE));
    }

    @Test
    @DisplayName("convert: DataInteger exceeding Short range to DataShort -> DataException")
    void convertDataIntegerOverflowToShort() {
        final var overflow = new DataInteger(40_000);
        assertThatCode(() -> converter.convert(DataShort.DATATYPE, overflow))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("exceeds SHORT range");
    }

    @Test
    @DisplayName("convert: DataInteger/DataLong within Short range -> DataShort accepted")
    void convertWithinShortRange() {
        assertThat(converter.convert(DataShort.DATATYPE, new DataInteger(1000))).isEqualTo(new DataShort((short) 1000));
        assertThat(converter.convert(DataShort.DATATYPE, new DataLong(1000L))).isEqualTo(new DataShort((short) 1000));
        assertThat(converter.convert(DataShort.DATATYPE, new DataLong((long) Short.MAX_VALUE)))
                .isEqualTo(new DataShort(Short.MAX_VALUE));
    }

    @Test
    @DisplayName("convert: DataDouble exceeding Float range to DataFloat -> DataException")
    void convertDataDoubleOverflowToFloat() {
        final var positiveOverflow = new DataDouble((double) Float.MAX_VALUE * 10);
        final var negativeOverflow = new DataDouble(-(double) Float.MAX_VALUE * 10);
        assertThatCode(() -> converter.convert(DataFloat.DATATYPE, positiveOverflow))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("FLOAT range");
        assertThatCode(() -> converter.convert(DataFloat.DATATYPE, negativeOverflow))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("FLOAT range");
    }

    @Test
    @DisplayName("convert: DataDouble non-finite to DataFloat -> passes through (cast preserves NaN/Infinity)")
    void convertDataDoubleNonFiniteToFloatPassesThrough() {
        assertThat(converter.convert(DataFloat.DATATYPE, new DataDouble(Double.NaN)))
                .isEqualTo(new DataFloat(Float.NaN));
        assertThat(converter.convert(DataFloat.DATATYPE, new DataDouble(Double.POSITIVE_INFINITY)))
                .isEqualTo(new DataFloat(Float.POSITIVE_INFINITY));
        assertThat(converter.convert(DataFloat.DATATYPE, new DataDouble(Double.NEGATIVE_INFINITY)))
                .isEqualTo(new DataFloat(Float.NEGATIVE_INFINITY));
    }

    @Test
    @DisplayName("convert: DataDouble within Float range -> DataFloat accepted, even when bit-exact precision is lost")
    void convertDataDoubleWithinFloatRangeAccepted() {
        // 0.1 is not exactly representable as float; conversion is still accepted (matches Java cast semantics).
        assertThat(converter.convert(DataFloat.DATATYPE, new DataDouble(0.1))).isEqualTo(new DataFloat(0.1f));
        assertThat(converter.convert(DataFloat.DATATYPE, new DataDouble((double) Float.MAX_VALUE)))
                .isEqualTo(new DataFloat(Float.MAX_VALUE));
    }

    @Test
    @DisplayName("convert: DataFloat with Infinity to DataLong -> DataException")
    void convertDataFloatInfiniteToLong() {
        final var infinity = new DataFloat(Float.POSITIVE_INFINITY);
        assertThatCode(() -> converter.convert(DataLong.DATATYPE, infinity))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("LONG");
    }

    @Test
    @DisplayName("convert: DataDouble non-finite to DataLong -> DataException")
    void convertDataDoubleNonFiniteToLong() {
        final var nan = new DataDouble(Double.NaN);
        assertThatCode(() -> converter.convert(DataLong.DATATYPE, nan))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("LONG");
    }

    @Test
    @DisplayName("convert: DataDouble finite but exceeding LONG range -> DataException (no silent clamp)")
    void convertDataDoubleFiniteExceedingLongRange() {
        final var maxDouble = new DataDouble(Double.MAX_VALUE);
        // 2^63 is the first double that does not fit in a signed long
        final var twoPow63 = new DataDouble(0x1.0p63);
        assertThatCode(() -> converter.convert(DataLong.DATATYPE, maxDouble))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("LONG");
        assertThatCode(() -> converter.convert(DataLong.DATATYPE, twoPow63))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("LONG");
    }

    @Test
    @DisplayName("convert: DataDouble exceeding Integer range to DataInteger -> DataException")
    void convertDataDoubleOverflowToInteger() {
        final var overflow = new DataDouble(3_000_000_000.0);
        assertThatCode(() -> converter.convert(DataInteger.DATATYPE, overflow))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("INT");
    }

    // ---- convertStringToDataObject: parseOrFail honours allowFail ----

    @Test
    @DisplayName("convertStringToDataObject: malformed numeric string with allowFail=false throws")
    void convertStringToDataObject_malformedNumeric_failsLoudly() {
        assertThatCode(() -> converter.convertStringToDataObject(DataInteger.DATATYPE, "abc", false))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Can not parse");
        assertThatCode(() -> converter.convertStringToDataObject(DataLong.DATATYPE, "not-a-number", false))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Can not parse");
        assertThatCode(() -> converter.convertStringToDataObject(DataDouble.DATATYPE, "nope", false))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("Can not parse");
    }

    @Test
    @DisplayName("convertStringToDataObject: malformed numeric string with allowFail=true returns null")
    void convertStringToDataObject_malformedNumeric_allowFail_returnsNull() {
        assertThat(converter.convertStringToDataObject(DataInteger.DATATYPE, "abc", true)).isNull();
        assertThat(converter.convertStringToDataObject(DataLong.DATATYPE, "not-a-number", true)).isNull();
    }

    @Test
    @DisplayName("convertStringToDataObject: valid numeric string parses to the requested type")
    void convertStringToDataObject_validNumeric_parses() {
        assertThat(converter.convertStringToDataObject(DataInteger.DATATYPE, "42", false)).isEqualTo(new DataInteger(42));
        assertThat(converter.convertStringToDataObject(DataLong.DATATYPE, "9999999999", false)).isEqualTo(new DataLong(9_999_999_999L));
    }

    // ---- convertStringToDataObject: DataBoolean parsing ----

    @Test
    @DisplayName("convertStringToDataObject: \"true\"/\"false\" (case-insensitive) parses to DataBoolean")
    void convertStringToDataObject_parsesBoolean() {
        assertThat(converter.convertStringToDataObject(DataBoolean.DATATYPE, "true", false)).isEqualTo(new DataBoolean(true));
        assertThat(converter.convertStringToDataObject(DataBoolean.DATATYPE, "TRUE", false)).isEqualTo(new DataBoolean(true));
        assertThat(converter.convertStringToDataObject(DataBoolean.DATATYPE, "false", false)).isEqualTo(new DataBoolean(false));
        assertThat(converter.convertStringToDataObject(DataBoolean.DATATYPE, "False", false)).isEqualTo(new DataBoolean(false));
    }

    @Test
    @DisplayName("convertStringToDataObject: non-boolean string to DataBoolean honours allowFail")
    void convertStringToDataObject_invalidBoolean_honoursAllowFail() {
        assertThatCode(() -> converter.convertStringToDataObject(DataBoolean.DATATYPE, "yes", false))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("BOOLEAN");
        assertThat(converter.convertStringToDataObject(DataBoolean.DATATYPE, "yes", true)).isNull();
    }

    // ---- complex (recursive) conversions ----

    @Test
    @DisplayName("convert: a list is converted element-by-element to the target value type")
    void convertListElements() {
        final var source = new DataList(DataInteger.DATATYPE);
        source.add(new DataInteger(1));
        source.add(new DataInteger(2));

        final var result = (DataList) converter.convert(new ListType(DataLong.DATATYPE), source);

        assertThat(result.size()).isEqualTo(2);
        assertThat(result.get(0)).isEqualTo(new DataLong(1L));
        assertThat(result.get(1)).isEqualTo(new DataLong(2L));
    }

    @Test
    @DisplayName("convert: a map's values are converted to the target value type")
    void convertMapValues() {
        final var source = new DataMap(DataInteger.DATATYPE);
        source.put("a", new DataInteger(1));

        final var result = (DataMap) converter.convert(new MapType(DataLong.DATATYPE), source);

        assertThat(result.get("a")).isEqualTo(new DataLong(1L));
    }

    @Test
    @DisplayName("convert: a tuple's elements are converted position-by-position to the target types")
    void convertTupleElements() {
        final var source = new DataTuple(new DataInteger(1), new DataInteger(2));

        final var result = (DataTuple) converter.convert(new TupleType(DataLong.DATATYPE, DataString.DATATYPE), source);

        assertThat(result.elements().get(0)).isEqualTo(new DataLong(1L));
        assertThat(result.elements().get(1)).isEqualTo(new DataString("2"));
    }

    @Test
    @DisplayName("convert: a DataNull becomes a (null) instance of the requested complex type")
    void convertNullToComplexType() {
        final var result = converter.convert(new ListType(DataLong.DATATYPE), DataNull.INSTANCE);

        assertThat(result).isInstanceOf(DataList.class);
    }
}
