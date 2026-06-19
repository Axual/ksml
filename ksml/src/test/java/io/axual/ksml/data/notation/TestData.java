package io.axual.ksml.data.notation;

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

import io.axual.ksml.data.object.DataEnum;
import io.axual.ksml.data.object.DataInteger;
import io.axual.ksml.data.object.DataList;
import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.ListSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.schema.UnionSchema;
import io.axual.ksml.data.type.EnumType;

import java.util.List;

import static io.axual.ksml.data.schema.DataSchemaConstants.NO_TAG;

public class TestData {
    private static final String NAMESPACE = "io.axual.ksml.data.test";
    private static final String NAME = "name";
    private static final String AGE = "age";
    private static final String ADDRESS = "address";
    private static final String SHIPPING_ADDRESS = "shippingAddress";
    private static final String STREET = "street";
    private static final String POSTAL_CODE = "postalCode";
    private static final String CITY = "city";
    private static final String COUNTRY = "country";
    private static final String EYE_COLOR = "eyeColor";
    private static final String LUCKY_NUMBERS = "luckyNumbers";
    private static final String ACCOUNT_NUMBER = "accountNumber";

    /**
     * Variants describing which subset of schema features a notation can carry losslessly through its wire format.
     */
    public enum Variant {
        /** All features: required fields, INTEGER age, enum default value. */
        FULL,
        /** {@link #FULL} but with LONG age — JSON Schema has no int32/int64 distinction. */
        JSON,
        /** {@link #FULL} but all fields optional and no enum default — proto3 has no {@code required} and drops defaults. */
        PROTOBUF
    }

    public static StructSchema testSchema() {
        return testSchema(Variant.FULL);
    }

    public static DataStruct testStruct() {
        return testStruct(Variant.FULL);
    }

    public static StructSchema testSchema(Variant variant) {
        final var addressSchema = addressSchema(variant);
        final var eyeColorSchema = eyeColorSchema();
        final var luckyNumbersSchema = new ListSchema(DataSchema.LONG_SCHEMA);
        final var accountNumberSchema = accountNumberSchema();

        final var ageSchema = variant == Variant.JSON ? DataSchema.LONG_SCHEMA : DataSchema.INTEGER_SCHEMA;
        final boolean fieldsRequired = variant != Variant.PROTOBUF;
        // Proto3 wire format drops field defaults; use the no-default constructor so the optional field's
        // defaultValue matches what the protobuf reader produces (DataNull.INSTANCE).
        final var eyeColor = variant == Variant.PROTOBUF
                ? new StructSchema.Field(EYE_COLOR, eyeColorSchema, "Eye color", 5, false)
                : new StructSchema.Field(EYE_COLOR, eyeColorSchema, "Eye color", 5, true, false, new DataString("BLUE"));

        final var fields = List.of(
                new StructSchema.Field(NAME, DataSchema.STRING_SCHEMA, "Name", 1, fieldsRequired),
                new StructSchema.Field(AGE, ageSchema, "Age", 2, fieldsRequired),
                new StructSchema.Field(ADDRESS, addressSchema, "Address", 3, false),
                new StructSchema.Field(SHIPPING_ADDRESS, addressSchema, "Shipping address", 4, false),
                eyeColor,
                new StructSchema.Field(LUCKY_NUMBERS, luckyNumbersSchema, "Lucky numbers", 6, false),
                new StructSchema.Field(ACCOUNT_NUMBER, accountNumberSchema, "Account number", NO_TAG, false));
        return new StructSchema(NAMESPACE, "TestSchema", "Schema used for testing", fields, false);
    }

    public static DataStruct testStruct(Variant variant) {
        final var schema = testSchema(variant);

        final var address = new DataStruct((StructSchema) schema.field(ADDRESS).schema());
        address.put(STREET, new DataString("Jaarbeursplein 22"));
        address.put(POSTAL_CODE, new DataString("3521AP"));
        address.put(CITY, new DataString("Utrecht"));
        address.put(COUNTRY, new DataString("Netherlands"));

        final var luckyNumbers = new DataList(DataLong.DATATYPE);
        luckyNumbers.add(new DataLong(7L));
        luckyNumbers.add(new DataLong(13L));
        luckyNumbers.add(new DataLong(42L));
        luckyNumbers.add(new DataLong(1111111111111111111L));

        final var result = new DataStruct(schema);
        result.put(NAME, new DataString("Jim Kirk"));
        result.put(AGE, variant == Variant.JSON ? new DataLong(74L) : new DataInteger(74));
        result.put(ADDRESS, address);
        result.put(EYE_COLOR, new DataEnum(new EnumType((EnumSchema) schema.field(EYE_COLOR).schema()), "BLUE"));
        result.put(LUCKY_NUMBERS, luckyNumbers);
        result.put(ACCOUNT_NUMBER, new DataString("NL99BANK123456789"));

        return result;
    }

    private static StructSchema addressSchema(Variant variant) {
        final boolean fieldsRequired = variant != Variant.PROTOBUF;
        final var fields = List.of(
                new StructSchema.Field(STREET, DataSchema.STRING_SCHEMA, "Street field", 11, fieldsRequired),
                new StructSchema.Field(POSTAL_CODE, DataSchema.STRING_SCHEMA, "Postal code field", 12, fieldsRequired),
                new StructSchema.Field(CITY, DataSchema.STRING_SCHEMA, "City field", 13, fieldsRequired),
                new StructSchema.Field(COUNTRY, DataSchema.STRING_SCHEMA, "Country field", 14, fieldsRequired));
        return new StructSchema(NAMESPACE, "AddressSchema", "Address schema used for testing", fields, false);
    }

    private static EnumSchema eyeColorSchema() {
        final var symbols = List.of(
                new EnumSchema.Symbol("UNKNOWN", "Unknown color", 0),
                new EnumSchema.Symbol("BLUE", "Blue eyes", 1),
                new EnumSchema.Symbol("GREEN", "Green eyes", 2),
                new EnumSchema.Symbol("BROWN", "Brown eyes", 3),
                new EnumSchema.Symbol("GREY", "Grey eyes", 4));
        return new EnumSchema(NAMESPACE, "EyeColor", "The color of one's eyes", symbols);
    }

    private static UnionSchema accountNumberSchema() {
        return new UnionSchema(
                new UnionSchema.Member("bban", DataSchema.LONG_SCHEMA, "BBAN", 23),
                new UnionSchema.Member("iban", DataSchema.STRING_SCHEMA, "IBAN", 24));
    }
}
