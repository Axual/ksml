package io.axual.ksml.data.notation;

/*-
 * ========================LICENSE_START=================================
 * KSML Data Library - PROTOBUF
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

import io.axual.ksml.data.mapper.DataTypeDataSchemaMapper;
import io.axual.ksml.data.object.*;
import io.axual.ksml.data.schema.*;
import io.axual.ksml.data.type.Symbol;

import java.util.List;

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

    public static StructSchema testSchema() {
        final var addressFields = List.of(
                new DataField(STREET, DataSchema.STRING_SCHEMA, "Street field", 11),
                new DataField(POSTAL_CODE, DataSchema.STRING_SCHEMA, "Postal code field", 12),
                new DataField(CITY, DataSchema.STRING_SCHEMA, "City field", 13),
                new DataField(COUNTRY, DataSchema.STRING_SCHEMA, "Country field", 14));
        final var addressSchema = new StructSchema(NAMESPACE, "AddressSchema", "Address schema used for testing", addressFields);

        final var eyeColorSymbols = List.of(
                new Symbol("UNKNOWN", "Unknown color", 0),
                new Symbol("BLUE", "Blue eyes", 1),
                new Symbol("GREEN", "Green eyes", 2),
                new Symbol("BROWN", "Brown eyes", 3),
                new Symbol("GREY", "Grey eyes", 4));
        final var eyeColorSchema = new EnumSchema(NAMESPACE, "EyeColor", "The color of one's eyes", eyeColorSymbols);

        final var luckyNumbersSchema = new ListSchema(DataSchema.LONG_SCHEMA);

        final var bbanSchema = DataSchema.LONG_SCHEMA;
        final var ibanSchema = DataSchema.STRING_SCHEMA;
        final var accountNumberSchema = new UnionSchema(
                new DataField("bban", bbanSchema, "BBAN", 23),
                new DataField("iban", ibanSchema, "IBAN", 24));

        final var fields = List.of(
                new DataField(NAME, DataSchema.STRING_SCHEMA, "Name", 1),
                new DataField(AGE, DataSchema.INTEGER_SCHEMA, "Age", 2),
                new DataField(ADDRESS, addressSchema, "Address", 3, false),
                new DataField(SHIPPING_ADDRESS, addressSchema, "Shipping address", 4, false),
                new DataField(EYE_COLOR, eyeColorSchema, "Eye color", 5, true, false, new DataValue("BLUE")),
                new DataField(LUCKY_NUMBERS, luckyNumbersSchema, "Lucky numbers", 6, false),
                new DataField(ACCOUNT_NUMBER, accountNumberSchema, "Account number", DataField.NO_TAG, false));
        return new StructSchema(NAMESPACE, "TestSchema", "Schema used for testing", fields);
    }

    public static DataStruct testStruct() {
        final var address = new DataStruct((StructSchema) testSchema().field(ADDRESS).schema());
        address.put(STREET, new DataString("Jaarbeursplein 22"));
        address.put(POSTAL_CODE, new DataString("3521AP"));
        address.put(CITY, new DataString("Utrecht"));
        address.put(COUNTRY, new DataString("Netherlands"));

        final var luckyNumbers = new DataList(DataLong.DATATYPE);
        luckyNumbers.add(new DataLong(7L));
        luckyNumbers.add(new DataLong(13L));
        luckyNumbers.add(new DataLong(42L));
        luckyNumbers.add(new DataLong(1111111111111111111L));

        final var result = new DataStruct(testSchema());
        result.put(NAME, new DataString("Jim Kirk"));
        result.put(AGE, new DataInteger(74));
        result.put(ADDRESS, address);
        result.put(EYE_COLOR, new DataString("BLUE"));
        result.put(LUCKY_NUMBERS, luckyNumbers);
        result.put(ACCOUNT_NUMBER, new DataString("NL99BANK123456789"));

        return result;
    }
}
