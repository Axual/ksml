package io.axual.ksml.data.object;

import io.axual.ksml.data.schema.DataField;
import io.axual.ksml.data.schema.DataSchema;
import io.axual.ksml.data.schema.EnumSchema;
import io.axual.ksml.data.schema.StructSchema;
import io.axual.ksml.data.type.Symbol;

import java.util.List;

public class TestSchema {
    private static final String NAMESPACE = "io.axual.ksml.data.test";
    private static final String NAME = "name";
    private static final String AGE = "age";
    private static final String ADDRESS = "address";
    private static final String STREET = "street";
    private static final String POSTAL_CODE = "postalCode";
    private static final String CITY = "city";
    private static final String COUNTRY = "country";
    private static final String EYE_COLOR = "eyeColor";

    public StructSchema testSchema() {
        final var addressFields = List.of(
                new DataField(STREET, DataSchema.STRING_SCHEMA, "Street field", 1),
                new DataField(POSTAL_CODE, DataSchema.STRING_SCHEMA, "Postal code field", 2),
                new DataField(CITY, DataSchema.STRING_SCHEMA, "City field", 3),
                new DataField(COUNTRY, DataSchema.STRING_SCHEMA, "Country field", 4));
        final var addressSchema = new StructSchema(NAMESPACE, "AddressSchema", "Address schema used for testing", addressFields);

        final var eyeColorSymbols = List.of(
                new Symbol("UNKNOWN", "Unknown color eyes", 0),
                new Symbol("BLUE", "Blue eyes", 1),
                new Symbol("GREEN", "Green eyes", 2),
                new Symbol("BROWN", "Brown eyes", 3),
                new Symbol("GREY", "Grey eyes", 4));
        final var eyeColorSchema = new EnumSchema(NAMESPACE, "EyeColor", "The color of one's eyes", eyeColorSymbols);

        final var fields = List.of(
                new DataField(NAME, DataSchema.STRING_SCHEMA, "Name field", 1),
                new DataField(AGE, DataSchema.INTEGER_SCHEMA, "Age field", 2),
                new DataField(ADDRESS, addressSchema, "Address fields", 3, false),
                new DataField(EYE_COLOR, eyeColorSchema, "The color of one's eyes"));
        return new StructSchema(NAMESPACE, "TestSchema", "Schema used for testing", fields);
    }

    public DataStruct testStruct() {
        final var address = new DataStruct((StructSchema) testSchema().field(ADDRESS).schema());
        address.put(STREET, new DataString("Jaarbeursplein 22"));
        address.put(POSTAL_CODE, new DataString("3521AP"));
        address.put(CITY, new DataString("Utrecht"));
        address.put(COUNTRY, new DataString("Netherlands"));

        final var result = new DataStruct(testSchema());
        result.put(NAME, new DataString("John Doe"));
        result.put(AGE, new DataInteger(42));
        result.put(ADDRESS, address);
        result.put(EYE_COLOR, new DataString("BLUE"));
        return result;
    }
}
