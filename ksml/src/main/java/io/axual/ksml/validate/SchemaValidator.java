package io.axual.ksml.validate;

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

public class SchemaValidator {
//    public record ValidationResult(boolean valid, String error) {
//        public ValidationResult() {
//            this(true);
//        }
//
//        public ValidationResult(boolean valid) {
//            this(valid, "");
//        }
//
//        public ValidationResult(String error) {
//            this(false, error);
//        }
//    }
//
//    public SchemaValidator() {
//    }
//
//    public ValidationResult validate(DataSchema schema, DataObject object) {
//        return switch (schema.dataType()) {
//            case NULL -> validateType(object, DataNull.DATATYPE);
//            case BOOLEAN -> validateType(object, DataBoolean.DATATYPE);
//            case BYTE -> validateType(object, DataByte.DATATYPE);
//            case BYTES -> validateType(object, DataBytes.DATATYPE);
//            case FIXED -> validateFixed(object, (FixedSchema) schema);
//            case SHORT -> validateType(object, DataShort.DATATYPE);
//            case DOUBLE -> validateType(object, DataDouble.DATATYPE);
//            case FLOAT -> validateType(object, DataFloat.DATATYPE);
//            case INTEGER -> validateType(object, DataInteger.DATATYPE);
//            case LONG -> validateType(object, DataLong.DATATYPE);
//            case STRING -> validateType(object, DataString.DATATYPE);
//            case ENUM -> validateEnum(object, (EnumSchema) schema);
//            case LIST -> validateArray(object, (ListSchema) schema);
//            case MAP -> validateMap(object, (MapSchema) schema);
//            case RECORD -> validateRecord(object, (RecordSchema) schema);
//            case UNION -> validateUnion(object, (UnionSchema) schema);
//        };
//    }
//
//    private ValidationResult typeError(DataType expectedType, DataObject object) {
//        return new ValidationResult("Value is not of dataType " + object.dataType() + ": " + expectedType);
//    }
//
//    private ValidationResult validateType(DataObject object, DataType dataType) {
//        if (object.dataType().equals(dataType)) return new ValidationResult();
//        return typeError(dataType, object);
//    }
//
//    private ValidationResult validateFixed(DataObject object, FixedSchema schema) {
//        var typeCheck = validateType(object, DataBytes.DATATYPE);
//        if (!typeCheck.valid) return typeCheck;
//        var dataBytes = (DataBytes) object;
//        if (dataBytes.value().length == schema.size()) return new ValidationResult();
//        return new ValidationResult("Size of Fixed value is " + dataBytes.value().length + " bytes instead of " + schema.size());
//    }
//
//    public ValidationResult validateArray(DataObject object, ListSchema schema) {
//        var typeCheck = validateType(object, new ListType(SchemaUtil.schemaToDataType(schema.valueType())));
//        if (!typeCheck.valid) return typeCheck;
//        var dataList = (DataList) object;
//        for (DataObject element : dataList) {
//            typeCheck = validateType(element, dataList.valueType());
//            if (!typeCheck.valid) return typeCheck;
//        }
//        return new ValidationResult();
//    }
//
//    private ValidationResult validateEnum(DataObject object, EnumSchema schema) {
//        var typeCheck = validateType(object, SchemaUtil.schemaToEnumType(schema));
//        if (!typeCheck.valid()) return typeCheck;
//        var dataEnum = (DataEnum) object;
//        for (final var symbol : schema.symbols()) {
//            if (dataEnum.value().equals(symbol)) return new ValidationResult();
//        }
//        return new ValidationResult("Illegal enumeration value: " + dataEnum.value());
//    }
//
//    private ValidationResult validateMap(DataObject object, MapSchema schema) {
//        var typeCheck = validateType(object, new MapType());
//        if (!typeCheck.valid) return typeCheck;
//        var dataRecord = (DataRecord) object;
//        var valueType = SchemaUtil.schemaToDataType(schema.valueSchema());
//        for (Map.Entry<String, DataObject> entry : dataRecord.entrySet()) {
//            typeCheck = validateType(entry.getValue(), valueType);
//            if (!typeCheck.valid) return typeCheck;
//        }
//        return new ValidationResult();
//    }
//
//    private ValidationResult validateRecord(DataObject object, RecordSchema schema) {
//        var typeCheck = validateType(object, new RecordType(schema));
//        if (!typeCheck.valid) return typeCheck;
//        var dataRecord = (DataRecord) object;
//
//        // Check if all fields in the record are correct
//        for (Map.Entry<String, DataObject> entry : dataRecord.entrySet()) {
//            typeCheck = validate(schema.field(entry.getKey()).schema(), entry.getValue());
//            if (!typeCheck.valid) return typeCheck;
//        }
//
//        // Then check if all non-existing fields in the record are allowed to be null
//        for (StructField field : schema.fields()) {
//            if (!dataRecord.containsKey(field.name())) {
//                typeCheck = validate(field.schema(), null);
//                if (!typeCheck.valid) return typeCheck;
//            }
//        }
//
//        // All is well, so return OK
//        return new ValidationResult();
//    }
//
//    private ValidationResult validateUnion(DataObject object, UnionSchema schema) {
//        //TODO
//        return new ValidationResult();
//    }
}
