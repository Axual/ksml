package io.axual.ksml.serde;

import io.axual.ksml.data.mapper.DataObjectMapper;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.notation.json.JsonDataObjectMapper;

public class JsonSerde extends StringSerde {
    private static final JsonDataObjectMapper MAPPER = new JsonDataObjectMapper();

    public JsonSerde(DataType expectedType) {
        super(new DataObjectMapper<>() {
            @Override
            public DataObject toDataObject(DataType expected, String value) {
                return MAPPER.toDataObject(expected, value);
            }

            @Override
            public String fromDataObject(DataObject value) {
                return MAPPER.fromDataObject(value);
            }
        }, expectedType);
    }
}
