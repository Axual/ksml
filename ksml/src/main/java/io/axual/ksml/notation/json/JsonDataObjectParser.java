package io.axual.ksml.notation.json;

import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.parser.DataObjectParser;

public class JsonDataObjectParser implements DataObjectParser {
    private static final JsonDataObjectMapper DATA_OBJECT_MAPPER = new JsonDataObjectMapper();

    @Override
    public DataObject parse(Object value) {
        if (value instanceof String str) {
            return DATA_OBJECT_MAPPER.toDataObject(str);
        }
        return null;
    }
}
