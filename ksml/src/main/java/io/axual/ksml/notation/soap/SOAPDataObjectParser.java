package io.axual.ksml.notation.soap;

import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.parser.DataObjectParser;

public class SOAPDataObjectParser implements DataObjectParser {
    private static final SOAPDataObjectMapper DATA_OBJECT_MAPPER = new SOAPDataObjectMapper();
    private static final SOAPStringMapper STRING_MAPPER = new SOAPStringMapper();

    @Override
    public DataObject parse(Object value) {
        if (value instanceof String str) {
            return DATA_OBJECT_MAPPER.toDataObject(STRING_MAPPER.fromString(str));
        }
        return null;
    }
}
