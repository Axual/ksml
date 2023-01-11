package io.axual.ksml.notation.xml;

import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.parser.DataObjectParser;

public class XmlDataObjectParser implements DataObjectParser {
    private static final XmlDataObjectMapper DATA_OBJECT_MAPPER = new XmlDataObjectMapper();

    @Override
    public DataObject parse(Object value) {
        if (value instanceof String str) {
            return DATA_OBJECT_MAPPER.toDataObject(str);
        }
        return null;
    }
}
