package io.axual.ksml.data.mapper;

import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataString;
import io.axual.ksml.data.type.DataType;

public class StringDataObjectMapper implements DataObjectMapper<String> {
    @Override
    public DataObject toDataObject(DataType expected, String value) {
        if (expected != DataString.DATATYPE) return null;
        return new DataString(value);
    }

    @Override
    public String fromDataObject(DataObject value) {
        if (value instanceof DataString val) return val.value();
        return null;
    }
}
