package io.axual.ksml.data.parser;

import io.axual.ksml.data.object.DataObject;

public interface DataObjectParser {
    DataObject parse(Object value);
}
