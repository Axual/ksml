package io.axual.ksml.schema.mapper;

import io.axual.ksml.schema.DataSchema;

public interface DataSchemaMapper<T> {
    DataSchema toDataSchema(T value);

    T fromDataSchema(DataSchema object);
}
