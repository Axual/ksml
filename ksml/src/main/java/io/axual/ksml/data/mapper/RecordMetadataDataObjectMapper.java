package io.axual.ksml.data.mapper;

import io.axual.ksml.data.object.DataLong;
import io.axual.ksml.data.object.DataObject;
import io.axual.ksml.data.object.DataStruct;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;

import static io.axual.ksml.dsl.RecordContextSchema.RECORD_CONTEXT_SCHEMA_HEADERS_FIELD;
import static io.axual.ksml.dsl.RecordContextSchema.RECORD_CONTEXT_SCHEMA_TIMESTAMP_FIELD;
import static io.axual.ksml.dsl.RecordMetadataSchema.*;

public class RecordMetadataDataObjectMapper implements DataObjectMapper<RecordMetadata> {
    private static final HeaderDataObjectMapper HEADER_MAPPER = new HeaderDataObjectMapper();

    @Override
    public DataObject toDataObject(DataType expected, RecordMetadata value) {
        final var result = new DataStruct(RECORD_METADATA_SCHEMA);
        result.put(RECORD_METADATA_SCHEMA_TIMESTAMP_FIELD, new DataLong(value.timestamp()));
        result.put(RECORD_METADATA_SCHEMA_HEADERS_FIELD, HEADER_MAPPER.toDataObject(value.headers()));
        return result;
    }

    @Override
    public RecordMetadata fromDataObject(DataObject value) {
        if (!(value instanceof DataStruct valueStruct)) {
            throw new IllegalArgumentException("Can not convert to RecordMetadata from type " + value);
        }
        final var timestamp = valueStruct.getAs(RECORD_CONTEXT_SCHEMA_TIMESTAMP_FIELD, DataLong.class);
        final var headers = valueStruct.getAs(RECORD_CONTEXT_SCHEMA_HEADERS_FIELD, DataStruct.class);
        return new RecordMetadata(
                timestamp != null ? timestamp.value() : null,
                headers != null ? HEADER_MAPPER.fromDataObject(headers) : new RecordHeaders()
        );
    }
}
