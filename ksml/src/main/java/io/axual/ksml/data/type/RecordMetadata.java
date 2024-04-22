package io.axual.ksml.data.type;

import org.apache.kafka.common.header.Headers;

import static io.axual.ksml.dsl.RecordMetadataSchema.RECORD_METADATA_SCHEMA;

public record RecordMetadata(Long timestamp, Headers headers) {
    public static StructType DATATYPE = new StructType(RECORD_METADATA_SCHEMA);
}
