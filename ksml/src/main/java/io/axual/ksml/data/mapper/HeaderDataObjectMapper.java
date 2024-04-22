package io.axual.ksml.data.mapper;

import io.axual.ksml.data.object.*;
import io.axual.ksml.data.serde.StringSerde;
import io.axual.ksml.data.type.DataType;
import io.axual.ksml.data.type.TupleType;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

public class HeaderDataObjectMapper implements DataObjectMapper<Headers> {
    private static final StringSerde STRING_SERDE = new StringSerde();
    private static final DataType HEADER_TYPE = new TupleType(DataString.DATATYPE, DataType.UNKNOWN);

    @Override
    public DataObject toDataObject(DataType expected, Headers value) {
        final var result = new DataList(HEADER_TYPE);
        value.forEach(header -> {
            final var element = new DataList(DataString.DATATYPE);
            element.add(new DataString(header.key()));
            element.add(convertHeaderValue(header.value()));
            result.add(element);
        });
        return result;
    }

    private DataObject convertHeaderValue(byte[] value) {
        try {
            var result = STRING_SERDE.deserializer().deserialize(null, value);
            return result != null ? new DataString(result.toString()) : DataNull.INSTANCE;
        } catch (Throwable t) {
            return new DataBytes(value);
        }
    }

    private byte[] convertHeaderValue(DataObject value) {
        if (value instanceof DataString val) {
            return STRING_SERDE.serializer().serialize(null, val);
        }
        if (value instanceof DataBytes val) {
            return val.value();
        }
        throw new IllegalArgumentException("Unsupported Kafka Header value type: " + value.type());
    }

    @Override
    public Headers fromDataObject(DataObject value) {
        final var result = new RecordHeaders();
        if (!(value instanceof DataList headers)) {
            throw new IllegalArgumentException("Invalid Kafka Headers type: " + value.type());
        }
        for (final var element : headers) {
            if (!(HEADER_TYPE.isAssignableFrom(element)) || !(element instanceof DataTuple header)) {
                throw new IllegalArgumentException("Invalid Kafka Header type: " + element.type());
            }
            if (header.size() != 2) {
                throw new IllegalArgumentException("Invalid Kafka Header: " + header);
            }
            final var hKey = header.get(0);
            final var hValue = header.get(1);
            if (!(hKey instanceof DataString headerKey)) {
                throw new IllegalArgumentException("Invalid Kafka Header key type: " + hKey.type());
            }
            result.add(headerKey.value(), convertHeaderValue(hValue));
        }
        return null;
    }
}
