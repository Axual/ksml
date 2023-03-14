package io.axual.ksml.execution;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;

public class ExecutionErrorHandler implements DeserializationExceptionHandler, ProductionExceptionHandler {

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
        return ExecutionContext.INSTANCE.handle(context, record, exception);
    }

    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
        return ExecutionContext.INSTANCE.handle(record, exception);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
