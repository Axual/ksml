package io.axual.ksml.testutil;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Marks an instance variable of type {@link org.apache.kafka.streams.TestInputTopic} as input to KSML tests.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface InputTopic {
}
