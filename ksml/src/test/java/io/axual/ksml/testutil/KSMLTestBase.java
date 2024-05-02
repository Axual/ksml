package io.axual.ksml.testutil;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;

/**
 * Base class for KSML stream tests.
 * Instance variables will be filled in by {@link KSMLTestExtension}.
 */
@Slf4j
public class KSMLTestBase {

    protected final StreamsBuilder streamsBuilder = new StreamsBuilder();

    protected TestInputTopic inputTopic;

    protected TestOutputTopic outputTopic;

}
