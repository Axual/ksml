package io.axual.ksml.client.resolving;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ResolvingClientConfigTest {
    private static final String TOPIC_PATTERN_CONFIG = "axual.topic.pattern";
    private static final String GROUP_ID_PATTERN_CONFIG = "axual.group.id.pattern";
    private static final String TRANSACTIONAL_ID_PATTERN_CONFIG = "axual.transactional.id.pattern";

    private static final String TOPIC_PATTERN = "{instance}-{topic}";
    private static final String GROUP_ID_PATTERN = "{instance}-{group.id}";
    private static final String TRANSACTIONAL_ID_PATTERN = "{instance}-{transactional.id}";

    private static final String EXPECTED_DEFAULT_TOPIC_PATTERN = "{topic}";
    private static final String EXPECTED_DEFAULT_GROUP_ID_PATTERN = "{group.id}";
    private static final String EXPECTED_DEFAULT_TRANSACTIONAL_ID_PATTERN = "{transactional.id}";

    @Test
    @DisplayName("Pattern Resolvers use provided patterns")
    void allResolverConfigsSet() {
        var configAll = Map.of(
                TOPIC_PATTERN_CONFIG, TOPIC_PATTERN,
                GROUP_ID_PATTERN_CONFIG, GROUP_ID_PATTERN,
                TRANSACTIONAL_ID_PATTERN_CONFIG, TRANSACTIONAL_ID_PATTERN
        );

        final var clientConfig = new ResolvingClientConfig(configAll);

        assertThat(clientConfig)
                .isNotNull();

        verifyPatterns(clientConfig, TOPIC_PATTERN, GROUP_ID_PATTERN, TRANSACTIONAL_ID_PATTERN);
    }

    @Test
    @DisplayName("Missing Pattern Resolver patterns use default patterns")
    void noResolverConfigsSet() {
        final var clientConfig = new ResolvingClientConfig(Map.of());

        assertThat(clientConfig)
                .isNotNull();

        verifyPatterns(clientConfig, EXPECTED_DEFAULT_TOPIC_PATTERN, EXPECTED_DEFAULT_GROUP_ID_PATTERN, EXPECTED_DEFAULT_TRANSACTIONAL_ID_PATTERN);
    }

    static void verifyPatterns(ResolvingClientConfig clientConfig, String expectedTopicPattern, String expectedGroupIdPattern, String expectedTransactionalIdPattern) {
        assertThat(clientConfig)
                .isNotNull();

        final var softly = new SoftAssertions();

        softly.assertThat(clientConfig.topicResolver())
                .as("Verify the topic resolver is a pattern resolver with the provided pattern")
                .isNotNull()
                .isInstanceOf(CachedPatternResolver.class)
                .asInstanceOf(InstanceOfAssertFactories.type(CachedPatternResolver.class))
                .returns(expectedTopicPattern, CachedPatternResolver::pattern);

        softly.assertThat(clientConfig.groupResolver())
                .as("Verify the group resolver is a pattern resolver with the provided pattern")
                .isNotNull()
                .isInstanceOf(CachedPatternResolver.class)
                .asInstanceOf(InstanceOfAssertFactories.type(CachedPatternResolver.class))
                .returns(expectedGroupIdPattern, CachedPatternResolver::pattern);

        softly.assertThat(clientConfig.transactionalIdResolver())
                .as("Verify the transactional id resolver is a pattern resolver with the provided pattern")
                .isNotNull()
                .isInstanceOf(CachedPatternResolver.class)
                .asInstanceOf(InstanceOfAssertFactories.type(CachedPatternResolver.class))
                .returns(expectedTransactionalIdPattern, CachedPatternResolver::pattern);

        softly.assertAll();
    }

}