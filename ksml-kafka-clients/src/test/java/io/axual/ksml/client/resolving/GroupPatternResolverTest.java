package io.axual.ksml.client.resolving;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GroupPatternResolverTest {

    public static final String PATTERN_WITH_DEFAULT_PLACEHOLDER = "{tenant}-{instance}-{environment}-{group.id}";
    public static final String PATTERN_WITH_ALIAS_PLACEHOLDER = "{tenant}-{instance}-{environment}-{group}";
    public static final String TENANT = "kmsl";
    public static final String INSTANCE = "testing";
    public static final String ENVIRONMENT = "test";

    public static final String GROUP_ID = "some.testing-group";
    public static final String RESOLVED_GROUP_ID = "%s-%s-%s-%s".formatted(TENANT, INSTANCE, ENVIRONMENT, GROUP_ID);

    public static final Map<String, String> BASE_CONFIG = Map.of(
            "tenant", TENANT,
            "instance", INSTANCE,
            "environment", ENVIRONMENT);

    @ParameterizedTest
    @DisplayName("Resolve groups")
    @ValueSource(strings = {PATTERN_WITH_DEFAULT_PLACEHOLDER, PATTERN_WITH_ALIAS_PLACEHOLDER})
    void resolveValidPatternAndData(String pattern) {
        var resolver = new GroupPatternResolver(pattern, BASE_CONFIG);
        var resolved = resolver.resolve(GROUP_ID);
        assertEquals(RESOLVED_GROUP_ID, resolved);

        var unresolved = resolver.unresolve(RESOLVED_GROUP_ID);
        assertEquals(GROUP_ID, unresolved);
    }

    @ParameterizedTest
    @DisplayName("Pattern null causes exception")
    @NullSource
    @ValueSource(strings = {"", "nothing-here", "{group.id"})
    void nullPatternException(String pattern) {
        assertThrows(IllegalArgumentException.class, () -> new GroupPatternResolver(pattern, BASE_CONFIG));
    }

}