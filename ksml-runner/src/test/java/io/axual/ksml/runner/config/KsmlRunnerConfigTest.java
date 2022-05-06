package io.axual.ksml.runner.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class KsmlRunnerConfigTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setup() {
        objectMapper = new ObjectMapper(new YAMLFactory());
    }

    @Test
    @DisplayName("complete config should load without exceptions")
    void shouldLoadWithoutExceptions() throws IOException {
        final var yaml = getClass().getClassLoader().getResourceAsStream("ksml-runner-config.yml");
        final var ksmlRunnerConfig = objectMapper.readValue(yaml, KSMLRunnerConfig.class);

        ksmlRunnerConfig.validate();

        assertNotNull(ksmlRunnerConfig.getKsmlConfig());
        assertNotNull(ksmlRunnerConfig.getBackendConfig());
        assertEquals("kafka", ksmlRunnerConfig.getBackendConfig().getType());
    }
}
