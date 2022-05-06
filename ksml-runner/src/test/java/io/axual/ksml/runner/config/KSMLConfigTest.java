package io.axual.ksml.runner.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.axual.ksml.runner.exception.KSMLRunnerConfigurationException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KSMLConfigTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setup() {
        objectMapper = new ObjectMapper(new YAMLFactory());
    }

    @Test
    @DisplayName("validate of complete config should not throw exceptions")
    void shouldValidateConfig() throws Exception {
        final var yaml = getClass().getClassLoader().getResourceAsStream("ksml-config.yml");
        final var ksmlConfig = objectMapper.readValue(yaml, KSMLConfig.class);

        ksmlConfig.validate();
    }

    @Test
    @DisplayName("validate of incorrect config directory should throw exception")
    void shouldThrowOnWrongConfigdir() throws Exception {
        final var yaml = getClass().getClassLoader().getResourceAsStream("ksml-config-wrong-configdir.yml");
        final var ksmlConfig = objectMapper.readValue(yaml, KSMLConfig.class);

        assertThrows(KSMLRunnerConfigurationException.class, ksmlConfig::validate, "should throw exception for wrong configdir");
    }

    @Test
    @DisplayName("if configdir is missing it should default to workdir")
    void shouldDefaultConfigToWorkdir() throws Exception {
        final var yaml = getClass().getClassLoader().getResourceAsStream("ksml-config-no-configdir.yml");
        final var ksmlConfig = objectMapper.readValue(yaml, KSMLConfig.class);

        ksmlConfig.validate();

        assertEquals(".", ksmlConfig.getConfigurationDirectory(), "config dir should default to working dir");
    }
}
