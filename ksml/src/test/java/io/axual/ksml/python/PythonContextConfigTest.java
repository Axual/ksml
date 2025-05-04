package io.axual.ksml.python;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PythonContextConfigTest {

    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Test
    @DisplayName("Builder defaults all flags to false")
    void defaultBuilderFlags() {
        var cfg = PythonContextConfig.builder().build();
        assertFalse(cfg.allowHostFileAccess(),     "allowHostFileAccess should default to false");
        assertFalse(cfg.allowHostSocketAccess(),   "allowHostSocketAccess should default to false");
        assertFalse(cfg.allowNativeAccess(),       "allowNativeAccess should default to false");
        assertFalse(cfg.allowCreateProcess(),      "allowCreateProcess should default to false");
        assertFalse(cfg.allowCreateThread(),       "allowCreateThread should default to false");
        assertFalse(cfg.inheritEnvironmentVariables(), "inheritEnvironmentVariables should default to false");
    }

    @Test
    @DisplayName("YAML mapping populates all flags correctly")
    void yamlMappingFlags() throws Exception {
        var yaml = """
            allowHostFileAccess: true
            allowHostSocketAccess: true
            allowNativeAccess: true
            allowCreateProcess: true
            allowCreateThread: true
            inheritEnvironmentVariables: true
            """;
        var cfg = mapper.readValue(yaml, PythonContextConfig.class);

        assertTrue(cfg.allowHostFileAccess(),     "allowHostFileAccess should be true");
        assertTrue(cfg.allowHostSocketAccess(),   "allowHostSocketAccess should be true");
        assertTrue(cfg.allowNativeAccess(),       "allowNativeAccess should be true");
        assertTrue(cfg.allowCreateProcess(),      "allowCreateProcess should be true");
        assertTrue(cfg.allowCreateThread(),       "allowCreateThread should be true");
        assertTrue(cfg.inheritEnvironmentVariables(), "inheritEnvironmentVariables should be true");
    }
}
