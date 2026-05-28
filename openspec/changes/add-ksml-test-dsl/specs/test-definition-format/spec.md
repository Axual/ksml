## ADDED Requirements

### Requirement: Centralized DSL constants for YAML field names
The Java implementation of the test-runner SHALL expose every YAML field name of the test-definition format as a `public static final String` constant on a single class, `io.axual.ksml.testrunner.KSMLTestDSL`, grouped by nested static class per YAML object (one nested class per object: `Streams`, `Tests`, `Produce`, `Assert`, `Message`). The class SHALL be non-instantiable and SHALL be the single source of truth for these names — neither the parser, the `@JsonSchema`-annotated records, the schema generator, nor validation error messages SHALL hard-code a YAML field-name string literal that is also represented by a `KSMLTestDSL` constant. This mirrors the established `io.axual.ksml.dsl.KSMLDSL` pattern used by the `ksml` module for the pipeline DSL.

#### Scenario: A consumer reads a field name through the DSL
- **WHEN** code in or outside the test-runner module needs to refer to the YAML field name for, e.g., the produce-block target stream
- **THEN** it SHALL be able to resolve the field name by referring to `KSMLTestDSL.Produce.TO` rather than the bare string `"to"`

#### Scenario: A YAML field-name string is added to the format
- **WHEN** a new YAML field is introduced to the test-definition format
- **THEN** its name SHALL be added as a constant on `KSMLTestDSL` (at the appropriate nesting level) before being used by the parser, by any `@JsonSchema` annotation, by the schema generator, or in a validation error message

#### Scenario: Stream and test identifier regex lives with the DSL
- **WHEN** the parser validates a stream key or a test key against the identifier regex
- **THEN** it SHALL obtain the regex from `KSMLTestDSL.IDENTIFIER_PATTERN` rather than from a constant on `TestDefinitionParser`
