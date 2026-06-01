## Why

The `ksml-test-runner` module hard-codes the YAML field names of the test-definition format as inline string literals across multiple files (`TestDefinitionParser`, `AssertBlock`, `ProduceBlock`, `TestSuiteDefinition`, `TestDefinitionSchemaGenerator`, and more). The same names appear in the YAML parsing code, in the validation messages, and in the `@JsonSchema` annotations used to generate the public JSON Schema — but there is no single source of truth tying them together. A rename or addition has to be hunted across the module and is easy to get partially wrong (e.g., parser updated, schema generator missed). The `ksml` module already solved this exact problem with `io.axual.ksml.dsl.KSMLDSL`: a single nested-class hierarchy that holds every YAML field name as a typed constant. Mirroring that pattern in the test runner removes the drift risk and makes the YAML surface of the test format readable from one file.

## What Changes

- Add `io.axual.ksml.testrunner.KSMLTestDSL`, a final-class-with-private-constructor (matching `KSMLDSL`'s `@NoArgsConstructor(access = PRIVATE)` style) holding the YAML field-name constants of the test-definition format, grouped by nested static class per YAML object (`Streams`, `Tests`, `Produce`, `Assert`, `Message`).
- Include the suite-key identifier regex (`IDENTIFIER_PATTERN`) currently public on `TestDefinitionParser`, so all parsing constants live in one place.
- Replace inline string literals across the test-runner sources with references to `KSMLTestDSL` constants: parser field reads, validation messages that name the offending field, `@JsonSchema` `anyOfRequired` / `oneOfRequired` references, and the schema generator's property names.
- No change to the YAML format itself, the JSON Schema produced, the validation error wording, or any public API.

## Capabilities

### New Capabilities
<!-- None - this is a code-organization refactor with no behavior change visible to spec scope. -->

### Modified Capabilities
<!-- None - the externally-visible behavior of test-definition-format, test-runner-execution and test-runner-packaging is unchanged. -->

## Impact

- **Code**: New class `ksml-test-runner/src/main/java/io/axual/ksml/testrunner/KSMLTestDSL.java`. Mechanical replacements in `TestDefinitionParser`, `TestDefinitionSchemaGenerator`, `AssertBlock`, `ProduceBlock`, `TestSuiteDefinition`, `TestCaseDefinition`, `StreamDefinition`, `TestMessage`, and any other module class that names a YAML field.
- **APIs**: None. `TestDefinitionParser.IDENTIFIER_PATTERN` moves to `KSMLTestDSL`; if any code references it externally it will need to follow the new path. Currently no consumers outside the module use it.
- **Generated artifacts**: The JSON Schema (`TestDefinitionSchemaGenerator` output) must be byte-identical before and after. The existing schema generator test guards this.
- **Tests**: No new tests required. Existing `KSMLTestRunnerTest`, `TestDefinitionParserTest`, and `TestDefinitionSchemaGeneratorTest` cover the surface and will catch any drift introduced by the rewrite.
- **Dependencies**: None.
