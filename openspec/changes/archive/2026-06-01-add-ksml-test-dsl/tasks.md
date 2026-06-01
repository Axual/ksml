## 1. Create the KSMLTestDSL class

- [x] 1.1 Add `ksml-test-runner/src/main/java/io/axual/ksml/testrunner/KSMLTestDSL.java` with the outer class structure: `@NoArgsConstructor(access = AccessLevel.PRIVATE)`, javadoc pointing at `io.axual.ksml.dsl.KSMLDSL` as the model, suite-level constants (`NAME`, `DEFINITION`, `SCHEMA_DIRECTORY`, `MODULE_DIRECTORY`, `STREAMS`, `TESTS`) and a placeholder for the moved `IDENTIFIER_PATTERN`.
- [x] 1.2 Add nested static class `Streams` with `TOPIC`, `KEY_TYPE`, `VALUE_TYPE`.
- [x] 1.3 Add nested static class `Tests` with `DESCRIPTION`, `PRODUCE`, `ASSERT`.
- [x] 1.4 Add nested static class `Produce` with `TO`, `MESSAGES`, `GENERATOR`, `COUNT`.
- [x] 1.5 Add nested static class `Assert` with `ON`, `STORES`, `CODE`.
- [x] 1.6 Add nested static class `Message` with `KEY`, `VALUE`, `TIMESTAMP`.
- [x] 1.7 Move `IDENTIFIER_PATTERN` from `TestDefinitionParser` to `KSMLTestDSL.IDENTIFIER_PATTERN` (keep `Pattern.compile(...)` the same). Pattern is kept; the source regex is also exposed as `KSMLTestDSL.IDENTIFIER_REGEX` so the schema generator can stop hard-coding its own copy.
- [x] 1.8 Verify `mvn -pl ksml-test-runner compile` succeeds before any call-site rewrites.

## 2. Rewrite TestDefinitionParser

- [x] 2.1 Replace every literal field-name string in field-reading calls (`requireString("...")`, `optionalString("...")`, `optionalLong("...")`, `optionalMap("...")`, `optionalStringList("...")`, `entryNode.get("...")`, `msgNode.get("...")`) with the matching `KSMLTestDSL.*.*` constant.
- [x] 2.2 Rebuild `SUITE_LEVEL_FIELDS` from the constants (`Set.of(KSMLTestDSL.NAME, KSMLTestDSL.DEFINITION, KSMLTestDSL.SCHEMA_DIRECTORY, KSMLTestDSL.MODULE_DIRECTORY, KSMLTestDSL.STREAMS, KSMLTestDSL.TESTS)`).
- [x] 2.3 Update validation error messages to interpolate the constant instead of the bare literal, keeping the surrounding wording identical.
- [x] 2.4 Delete the now-unused `public static final Pattern IDENTIFIER_PATTERN` declaration on `TestDefinitionParser`; update `validateIdentifier` to use `KSMLTestDSL.IDENTIFIER_PATTERN`.

## 3. Rewrite the annotated record classes

- [x] 3.1 `AssertBlock`: replace `anyOfRequired = {"on", "stores"}` with `{KSMLTestDSL.Assert.ON, KSMLTestDSL.Assert.STORES}`. Also updated the `validate()` error message to interpolate the same constants.
- [x] 3.2 `ProduceBlock`: replace `oneOfRequired = {"messages", "generator"}` with `{KSMLTestDSL.Produce.MESSAGES, KSMLTestDSL.Produce.GENERATOR}`; update the `validate()` error message strings to interpolate the constants.
- [x] 3.3 Audit `TestSuiteDefinition`, `TestCaseDefinition`, `StreamDefinition`, `TestMessage` for any `@JsonProperty(...)` / `@JsonSchema(...)` arguments that name YAML fields; replace with the matching `KSMLTestDSL.*.*` constant. Only one hit: `TestCaseDefinition.assertions` had `yamlName = "assert"` → now `yamlName = KSMLTestDSL.Tests.ASSERT`. The other records carry only descriptive prose, examples, and notation-type defaults like `"string"` (owned by ksml-data, not a YAML field name) — left untouched.

## 4. Rewrite TestDefinitionSchemaGenerator

- [x] 4.1 Replace every literal property-name string used as a JSON Schema property key with the matching `KSMLTestDSL.*.*` constant. (The output schema text must remain byte-identical — values, not keys, must continue to match the YAML names.) Confirmed in source review: this generator is reflection-driven, so it carries no inline YAML-field-name property keys. The one duplicated literal it did own — `IDENTIFIER_REGEX` — now reads from `KSMLTestDSL.IDENTIFIER_REGEX`.
- [x] 4.2 If the generator references the suite-level allow-list, swap it for a reference to the same `SUITE_LEVEL_FIELDS` constant set used by the parser (introduce a package-private accessor if needed; do not duplicate the literal list). **N/A**: the generator does not maintain a suite-level allow-list. `additionalProperties: false` is enforced per-record via reflection on the record components themselves.

## 5. Sweep remaining call sites

- [x] 5.1 `grep -RE '"(name|definition|schemaDirectory|moduleDirectory|streams|tests|description|produce|assert|to|on|messages|generator|count|stores|code|topic|keyType|valueType|key|value|timestamp)"' ksml-test-runner/src/main/java/` and review each surviving hit. Replace YAML-field-name literals; leave non-field literals (Python identifiers, schema dialect URIs, file paths, error words) untouched, with a one-line comment when the meaning isn't obvious.
    - Replaced: `AssertionRunner.collectOutputRecords` keys `"key"`/`"value"`/`"timestamp"` → `KSMLTestDSL.Message.*`. These describe the Python-visible output-record dict whose vocabulary mirrors the YAML produce-message vocabulary; consolidating keeps them in lockstep.
    - Left untouched (not test-runner-DSL vocabulary):
        - `KSMLTestRunner` — `"definition"` is the `TopologyGenerator.create()` input-map key, an internal `ksml` module API contract that happens to collide in spelling with `KSMLTestDSL.DEFINITION`.
        - `TestDefinitionSchemaGenerator` — `"description"` is a JSON Schema keyword used as a property key in the *generated* schema, not a YAML field of the test-definition format.
    - **Reclassified after follow-up review** (covered by 5.2 below): `TestDataProducer.produceGeneratedMessages` reads `"name"`, `"globalCode"`, `"code"`, `"expression"` from the test YAML's `produce.generator:` block. The spelling collides with `KSMLDSL.Functions.*`, but they are nonetheless fields of the *test-runner* YAML format and belong on `KSMLTestDSL`. Reusing the `ksml`-module constants would couple the two modules' DSLs together and is the wrong fix. The function-type literal `"generator"` passed to `FunctionDefinition.as(...)` stays as-is — that one is a KSML function-type identifier, not a test-runner YAML field.

## 5.2 Add generator-block field constants and rewire TestDataProducer

- [x] 5.2.1 Add a nested static class `Produce.Generator` to `KSMLTestDSL` with constants `NAME`, `GLOBAL_CODE`, `CODE`, `EXPRESSION` matching the YAML field names under `produce.generator:`. Class javadoc documents the intentional duplication with `KSMLDSL.Functions`.
- [x] 5.2.2 In `TestDataProducer.produceGeneratedMessages`, replace each `getStringOrDefault(generatorMap, "<literal>", ...)` call with the matching `KSMLTestDSL.Produce.Generator.*` constant. Defaults and call shape stay identical.
- [x] 5.2.3 Re-run `mvn -pl ksml-test-runner test` to confirm the existing test-runner suite (including any generator-based produce tests) still passes. **103/103 pass.**

## 6. Verify

- [x] 6.1 `mvn -pl ksml,ksml-test-runner -am install -DskipTests` to confirm a clean compile after all rewrites. First attempt revealed that `TestDefinitionSchemaGeneratorTest` referenced the now-removed `TestDefinitionSchemaGenerator.IDENTIFIER_REGEX`; the references were repointed to `KSMLTestDSL.IDENTIFIER_REGEX` and the build then succeeded.
- [x] 6.2 `mvn -pl ksml-test-runner test` to run the existing test-runner suite; all must pass. **103/103 passed.**
- [x] 6.3 Confirm `TestDefinitionSchemaGeneratorTest` (or whichever test guards the generated schema) passes unchanged — the generated schema must be byte-identical to the pre-change reference. Included in the 103-pass run; the schema-equality assertion held.
- [x] 6.4 Run any existing parser unit tests; validation error messages must still match the pre-change text (only the source of the field-name token has changed). Included in the 103-pass run; parser-error-text assertions held.

## 7. Document

- [x] 7.1 Add a one-line class-level javadoc on `KSMLTestDSL` referencing the `ksml` module's `io.axual.ksml.dsl.KSMLDSL` as the design model, so the parallel pattern is discoverable. Included in the initial class javadoc: *"This class mirrors the design of `io.axual.ksml.dsl.KSMLDSL` in the `ksml` module, which serves the same role for the KSML pipeline DSL."*

## 8. Close the schema-side gap: make KSMLTestDSL the single authority (fix (a))

The Open Questions section in design.md documented that for record components whose Java identifier happens to equal the YAML name, the schema generator picks up the YAML name via reflection (`component.getName()`) — leaving two unlinked authorities. This section applies fix (a): annotate every component with an explicit `yamlName = KSMLTestDSL.X.Y` so the constant is the only source the schema generator consults. After this work, `TestDefinitionSchemaGenerator.yamlNameOf(...)` always finds an explicit `yamlName` and never falls through to the Java field name.

- [x] 8.1 Capture the generated schema as a pre-edit baseline at `/tmp/ksml-schema-snapshot/before.json` for a byte-identical comparison after the rewrite.
- [x] 8.2 `TestSuiteDefinition` — add `yamlName = KSMLTestDSL.NAME`, `KSMLTestDSL.DEFINITION`, `KSMLTestDSL.SCHEMA_DIRECTORY`, `KSMLTestDSL.MODULE_DIRECTORY`, `KSMLTestDSL.STREAMS`, `KSMLTestDSL.TESTS` to the six component annotations.
- [x] 8.3 `TestCaseDefinition` — add `yamlName = KSMLTestDSL.Tests.DESCRIPTION`, `KSMLTestDSL.Tests.PRODUCE` to the two components that don't have an explicit `yamlName` yet (the `assertions` component already carries `yamlName = KSMLTestDSL.Tests.ASSERT`).
- [x] 8.4 `StreamDefinition` — add `yamlName = KSMLTestDSL.Streams.TOPIC`, `KSMLTestDSL.Streams.KEY_TYPE`, `KSMLTestDSL.Streams.VALUE_TYPE` to its three components.
- [x] 8.5 `TestMessage` — add `yamlName = KSMLTestDSL.Message.KEY`, `KSMLTestDSL.Message.VALUE`, `KSMLTestDSL.Message.TIMESTAMP` to its three components.
- [x] 8.6 `ProduceBlock` — add `yamlName = KSMLTestDSL.Produce.TO`, `KSMLTestDSL.Produce.MESSAGES`, `KSMLTestDSL.Produce.GENERATOR`, `KSMLTestDSL.Produce.COUNT` to its four components.
- [x] 8.7 `AssertBlock` — add `yamlName = KSMLTestDSL.Assert.ON`, `KSMLTestDSL.Assert.STORES`, `KSMLTestDSL.Assert.CODE` to its three components.
- [x] 8.8 Regenerate the schema; diff against the pre-edit baseline; assert byte-identical output. `diff -u before.json after.json` produced no output — schema is byte-identical.
- [x] 8.9 `mvn -pl ksml-test-runner test` — confirm all 103 tests still pass (the assertions encode the expected YAML field names as literals, so any drift would be loud). **103/103 pass.**
- [x] 8.10 Update the Open Questions section of `design.md` to record that fix (a) has been applied and what remains open (the silent-drift window for cosmetic-optional parser-only constants like `Tests.DESCRIPTION`, which only fix (b) would close).
- [x] 8.11 Sanity-check that fix (a) actually closed the schema-side gap: re-run Experiment 1's perturbation (`Produce.COUNT = "countXYZ"`) and regenerate the schema. The generated schema now reflects the rename (`"countXYZ"` appears as the property key under produce-block properties), whereas before fix (a) the schema retained `"count"` because the schema generator picked it up from the Java field name. Confirms that `KSMLTestDSL.Produce.COUNT` is now the only authority the schema generator consults for that field. Reverted after the check.
