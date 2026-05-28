## Context

The KSML test-runner YAML format is parsed by `TestDefinitionParser` and described by `@JsonSchema`-annotated records (`AssertBlock`, `ProduceBlock`, `TestSuiteDefinition`, `TestCaseDefinition`, `StreamDefinition`, `TestMessage`), then projected to a public JSON Schema by `TestDefinitionSchemaGenerator`. Every YAML field name (`name`, `definition`, `streams`, `tests`, `produce`, `assert`, `to`, `on`, `messages`, `code`, `topic`, `keyType`, `valueType`, `key`, `value`, `timestamp`, …) currently appears as a bare string literal in each of those locations. There is no compile-time link between the literals: a rename touches at least the parser, the relevant record's `@JsonSchema` `required` lists, and the schema generator's property keys, and silently slips in any spot that's missed.

The `ksml` module solved the same problem two years ago with `io.axual.ksml.dsl.KSMLDSL`: a final class (`@NoArgsConstructor(access = PRIVATE)`) holding the field-name constants for the KSML pipeline DSL, grouped by nested static class per YAML object. The pattern is well-understood inside this codebase and we want to mirror it exactly rather than invent a new convention for the smaller test-runner DSL.

## Goals / Non-Goals

**Goals:**
- One Java type, `io.axual.ksml.testrunner.KSMLTestDSL`, that lists every YAML field name of the test-definition format.
- Structural parity with `KSMLDSL`: outer class is non-instantiable, nested static classes per YAML object, all constants `public static final String`, member name = `UPPER_SNAKE_CASE` of the field, value = the exact YAML field name.
- Mechanical replacement of every inline `"name"`/`"definition"`/`"produce"`/… literal in `ksml-test-runner/src/main/java/io/axual/ksml/testrunner/` with a `KSMLTestDSL.*.*` reference, including inside `@JsonSchema(anyOfRequired = …)`, `@JsonSchema(oneOfRequired = …)`, validation error messages, and `TestDefinitionSchemaGenerator` property names.
- Move `TestDefinitionParser.IDENTIFIER_PATTERN` into `KSMLTestDSL` so the regex used for stream and test keys lives with the field names that use it.

**Non-Goals:**
- No change to the YAML test-definition format. Field names, casing, optionality, validation rules are all unchanged.
- No change to the generated JSON Schema. Property names and order must match byte-for-byte (the existing schema-generator test is the regression guard).
- No change to error message wording. Validation errors that name a field today must continue to name the same field after the rewrite.
- No tightening or loosening of the `IDENTIFIER_PATTERN` regex itself, only its location.
- No constants for values whose canonical home is elsewhere (e.g., notation type strings like `"string"`/`"avro:SensorData"` belong to `ksml-data`, not here).

## Decisions

**1. Mirror `KSMLDSL` structurally, not just stylistically.**
Use `@NoArgsConstructor(access = AccessLevel.PRIVATE)` from Lombok on the outer class and on every nested class, the same `public static final String` field shape, and the same nesting pattern (one nested class per YAML object). This keeps a developer who already knows `KSMLDSL` productive immediately, and lets future cross-module clean-ups treat the two classes uniformly.

Alternatives considered:
- A Java `enum` of field names: rejected — values would need a separate accessor and the call sites get noisier (`KSMLTestDSL.SuiteField.NAME.value()` vs `KSMLTestDSL.NAME`), and we'd diverge from the in-house pattern for no gain.
- A flat constants class with no nesting: rejected — the test-format DSL has clear object boundaries (`Streams`, `Tests`, `Produce`, `Assert`, `Message`), and flat constants would force per-prefix naming (`STREAM_TOPIC`, `TEST_DESCRIPTION`, `MESSAGE_KEY`) that's less readable than `Streams.TOPIC`.

**2. Nested-class layout matches the YAML object graph.**

```
KSMLTestDSL
├── NAME              (suite top-level fields)
├── DEFINITION
├── SCHEMA_DIRECTORY
├── MODULE_DIRECTORY
├── STREAMS
├── TESTS
├── IDENTIFIER_PATTERN  (regex constant moved from TestDefinitionParser)
├── Streams
│   ├── TOPIC
│   ├── KEY_TYPE
│   └── VALUE_TYPE
├── Tests
│   ├── DESCRIPTION
│   ├── PRODUCE
│   └── ASSERT
├── Produce
│   ├── TO
│   ├── MESSAGES
│   ├── GENERATOR
│   └── COUNT
├── Assert
│   ├── ON
│   ├── STORES
│   └── CODE
└── Message
    ├── KEY
    ├── VALUE
    └── TIMESTAMP
```

Each outer-level constant whose value names a nested object (e.g., `STREAMS`, `TESTS`) sits next to the nested class with the same name. This is the same convention `KSMLDSL` uses for `FUNCTIONS` + `Functions`, `PRODUCERS` + `Producers`, etc.

**3. Suite-level field whitelist (`SUITE_LEVEL_FIELDS` in `TestDefinitionParser`) is rebuilt from the constants.**
The current parser hard-codes `Set.of("name", "definition", "schemaDirectory", "moduleDirectory", "streams", "tests")` to detect suite-level fields wrongly nested under a test entry. After the rewrite, this set is built from the `KSMLTestDSL.NAME, DEFINITION, …` constants — eliminating the second copy of the same list.

**4. `@JsonSchema(anyOfRequired = {...})` / `oneOfRequired` references move to constants.**
On `AssertBlock` this currently reads `anyOfRequired = {"on", "stores"}`; on `ProduceBlock`, `oneOfRequired = {"messages", "generator"}`. These become `anyOfRequired = {KSMLTestDSL.Assert.ON, KSMLTestDSL.Assert.STORES}` and `oneOfRequired = {KSMLTestDSL.Produce.MESSAGES, KSMLTestDSL.Produce.GENERATOR}`. Annotation argument values must be compile-time constants — Java `static final String` initialized to a string literal qualifies, so no obstacle.

**5. Validation error messages keep their wording but use constants for the field name.**
Strings like `"Test '" + testKey + "' is missing required field 'produce' in " + testFile` become `"Test '" + testKey + "' is missing required field '" + KSMLTestDSL.Tests.PRODUCE + "' in " + testFile`. The visible text is identical, but a future rename of the YAML field flows through automatically.

## Risks / Trade-offs

- **Byte-identical JSON Schema** → Regression guard: `TestDefinitionSchemaGeneratorTest` (existing) checks the generated schema against a checked-in reference. The rewrite must keep its output unchanged; if it drifts, the test will fail before the change lands.
- **Silent miss on a literal** → Mitigation: after the bulk rewrite, grep the module for surviving string literals that match a YAML field name (`"name"`, `"definition"`, etc.). Anything remaining gets reviewed individually — some literals are legitimate (e.g., Python variable names, JSON schema dialect URIs, test fixture file paths) and must stay.
- **`IDENTIFIER_PATTERN` was public on `TestDefinitionParser`** → Mitigation: only the test-runner module references it today; the move keeps the symbol public on `KSMLTestDSL`. If any future external consumer references the old location, the compile error is its own migration prompt.
- **Annotation argument constraint** → `@JsonSchema(anyOfRequired = ...)` requires compile-time constant arrays of strings. Lombok's `@NoArgsConstructor` doesn't alter that. Verified by the pattern's existence in `KSMLDSL` (same shape, same constraints, works).
