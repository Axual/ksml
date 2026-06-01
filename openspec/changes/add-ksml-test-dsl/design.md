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

## Open Questions

### Status: fix (a) has been applied

The two subsections below are kept as the documented record of the gap and the empirical experiments that informed the decision. As of the work captured in section 8 of `tasks.md`, **fix (a) has been applied**: every annotated record component in the test-runner module now carries an explicit `yamlName = KSMLTestDSL.X.Y`. After this change `TestDefinitionSchemaGenerator.yamlNameOf(...)` always finds an explicit `yamlName` and never falls through to `component.getName()`. The generated JSON Schema is byte-identical to the pre-edit baseline (verified by `diff -u`), and 103/103 test-runner tests still pass.

The codebase-internal duality (Java field name vs. constant value, both feeding the schema generator) is therefore closed. A confirming experiment: with fix (a) in place, perturbing `KSMLTestDSL.Produce.COUNT` to `"countXYZ"` now produces a schema in which `"countXYZ"` appears as the property key under produce-block properties — proving the constant is the only authority the schema generator consults for that field. Before fix (a) the schema retained `"count"` because reflection on the Java component name overrode any rename of the constant.

**Still open**: the cosmetic-optional silent-drift window described in the parser-side subsection below. That gap is structurally orthogonal to fix (a) — it concerns whether the constant agrees with the YAML field names users actually write, not whether the schema agrees with the parser. Only fix (b) (a reflection-based guard test) would address it. Fix (b) is **not** part of this change.

### Two sources of truth for fields where the Java identifier matches the YAML name (resolved by fix (a))

After the rewrite, the YAML field-name spelling for any given record component has **two** authorities that must agree but aren't compile-time linked:

1. The **Java record-component name** itself (e.g., `messages` in `ProduceBlock`). `TestDefinitionSchemaGenerator` reads it via reflection (`component.getName()`) to populate the generated JSON Schema's property keys.
2. The corresponding **`KSMLTestDSL.X.Y` constant** (e.g., `KSMLTestDSL.Produce.MESSAGES`). `TestDefinitionParser` uses it to fetch the field from the parsed `JsonNode`; `ProduceBlock`'s validation error messages interpolate it.

These must spell the same string. The compiler does not enforce that; convention does.

A constant only flows into the **generated schema** when it appears as an `@JsonSchema` annotation argument — i.e., as a value in `oneOfRequired`, `anyOfRequired`, or `yamlName`. Today that covers exactly five constants:

| Constant | Where it enters the schema |
|---|---|
| `KSMLTestDSL.Produce.MESSAGES` | `ProduceBlock.@JsonSchema(oneOfRequired = ...)` |
| `KSMLTestDSL.Produce.GENERATOR` | `ProduceBlock.@JsonSchema(oneOfRequired = ...)` |
| `KSMLTestDSL.Assert.ON` | `AssertBlock.@JsonSchema(anyOfRequired = ...)` |
| `KSMLTestDSL.Assert.STORES` | `AssertBlock.@JsonSchema(anyOfRequired = ...)` |
| `KSMLTestDSL.Tests.ASSERT` | `TestCaseDefinition.assertions.@JsonSchema(yamlName = ...)` |

Every other constant (the top-level ones `NAME`, `DEFINITION`, `SCHEMA_DIRECTORY`, `MODULE_DIRECTORY`, `STREAMS`, `TESTS`; `Streams.*`, `Tests.{DESCRIPTION, PRODUCE}`, `Produce.{TO, COUNT}`, `Produce.Generator.*`, `Assert.CODE`, `Message.*`) is used **only** by the parser, by validation error wording, or by the Python-facing record dict in `AssertionRunner`. None of them flows into the schema generator, which obtains those field names independently from Java component names.

### What the existing schema-generator test catches today (measured)

Two perturbation experiments, applied to the change branch and reverted:

| Experiment | Modified | Schema test (`TestDefinitionSchemaGeneratorTest`) | Full test-runner suite (`mvn -pl ksml-test-runner test`) |
|---|---|---|---|
| 1. Parser-only constant rename | `Produce.COUNT = "count"` → `"countXYZ"` | 15/15 PASS — drift invisible | 103/103 PASS — drift invisible end-to-end (the parser's `optionalLong` returns null on a missing field, no fixture asserts on a generator-block record count) |
| 2. Annotation-arg constant rename | `Produce.MESSAGES = "messages"` → `"messagesXYZ"` | 14/15 PASS, **1 FAIL** in `produceBlockOneOfBetweenMessagesAndGenerator`: `expected: <[messages, generator]> but was: <[generator, messagesXYZ]>` | (not run — schema break is loud enough) |

Reading: the schema-generator test catches drift in the **five constants that flow into the schema** and only those. For every other constant the test is blind, and so are the integration tests when the affected field is optional with a sensible default. There is therefore a real silent-drift window covering the majority of `KSMLTestDSL` constants.

### Two ways to close the gap

The gap is **left open** for now. Calling it out so a future reader doesn't have to rediscover it.

**(a) — preferred when this is taken on.** Add an explicit `yamlName = KSMLTestDSL.X.Y` to every annotated record component, even when the Java identifier already matches. After this change, `TestDefinitionSchemaGenerator.yamlNameOf(...)` always returns the constant rather than ever falling through to `component.getName()`, and the constant becomes the single source of truth in both the parser direction and the schema-generator direction. Concrete shape:

```java
public record ProduceBlock(
        @JsonSchema(yamlName = KSMLTestDSL.Produce.TO,        required = true, ...) String to,
        @JsonSchema(yamlName = KSMLTestDSL.Produce.MESSAGES,  ...)                  List<TestMessage> messages,
        @JsonSchema(yamlName = KSMLTestDSL.Produce.GENERATOR, ...)                  Map<String, Object> generator,
        @JsonSchema(yamlName = KSMLTestDSL.Produce.COUNT,     ...)                  Long count
) { ... }
```

Cost: roughly one extra `yamlName = ...` argument per record component — on the order of ~17 lines across the four annotated records. No runtime cost, no test changes required beyond keeping the schema-generator test as the regression guard.

**(b) — alternative, lighter-weight.** Add a single guard unit test in the test-runner module that, by reflection, asserts every `KSMLTestDSL.X.Y` constant's value matches either (i) a record-component name in the corresponding record class, or (ii) some component's `yamlName`. Walks both `KSMLTestDSL`'s nested-class tree and the record-component lists; fails with a focused message naming the offending constant when they drift apart. Cheaper to write than (a) and catches drift in either direction, but its failure message is one step removed from the user (it points at "constant X disagrees with record component Y" rather than at a concrete schema-output mismatch), and it doesn't unify the underlying source of truth — it just enforces that the two stay in lockstep.

**Why (a) over (b)** when this is acted on: (a) eliminates the duality at the source by making the constant the only authority the schema generator consults; (b) leaves the duality in place and bolts on a watchdog. (a) is also a consistent extension of what `TestCaseDefinition.assertions` already does for the one component where the Java identifier differs from the YAML name. Both options preserve the existing `TestDefinitionSchemaGeneratorTest` as the high-signal regression guard for the five constants already covered today.

### Parser-side drift detection (the mirror question)

The schema-generator subsection above is about *codebase ⇄ codebase* drift: the Java component name and the `KSMLTestDSL` constant could disagree because two pieces of code spell the same string twice. The parser side is structurally different: `TestDefinitionParser` reads YAML *exclusively* through `KSMLTestDSL` constants (string-keyed `JsonNode.get(...)`, `FieldExtractor.requireString(...)` etc.) and then builds records via *positional* constructor calls. The Java field names of the records don't participate in parsing at all. So there is no parser-side dual-authority — the constant is the only authority.

The interesting question on the parser side is therefore: when somebody renames a `KSMLTestDSL` constant in the codebase, what tells them that the rename has silently diverged from the field name the YAML still uses? Three regimes, all measured:

| Regime | Example | Caught by | Test output |
|---|---|---|---|
| **Required + flows into schema** | `Produce.MESSAGES`, `Produce.GENERATOR`, `Assert.ON`, `Assert.STORES`, `Tests.ASSERT` | `TestDefinitionSchemaGeneratorTest` (and integration tests too) | Documented in Experiment 2 above. |
| **Required, parser-only** | `Tests.PRODUCE` (and structurally: `DEFINITION`, `STREAMS`, `TESTS`, `Streams.TOPIC`, `Produce.TO`, `Assert.CODE`) | Integration + parser unit tests, very loudly | Experiment 3: `Tests.PRODUCE = "produceXYZ"` → 10+ test failures across `TestDefinitionParserTest` and `KSMLTestRunnerTest`, exception messages name the broken constant directly: `Test 'X' is missing required field 'produceXYZ' in <fixture path>`. Schema generator test: 15/15 PASS (blind, as expected — the constant doesn't flow into the schema). |
| **Optional, parser-only, value asserted somewhere** | `NAME` (suite-level, parsed value cross-checked in `TestDefinitionParserTest`) | Integration tests, with a value-mismatch message | Experiment 4: `NAME = "nameXYZ"` → 2 failures in `TestDefinitionParserTest` with `expected: <Filter keeps only blue sensors> but was: <valid-test-definition>`. The parser silently fell back to the filename because the renamed constant didn't match the YAML's `name:` key, and the assertion against the expected suite name caught it one level down. |
| **Optional, parser-only, value not asserted (truly cosmetic)** | `Tests.DESCRIPTION` (falls back to the test key for display, no fixture's parsed description is compared to the YAML's `description:` value) | **Nothing** | Experiment 5: `Tests.DESCRIPTION = "descriptionXYZ"` → 103/103 PASS. Drift is end-to-end invisible. |

So on the parser side, the **silent-drift class** is narrower than on the schema-generator side but non-empty: optional parser-only constants whose parsed value is never directly compared to the YAML's field value. From the inventory: `Tests.DESCRIPTION` is the clearest example. `Produce.COUNT` and the four `Produce.Generator.*` constants are similar — they have lenient defaults, and the fixtures don't pin them tightly enough for the integration tests to notice when the parser silently no-ops the read. `SCHEMA_DIRECTORY` and `MODULE_DIRECTORY` fall in the same bucket when no fixture exercises those optional features.

### Implications for the two fix options

The two fix options proposed for the schema side have asymmetric impact on the parser side:

- **(a) — explicit `yamlName = KSMLTestDSL.X.Y` on every component.** Helpful on the schema side; **does not** improve detection on the parser side. The parser already uses the constants exclusively, and a `yamlName` annotation has no effect on the parser path.
- **(b) — reflection-based guard test.** Catches drift in *both* directions simultaneously. If the test asserts that each `KSMLTestDSL.X.Y` value matches either a component name or some `yamlName`, then renaming a parser-only constant immediately breaks the guard because the constant's new value no longer matches any record component. That covers the cosmetic-optional case (Experiment 5) too.

This nuances the earlier "(a) preferred" recommendation: (a) closes the schema-side duality cleanly, but only (b) closes the parser-side silent-drift window for cosmetic optionals. The thorough fix is **both**: (a) makes the constant the canonical authority for the schema generator, and (b) is the catch-all guard against the small set of cosmetic-optional constants that no other test would notice. If only one is taken, (a) addresses the larger surface; (b) addresses the longer tail.
