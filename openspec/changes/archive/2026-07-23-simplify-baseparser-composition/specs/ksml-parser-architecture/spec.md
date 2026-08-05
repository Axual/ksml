## ADDED Requirements

### Requirement: Parsing and schema output equivalence
Restructuring the `BaseParser`/`DefinitionParser` hierarchy from inheritance to composition SHALL NOT change the result of parsing any valid KSML YAML definition, nor the `DataSchema`/`StructSchema`/`UnionSchema` tree produced by any parser's `schemas()` method. Any JSON Schema document derived from that tree via `JsonSchemaMapper` (used by the `ksml --schema` CLI flag and validated by `ksml-docs-test`) SHALL remain identical before and after this change.

#### Scenario: Existing KSML definitions parse identically
- **WHEN** any KSML definition YAML file that parsed successfully before this change is parsed with `TopologyDefinitionParser` after this change
- **THEN** the resulting object graph is equivalent, and no previously-successful parse fails or produces different values

#### Scenario: Generated JSON Schema is unchanged
- **WHEN** the JSON Schema is generated from `TopologyDefinitionParser.schema()` via `JsonSchemaMapper` after this change
- **THEN** it is identical to the JSON Schema generated the same way before this change

#### Scenario: Documented example definitions still validate
- **WHEN** `ksml-docs-test`'s `AllDefinitionsSchemaValidationTest` and `AllRunnerConfigSchemaValidationTest` run against the schema generated after this change
- **THEN** every example and test YAML file that validated successfully before this change still validates successfully

### Requirement: Resource-phase safety invariant
The composed replacement for `TopologyBaseResourceAwareParser`/`TopologyResourceAwareParser` SHALL preserve the compile-time distinction between the base-resources phase (functions and state stores only, used while streams/tables/topics are themselves being defined) and the full-resources phase (adds topic lookup and unique-operation-naming, used once all streams/tables/topics are assembled). A parser operating in the base-resources phase SHALL NOT have compile-time access to topic-lookup capability (`topicField` or equivalent).

#### Scenario: Stream/table/topic definition parsers cannot reach topic lookup
- **WHEN** `StreamDefinitionParser`, `TableDefinitionParser`, `GlobalTableDefinitionParser`, or `TopicDefinitionParser` are constructed with only base resources (functions and state stores)
- **THEN** no method for looking up a topic by name is reachable from within those classes at compile time

#### Scenario: Operation and pipeline parsers retain topic lookup
- **WHEN** `OperationParser` subclasses or other parsers constructed with full resources (after streams/tables/topics have been assembled) need to reference a topic, stream, or table by name
- **THEN** the composed full-resources helper provides that lookup capability, with the same behavior as the current `topicField`

### Requirement: Field-builder DSL behavioral contracts
The field-builder helpers extracted from `DefinitionParser` (`stringField`, `booleanField`, `integerField`, `longField`, `durationField`, `enumField`, `codeField`, `userTypeField`, `listField`, `mapField`, `customField`, `freeField`, `structsParser(...)`, `optional(...)`, `withDefault(...)`) SHALL produce parse functions and schema fragments equivalent to their current implementations. The memoize-once behavior (a parser's `parser()`-equivalent construction logic runs at most once per instance) and the fatal-error-wrapping behavior currently provided by `DefinitionParser.parse()` SHALL be preserved for all parsers that relied on them.

#### Scenario: Parser construction logic runs at most once
- **WHEN** a parser's `parse()` or `schemas()` method is called multiple times on the same instance
- **THEN** the underlying parser-construction logic (equivalent to today's `parser()` method) executes at most once, with subsequent calls reusing the cached result

#### Scenario: Parsing exceptions are still wrapped as fatal errors
- **WHEN** an exception occurs during parsing through any parser that previously relied on `DefinitionParser.parse()`'s exception handling
- **THEN** the exception is still reported via `FatalError.report(...)`, matching current behavior
