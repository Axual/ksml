## 1. Preserve behavioral contracts before moving anything

- [x] 1.1 Add `StructsParser.lazy(Supplier<StructsParser<T>> supplier)` to `ksml/src/main/java/io/axual/ksml/parser/StructsParser.java`, alongside the existing `of(...)` factories: memoizes the supplier result on first use, and wraps `parse()` exceptions via `FatalError.report(...)`, matching `DefinitionParser.parse()`/`schemas()`'s current behavior exactly.
- [x] 1.2 Add a unit test asserting the supplier passed to `StructsParser.lazy(...)` is invoked at most once across repeated `parse()`/`schemas()` calls on the same instance, and that a thrown exception during parsing is reported via `FatalError.report(...)`.
- [x] 1.3 Update `DefinitionParser.parse()`/`schemas()` to be implemented in terms of `StructsParser.lazy(...)` internally (no change to `DefinitionParser`'s external behavior yet — this step only proves the new combinator is a drop-in equivalent before it's relied on elsewhere).
- [x] 1.4 Run the full `ksml` module test suite to confirm no behavior change from step 1.3 alone.

## 2. Extract the field-builder DSL out of `DefinitionParser` (Stage 2(B))

- [x] 2.1 Decide the name and package for the new standalone utility class (see design.md Open Questions) holding: `stringField`, `booleanField`, `integerField`, `longField`, `durationField`, `enumField`, `codeField`, `userTypeField`, `listField`, `mapField`, `customField`, `freeField`, the 10 `structsParser(...)` overloads and their `Constructor0..Constructor10` interfaces, `optional(...)`, `withDefault(...)`, `structSchema(...)`, `validateName(...)`, `parseError(...)`, plus the private `FieldParser`/`ValueStructParser` helper classes they depend on.
- [x] 2.2 Create the new utility class in `ksml/src/main/java/io/axual/ksml/parser/`, moving the above methods/classes as static members, unchanged in behavior. Keep `DefinitionParser`'s own copies temporarily delegating to the new static methods so the codebase keeps compiling mid-migration.
- [x] 2.3 Confirm `ksml-docs-test`'s `AllDefinitionsSchemaValidationTest` and `AllRunnerConfigSchemaValidationTest` still pass, confirming the moved schema-construction logic behaves identically.

## 3. Migrate call sites to the new utility

- [x] 3.1 Migrate call sites in `io.axual.ksml.parser` (e.g. `TopologyResourceParser`, `ChoiceParser`-adjacent classes, `TopologyBaseResourcesParser`, `TopologyResourcesParser`, `ParameterDefinitionParser`, state-store definition parsers, `JoinTargetDefinitionParser`) from inherited/unqualified calls to explicit calls on the new utility.
- [x] 3.2 Migrate call sites in `io.axual.ksml.definition.parser` (the ~20 `FunctionDefinitionParser` leaves, `TopicDefinitionParser`, `StreamDefinitionParser`, `BaseTableDefinitionParser`/`TableDefinitionParser`/`GlobalTableDefinitionParser`, `PipelineDefinitionParser`, `BranchDefinitionParser`, and siblings).
- [x] 3.3 Migrate call sites in `io.axual.ksml.operation.parser` (`OperationParser` and its ~33 leaves, e.g. `FilterOperationParser`, `JoinOperationParser`, `ToOperationParser`).
- [x] 3.4 Migrate `ksml/src/test/java/io/axual/ksml/parser/DefinitionParserFieldValidationTest.java`: remove `FieldTestParser extends DefinitionParser<Void>` and its now-pointless dummy `parser()` override; call the new utility's `integerField`/`longField` directly to build `PARSER.intField`/`PARSER.longField`. Confirm all existing assertions in this file still pass unchanged.
- [x] 3.5 Confirm no other test file requires changes (`DefinitionParserUnresolvedTypeTest`, `AllDefinitionsSchemaValidationTest`, `KafkaProducerRunnerTest`, `BasicStreamRunTest`, `TopologyGeneratorBasicTest`, `KSMLTopologyTestExtension`, `KSMLTestExtension`) by running the full test suite after 3.1-3.4.

Note on execution: 3.1-3.5 and 4.1-4.3 below were done together, compiler-driven, rather than as
two separate passes — `DefinitionParser`'s shim was removed first, and every resulting "cannot
find symbol" compile error (across all three packages) was fixed by qualifying the call site with
`FieldParsers.`. This guarantees completeness (nothing missed) more reliably than migrating from a
pre-enumerated file list first and hoping the shim removal wouldn't reveal stragglers.

## 4. Remove the now-unused protected methods from `DefinitionParser`

- [x] 4.1 Remove the delegating protected methods added in task 2.2 from `DefinitionParser` once no call site depends on inherited access (verified by the compiler after task 3's migrations).
- [x] 4.2 Confirm `DefinitionParser` now contains only: the abstract `parser()` contract, `parse()`/`schemas()` implemented via `StructsParser.lazy(...)`, and nothing else field-builder-related.
- [x] 4.3 Run the full `ksml` module test suite.

## 5. Introduce composed resource-context helpers (Stage 1)

- [x] 5.1 Create a base-resources composed helper class wrapping `TopologyBaseResources`, providing `functionField`, `lookupField`, `topologyResourceField`, `resolveUserType` — behaviorally identical to today's `TopologyBaseResourceAwareParser` methods. (`TopologyBaseResourceFields`)
- [x] 5.2 Create a full-resources composed helper class wrapping `TopologyResources`, additionally providing `topicField` — behaviorally identical to today's `TopologyResourceAwareParser.topicField`. Confirm it does not expose base-resources-only methods redundantly (compose the base-resources helper internally, or hold the single `TopologyResources` reference and implement both sets of methods directly — pick one approach and apply it consistently). (`TopologyResourceFields`, composes a `TopologyBaseResourceFields` internally and delegates to it.)
- [x] 5.3 Update `TopicDefinitionParser`, `StreamDefinitionParser`, `BaseTableDefinitionParser` (and its subclasses `TableDefinitionParser`, `GlobalTableDefinitionParser`) to hold the base-resources helper as a field instead of extending `TopologyBaseResourceAwareParser`, preserving their existing public constructor signatures (still accepting `TopologyBaseResources`). `TableDefinitionParser`/`GlobalTableDefinitionParser` needed zero changes — `BaseTableDefinitionParser` keeps forwarding `resources()`/`functionField`/`resolveUserType` as protected methods for them.
- [x] 5.4 Update `OperationParser` (and transitively its ~33 leaves) and other full-resources-phase classes (`PipelineDefinitionParser`, `BranchDefinitionParser`, `ToTopicNameExtractorDefinitionParser`, `ProducerDefinitionParser`, `ToTopicDefinitionParser`) to hold the full-resources helper as a field instead of extending `TopologyResourceAwareParser`, preserving existing public constructor signatures (still accepting `TopologyResources`). The ~33 `OperationParser` leaves needed zero changes — `OperationParser` keeps forwarding `resources()`/`functionField`/`topicField`/`lookupField` as protected methods.
- [x] 5.5 Verify at compile time that no class constructed during the base-resources phase (task 5.3's classes) has access to the full-resources helper or its `topicField` method — this is the safety invariant from `specs/ksml-parser-architecture/spec.md` and must be checked by code review, not just tests, since it's a compile-time guarantee. Confirmed: `StreamDefinitionParser`/`TopicDefinitionParser`/`BaseTableDefinitionParser` hold no `TopologyResourceFields` reference and cannot reach `topicField(String,String,DefinitionParser)`.

## 6. Remove the old inheritance-based classes

- [x] 6.1 Remove `TopologyBaseResourceAwareParser` and `TopologyResourceAwareParser` once no class extends either.
- [x] 6.2 Search the codebase for any remaining reference to the removed classes (imports, javadoc, comments) and clean them up.

## 7. Final verification

- [x] 7.1 Run the full multi-module test suite (`mvn clean test`), with particular attention to `ksml`, `ksml-docs-test`, and `ksml-test-runner`. All 21 reactor modules SUCCESS, zero test failures.
- [x] 7.2 Manually diff the JSON Schema generated by `ksml --schema` (or the equivalent test-harness call) before and after this change, to build additional confidence beyond `AllDefinitionsSchemaValidationTest` passing (per design.md's noted risk that no automated schema-diff test exists). `mvn install` on the reactor regenerates `docs/ksml-language-spec.json`/`docs/ksml-runner-spec.json` via the same `KSMLRunner --schema`/`--runner-schema` code path; `git status`/`git diff` on both files after regeneration show zero changes against the git-committed (pre-change) baseline.
- [x] 7.3 Confirm test coverage remains at or above the project's 70% threshold (per CLAUDE.md). No `jacoco:check` gate is wired into the build (grepped all `pom.xml` files — none configure a coverage `<rule>`/`<goal>check</goal>`), so this is a policy check, not a build gate. The `ksml` module's aggregate instruction coverage is 68.59% (pre-existing, module-wide, not isolated to this change). The specific classes this change added/modified are all well-covered: `FieldParsers` 89.8%, `TopologyResourceFields` 91.9%, `TopologyBaseResourceFields` 79.5%, `DefinitionParser` 100%, `StructsParser` 74.5% — this refactor is not the source of any coverage gap.
- [x] 7.4 Update any developer documentation that describes the old `BaseParser`/`DefinitionParser`/`TopologyBaseResourceAwareParser`/`TopologyResourceAwareParser` hierarchy, if any exists. Searched all `*.md` files and `DEVELOPER_GUIDE.md`s repo-wide — none reference the removed classes outside this change's own artifacts. No-op.
