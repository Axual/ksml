## Context

`ksml`'s parser code builds each parser as a combinator that simultaneously produces (a) a function from `ParseNode` to a typed value and (b) a `StructSchema`/`DataSchema` fragment describing that value, so that the generated JSON Schema (via `JsonSchemaMapper`, consumed by `ksml --schema` and validated by `ksml-docs-test`) can never drift from the actual parsing logic. `DefinitionParser<T>` is both the root of this combinator's class hierarchy and the holder of the stateless helper methods (`stringField`, `structsParser`, etc.) that build these combinators. `TopologyBaseResourceAwareParser`/`TopologyResourceAwareParser` sit below it, each wrapping a resource registry (`TopologyBaseResources`/`TopologyResources`) needed by resource-lookup helpers (`functionField`, `topicField`, `lookupField`).

Two problems were identified through direct codebase analysis (see proposal for full detail):

1. `TopologyBaseResourceAwareParser` and `TopologyResourceAwareParser` each declare their own private `resources` field holding the same object reference, solely to narrow its static type from `TopologyBaseResources` to `TopologyResources` one level down. `TopologyResources extends TopologyBaseResources`, and the two-level split reflects a genuine two-phase construction order in `TopologyResourcesParser` (base resources — functions/stores — are parsed first; streams/tables/topics are parsed next using only that base set, since they cannot reference topics that don't exist yet; only afterward is a full `TopologyResources` assembled for pipeline/operation parsing). This phase distinction is a real compile-time safety guarantee and must survive any restructuring; only the field duplication is accidental.
2. `DefinitionParser`'s field-builder DSL (~15 methods plus 10 `structsParser(...)` overloads and their `Constructor0..10` interfaces) is implemented as protected instance methods, inherited by every leaf parser purely for code reuse. Nothing in the codebase ever references `DefinitionParser`, `OperationParser`, or `FunctionDefinitionParser` polymorphically — every consumer that collects multiple parsers (`PipelineOperationParser`, `TypedFunctionDefinitionParser`) stores them as the `StructsParser<T>` interface. The inheritance link exists only to distribute helper methods, which is exactly what composition is for.

## Goals / Non-Goals

**Goals:**
- Remove the duplicated `resources` field storage while preserving the exact compile-time distinction between base-resources-phase and full-resources-phase parsers.
- Move `DefinitionParser`'s stateless field-builder DSL out of the inheritance chain into a standalone, statically-callable utility.
- Preserve the memoize-once and fatal-error-wrapping behavioral contracts currently delivered by `DefinitionParser.parse()`/`schemas()`.
- Leave every public constructor signature, and every externally observable parsing/schema-generation behavior, unchanged.

**Non-Goals:**
- Converting the ~33 `OperationParser` leaf classes or ~21 `FunctionDefinitionParser` leaf classes from inheritance to composition (Stage 3, out of scope — a possible future change).
- Changing `ChoiceParser`'s relationship to `BaseParser`/`DefinitionParser`.
- Changing the `NamedObjectParser` interface or its usage pattern (already composition-friendly and left as-is).
- Any change to KSML YAML syntax, the generated JSON Schema shape, or any public API signature.

## Decisions

**Decision: Two composed resource-context objects, not one merged class.**
An earlier version of this plan considered simply merging `TopologyBaseResourceAwareParser` and `TopologyResourceAwareParser` into one class. That was rejected once the two-phase construction order in `TopologyResourcesParser` was traced: merging would give every parser (including `StreamDefinitionParser`/`TopicDefinitionParser`/`TableDefinitionParser`/`GlobalTableDefinitionParser`, which are constructed during the base-resources phase, before topics exist) compile-time access to topic lookup, silently removing a safety guarantee. Instead:
- A base-resources helper (wrapping `TopologyBaseResources`) provides `functionField`, `lookupField`, `topologyResourceField`, `resolveUserType` — equivalent to today's `TopologyBaseResourceAwareParser`.
- A full-resources helper (wrapping `TopologyResources`, which *is a* `TopologyBaseResources`) additionally provides `topicField` — equivalent to today's `TopologyResourceAwareParser`.
- Classes constructed during the base-resources phase hold only the base-resources helper; classes constructed during the full-resources phase (`OperationParser` and its subclasses, `PipelineDefinitionParser`, `BranchDefinitionParser`, etc.) hold the full-resources helper. Each holds its resource reference exactly once — no re-declaration for type narrowing.
- Alternative considered: keep the two-level class split but just fix the field duplication (e.g., have the subclass read the parent's field via the inherited `resources()` accessor instead of storing its own copy). Rejected because it still leaves the toolkit distributed via inheritance, which is the broader problem this change is meant to reduce; composition addresses both the duplication and the "which ancestor defines this helper" traceability problem in one move.

**Decision: Real decoupling of the field-builder DSL (Stage 2(B)), not a delegate-only shim.**
A lighter-weight alternative (Stage 2(A)) would keep `DefinitionParser`'s protected methods in place and have their bodies delegate to a new static utility, changing zero call sites. That was considered and rejected for this change: it is lower-risk but delivers none of the traceability benefit, since every call site still reads as an unqualified inherited method call with an invisible origin. This change does the real migration: the utility's methods are called explicitly at every call site, and `DefinitionParser` no longer exposes them as inherited methods.

**Decision: Preserve memoize-once and fatal-error-wrapping via a new `StructsParser` combinator, not by keeping them on `DefinitionParser`.**
`DefinitionParser.parse()`/`schemas()` are currently `final` and (a) invoke the abstract `parser()` at most once, caching the result, and (b) wrap `parse()`'s exceptions via `FatalError.report(...)`. Since leaf classes no longer inherit from a shared base purely for this behavior under the eventual (out-of-scope) Stage 3 direction, this change introduces the equivalent behavior as an explicit combinator — a `StructsParser.lazy(Supplier<StructsParser<T>>)` factory alongside the existing `StructsParser.of(...)` — so that any class (whether or not it still extends `DefinitionParser` after this change) gets the same guarantees by wrapping its construction logic in `StructsParser.lazy(...)`. `DefinitionParser` itself is updated to implement `parser()`/`parse()`/`schemas()` in terms of this same combinator, so its own behavior is unchanged.

**Decision: No new capability/product behavior; specs describe the regression-safety contract.**
This is a pure internal refactor with no change to KSML's language or observable behavior. The `ksml-parser-architecture` capability spec exists to make the regression-safety requirements (parsing/schema equivalence, phase-safety invariant, DSL behavioral contracts) explicit and testable, not to describe a new feature.

## Risks / Trade-offs

- **[Risk] Migrating ~90 call sites for the field-builder DSL is mechanical but wide-reaching; a missed call site fails to compile (safe) but a subtly-wrong migration (e.g. dropping the memoization) could silently change performance or, in the worst case, duplicate side effects.** → Mitigation: the `StructsParser.lazy(...)` combinator centralizes the memoize-once and error-wrapping behavior in one tested place; migration is compile-checked (no inherited method left behind means the compiler catches every missed call site); add/extend a test asserting `parser()`-equivalent construction logic executes exactly once per instance.
- **[Risk] Splitting resource-context into two composed objects, if done inconsistently, could re-introduce a variant of today's duplication (e.g. a class holding both a base-resources and a full-resources helper "just in case").** → Mitigation: each parser class holds exactly one resource-context helper, chosen by which construction phase it participates in (mirrors today's class choice of which ancestor to extend); code review checklist item in tasks.
- **[Risk] Schema-generation equivalence (JSON Schema byte-for-byte or structurally identical) is not currently covered by an automated diff test — it's inferred from `AllDefinitionsSchemaValidationTest` continuing to pass, which validates that examples still conform, not that the schema is unchanged.** → Mitigation: acceptable for this change since no field-builder logic's actual schema-construction behavior is being changed, only where it lives; if higher confidence is wanted, a one-time before/after schema snapshot diff can be run manually during implementation (not proposed as a permanent test, to avoid a brittle golden-file test).
- **[Trade-off] This change alone does not reduce the depth of the largest, most-cited-for-readability chain (`OperationParser` → 33 leaves), since that is explicitly out of scope (Stage 3). Readability improves for the upper part of the hierarchy but the leaf classes most people read day-to-day still extend `OperationParser`/`FunctionDefinitionParser`.** → Accepted; flagged in the proposal as a deliberate scope boundary, not an oversight.

## Migration Plan

1. Introduce `StructsParser.lazy(Supplier<StructsParser<T>>)`, matching the memoize-once and `FatalError.report(...)` behavior of `DefinitionParser.parse()`/`schemas()`. Add a unit test asserting the supplier runs at most once across repeated `parse()`/`schemas()` calls.
2. Extract the stateless field-builder DSL from `DefinitionParser` into a new standalone class (name to be chosen during implementation). Update `DefinitionParser` to call the new class internally so behavior is provisionally unchanged while call sites are migrated incrementally.
3. Migrate call sites (main source, then the one affected test) from inherited method calls to explicit calls on the new utility, module by module, relying on the compiler to surface anything missed.
4. Remove the now-unused protected methods from `DefinitionParser` once no call site depends on inherited access.
5. Introduce the two composed resource-context helpers; update `TopologyBaseResourceAwareParser`'s and `TopologyResourceAwareParser's` former subclasses to hold the appropriate helper as a field instead of extending.
6. Remove `TopologyBaseResourceAwareParser` and `TopologyResourceAwareParser` once no subclass remains.
7. Run the full `ksml` test suite, with particular attention to `AllDefinitionsSchemaValidationTest`, `AllRunnerConfigSchemaValidationTest`, and `DefinitionParserUnresolvedTypeTest`.
8. No rollback complexity beyond normal git revert — this change has no data migration, no external interface change, and no deployment sequencing concern.

## Open Questions

- Naming for the new field-builder utility class and the two resource-context helper classes — to be settled during implementation (task list treats this as a first sub-task rather than deferring it indefinitely).
- Whether `StructsParser.lazy(...)` should live on the `StructsParser` interface (as a static factory, matching `of(...)`) or in a separate combinator class — leaning toward `StructsParser` for discoverability, to confirm during implementation.
