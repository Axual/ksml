## Context

A KSML definition is parsed top-down, one parser per level of the YAML (whole file → `streams` / `pipelines` maps → each entry → nested objects such as an inline `to:` or each step in `via`). An object parser is built with `FieldParsers.structsParser(...)` from small field parsers. Each field parser asks the object for its own key (`node.get("from")`) and parses the value; an optional field returns `null` when the key is missing. The results go into one constructor.

No parser ever looks at which keys are actually present, so a key that no field parser asks for is ignored.

Each parser also exposes `schemas()`: the `StructSchema`(s) of the object it parses, built from the same field parsers (`SchemaUtil.addToSchemas`), with additional fields not allowed. `JsonSchemaMapper` turns this into the published language schema (`ksml --schema`, `docs/ksml-language-spec.*`). So the knowledge of the allowed fields already exists, per parser, and comes from the same declaration as the parsing itself.

A few places keep reading and schema apart:

- `TopologyDefinitionParser` reads `name`, `version` and `description` at the top level but leaves them out of the root schema.
- `ChoiceParser` (the `type:` field on operations, functions and stores) adds the `type` field to each variant's schema by hand; the variant's own parser does not have it.
- `ReduceOperationParser` and `JoinTargetDefinitionParser` look at some keys only to decide which variant applies.

`ksml-docs-test`'s `AllDefinitionsSchemaValidationTest` already validates every definition under `ksml/src/test/resources` and `ksml-integration-tests/src/test/resources/docs-examples` against this strict schema, and they all pass.

## Goals / Non-Goals

**Goals:**
- Stop at start-up when a definition contains an unknown field, naming the path and all unknown fields of that object, and listing the known fields.
- For a `type:` object, accept only the fields of the chosen type.
- Use the existing per-parser field knowledge (the schemas), so there is no second list to maintain.
- Make the published schema accept the top-level `name`, `version` and `description`.

**Non-Goals:**
- "Did you mean …?" suggestions.
- Validating values (types, ranges); that is unchanged.
- Validating the `ksml-test-runner` suite format, or data schema files (`.avsc`, `.json` schemas).
- A warning-only mode or an option to turn the check off.

## Decisions

**D1 — Check where an object is handed to its parser, not inside `ValueStructParser`.**
A `ValueStructParser` for a `type:` variant does not know the `type` field, so a check inside it would reject `type`. The check therefore runs at the handoff points, where the code descends into an object and calls the parser for it:
- `FieldParser.parse` (the value of a field, when the value parser is a struct parser);
- `MapParser` and `ListParser` (each entry);
- `TopologyResourceParser` (an inline definition);
- the root in `TopologyDefinitionParser`.

*Alternative:* track which keys were read (`node.get`) during parsing, and report the rest afterwards. Rejected: it needs mutable state shared over the whole parse, it depends on parse order, and it would hide differences between parsing and the published schema instead of making them visible.

**D2 — The parser decides its known fields: `StructsParser.knownFields(ParseNode)`.**
- **Default:** the union of the field names of all its `schemas()`.
- **`ChoiceParser`:** overrides this with the `type` field plus the known fields of the chosen variant. So `store:` on a `filter` step is rejected, even though another operation type accepts it.
- **Delegating parsers** (`DefinitionParser`, `StructsParser.lazy`): pass the call on.
- A schema that allows additional fields (`StructSchema.SCHEMALESS`, used by `IgnoreParser`) disables the check for that object.

**D3 — One check helper.**
`FieldParsers.checkKnownFields(ParseNode node, StructsParser<?> parser)`:
- does nothing for non-object nodes;
- otherwise compares the node's keys with `parser.knownFields(node)`;
- throws a `ParseException` on the node, so the message carries the path, all unknown fields of the object (sorted), and the known fields (sorted).

For example: `Unknown field "loging" in ...->stores->count_store. Known fields: caching, keyType, logging, persistent, timestamped, type, valueType, versioned, ...`

**D4 — Fix the root schema.**
`TopologyDefinitionParser.parser()` adds the `name`, `version` and `description` fields to the root schema, using the same field parsers that it already uses to read them. This fixes the published schema too.

## Risks / Trade-offs

- [**Breaking for definitions with typos or leftover fields**] → Intended. The migration guide and release notes explain it. The message names the field and its path, so it is quick to fix.
- [**A field that is read but missing from a schema would now be rejected**] → The existing schema test already validates all test definitions and docs examples against the strict schema. The new tests load all of them through the parser too. The root-schema fix (D4) covers the one known case.
- [**Union of variants where no `type:` decides**] (for example `JoinTargetDefinitionParser`, which picks the variant by the key that is present) → Typos are still caught; only a field of a sibling variant can pass. Accepted as a known limit.
- [**Files not covered by the schema test**] (`examples/`, YAML in markdown docs) → A task checks them and fixes any that fail.

## Migration Plan

No data migration. Users fix the fields named in the start-up error. Rollback is reverting the change.

## Open Questions

None.
