## Why

The published KSML language schema says unknown fields are not allowed, but the parser silently ignores them. A typo then changes behavior without any warning: `loging: true` on a store gives no changelog topic, and `grase: 5s` on `windowByTime` gives no grace period. KSML 2.0 already rejects unknown runner config keys and duplicate keys in definitions; definitions should reject unknown fields too.

## What Changes

- **BREAKING**: KSML SHALL stop at start-up when a definition contains a field that the parser does not know, with a message that names the path and the unknown field(s), and lists the known fields.
- For an object with a `type:` field (operations, functions, stores), only the fields of the chosen type SHALL be accepted, not the fields of other types.
- The top-level fields `name`, `version` and `description`, which the parser already reads and the docs describe as optional, are added to the published language schema.
- The same check applies to `ksml-test-runner`, because it uses the same definition parser. Its own test suite format is not affected.
- Documentation: `docs/migration-to-2.0.0.md` and `docs/release-notes.md`; the generated `docs/ksml-language-spec.*` gains the three top-level fields.

## Capabilities

### New Capabilities
- `definition-validation`: KSML definitions are validated against the fields each parser knows; unknown fields stop parsing with a clear message.

### Modified Capabilities
(none — `ksml-parser-architecture`'s "Parsing and schema output equivalence" requirement was written for an earlier internal parser refactor. This change changes parse results on purpose, but only for invalid input, and adds three missing top-level fields to the schema. It does not change the requirements of that capability.)

## Impact

- **Code** (`ksml` module, `io.axual.ksml.parser` and `io.axual.ksml.definition.parser`): `StructsParser` (new known-fields method), `FieldParsers` (check helper, `FieldParser`), `MapParser`, `ListParser`, `ChoiceParser`, `TopologyResourceParser`, `DefinitionParser`, `TopologyDefinitionParser`.
- **Tests**: new parser tests; all existing test definitions and docs examples must still load. The `examples/` folder and YAML in the markdown docs are not covered by the existing schema test and need a check.
- **Users**: definitions with unknown or misspelled fields now fail at start-up instead of being silently accepted. The migration guide explains this.
- **Dependencies**: none.
