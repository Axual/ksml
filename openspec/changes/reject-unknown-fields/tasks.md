## 1. Tests first — prove the defect

- [x] 1.1 Add parser tests (`UnknownFieldsTest`, parameterized over definitions in `ksml/src/test/resources/pipelines/unknown-keys/`, which `AllDefinitionsSchemaValidationTest` skips): store with `loging: true`; `windowByTime` with `grase: 5s`; top-level `pipeline:`; `duration:` (a window operation field) on a `filter` step; unknown field in an inline `to:` and in an inline function; two unknown fields in one object (both named in the error)
- [x] 1.2 Add a test that top-level `name`, `version` and `description` parse, and that the generated schema accepts them
- [x] 1.3 Run the new tests against the current code and confirm each fails for the expected reason (no error raised, or schema rejects the top-level fields); report to the user before continuing

## 2. Fix

- [ ] 2.1 Add `knownFields(ParseNode)` to `StructsParser`: default is the union of the field names of `schemas()`; return "any field allowed" when a schema allows additional fields
- [ ] 2.2 Override `knownFields` in `ChoiceParser` (the `type` field plus the chosen variant's known fields), and pass it on in `DefinitionParser` and `StructsParser.lazy`
- [ ] 2.3 Add the check helper `FieldParsers.checkKnownFields(ParseNode, StructsParser<?>)`: object nodes only; `ParseException` with the path, all unknown fields and the known fields, sorted
- [ ] 2.4 Call the check at each handoff: `FieldParser.parse` (struct value parsers), `MapParser` and `ListParser` (each entry), `TopologyResourceParser` (inline definition), and the root in `TopologyDefinitionParser`
- [ ] 2.5 Add `name`, `version` and `description` to the root schema in `TopologyDefinitionParser.parser()`
- [ ] 2.6 Run the tests from section 1 and the full `ksml`, `ksml-runner`, `ksml-test-runner` and `ksml-docs-test` builds, including the coverage gate

## 3. Fallout

- [ ] 3.1 Check that every file in `examples/` parses, and fix the ones that do not
- [ ] 3.2 Check the full definitions shown as YAML in `docs/**/*.md`, and fix the ones with unknown fields
- [ ] 3.3 Regenerate `docs/ksml-language-spec.*` (the three top-level fields)

## 4. Docs and verification

- [ ] 4.1 `docs/migration-to-2.0.0.md`: extend "Stricter runner config validation" to cover definitions, with an example error
- [ ] 4.2 `docs/release-notes.md`: add an entry
- [ ] 4.3 Run all ITs with `mvn verify -pl ksml-integration-tests -Pintegration-tests` (after `mvn install` of the changed modules)
- [ ] 4.4 Manual check with Docker: a store with `loging: true` and a `windowByTime` with `grase: 5s` each stop KSML at start-up with the error
