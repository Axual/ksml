# definition-validation Specification

## Purpose
Defines how KSML validates the fields of a definition: every object may only contain the fields that its parser knows, so a misspelled or unknown field stops KSML at start-up with an error that names the field and its path, instead of silently changing behavior. The parser's known fields and the published language schema come from the same declarations, so a definition that parses also validates against the schema.

## Requirements
### Requirement: Unknown fields stop parsing
When a KSML definition contains a field in an object that the parser for that object does not know, parsing SHALL fail with an error. The error SHALL name the path of the object, all unknown fields of that object, and the known fields.

#### Scenario: Misspelled store option
- **WHEN** a store definition contains `loging: true` instead of `logging: true`
- **THEN** parsing fails with an error that names the store's path and the unknown field `loging`

#### Scenario: Misspelled operation option
- **WHEN** a `windowByTime` step contains `grase: 5s` instead of `grace: 5s`
- **THEN** parsing fails with an error that names the step's path and the unknown field `grase`

#### Scenario: Misspelled top-level section
- **WHEN** a definition contains a top-level `pipeline:` instead of `pipelines:`
- **THEN** parsing fails with an error that names the unknown field `pipeline`

#### Scenario: Unknown field in an inline definition
- **WHEN** an inline definition, such as an inline `to:` topic or an inline function, contains an unknown field
- **THEN** parsing fails with an error that names the inline definition's path and the unknown field

#### Scenario: Several unknown fields in one object
- **WHEN** one object contains more than one unknown field
- **THEN** the error names all unknown fields of that object

#### Scenario: KSML runner stops at start-up
- **WHEN** the KSML runner loads a definition with an unknown field
- **THEN** it stops at start-up with the parse error, and does not start the pipelines

### Requirement: Fields of another type are not accepted
For an object whose variant is chosen with a `type` field (operations, functions and stores), parsing SHALL accept only the `type` field and the fields of the chosen variant.

#### Scenario: Field of another operation type
- **WHEN** a pipeline step with `type: filter` contains `duration:`, which is a field of the window operations but not of `filter`
- **THEN** parsing fails with an error that names the unknown field `duration`

### Requirement: Valid definitions keep parsing
Every field that the parser reads SHALL be part of the parser's schema, so that valid definitions are not rejected and the published language schema accepts the same fields as the parser.

#### Scenario: Top-level name, version and description
- **WHEN** a definition contains the optional top-level fields `name`, `version` and `description`
- **THEN** it parses successfully, and the published language schema accepts these fields

#### Scenario: Existing definitions and examples
- **WHEN** the test definitions, the docs examples and the files in `examples/` are parsed
- **THEN** they all parse successfully

#### Scenario: Objects that allow any field
- **WHEN** an object is parsed by a parser whose schema allows additional fields
- **THEN** no unknown-field error is raised for that object

