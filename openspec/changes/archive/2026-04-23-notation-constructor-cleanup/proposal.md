## Why

`BaseNotation` has a 7-argument constructor, and `StringNotation` extends that to 8. These arguments are all forwarded through intermediate classes, but in every leaf type they are constants — the only value that genuinely varies per instance is `context`. The long constructors obscure what's fixed and what's variable, making the hierarchy harder to read and extend.

## What Changes

Replace `BaseNotation`'s constructor parameters (except `context`) with abstract methods that leaf types override with constants. This keeps the `Notation` interface unchanged while simplifying the constructor chain.

- `BaseNotation` constructor shrinks from 7 args to 1 (`context`)
- `StringNotation` constructor shrinks from 8 args to 1 (`context`), with an abstract `stringMapper()` method for the mapper that `serde()` uses
- `VendorNotation` constructor shrinks from 6 args to 1 (`VendorNotationContext`)
- Leaf types override abstract methods with constant returns instead of passing values to `super()`
- Introduce `notationName()` (matching the existing pattern on `NotationProvider`) so `VendorNotation.name()` can compose the vendor-prefixed name without relying on a stored field

## Impact

- **BaseNotation**: Remove 6 fields, keep `context`. Keep the `@Getter` on the class which now only creates a getter for `context()`. Add `notationName()` abstract method with a default `name()` that delegates to it.
- **StringNotation**: Remove `stringMapper` field and constructor parameter. Add abstract `stringMapper()` method.
- **VendorNotation**: Remove constructor parameters forwarded to `BaseNotation`. Override `name()` using `notationName()` + `vendorName` (same composition logic as `NotationProvider.name()`).
- **All leaf notations** (JsonNotation, BinaryNotation, XmlNotation, CsvNotation, SoapNotation, AvroNotation, ProtobufNotation, JsonSchemaNotation): Override abstract methods with constant returns. Constructors shrink to just `context`.
- **XmlNotation**: `schemaParser()` uses a lazy holder since it depends on `context()`, initialized on first call.
- **ConfluentAvroNotation, ApicurioAvroNotation**: Unaffected — they extend `AvroNotation` and their extra constructor args (`registryClient`, `topicResolver`) are their own, not relayed through the chain.
- **NotationProvider and its implementations**: Unaffected.
- **Tests**: `BaseNotationTest`, `StringNotationTest`, `VendorNotationTest` need their dummy inner subclasses updated to use method overrides instead of constructor args. Leaf notation tests are unaffected (their constructor APIs don't change).
