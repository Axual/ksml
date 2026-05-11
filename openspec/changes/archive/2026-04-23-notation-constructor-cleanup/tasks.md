## 1. BaseNotation

- [x] 1.1 Remove the 6 constant fields (`name`, `filenameExtension`, `schemaUsage`, `defaultType`, `converter`, `schemaParser`) from `BaseNotation`. Keep only `context`.
- [x] 1.2 Shrink the constructor to `BaseNotation(NotationContext context)`.
- [x] 1.3 Add abstract method `notationName()` and a default `name()` implementation that delegates to it.
- [x] 1.4 Leave `filenameExtension()`, `schemaUsage()`, `defaultType()`, `converter()`, `schemaParser()` unimplemented (abstract by omission from the `Notation` interface).

## 2. StringNotation

- [x] 2.1 Remove `stringMapper` field and constructor parameter. Shrink constructor to `StringNotation(NotationContext context)`.
- [x] 2.2 Add abstract method `stringMapper()` and update `serde()` to call it.

## 3. VendorNotation

- [x] 3.1 Shrink constructor to `VendorNotation(VendorNotationContext context)`. Extract `serdeSupplier`, `serdeMapper`, `vendorName` from context as before.
- [x] 3.2 Override `name()` to use `notationName()` + vendor prefix composition (matching `NotationProvider.name()` logic).

## 4. Leaf notations

- [x] 4.1 `JsonNotation`: Override `notationName()`, `filenameExtension()`, `schemaUsage()`, `defaultType()`, `converter()`, `schemaParser()`. Constructor takes only `NotationContext`.
- [x] 4.2 `BinaryNotation`: Same pattern. Constructor takes only `NotationContext` and optional `SerdeSupplier`.
- [x] 4.3 `XmlNotation`: Same pattern, with lazy `schemaParser()` that constructs on first call using `context()`.
- [x] 4.4 `CsvNotation`: Same pattern. Override `stringMapper()` from `StringNotation`.
- [x] 4.5 `SoapNotation`: Same pattern (deprecated, minimal effort).
- [x] 4.6 `AvroNotation`: Override `notationName()`, `filenameExtension()`, `defaultType()`, `converter()`, `schemaParser()`. Constructor takes only `VendorNotationContext`.
- [x] 4.7 `ProtobufNotation`: Same pattern. `schemaParser()` override returns the parser (passed via constructor or created internally).
- [x] 4.8 `JsonSchemaNotation`: Same pattern.

## 5. Tests

- [x] 5.1 Update `BaseNotationTest`: rewrite dummy inner subclass to use method overrides instead of constructor args.
- [x] 5.2 Update `StringNotationTest`: rewrite dummy inner subclass.
- [x] 5.3 Update `VendorNotationTest`: rewrite dummy inner subclass.
- [x] 5.4 Verify all existing leaf notation tests pass unchanged.
- [x] 5.5 Run full `mvn verify` to confirm no regressions.
