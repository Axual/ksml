## Context

The `Notation` interface declares methods like `name()`, `defaultType()`, `schemaUsage()`, `converter()`, `schemaParser()`, `filenameExtension()`. Today `BaseNotation` implements all of them via `@Getter` on private final fields set in a 7-argument constructor. Every intermediate and leaf class passes these values up the chain, even though they're constants per leaf type.

The `NotationProvider` interface already has the `notationName()` / `name()` split we need — `notationName()` returns the base name (e.g. `"avro"`), and `name()` composes it with an optional vendor prefix (e.g. `"confluent_avro"`). We align the `Notation` hierarchy to follow the same pattern.

## Goals / Non-Goals

**Goals:**
- Simplify constructor chains so only `context` flows through
- One consistent pattern: leaf types define constants via method overrides
- Keep the `Notation` interface unchanged
- Align `name()` / `notationName()` with the existing `NotationProvider` pattern

**Non-Goals:**
- Changing the `Notation` interface
- Changing `NotationProvider` or its implementations
- Modifying `ConfluentAvroNotation`, `ApicurioAvroNotation`, or other classes that extend leaf notations

## Decisions

### Constants as method overrides, not constructor arguments

Every value that is constant per leaf type becomes an abstract method in `BaseNotation` (or `StringNotation` for `stringMapper`) that the leaf overrides. This is consistent — all constants follow the same pattern regardless of where in the hierarchy they're defined.

### `notationName()` / `name()` split

`BaseNotation` declares an abstract `notationName()` method returning the base notation name. It provides a default `name()` implementation that simply delegates to `notationName()`. `VendorNotation` overrides `name()` to prepend the vendor name, using the same composition logic as `NotationProvider.name()`:

```
BaseNotation:
  abstract String notationName();           // leaf overrides: "avro", "json", etc.
  String name() { return notationName(); }  // default: just the notation name

VendorNotation:
  @Override
  String name() {
      return (vendorName != null && !vendorName.isEmpty()
          ? vendorName + "_" : "") + notationName();
  }
```

### Lazy schema parser for XmlNotation

`XmlNotation`'s schema parser depends on `context()` (it needs `nativeDataObjectMapper()` and `typeSchemaMapper()`). Rather than constructing it eagerly with null checks, use a lazy holder:

```java
private SchemaParser lazySchemaParser;

@Override
public SchemaParser schemaParser() {
    if (lazySchemaParser == null) {
        lazySchemaParser = new XmlSchemaParser(
            context().nativeDataObjectMapper(),
            context().typeSchemaMapper());
    }
    return lazySchemaParser;
}
```

This is safe because `context()` is guaranteed non-null (BaseNotation defaults it), and schema parsing is an I/O-bound operation where lazy init cost is negligible.

### VendorNotation internal fields

`VendorNotation` extracts `serdeSupplier`, `serdeMapper`, and `vendorName` from `VendorNotationContext`. These stay as fields — they vary per instance (via the context) and are used by `VendorNotation.serde()` and `VendorNotation.name()`.

## Structure after refactoring

```
Notation (interface) — UNCHANGED
  │ name(), filenameExtension(), schemaUsage(), defaultType(),
  │ converter(), schemaParser(), serde()
  │
BaseNotation (abstract)
  │ field: context
  │ constructor: (NotationContext)
  │ abstract: notationName(), filenameExtension(), schemaUsage(),
  │           defaultType(), converter(), schemaParser()
  │ default: name() → notationName()
  │
  ├── JsonNotation         ctx only → overrides all abstract methods
  ├── BinaryNotation       ctx only → overrides all abstract methods
  │
  ├── StringNotation (abstract)
  │   │ abstract: stringMapper()
  │   │ implements: serde() using stringMapper()
  │   │ constructor: (NotationContext)
  │   │
  │   ├── XmlNotation      ctx only → overrides all, lazy schemaParser()
  │   ├── CsvNotation      ctx only → overrides all
  │   └── SoapNotation     ctx only → overrides all (@Deprecated)
  │
  └── VendorNotation (abstract)
      │ fields: serdeSupplier, serdeMapper, vendorName (from VendorNotationContext)
      │ overrides: name() → vendorName + "_" + notationName()
      │ implements: serde() using serdeSupplier/serdeMapper
      │ constructor: (VendorNotationContext)
      │
      ├── AvroNotation         ctx only → overrides notationName(), defaultType(), etc.
      │   └── ConfluentAvroNotation(ctx, registryClient, resolver) — UNCHANGED
      │   └── ApicurioAvroNotation(ctx, registryClient, resolver) — UNCHANGED
      ├── ProtobufNotation     ctx only → overrides all
      └── JsonSchemaNotation   ctx only → overrides all
```

## Files to modify

| File | Change |
|------|--------|
| `BaseNotation.java` | Remove 6 fields, shrink constructor, add abstract `notationName()`, default `name()` |
| `StringNotation.java` | Remove `stringMapper` field/param, add abstract `stringMapper()` |
| `VendorNotation.java` | Shrink constructor, override `name()` using `notationName()` |
| `JsonNotation.java` | Override abstract methods with constants |
| `BinaryNotation.java` | Override abstract methods with constants |
| `XmlNotation.java` | Override abstract methods, lazy `schemaParser()` |
| `CsvNotation.java` | Override abstract methods with constants |
| `SoapNotation.java` | Override abstract methods with constants |
| `AvroNotation.java` | Override abstract methods with constants |
| `ProtobufNotation.java` | Override abstract methods with constants |
| `JsonSchemaNotation.java` | Override abstract methods with constants |
| `BaseNotationTest.java` | Update dummy subclass to use overrides |
| `StringNotationTest.java` | Update dummy subclass to use overrides |
| `VendorNotationTest.java` | Update dummy subclass to use overrides |
