# Parse error in 09-example-aggregate2.yaml

## Error

When loaded through `KSMLTopologyTestExtension`, the example fails with:

```
java.lang.RuntimeException: KSML parse error: Error in input node exampleTopologyIsValid->pipelines: Error in pipeline definition "main"
```

## Root cause

The parse error occurs in the `convertKey` operation at the end of the pipeline:

```yaml
- type: convertKey
  into: json:windowed(struct)
```

When parsing `json:windowed(struct)`, the `UserTypeParser`:

1. Decomposes it into notation `json` and data type `windowed(struct)`.
2. Recursively parses the windowed type, producing `WindowedType(StructType)`.
3. Validates the type against the JSON notation's allowed types.

The JSON notation's default type (`JsonNotation.java`) is defined as:

```java
public static final DataType DEFAULT_TYPE = new UnionType(
    new UnionType.Member(new StructType()),
    new UnionType.Member(new ListType()));
```

This union allows `StructType` or `ListType`, but **not** `WindowedType`. The `isAssignableFrom` check in `ComplexType.java` returns `NOT_ASSIGNABLE` because `WindowedType(StructType)` is neither a `StructType` nor a `ListType`.

## Comparison with the working example

`09-example-aggregate.yaml` passes because it uses a simpler `groupBy` with `resultType: string` and does not include the `convertKey` with `json:windowed(struct)`.

## Possible fixes

1. **Fix the example** — `json:windowed(struct)` may not be a valid type for the JSON notation; the example could use a different target type.
2. **Fix the JSON notation's type validation** — it could be extended to accept windowed types that wrap an already-accepted inner type.

## Relevant code locations

- `UserTypeParser.java` (lines 115-251) — type parsing and validation
- `JsonNotation.java` (line 58) — JSON notation default type definition
- `ComplexType.java` (lines 101-119) — type assignability checking
