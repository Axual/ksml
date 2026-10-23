# Upgrading to KSML 2.0.0

KSML 2.0.0 is a major release. It upgrades the core libraries that KSML is built on: Jackson (2 to 3), Apicurio Registry (2 to 3), Protobuf (4.x), Wire (6.x) and the Confluent serdes (8.x). It also upgrades Kafka Streams and the Kafka clients from 4.3 to 4.4.0.

Most of these upgrades are internal, but a few of them change how you configure KSML. Please read this page before you upgrade a running deployment.

This guide is written as "what changed" followed by "what you need to do".

## Quick checklist

Before you upgrade, check each item below:

* Update every Apicurio registry URL to the `/apis/registry/v3` endpoint.
* Rename the Apicurio basic-auth keys from `apicurio.auth.*` to `apicurio.registry.auth.*`.
* Replace any `apicurio.registry.auto-register.if-exists: RETURN` with a valid v3 value such as `FIND_OR_CREATE_VERSION`.
* Remove `apicurio.registry.as-confluent`, and replace `Legacy4ByteIdHandler` with `Default4ByteIdHandler` if you set `apicurio.registry.id-handler` yourself.
* Fix any typo'd keys in your runner config, because unknown keys now fail at startup again.
* If you read an Avro `decimal` field in Python, change the code to expect a string instead of bytes.
* If a `streamPartitioner` or `valueJoiner` function uses a global variable called `headers` or `mappedKey`, rename it.
* If Python code calls a `streamPartitioner` function by position, pass `headers` before `numPartitions`, or call it by name.
* Rename any `as` name that is also a name under `streams`, `tables` or `globalTables`.
* Add `toTable` before any `join` or `leftJoin` with `foreignKeyExtractor` that has a stream as input.
* If you build your own code on top of the KSML libraries, update the Jackson package names.

## Apicurio Registry 3

KSML 2.0.0 upgrades to the Apicurio Registry 3 client. Point `apicurio.registry.url` at the `/apis/registry/v3` endpoint of an Apicurio 3.x server. The client can still fall back to the v2 API when the URL contains `/apis/registry/v2`, or when you set `apicurio.registry.url.version` to `2`, but KSML 2.0.0 is only tested against Apicurio 3.x and we do not support that path. If you cannot upgrade your registry yet, stay on KSML 1.x.

### Registry URL must use the v3 endpoint

Use the v3 endpoint. The v2 endpoint is not tested or supported by KSML 2.0.0.

Update the `apicurio.registry.url` in your schema registry config:

```yaml
# Before (KSML 1.x)
apicurio.registry.url: http://schema-registry:8081

# After (KSML 2.0.0)
apicurio.registry.url: http://schema-registry:8081/apis/registry/v3
```

If you use the Confluent-compatible endpoint of Apicurio, that path stays `/apis/ccompat/v7`.

### Basic-auth config keys were renamed

Apicurio v3 renamed the basic-auth keys. KSML no longer maps these keys itself, it passes them straight to Apicurio.

Update the keys:

```yaml
# Before (KSML 1.x)
apicurio.auth.username: my-user
apicurio.auth.password: my-secret

# After (KSML 2.0.0)
apicurio.registry.auth.username: my-user
apicurio.registry.auth.password: my-secret
```

To protect you from a silent failure, KSML now stops at startup with a clear error if it still finds the old `apicurio.auth.username` or `apicurio.auth.password` keys. This is on purpose: if the old keys were simply ignored, KSML would connect without a login and you would only find out later with a `401` error.

### The `auto-register.if-exists` values changed

Apicurio v3 changed the accepted values for `apicurio.registry.auto-register.if-exists`. The old `RETURN` value is gone. The valid values are now `FAIL`, `CREATE_VERSION` and `FIND_OR_CREATE_VERSION`. Apicurio validates this value at startup even when auto-register is off, so a stale value fails the serde immediately.

```yaml
# Before (KSML 1.x)
apicurio.registry.auto-register.if-exists: RETURN

# After (KSML 2.0.0)
apicurio.registry.auto-register.if-exists: FIND_OR_CREATE_VERSION
```

### The id-handler settings changed

Apicurio v3 dropped the `apicurio.registry.as-confluent` key, and removed the `Legacy4ByteIdHandler` class. The payload id format is now set by `apicurio.registry.id-handler` and `apicurio.registry.use-id` alone.

KSML sets both for you, so most users have nothing to do. But KSML never overwrites a value you set yourself, so a leftover v2 setting would quietly change your wire format or fail when the serde tries to load a class that is gone. KSML therefore stops at startup if it still finds either of them.

```yaml
# Before (KSML 1.x)
apicurio.registry.as-confluent: true
apicurio.registry.id-handler: io.apicurio.registry.serde.Legacy4ByteIdHandler

# After (KSML 2.0.0): drop as-confluent, and use the new handler if you set one at all
apicurio.registry.id-handler: io.apicurio.registry.serde.Default4ByteIdHandler
```

### The Apicurio on-wire format is unchanged

Good news: nothing to do here. The Apicurio notations (`apicurio_avro`, `apicurio_jsonschema`, `apicurio_protobuf`) keep the same on-wire format as KSML 1.x.

KSML still sets the Apicurio serde options explicitly (now using the Apicurio v3 config keys) so the format is pinned by KSML rather than left to Apicurio's defaults: the schema id is a 4-byte content id in the message payload, headers are off. We verified this on KSML 2.0.0 for all three notations: the message value starts with a `0x00` magic byte, then a 4-byte schema id, then the payload, and no schema id is written into Kafka headers. So existing topics keep working and you do not need to reprocess them.

If you deliberately want the header-based id format instead, set `apicurio.registry.headers.enabled: true` in the notation config; KSML only applies the payload-id defaults when headers are off, and your own settings always win.

### Resolving pre-registered schemas (issue #290)

KSML keeps defaulting `apicurio.registry.find-latest` to `true`, so that with auto-register disabled and a pre-registered schema, the serializer resolves the artifact by its coordinates instead of by its content. This is the same default that was introduced in KSML 1.3.0, and it keeps working on Apicurio v3. You do not need to change anything for this. If you had set `find-latest` yourself, KSML still respects your value.

## Stricter runner config validation

KSML validates your runner config again like it did on the 1.x line: an unknown or misspelled key now makes KSML stop at startup with an error, instead of being silently ignored.

Jackson 3 changed its default so that unknown keys are ignored. KSML re-enables the strict check on purpose, so a typo such as `schemaRegsitry` fails fast instead of quietly disabling a setting.

What you need to do: make sure your `ksml-runner.yaml` has no leftover or misspelled keys before you upgrade.

## Duplicate keys in KSML definitions are now an error

KSML now rejects a definition file that uses the same key twice, instead of silently keeping the last value. This catches copy-and-paste mistakes early.

On the 1.x line a repeated key was accepted and the last one quietly won, which could hide a real mistake in a pipeline or function definition.

KSML stops at startup and names the file and the position:

```
Configuration Key   : 'definitionFile'
Configuration Value : '/ksml/processor.yaml'
Could not read the KSML definition: Duplicate field 'topic'
 at [Source: (File); line: 4, column: 12]
```

This also changed for any other unreadable definition file. On the 1.x line a definition KSML could not parse was skipped with one log line, and the runner started without that pipeline. It now stops, because a runner that quietly omits a pipeline looks like an idle topic rather than a broken config.

What you need to do: make sure each KSML definition file has no duplicate keys before you upgrade.

## An `as` name can not be a defined stream or table name

KSML now stops with a parse error when a pipeline saves its result with `as` under a name that is also defined under `streams`, `tables` or `globalTables`:

```
"as: orders" is already defined under "tables". Use another name
```

On the 1.x line this was accepted. A later `from` with that name then silently got the saved result, not the defined stream or table, because `from` looks for a saved result first.

A pipeline without a sink saves its result under the pipeline name. If that name is also defined under `streams`, `tables` or `globalTables`, KSML only logs a warning, so these definitions still start.

What you need to do: rename the `as` name, and every `from` or `cogroup` that uses it. Do not change a `join` or `merge` with that name: it already uses the defined stream or table, and would get the saved result instead.

For a pipeline without a sink, add `as` with another name, and use that name in the later `from` or `cogroup`. Adding `as` leaves the Kafka Streams topology unchanged, so a running application keeps its topics and state.

## A foreign-key join needs a table as input

KSML now stops at startup when a `join` or `leftJoin` with `foreignKeyExtractor` has a stream as input:

```
A join with "foreignKeyExtractor" needs a table as input, but found a stream. Add "toTable" before the join, or remove "foreignKeyExtractor"
```

Kafka Streams only has foreign-key joins between two tables. On the 1.x line, KSML accepted a `foreignKeyExtractor` on a stream, but never called it: the stream was joined with the table by the stream's own key. So the join silently found the wrong records, or none.

What you need to do: add `toTable` before the join, and `toStream` after it if the next steps need a stream:

```yaml
# Before (KSML 1.x)
via:
  - type: join
    table: customers
    foreignKeyExtractor: extract_customer_id
    valueJoiner: join_order_customer

# After (KSML 2.0.0)
via:
  - type: toTable
  - type: join
    table: customers
    foreignKeyExtractor: extract_customer_id
    valueJoiner: join_order_customer
  - type: toStream
```

The result of the join is then a table that is updated when an order or its customer changes. If you meant to join the stream by its own key, remove `foreignKeyExtractor` instead.

The new steps add Kafka topics to your application: `toTable` adds a state store with a changelog topic, and the foreign-key join adds a subscription store with a changelog topic and two internal topics for subscription registrations and responses. If topics are created by hand or need ACLs in your cluster, set them up before the upgrade.

## Avro logical types

KSML now understands Avro logical types (`uuid`, `decimal`, `date`, `time-millis`, `time-micros`,
`timestamp-millis`, `timestamp-micros`, `local-timestamp-millis`, `local-timestamp-micros`). It keeps
the logical type when it reads and writes a record. One of these changes what your Python code sees.

### A decimal is now a string

An Avro `decimal` used to arrive in Python as raw bytes, because KSML treated it as its base `bytes`
type. It is now a string holding the exact number.

```python
# Before (KSML 1.x): amount was bytes, and you had to decode it yourself
amount = value["amount"]          # b'\x30\x39'

# After (KSML 2.0.0): amount is the number as text
amount = value["amount"]          # "123.45"
```

What you need to do: if a Python function reads a `decimal` field, treat it as a string. Use
`float(value["amount"])` or Python's `decimal.Decimal` when you need to calculate with it. When you
write the field, write a string such as `"123.45"`.

### Values are checked against their logical type

KSML checks that a value fits its logical type, for example that a `uuid` is a real UUID and that a
`time-millis` is between `0` and `86399999`.

Writing an invalid value fails, which points at the mistake in your own pipeline. Reading an invalid
value only writes a warning to the log and passes the value on. That is on purpose: a bad record in a
topic comes from another system, and the default consume error handler is `stopOnFail`, so failing
there would stop your application because of someone else's data.

What you need to do: nothing to upgrade, but watch the log for these warnings after you upgrade.

### Decimal precision may grow, scale may not change

Two versions of a schema whose `decimal` differs only in precision are compatible, as long as the
scale is the same and the precision grows. Narrowing the precision or changing the scale is rejected,
because both can lose digits.

### Limits

* Only a `bytes`-backed decimal is supported. A `fixed`-backed decimal is treated as a plain `fixed`
  and reaches Python as bytes.
* Logical types come from the schema, so they work when the schema is loaded from a `.avsc` file or
  from the schema registry. An inline KSML schema cannot express one.

## Kafka Streams error handlers

For pipeline authors who write KSML YAML, there is nothing to change here.

KSML did move off the Kafka Streams handler methods that 4.x deprecated. If you implement your own Kafka Streams error handler against the KSML libraries, `handle` became `handleError`, and the enum results were replaced by `Response.resume()`, `Response.fail()` and `Response.retry()`.

## Kafka 4.4

KSML 2.0.0 upgrades Kafka Streams and the Kafka clients from 4.3 (used by KSML 1.3.x) to 4.4.0. For pipeline authors, there is nothing to change.

Two function types get a new parameter:

* A `streamPartitioner` gets the record headers in `headers`. Its parameters are now `topic`, `key`, `value`, `headers` and `numPartitions`, in the order that Kafka Streams uses.
* A `valueJoiner` in a join with a globalTable gets the key that the `mapper` returned in `mappedKey`. This parameter is optional and comes last, so `key`, `value1` and `value2` keep their position.

Existing functions work without changes, with two exceptions:

* If such a function uses a global variable called `headers` or `mappedKey`, for example from `globalCode`, rename that variable. The new parameter hides it, and `global headers` in the function code fails at startup.
* If Python code calls a `streamPartitioner` function by position, for example `my_partitioner(topic, key, value, 4)`, pass `headers` before `numPartitions`, or call the function by name. A `generic` function with only `topic`, `key`, `value` and `numPartitions` still works as a partitioner.

## Protobuf and Wire

Protobuf was upgraded to the 4.x line and Wire to the 6.x line. There is no configuration change for pipeline authors. If you use Protobuf schemas, test them after upgrading.

## Jackson 3 (only if you extend KSML in Java)

If you only write KSML definitions and runner config, you can skip this section.

If you have Java code that depends on the KSML data libraries, KSML moved from Jackson 2 to Jackson 3. The main package root changed:

* `com.fasterxml.jackson.core`, `com.fasterxml.jackson.databind` and the dataformat packages moved to `tools.jackson.*`.
* Jackson annotations stay under `com.fasterxml.jackson.annotation`, so your annotated model classes do not need to change.

Update your imports and rebuild against the KSML 2.0.0 libraries.
