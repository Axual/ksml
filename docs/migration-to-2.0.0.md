# Upgrading to KSML 2.0.0

KSML 2.0.0 is a major release. It upgrades the core libraries that KSML is built on: Jackson (2 to 3), Apicurio Registry (2 to 3), Kafka (4.x), Protobuf (4.x), Wire (6.x) and the Confluent serdes (8.x).

Most of these upgrades are internal. But a few of them change how you configure KSML, and one of them changes the bytes that are written to Kafka. Please read this page before you upgrade a running deployment.

This guide is written as "what changed" followed by "what you need to do".

## Quick checklist

Before you upgrade, check each item below:

* Update every Apicurio registry URL to the `/apis/registry/v3` endpoint.
* Rename the Apicurio basic-auth keys from `apicurio.auth.*` to `apicurio.registry.auth.*`.
* Replace any `apicurio.registry.auto-register.if-exists: RETURN` with a valid v3 value such as `FIND_OR_CREATE_VERSION`.
* Fix any typo'd keys in your runner config, because unknown keys now fail at startup again.
* If you build your own code on top of the KSML libraries, update the Jackson package names.

## Apicurio Registry 3

### Registry URL must use the v3 endpoint

KSML 2.0.0 talks to Apicurio Registry over the v3 API only. The v2 endpoint is not supported.

Update the `apicurio.registry.url` in your schema registry config:

```yaml
# Before (1.x)
apicurio.registry.url: http://schema-registry:8081

# After (2.0.0)
apicurio.registry.url: http://schema-registry:8081/apis/registry/v3
```

If you use the Confluent-compatible endpoint of Apicurio, that path stays `/apis/ccompat/v7`.

### Basic-auth config keys were renamed

Apicurio v3 renamed the basic-auth keys. KSML no longer maps these keys itself, it passes them straight to Apicurio.

Update the keys:

```yaml
# Before (1.x)
apicurio.auth.username: my-user
apicurio.auth.password: my-secret

# After (2.0.0)
apicurio.registry.auth.username: my-user
apicurio.registry.auth.password: my-secret
```

To protect you from a silent failure, KSML now stops at startup with a clear error if it still finds the old `apicurio.auth.username` or `apicurio.auth.password` keys. This is on purpose: if the old keys were simply ignored, KSML would connect without a login and you would only find out later with a `401` error.

### The `auto-register.if-exists` values changed

Apicurio v3 changed the accepted values for `apicurio.registry.auto-register.if-exists`. The old `RETURN` value is gone. The valid values are now `FAIL`, `CREATE_VERSION` and `FIND_OR_CREATE_VERSION`. Apicurio validates this value at startup even when auto-register is off, so a stale value fails the serde immediately.

```yaml
# Before (1.x)
apicurio.registry.auto-register.if-exists: RETURN

# After (2.0.0)
apicurio.registry.auto-register.if-exists: FIND_OR_CREATE_VERSION
```

### The Apicurio on-wire format is unchanged

Good news: nothing to do here. The Apicurio notations (`apicurio_avro`, `apicurio_jsonschema`, `apicurio_protobuf`) keep the same on-wire format as KSML 1.x.

KSML still sets the Apicurio serde options explicitly (now using the Apicurio v3 config keys) so the format is pinned by KSML rather than left to Apicurio's defaults: the schema id is a 4-byte content id in the message payload, headers are off. We verified this on 2.0.0 for all three notations: the message value starts with a `0x00` magic byte, then a 4-byte schema id, then the payload, and no schema id is written into Kafka headers. So existing topics keep working and you do not need to reprocess them.

If you deliberately want the header-based id format instead, set `apicurio.registry.headers.enabled: true` in the notation config; KSML only applies the payload-id defaults when headers are off, and your own settings always win.

### Resolving pre-registered schemas (issue #290)

KSML keeps defaulting `apicurio.registry.find-latest` to `true`, so that with auto-register disabled and a pre-registered schema, the serializer resolves the artifact by its coordinates instead of by its content. This is the same default that was introduced in 1.3.0, and it keeps working on Apicurio v3. You do not need to change anything for this. If you had set `find-latest` yourself, KSML still respects your value.

## Stricter runner config validation

KSML validates your runner config again like it did on the 1.x line: an unknown or misspelled key now makes KSML stop at startup with an error, instead of being silently ignored.

Jackson 3 changed its default so that unknown keys are ignored. KSML re-enables the strict check on purpose, so a typo such as `schemaRegsitry` fails fast instead of quietly disabling a setting.

What you need to do: make sure your `ksml-runner.yaml` has no leftover or misspelled keys before you upgrade.

## Kafka 4.x

For pipeline authors who write KSML YAML, there is nothing to change here.

If you build your own code against the KSML or Kafka client libraries, note that some deprecated admin and Kafka Streams APIs were updated for Kafka 4.x.

## Protobuf and Wire

Protobuf was upgraded to the 4.x line and Wire to the 6.x line. There is no configuration change for pipeline authors. If you use Protobuf schemas, test them after upgrading.

## Jackson 3 (only if you extend KSML in Java)

If you only write KSML definitions and runner config, you can skip this section.

If you have Java code that depends on the KSML data libraries, KSML moved from Jackson 2 to Jackson 3. The main package root changed:

* `com.fasterxml.jackson.core`, `com.fasterxml.jackson.databind` and the dataformat packages moved to `tools.jackson.*`.
* Jackson annotations stay under `com.fasterxml.jackson.annotation`, so your annotated model classes do not need to change.

Update your imports and rebuild against the 2.0.0 libraries.
