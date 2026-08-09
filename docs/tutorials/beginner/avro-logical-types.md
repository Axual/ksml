# Avro Logical Types

This tutorial explains what Avro logical types are, what KSML gives your Python code, and why the
change to `decimal` in KSML 2.0.0 breaks older pipelines. You then run a working example end to end.

## Background you need first

Avro is a format for putting structured data on Kafka. Every message has a schema that says which
fields exist and what type each one is.

Avro only has a few basic types: string, int, long, bytes, and so on. There is no "money" type and no
"UUID" type. So Avro lets you put a label on a basic type. This label is a *logical type*. You are
saying: "this is stored as bytes, but please treat it as a decimal number."

A money field looks like this in a schema:

```json
{ "name": "value",
  "type": { "type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2 } }
```

The `bytes` part is how it travels. The `decimal` part is what it means.

A price like `123.45` must stay exact. If you store it as a floating point number, you can get
`123.44999999999999` back, which is wrong for money. So Avro stores the digits `12345` as raw bytes,
plus a note that says "move the decimal point 2 places left". The bytes are exact.

In KSML you write functions in Python that read and change message fields, like `value["value"]`.
Whatever KSML hands your Python code is what you have to work with.

## What changed in KSML 2.0.0

KSML used to ignore the `logicalType` label. It saw only the `bytes` part, so your Python code
received raw bytes:

```python
amount = value["value"]     # b'\x30\x39'  - useless without decoding it yourself
```

Now KSML reads the label, decodes the bytes, and hands you the number as text:

```python
amount = value["value"]     # "123.45"
```

Text is used rather than a Python float so that no digit is ever lost.

## Why this is a breaking change

"Breaking" means existing code stops working after an upgrade, without you changing anything
yourself. Python that treated `value["value"]` as bytes breaks, because the field is now a string.

Use `decimal.Decimal(value["value"])` when you need to calculate with it. Do not use `float()` for
money, because that brings back the rounding problem the decimal type exists to avoid.

## The nine logical types

| Avro logical type        | Avro base type | Value in KSML                                         |
|--------------------------|----------------|-------------------------------------------------------|
| `uuid`                   | string         | string, e.g. `"123e4567-e89b-12d3-a456-426614174000"` |
| `decimal`                | bytes          | string, e.g. `"123.45"`                               |
| `date`                   | int            | int, days since 1970-01-01                            |
| `time-millis`            | int            | int, milliseconds after midnight                      |
| `time-micros`            | long           | long, microseconds after midnight                     |
| `timestamp-millis`       | long           | long, milliseconds since 1970-01-01                   |
| `timestamp-micros`       | long           | long, microseconds since 1970-01-01                   |
| `local-timestamp-millis` | long           | long, milliseconds, no time zone                      |
| `local-timestamp-micros` | long           | long, microseconds, no time zone                      |

Two limits are worth knowing. Only a `bytes`-backed decimal is supported; a `fixed`-backed one is
treated as a plain `fixed`. And logical types come from the schema, so they need a `.avsc` file or a
schema registry. An inline KSML schema cannot express one.

## Validation: strict on write, forgiving on read

KSML checks that a value fits its logical type. A `uuid` must be a real UUID, a `time-millis` must be
between `0` and `86399999`, and a `decimal` must fit the precision and scale in the schema.

Writing an invalid value fails. That points at the mistake in your own pipeline.

Reading an invalid value only writes a warning to the log and passes the value on. That is
deliberate. A bad record in a topic comes from another system, and the default consume error handler
is `stopOnFail`. So failing on read would stop your application because of someone else's data.

## Run the example

You need Docker and a local build of KSML, because logical types are a 2.0.0 feature and the
published image is older.

### 1. Build the image

```bash
cd /path/to/ksml
./build-local-docker.sh
```

This produces `axual/ksml:local`.

### 2. Point the compose setup at your build

In `docs/local-docker-compose-setup-with-sr/docker-compose.yml`, change the `ksml` service image:

```yaml
  ksml:
    image: axual/ksml:local
```

The topics this example uses, `logical_types_avro` and `logical_types_json`, are already created by
the `kafka-setup` service, so nothing else in the compose file needs to change.

### 3. Add the schema

Save this as `examples/SensorReading.avsc`. Each field carries a different logical type, so you can
see all of them in one message.

??? info "Avro schema with logical types (click to expand)"

    ```json
    {%
      include "../../../ksml-integration-tests/src/test/resources/docs-examples/beginner-tutorial/avro-logical-types/SensorReading.avsc"
    %}
    ```

### 4. The producer

Save this as `examples/producer-logical-types.yaml`. Every logical value is written as the plain value you would
expect. The decimal is written as **text**, never as a float.

??? info "Producer definition (click to expand)"

    ```yaml
    {%
      include "../../../ksml-integration-tests/src/test/resources/docs-examples/beginner-tutorial/avro-logical-types/producer-logical-types.yaml"
    %}
    ```

### 5. The processor

Save this as `examples/processor-logical-types.yaml`. This reads the Avro record, logs what Python receives, does
exact arithmetic on the decimal, and writes the result as JSON so it is easy to read.

??? info "Processor definition (click to expand)"

    ```yaml
    {%
      include "../../../ksml-integration-tests/src/test/resources/docs-examples/beginner-tutorial/avro-logical-types/processor-logical-types.yaml"
    %}
    ```

### 6. The runner configuration

Save this as `examples/ksml-runner.yaml`. This one uses the Confluent Avro serdes against the
registry's Confluent-compatible endpoint.

??? info "Runner configuration for Confluent Avro (click to expand)"

    ```yaml
    {%
      include "../../../ksml-integration-tests/src/test/resources/docs-examples/beginner-tutorial/avro-logical-types/confluent_avro/ksml-runner.yaml"
    %}
    ```

Logical types work the same way with the Apicurio serdes. Only the runner configuration changes.

??? info "Runner configuration for Apicurio Avro (click to expand)"

    ```yaml
    {%
      include "../../../ksml-integration-tests/src/test/resources/docs-examples/beginner-tutorial/avro-logical-types/apicurio_avro/ksml-runner.yaml"
    %}
    ```

### 7. Run it

```bash
cd docs/local-docker-compose-setup-with-sr
docker compose down --volumes && docker compose up -d
docker compose logs ksml -f
```

### 8. Check the result

Read the output topic:

```bash
docker exec $(docker compose ps -q broker) /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server broker:9093 --topic logical_types_json --from-beginning --max-messages 3
```

You should see something like this:

```json
{"measuredAt":1700000000123,"measuredOn":19785,"readingId":"123e4567-e89b-12d3-a456-426614174000","sensor":"sensor1","value":"123.50"}
{"measuredAt":1700000000123,"measuredOn":19785,"readingId":"123e4567-e89b-12d3-a456-426614174000","sensor":"sensor2","value":"123.50"}
{"measuredAt":1700000000123,"measuredOn":19785,"readingId":"123e4567-e89b-12d3-a456-426614174000","sensor":"sensor3","value":"123.50"}
```

This is the whole point of the tutorial in one line. The producer wrote `"123.45"`, the schema stored
it as exact bytes, KSML gave Python the string `"123.45"`, Python added `0.05` with `Decimal`, and
`"123.50"` came out. No digit was lost anywhere.

You can also browse the messages in Kowl at [http://localhost:8080](http://localhost:8080).

### 9. Clean up

```bash
docker compose down --volumes
```

Then put the original `image:` line back if you want the compose setup as it was.

## Try breaking it

Two experiments make the rules concrete.

Set the value to more fraction digits than the schema allows, for example `"123.456"` with
`scale: 2`. The write fails and tells you the value has more fraction digits than the schema scale.
This is the strict-on-write rule.

Set `readingId` to `"not-a-uuid"`. The write fails as well, because a uuid must be a real UUID. If a
record with a bad uuid were already on the topic, reading it would log a warning and carry on
instead. You cannot create such a record with KSML, exactly because writing is strict. It has to come
from another producer.

## Troubleshooting

If KSML logs nothing at all, make sure your image is built from a clean `build-output/` directory.
`build-local-docker.sh` cleans it for you now, but an image built before that change can contain
several versions of the same library.

If the decimal comes back with the wrong number of digits, match the schema scale when you write it,
for example `Decimal("...").quantize(Decimal("0.01"))` for `scale: 2`.
