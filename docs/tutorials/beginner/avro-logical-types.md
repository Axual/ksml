# Avro Logical Types

This tutorial explains what Avro logical types are, what KSML gives your Python code, and why the
change to `decimal` in KSML 2.0.0 breaks older pipelines. You then run a working example end to end.

## Background you need first

**What Avro is.** Avro is a format for putting structured data on Kafka. Every message has a schema
that says which fields exist and what type each one is.

**What a logical type is.** Avro only has a handful of basic types: string, int, long, bytes, and so
on. There is no "money" type and no "UUID" type. So Avro lets you tag a basic type with extra
meaning, called a *logical type*. You are saying: "this is stored as bytes, but please treat it as a
decimal number."

A money field looks like this in a schema:

```json
{ "name": "amount",
  "type": { "type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2 } }
```

The `bytes` part is how it travels. The `decimal` part is what it means.

**Why decimal is stored as bytes.** A price like `123.45` must stay exact. If you store it as a
floating point number, you can get `123.44999999999999` back, which is unacceptable for money. So
Avro stores the digits `12345` as raw bytes plus a note that says "move the decimal point 2 places
left." The bytes are exact.

**Where Python comes in.** In KSML you write functions in Python that read and change message fields,
like `value["amount"]`. Whatever KSML hands your Python code is what you have to work with.

## What actually changed in KSML 2.0.0

KSML used to ignore the `logicalType` tag. It saw only the `bytes` part, so your Python code received
raw bytes:

```python
amount = value["amount"]     # b'\x30\x39'  - useless without decoding it yourself
```

Now KSML reads the tag, decodes the bytes, and hands you the number as text:

```python
amount = value["amount"]     # "123.45"
```

Text is used rather than a Python float precisely so no digit is ever lost.

## Why it is called a breaking change

"Breaking" means existing code stops working after an upgrade, without you changing anything
yourself. Python that treated `value["amount"]` as bytes breaks, because the field is now a string.

Use `decimal.Decimal(value["amount"])` when you need to calculate with it. Do not use `float()` for
money, because that reintroduces the rounding problem the decimal type exists to avoid.

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

Writing an invalid value fails, which points at the mistake in your own pipeline. Reading an invalid
value only writes a warning to the log and passes the value on. That is deliberate: a bad record in a
topic comes from another system, and the default consume error handler is `stopOnFail`, so failing on
read would stop your application because of someone else's data.

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

The topics this example uses, `sensor_data_avro` and `sensor_data_json`, are already created by the
`kafka-setup` service, so nothing else in the compose file needs to change.

### 3. Add the schema

`docs/local-docker-compose-setup-with-sr/examples/SensorReading.avsc`:

```json
{
  "namespace": "io.ksml.example",
  "name": "SensorReading",
  "type": "record",
  "doc": "A sensor reading whose measured value must stay exact",
  "fields": [
    { "name": "readingId", "type": { "type": "string", "logicalType": "uuid" } },
    { "name": "sensor", "type": "string" },
    { "name": "value", "type": { "type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2 } },
    { "name": "measuredOn", "type": { "type": "int", "logicalType": "date" } },
    { "name": "measuredAt", "type": { "type": "long", "logicalType": "timestamp-millis" } }
  ]
}
```

### 4. The producer

`examples/producer.yaml`. Every logical value is written as the plain value you would expect. The
decimal is written as **text**, never as a float.

```yaml
functions:
  generate_reading:
    type: generator
    globalCode: |
      import time
      counter = 0
    code: |
      global counter
      counter = (counter + 1) % 10

      key = "sensor" + str(counter)

      value = {
        "readingId": "123e4567-e89b-12d3-a456-42661417400" + str(counter),
        "sensor": key,
        "value": "123.4" + str(counter),
        "measuredOn": 19785,
        "measuredAt": round(time.time() * 1000)
      }
    expression: (key, value)
    resultType: (string, json)

producers:
  sensor_reading_producer:
    generator: generate_reading
    interval: 3s
    to:
      topic: sensor_data_avro
      keyType: string
      valueType: avro:SensorReading
```

### 5. The processor

`examples/processor.yaml`. This reads the Avro record, shows what Python receives, does exact
arithmetic on the decimal, and writes the result as JSON so it is easy to read.

```yaml
streams:
  reading_input:
    topic: sensor_data_avro
    keyType: string
    valueType: avro:SensorReading
    offsetResetPolicy: earliest

  reading_output:
    topic: sensor_data_json
    keyType: string
    valueType: json

functions:
  show_what_python_receives:
    type: forEach
    code: |
      log.info("readingId  = {}   (uuid, a string)", value.get("readingId"))
      log.info("value      = {}   (decimal, a STRING so no digit is lost)", value.get("value"))
      log.info("measuredOn = {}   (date, days since 1970-01-01)", value.get("measuredOn"))
      log.info("measuredAt = {}   (timestamp-millis)", value.get("measuredAt"))

  apply_calibration:
    type: valueTransformer
    globalCode: |
      from decimal import Decimal
    code: |
      # The decimal is text, so convert before doing arithmetic.
      # Decimal keeps every digit exact; float would not.
      measured = Decimal(value.get("value"))
      calibrated = measured + Decimal("0.05")

      result = dict(value)
      # Write it back as text, with 2 fraction digits to match the schema scale.
      result["value"] = str(calibrated.quantize(Decimal("0.01")))
    expression: result
    resultType: avro:SensorReading

pipelines:
  main:
    from: reading_input
    via:
      - type: peek
        forEach: show_what_python_receives

      - type: transformValue
        mapper: apply_calibration

      - type: peek
        forEach:
          code: |
            log.info("calibrated value = {}   (still exact, still text)", value.get("value"))

      - type: convertValue
        into: json
    to: reading_output
```

`examples/ksml-runner.yaml` already points at `producer.yaml` and `processor.yaml`, so it needs no
change.

### 6. Run it

```bash
cd docs/local-docker-compose-setup-with-sr
docker compose down --volumes && docker compose up -d
docker compose logs ksml -f
```

### 7. Check the result

Read the output topic:

```bash
docker exec $(docker compose ps -q broker) /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server broker:9093 --topic sensor_data_json --from-beginning --max-messages 3
```

You should see something like this:

```json
{"measuredAt":1786174854737,"measuredOn":19785,"readingId":"123e4567-e89b-12d3-a456-426614174001","sensor":"sensor1","value":"123.46"}
{"measuredAt":1786174857700,"measuredOn":19785,"readingId":"123e4567-e89b-12d3-a456-426614174002","sensor":"sensor2","value":"123.47"}
{"measuredAt":1786174860711,"measuredOn":19785,"readingId":"123e4567-e89b-12d3-a456-426614174003","sensor":"sensor3","value":"123.48"}
```

This is the whole point of the tutorial in one line. The producer wrote `"123.41"`, the schema stored
it as exact bytes, KSML gave Python the string `"123.41"`, Python added `0.05` with `Decimal`, and
`"123.46"` came out. No digit was lost anywhere.

You can also browse the messages in Kowl at [http://localhost:8080](http://localhost:8080).

### 8. Clean up

```bash
docker compose down --volumes
```

Then put the original `image:` line, `producer.yaml` and `processor.yaml` back if you want the
compose setup as it was.

## Try breaking it

Two experiments make the rules concrete.

Set the value to more fraction digits than the schema allows, for example `"123.456"` with
`scale: 2`. The write fails and tells you the value has more fraction digits than the schema scale.
This is the strict-on-write rule.

Set `readingId` to `"not-a-uuid"`. The write fails as well, because a uuid must be a real UUID. If a
record with a bad uuid were already on the topic, reading it would log a warning and carry on
instead.

## Troubleshooting

**KSML logs nothing at all.** Make sure your image is built from a clean `build-output/` directory.
`build-local-docker.sh` cleans it for you now, but an image built before that change can contain
several versions of the same library.

**"Can not convert schema type bytes"** on a Protobuf or XML pipeline means the schema carries a
logical type that the target notation has no concept of. KSML falls back to the base primitive, so
this should not happen; if it does, the schema is reaching the mapper by an unexpected route.

**The decimal comes back with the wrong number of digits.** Match the schema scale when you write it,
for example `Decimal("...").quantize(Decimal("0.01"))` for `scale: 2`.
