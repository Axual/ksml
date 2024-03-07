[<< Back to index](index.md)

# Functions

### Table of Contents

## Introduction

Functions can be specified in the `functions` section of a KSML definition file. The layout typically looks like this:

```yaml
functions:
  my_first_predicate:
    type: predicate
    expression: key=='Some string'

  compare_params:
    type: generic
    parameters:
      - name: firstParam
        type: string
      - name: secondParam
        type: int
    globalCode: |
      import something from somepackage
      globalVar = 3
    code: |
      print('Hello there!')
    expression: firstParam == str(secondParam)
```

Functions are defined by the following tags:

| Parameter    | Value Type                    | Default      | Description                                                                                                                                                                                                            |
|:-------------|:------------------------------|:-------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `type`       | `string`                      | `generic`    | The [type](#function-types) of the function defined                                                                                                                                                                    |
| `parameters` | List of parameter definitions | _empty list_ | A list of parameters, each of which contains the mandatory fields `name` and `type`. See example above.                                                                                                                |
| `globalCode` | `string`                      | _empty_      | Snippet of Python code that is executed once upon creation of the Kafka Streams topology. This section can contain statements like `import` to import function libraries used in the `code` and `expression` sections. |
| `code`       | `string`                      | _empty_      | Python source code, which will be included in the called function.                                                                                                                                                     |Bla
| `expression` | `string`                      | _empty_      | Python expression that contains the returned function result.                                                                                                                                                          |Typically the code `"return expression"` is generated for the Python interpreter. For example `expression: key` would generate the Python code `return key` for the function.

See below for the list of supported function types.

## Function Types

| Type                                | Returns            | Parameter         | Value Type | Description                           |
|:------------------------------------|:-------------------|:------------------|:-----------|:--------------------------------------|
| `aggregator`                        | _any_              | `key`             | _any_      | The key of the message                |
|                                     |                    | `value`           | _any_      | The value of the message              |
|                                     |                    | `aggregatedValue` | _any_      | The aggregated value thus far.        |
| `forEach`                           | _none_             | `key`             | _any_      | The key of the message                |
|                                     |                    | `value`           | _any_      | The value of the message              |
| `initializer`                       | _any_              | _none_            |            |                                       |
| `keyTransformer`                    | _any_              | `key`             | _any_      | The key of the message                |
|                                     |                    | `value`           | _any_      | The value of the message              |
| `keyValueMapper`                    | _any_              | `key`             | _any_      | The key of the message                |
|                                     |                    | `value`           | _any_      | The value of the message              |
| `keyValueToKeyValueListTransformer` | [ (_any_, _any_) ] | `key`             | _any_      | The key of the message                |
|                                     |                    | `value`           | _any_      | The value of the message              |
| `keyValueToValueListTransformer`    | [ _any_ ]          | `key`             | _any_      | The key of the message                |
|                                     |                    | `value`           | _any_      | The value of the message              |
| `keyValueTransformer`               | (_any_, _any_)     | `key`             | _any_      | The key of the message                |
|                                     |                    | `value`           | _any_      | The value of the message              |
| `merger`                            | _any_              | `key`             | _any_      | The key of the message                |
|                                     |                    | `value1`          | _any_      | The first value to be merged          |
|                                     |                    | `value2`          | _any_      | The second value to be merged         |
| `predicate`                         | `boolean`          | `key`             | _any_      | The key of the message                |
|                                     |                    | `value`           | _any_      | The value of the message              |
| `reducer`                           | _any_              | `value1`          | _any_      | The first value to be reduced         |
|                                     |                    | `value2`          | _any_      | The second value to be reduced        |
| `streamPartitioner`                 | `int`              | `topic`           | `String`   | The topic of the message              |
|                                     |                    | `key`             | _any_      | The key of the message                |
|                                     |                    | `value`           | _any_      | The value of the message              |
|                                     |                    | `numPartitions`   | `int`      | The number of partitions on the topic |
| `topicNameExtractor`                | `string`           | `key`             | _any_      | The key of the message                |
|                                     |                    | `value`           | _any_      | The value of the message              |
| `valueJoiner`                       | _any_              | `value1`          | _any_      | The first value to join               |
|                                     |                    | `value2`          | _any_      | The second value to join              |
| `valueTransformer`                  | _any_              | `key`             | _any_      | The key of the message                |
|                                     |                    | `value`           | _any_      | The value of the message              |
