## Summary

Adds a `punctuate` operation: a Python `punctuator` function that runs on a `wallClockTime` or `streamTime` schedule, like a Kafka Streams `Punctuator`.
Records that reach the operation pass through unchanged. The punctuator can read and write state stores, and emits records with `context.forward(key, value)`.
`forward` can also set a timestamp and headers, and `to=` the name of a later operation in the same pipeline, as long as no repartition is in between.
To support `context`, functions now get injected variables (`stores`, `context`) as locals of the function, no longer as Python globals.
The test runner gets `advanceWallClockTime` on produce blocks, to test wall-clock punctuations.

```text
input ─> filter ─> punctuate ─> transformValue ─> peek (name: audit) ─> to
                      │ every interval: punctuator(timestamp)       ▲
                      ├─ context.forward(k, v)           → next operation
                      └─ context.forward(k, v, to="audit") ─────────┘
```

```yaml
- type: punctuate
  interval: 1m
  punctuationType: wallClockTime
  punctuator:
    stores: [totals]
    code: |
      with totals.all() as entries:
        for e in entries:
          context.forward(e["key"], e["value"])
```

Python runtime changes that came with it:

```diff
 ksml/src/main/resources/io/axual/ksml/python/
+└── ksml.py   # shipped with KSML, loaded into every Python context as module "ksml"
+    ├── KSMLProcessorContext   # was a Python string in ProcessorContextProxy
+    ├── KSMLStoreIterator      # new: makes `with store.all() as entries:` work
+    └── log, metrics           # builtins, also `from ksml import log, metrics`
```

- **`ksml` module shipped with KSML:** `ksml.py` was a file users copied into their module path. It is now part of the jar, and is registered before user code runs, so a copy in the module path is ignored at runtime. The Python wrapper classes for the context and for store iterators now live there instead of in Java strings.
- **`--python-module [file]`:** the runner prints the `ksml` module, with the KSML version in its first line, so users can keep an up-to-date copy for code completion in their IDE.
- **`log` and `metrics` everywhere:** they now work in `globalCode` helpers, in users' own modules and outside functions. `log` logs under the name of the KSML function that is running, found through a context variable that each call sets and restores, so it is correct per stream thread.
- **Store iterators in a `with` block:** `with store.all() as entries:` now works and closes the iterator at the end of the block.
- **Name clash check:** a store, parameter or injected variable with the same name as another one of the function (for example a store named `context` in a punctuator) is rejected when the topology is built.

Also in this PR:
- Apicurio mutual TLS (mTLS) to the registry, with the new `apicurio.registry.tls.keystore.*` settings.
- A Python float without an expected type now stays a double. Before, `3.0` became a long and `1e20` failed.
- The `apicurio_jsonschema` and `apicurio_protobuf` notations now create their registry client at startup, like `apicurio_avro`.

## Evidence

Quality gates:

- **Full build:** `mvn clean verify -Pintegration-tests` passes.
  - BUILD_UNIT_TESTS
  - BUILD_INTEGRATION_TESTS
- **Review:** a priority-ranked review of the branch found 27 items. All are fixed, except 3 that were consciously closed: #3 stale NOTICE files, #8 line endings in history (squash-merge), #9 the Apicurio import in `FatalError`. Highlights:
  - **Forwarding past a repartition:** `forward(to=...)` can no longer skip a repartition. Records would otherwise run in the wrong task. The limitation is documented.
  - **Changed tests:** each changed test was checked for why it changed. The produce-block schema rule "not both `messages` and `generator`" is enforced again, with a new `mutuallyExclusive` attribute on `@JsonSchema`.
  - **Result conversion:** a function result that already fits its type skips conversion. A regression test proves the converter would return an equal value.
  - **Formatting:** formatting-only changes and moved methods were reverted, so the diff shows only real changes.
  - **Docs example:** the punctuate docs use a tested example (`punctuate-processor.yaml`) instead of inline YAML. Writing its test found that `with store.all()` failed, which led to the `with` support above.
- **Thread safety of `log`:** before building on context variables, a throwaway test called one Python function from 4 Java threads in a shared GraalPy context, 20,000 times; every call read back its own value. `PythonFunctionTest` keeps a version of that check: two functions share one helper on 4 threads, and each always logs under its own name.
- **New tests:**
  - `PunctuateOperationTest`: no `to=` target after a repartition, after a join that follows a key change, or after `groupBy → aggregate → toStream`; a `globalCode` helper that reads a store in a `with` block; a store named `context` is rejected.
  - `StoreIteratorProxyTest`: a `with` block closes the iterator after reading everything, after an early `return` and after an exception.
  - `PythonFunctionTest`: name clashes between stores, parameters and injected variables; `log` names for functions, `globalCode` helpers and module functions.
  - `KsmlPyModuleTest`: the module imports without a module path, a `ksml.py` in the module path is ignored, and each context gets its own module.
  - `KSMLRunnerTest`: `--python-module` to a file and to stdout. The packaged jar was also run: stdout holds only the module.
  - Example 21: a behaviour test that sums, emits per minute and clears the store.
  - A `globalCode` helper that gets a store as an argument, and the `NameError` for the old `stores` global.
  - Python floats `3.0` and `1e20`.
  - The `FatalError` description for an Apicurio `ProblemDetails`.
  - Error handling without a record and with `logPayload: false`.
  - `advanceWallClockTime` given in milliseconds.
- **Sandbox:** the `VulnerabilitiesTest` pipelines that reached the Java logger through the removed `loggerBridge` global now reach it through the `ksml` module, and `getClass` is still blocked.
- **Schemas:**
  - `ksml-language-spec.json` and `ksml-test-spec.json` are regenerated.
  - The test spec was checked with a JSON Schema validator: valid samples pass, `messages` + `generator` is rejected, and an empty produce block is rejected.
- **OpenSpec:** `openspec validate` passes, and the change is archived with its specs merged into `openspec/specs`.
- **Hygiene:** all files are LF only, and the docs are updated (operation, function and state store reference, editing Python code, test-runner docs, release notes, migration guide).

## Merge Danger

**Door:** two-way for the code; one-way for the YAML syntax and the `ksml` module API once released.

The new `punctuate` and `punctuator` syntax is added, not changed. Once users write it in their definitions, or import `log` and the wrapper classes from `ksml`, it can't easily be changed.

**Blast Radius:** wide

- **Every Python function call:** injected variables are now passed as locals and saved and restored on nested calls, and each call sets the current function for `log`.
- **Every Python context:** loads the shipped `ksml` module at startup.
- **Breaking for users:**
  - `stores` is no longer a Python global, so a `globalCode` helper that reads it raises `NameError`. The migration guide shows the fix.
  - A `globalCode` helper now logs under the name of the function that calls it, not under its own name. Logback configuration that names helper loggers must change.
  - A `ksml.py` in the module path is no longer loaded; KSML always uses its own. Code that relied on a changed copy behaves differently.
  - The undocumented `loggerBridge` global is gone.
  - A store, parameter or injected variable that clashes with another name of its function now fails the topology build.
  - A Python number that doesn't fit its type now fails earlier, with a new error text.
- **Apicurio:** a broken key store or registry setting now fails at startup instead of on first use.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
